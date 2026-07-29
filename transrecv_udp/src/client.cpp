#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <control_msgs/msg/joint_trajectory_controller_state.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <sensor_msgs/msg/joy.hpp>
#include <trajectory_msgs/msg/joint_trajectory.hpp>

#include "transrecv_udp/periodic_thread.hpp"
#include "transrecv_udp/protocol.hpp"
#include "transrecv_udp/udp_socket.hpp"

namespace
{
using transrecv_udp::ArmPositions;
using transrecv_udp::ArmSide;
using transrecv_udp::kNumArmJoints;
using transrecv_udp::kNumHandJoints;

// Joint commands going into the local controllers, mirrored to the server.
constexpr const char * kLeftArmCommandTopic =
  "/left_joint_trajectory_controller/joint_trajectory";
constexpr const char * kRightArmCommandTopic =
  "/right_joint_trajectory_controller/joint_trajectory";

// Controller state republished locally. Nothing feeds these yet.
constexpr const char * kLeftControllerStateTopic =
  "/left_joint_trajectory_controller/controller_state";
constexpr const char * kRightControllerStateTopic =
  "/right_joint_trajectory_controller/controller_state";

// VR face buttons, mirrored to the server. Only the `buttons` array is used;
// `axes` is left alone because the joysticks travel on their own path.
constexpr const char * kVrButtonsTopic = "/vr_buttons";

// FK end-effector pose, mirrored to the server. Published by dora_ros2_bridge
// only when the dataflow wires an fk node's pose_{left,right} output.
constexpr const char * kLeftEePoseTopic = "/left_ee_pose";
constexpr const char * kRightEePoseTopic = "/right_ee_pose";

// Revo2 hand (gripper) commands, mirrored to the server.
constexpr const char * kLeftHandCommandTopic = "/left_revo2_hand_controller/joint_trajectory";
constexpr const char * kRightHandCommandTopic = "/right_revo2_hand_controller/joint_trajectory";

constexpr std::size_t kStateQueueDepth = 10;

// Buttons are events, not a stream, so a small depth is plenty. The publisher
// (dora_to_ros2) offers BEST_EFFORT; a RELIABLE subscription is incompatible
// with that (a stricter subscriber cannot be satisfied by a laxer publisher),
// so the subscription below must match it or DDS never delivers a message.
constexpr std::size_t kButtonQueueDepth = 10;

// Same BEST_EFFORT reasoning as kButtonQueueDepth above: dora_ros2_bridge
// publishes ee_pose with its qos_best_effort profile too.
constexpr std::size_t kPoseQueueDepth = 10;

// Commands arrive at a high rate: throttle per-packet logs.
constexpr int kLogThrottleMs = 1000;

// --- Connection thread ------------------------------------------------------
// UDP has no session, so being "connected" is something that has to be proven
// and then kept proven: the client pings on this period and the server answers.
// The same period serves as the retry interval before the link is up and as the
// keepalive once it is -- there is no difference between the two cases here.
constexpr std::chrono::milliseconds kPingPeriod{500};

// How long without a pong before the link is declared down. Four missed pings:
// long enough that one lost datagram is not treated as an outage, short enough
// that a dead server is noticed in about two seconds.
constexpr std::chrono::milliseconds kPongTimeout{2000};

// The receive thread parks in a blocking recv(). This timeout is only how long
// it can go without noticing a stop request.
constexpr std::chrono::milliseconds kReceiveTimeout{200};

// --- Send queue -------------------------------------------------------------
// Arm commands are handed to a sender thread instead of leaving from inside the
// subscription callback. The callback runs on the executor, so a send() that
// blocks there stalls every other subscription on this node with it; a queue
// plus one thread keeps the socket's latency off the ROS side entirely.
//
// One queue per arm: the two arms arrive as separate topics at rates nobody
// coordinates, and a burst on one must not push the other's commands out of a
// buffer they share. They are recombined at the last moment, into one datagram
// carrying both.

// Bounded on purpose. A joint command is only worth sending while it is fresh,
// so an overflow drops the OLDEST entry rather than refusing the newest: the
// arm should end up at the most recent commanded pose, and a queue that grows
// without limit would just keep widening the lag to it.
//
// This bounds a burst, not an outage: while the link is down the sender still
// pops and discards, so nothing accumulates to be replayed on reconnect.
constexpr std::size_t kSendQueueDepth = 64;

// The sender parks on a condition variable. This bound is only how long it can
// go without noticing a stop request.
constexpr std::chrono::milliseconds kSendIdleTimeout{200};

/// One arm's pending commands.
struct JointCommandQueue
{
  std::deque<ArmPositions> items;
};

// One mutex and one condition variable for both queues: a single sender thread
// cannot wait on two condition variables at once. The critical section is only
// ever a push or a pop -- never a send -- so the two arms do not contend for it
// in any meaningful way.
std::mutex g_queue_mutex;
std::condition_variable g_queue_cv;
JointCommandQueue g_left_queue;
JointCommandQueue g_right_queue;

/// Picks the queue for `side`. The caller must already hold g_queue_mutex.
JointCommandQueue & queue_for(ArmSide side)
{
  return side == ArmSide::kLeft ? g_left_queue : g_right_queue;
}

constexpr std::chrono::milliseconds kHealthCheckPeriod{5000};
constexpr std::chrono::seconds kSendStaleThreshold{2};

constexpr std::uint16_t kMinUserPort = 1024;

/// Parses a port argument. Returns nullopt on a malformed value so the caller
/// can fail loudly instead of silently talking to the wrong port.
std::optional<std::uint16_t> parse_port(const std::string & text)
{
  try {
    const int value = std::stoi(text);
    if (value < kMinUserPort || value > UINT16_MAX) {        // Rule 3: range guard
      return std::nullopt;
    }
    return static_cast<std::uint16_t>(value);
  } catch (const std::exception &) {
    return std::nullopt;
  }
}
}  // namespace

/// Sending end of the UDP link.
///
/// Subscribes to what the local bridge publishes -- arm and hand trajectory
/// commands, VR buttons, FK end-effector poses -- and mirrors each to the
/// server. The one thing it publishes locally is the arm controller state the
/// server pushes back.
///
/// Both arms leave in a single datagram. They arrive as two topics at rates
/// nobody coordinates, so each side is queued separately and recombined by the
/// send thread; a side with nothing new has its last value repeated, so every
/// datagram states the whole robot's pose rather than half of it.
///
/// Four threads beside the executor, each with one job:
///   send       -- drains the two arm command queues into one both-arms
///                 datagram, so a slow socket never blocks a subscription
///   connection -- pings the server every 500 ms and declares the link down
///                 when the answers stop
///   receive    -- parks in recv() waiting for those answers
///   health     -- periodic self-report, separate so a stalled send path is
///                 still reported by a thread that did not stall
///
/// Only arm commands are queued. Buttons are already rate-limited to one
/// datagram per press, and poses/hand commands are sent straight from their
/// callbacks -- adding a queue there would buy nothing.
///
/// The ping/pong is the whole point: connect() on a datagram socket sends
/// nothing and cannot fail, so without an answer from the far end the client
/// has no way to tell a running server from a missing one.
class TrajectoryClient : public rclcpp::Node
{
public:
  /// `socket` must outlive this node; ownership stays with the caller.
  TrajectoryClient(
    transrecv_udp::UdpSocket & socket, const sockaddr_in & server,
    const std::string & server_text)
  : rclcpp::Node("transrecv_udp_client"),
    socket_(socket),
    server_(server),
    server_text_(server_text)
  {
    if (!socket_.valid()) {                                  // Rule 3: precondition
      RCLCPP_ERROR(get_logger(), "constructed with an invalid socket");
      throw std::invalid_argument("TrajectoryClient needs an open socket");
    }

    // Fixes the peer so send()/recv() can be used and stray datagrams from
    // anyone else are dropped by the kernel before we ever see them.
    if (!socket_.connect_to(server_, get_logger())) {        // Rule 3: setup guard
      RCLCPP_ERROR(get_logger(), "cannot point the socket at %s", server_text_.c_str());
      throw std::runtime_error("connect() failed");
    }

    const rclcpp::QoS qos = rclcpp::QoS(kStateQueueDepth).reliable();

    left_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kLeftArmCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_joint_trajectory(ArmSide::kLeft, msg);
      });
    right_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kRightArmCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_joint_trajectory(ArmSide::kRight, msg);
      });

    buttons_sub_ = create_subscription<sensor_msgs::msg::Joy>(
      kVrButtonsTopic, rclcpp::QoS(kButtonQueueDepth).best_effort(),
      [this](sensor_msgs::msg::Joy::ConstSharedPtr msg) { on_vr_buttons(msg); });

    const rclcpp::QoS pose_qos = rclcpp::QoS(kPoseQueueDepth).best_effort();
    left_ee_pose_sub_ = create_subscription<geometry_msgs::msg::PoseStamped>(
      kLeftEePoseTopic, pose_qos,
      [this](geometry_msgs::msg::PoseStamped::ConstSharedPtr msg) {
        on_ee_pose(ArmSide::kLeft, msg);
      });
    right_ee_pose_sub_ = create_subscription<geometry_msgs::msg::PoseStamped>(
      kRightEePoseTopic, pose_qos,
      [this](geometry_msgs::msg::PoseStamped::ConstSharedPtr msg) {
        on_ee_pose(ArmSide::kRight, msg);
      });

    left_hand_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kLeftHandCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_hand_trajectory(ArmSide::kLeft, msg);
      });
    right_hand_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kRightHandCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_hand_trajectory(ArmSide::kRight, msg);
      });

    left_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kLeftControllerStateTopic, qos);
    right_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kRightControllerStateTopic, qos);

    RCLCPP_INFO(
      get_logger(), "forwarding '%s' and '%s' to %s",
      kLeftArmCommandTopic, kRightArmCommandTopic, server_text_.c_str());
    RCLCPP_INFO(
      get_logger(), "registered publishers for '%s' and '%s' (depth %zu, reliable)",
      kLeftControllerStateTopic, kRightControllerStateTopic, kStateQueueDepth);
    RCLCPP_INFO(
      get_logger(), "mirroring '%s' to %s, on change only",
      kVrButtonsTopic, server_text_.c_str());
    RCLCPP_INFO(
      get_logger(), "forwarding '%s' and '%s' to %s (best_effort)",
      kLeftEePoseTopic, kRightEePoseTopic, server_text_.c_str());
    RCLCPP_INFO(
      get_logger(), "forwarding '%s' and '%s' to %s",
      kLeftHandCommandTopic, kRightHandCommandTopic, server_text_.c_str());
    RCLCPP_INFO(
      get_logger(), "pinging %s every %ldms, link is down after %ldms without an answer",
      server_text_.c_str(), static_cast<long>(kPingPeriod.count()),
      static_cast<long>(kPongTimeout.count()));
  }

  ~TrajectoryClient() override
  {
    stop();
  }

  /// Starts all three threads. Separate from the constructor so `this` is fully
  /// built before another thread starts reading its members.
  void start()
  {
    if (receive_thread_.joinable()) {                        // Rule 3: double-start guard
      RCLCPP_WARN(get_logger(), "already started, ignoring start request");
      return;
    }

    receive_running_.store(true, std::memory_order_relaxed);
    receive_thread_ = std::thread([this]() { run_receive_loop(); });
    RCLCPP_INFO(get_logger(), "receive thread started");

    send_running_.store(true, std::memory_order_relaxed);
    send_thread_ = std::thread([this]() { run_send_loop(); });
    RCLCPP_INFO(
      get_logger(), "send thread started (one queue per arm, depth %zu)", kSendQueueDepth);

    connection_thread_.start();
    health_thread_.start();
  }

  /// Stops every thread and joins them. Safe to call twice.
  void stop()
  {
    health_thread_.stop();
    connection_thread_.stop();
    stop_send_thread();

    if (!receive_thread_.joinable()) {
      return;
    }
    receive_running_.store(false, std::memory_order_relaxed);
    receive_thread_.join();          // wakes within kReceiveTimeout at the latest
    RCLCPP_INFO(get_logger(), "receive thread stopped");
  }

private:
  // --- connection thread -----------------------------------------------------

  /// One tick: ping, then decide whether the answers are still arriving.
  ///
  /// Pinging unconditionally -- up or down -- means the same code path recovers
  /// a server that was never up, one that restarted, and one that just went
  /// away. Only the receive thread ever promotes the link to connected; this
  /// one only demotes it.
  void maintain_connection()
  {
    if (!socket_.valid()) {                                  // Rule 3: socket sanity
      RCLCPP_ERROR(get_logger(), "connection thread: socket is not open, cannot ping");
      return;
    }

    send_ping();

    const auto last_pong = last_pong_time_.load(std::memory_order_relaxed);
    const bool ever_answered = pongs_received_.load(std::memory_order_relaxed) > 0;
    const auto silence = std::chrono::steady_clock::now() - last_pong;

    if (!ever_answered) {                                    // Rule 2: still waiting
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "no answer from %s yet, retrying every %ldms",
        server_text_.c_str(), static_cast<long>(kPingPeriod.count()));
      return;
    }

    if (silence > kPongTimeout) {
      mark_disconnected("no pong within the timeout");
    }
  }

  void send_ping()
  {
    const transrecv_udp::ControlPacket ping =
      transrecv_udp::make_control_packet(transrecv_udp::ControlType::kPing);

    const ssize_t sent = send(socket_.get(), &ping, sizeof(ping), 0);
    if (sent < 0) {
      // ECONNREFUSED is the ICMP port-unreachable from a host with nothing
      // bound. It is a useful hint where it survives, but the pong timeout is
      // what the decision actually rests on.
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: log the failure path
        get_logger(), *get_clock(), kLogThrottleMs,
        "ping to %s failed: %s", server_text_.c_str(), std::strerror(errno));
      mark_disconnected(std::strerror(errno));
      return;
    }

    if (static_cast<std::size_t>(sent) != sizeof(ping)) {     // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "short ping: %zd of %zu bytes", sent, sizeof(ping));
      return;
    }

    pings_sent_.fetch_add(1, std::memory_order_relaxed);
  }

  /// Flips the link down, logging only on the transition so a long outage does
  /// not repeat itself every tick.
  void mark_disconnected(const char * why)
  {
    if (!connected_.exchange(false, std::memory_order_relaxed)) {
      return;                                                // already down, stay quiet
    }
    RCLCPP_WARN(                                             // Rule 2: log the transition
      get_logger(), "lost connection to %s (%s), retrying every %ldms",
      server_text_.c_str(), why, static_cast<long>(kPingPeriod.count()));
  }

  // --- receive thread --------------------------------------------------------

  /// Parks in recv() waiting for the server's pongs. Its own thread so it can
  /// block instead of poll, and so nothing else has to wait on it.
  void run_receive_loop()
  {
    unsigned char buffer[transrecv_udp::kReceiveBufferBytes];

    while (receive_running_.load(std::memory_order_relaxed)) {
      if (!socket_.valid()) {                                // Rule 3: socket sanity
        RCLCPP_ERROR(get_logger(), "receive thread: socket is not open, exiting");
        return;
      }

      const ssize_t received = recv(socket_.get(), buffer, sizeof(buffer), 0);

      if (received < 0) {
        // EAGAIN/EWOULDBLOCK is the SO_RCVTIMEO expiring, which is how this
        // loop gets to re-check the stop flag. Not an error.
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          continue;
        }
        if (errno == ECONNREFUSED) {
          // A queued ICMP port-unreachable, surfaced here instead of on send.
          mark_disconnected("connection refused");
          continue;
        }
        RCLCPP_WARN_THROTTLE(
          get_logger(), *get_clock(), kLogThrottleMs, "recv() failed: %s",
          std::strerror(errno));
        continue;
      }

      handle_datagram(buffer, static_cast<std::size_t>(received));
    }
  }

  void handle_datagram(const unsigned char * data, std::size_t length)
  {
    // Classify by length first: the two packet types have different fixed
    // sizes, so nothing has to be decoded to tell them apart.
    if (length == sizeof(transrecv_udp::JointPacket)) {
      handle_joint_datagram(data, length);
      return;
    }

    if (length != sizeof(transrecv_udp::ControlPacket)) {    // Rule 3: size guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "ignoring a %zu byte datagram, no packet type has that size", length);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    transrecv_udp::ControlType type = transrecv_udp::ControlType::kPing;
    std::string reason;
    if (!transrecv_udp::decode_control_packet(data, length, type, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed control datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (type != transrecv_udp::ControlType::kPong) {         // Rule 2: unexpected but handled
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "ignoring a '%s' packet, the client only expects pongs",
        transrecv_udp::to_string(type));
      return;
    }

    last_pong_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
    pongs_received_.fetch_add(1, std::memory_order_relaxed);

    if (!connected_.exchange(true, std::memory_order_relaxed)) {
      RCLCPP_INFO(                                           // Rule 2: log the transition
        get_logger(), "connected to %s (answered after %lu pings)",
        server_text_.c_str(),
        static_cast<unsigned long>(pings_sent_.load(std::memory_order_relaxed)));
    }
  }

  // --- send path -------------------------------------------------------------

  void on_joint_trajectory(
    ArmSide side, trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg)
  {
    if (msg == nullptr) {                                    // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] null trajectory, dropping", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (msg->points.empty()) {                               // Rule 3: empty guard
      RCLCPP_ERROR(
        get_logger(), "[%s] trajectory has no points, dropping", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // The bridge publishes one point per message; when there are several, the
    // last one is the target the arm ends up at, which is what the server wants.
    const std::vector<double> & commanded = msg->points.back().positions;
    if (commanded.size() < kNumArmJoints) {                  // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] trajectory point has %zu positions, need %zu, dropping",
        transrecv_udp::to_string(side), commanded.size(), kNumArmJoints);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // The message may carry more than the arm joints; take the first seven,
    // matching what the bridge writes on this same topic.
    const std::vector<double> positions(commanded.begin(), commanded.begin() + kNumArmJoints);

    // The callback's job ends here. Whether the datagram actually goes out is
    // the sender thread's problem, and it logs that separately.
    enqueue_joint_command(side, positions);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[%s] queued: [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), positions[0], positions[1], positions[2],
      positions[3], positions[4], positions[5], positions[6]);
  }

  /// Puts one arm's positions on its queue. Runs on the executor thread, so it
  /// must never touch the socket.
  void enqueue_joint_command(ArmSide side, const std::vector<double> & positions)
  {
    if (positions.size() != kNumArmJoints) {                 // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] %zu positions, expected %zu, not queuing",
        transrecv_udp::to_string(side), positions.size(), kNumArmJoints);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    ArmPositions entry{};
    std::copy(positions.begin(), positions.end(), entry.begin());

    bool overflowed = false;
    {
      const std::lock_guard<std::mutex> lock(g_queue_mutex);
      JointCommandQueue & queue = queue_for(side);

      if (queue.items.size() >= kSendQueueDepth) {           // Rule 3: bound the queue
        queue.items.pop_front();                             // oldest is the least useful
        overflowed = true;
      }
      queue.items.push_back(entry);
    }
    g_queue_cv.notify_one();

    if (overflowed) {                                        // Rule 2: say what was lost
      queue_dropped_.fetch_add(1, std::memory_order_relaxed);
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] send queue full at %zu, dropped the oldest command "
        "(commands arriving faster than the socket drains them)",
        transrecv_udp::to_string(side), kSendQueueDepth);
    }
  }

  /// Drains both queues and puts one both-arms datagram on the wire per pass.
  /// Its own thread so a send that blocks delays nothing but the next send.
  ///
  /// An arm with nothing queued this pass is not skipped -- its last known
  /// positions are repeated, so every datagram states where the whole robot
  /// should be rather than leaving the far end to remember half of it.
  void run_send_loop()
  {
    while (send_running_.load(std::memory_order_relaxed)) {
      bool left_fresh = false;
      bool right_fresh = false;

      {
        std::unique_lock<std::mutex> lock(g_queue_mutex);
        g_queue_cv.wait_for(
          lock, kSendIdleTimeout,
          [this]() {
            return !g_left_queue.items.empty() || !g_right_queue.items.empty() ||
                   !send_running_.load(std::memory_order_relaxed);
          });

        if (!send_running_.load(std::memory_order_relaxed)) {   // Rule 2: log the exit
          RCLCPP_INFO(get_logger(), "send thread: stop requested, exiting");
          return;
        }

        // One from each side per pass, so a fast arm cannot starve the other.
        // Whatever is popped replaces the cached value; whatever is not stays
        // as it was and gets repeated below.
        if (!g_left_queue.items.empty()) {
          last_left_ = g_left_queue.items.front();
          g_left_queue.items.pop_front();
          left_fresh = true;
          have_left_ = true;
        }
        if (!g_right_queue.items.empty()) {
          last_right_ = g_right_queue.items.front();
          g_right_queue.items.pop_front();
          right_fresh = true;
          have_right_ = true;
        }
      }

      // Both queues were empty: the wait timed out on its way to re-checking
      // the stop flag, which is not a reason to send anything.
      if (!left_fresh && !right_fresh) {
        continue;
      }

      // Rule 3: an arm that has never published has no old value to repeat, and
      // a default-constructed one is not "unknown" on the wire -- it is a valid
      // command to drive that arm to zero. Nothing goes out until both sides
      // have been heard from at least once.
      if (!have_left_ || !have_right_) {                     // Rule 2: say what is missing
        RCLCPP_WARN_THROTTLE(
          get_logger(), *get_clock(), kLogThrottleMs,
          "waiting for the %s arm's first command before sending "
          "(a both-arms packet cannot invent the side it has never seen)",
          have_left_ ? "right" : "left");
        continue;
      }

      // Outside the lock: the callbacks filling these queues must never wait on
      // a syscall.
      send_dual_packet(left_fresh, right_fresh);
    }
  }

  /// Signals the sender, joins it, and throws away whatever it never got to.
  void stop_send_thread()
  {
    if (!send_thread_.joinable()) {
      return;
    }

    {
      // Set under the lock: a sender that has already evaluated the wait
      // predicate would otherwise sleep through the notify below.
      const std::lock_guard<std::mutex> lock(g_queue_mutex);
      send_running_.store(false, std::memory_order_relaxed);
    }
    g_queue_cv.notify_all();
    send_thread_.join();

    const std::lock_guard<std::mutex> lock(g_queue_mutex);
    const std::size_t left = g_left_queue.items.size();
    const std::size_t right = g_right_queue.items.size();
    if (left > 0 || right > 0) {                             // Rule 2: say what was discarded
      RCLCPP_WARN(
        get_logger(), "send thread stopped with %zu left and %zu right commands unsent",
        left, right);
    }

    // Nothing here will ever be acted on, and leaving it would let a restart
    // replay poses the arm has long since moved past.
    g_left_queue.items.clear();
    g_right_queue.items.clear();
  }

  /// Single send path for arm commands, so every failure is counted and logged
  /// one way. Runs on the sender thread, and reads the cached positions that
  /// only that thread ever writes.
  ///
  /// `left_fresh`/`right_fresh` say which sides came off a queue this pass; the
  /// other side is being repeated, which the log spells out so a stalled topic
  /// is not mistaken for a healthy one.
  bool send_dual_packet(bool left_fresh, bool right_fresh)
  {
    if (!socket_.valid()) {                                  // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs, "socket not open, cannot send");
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "not connected to %s, dropping command", server_text_.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const transrecv_udp::DualJointPacket packet =
      transrecv_udp::make_dual_packet(last_left_, last_right_);

    // sendto() with the address main() resolved, so the destination is stated
    // on the datagram itself instead of being inherited from connect(). Linux
    // permits that on a connected socket; connect() stays because it is what
    // lets the receive thread use recv() and what routes ECONNREFUSED back.
    const ssize_t sent = sendto(
      socket_.get(), &packet, sizeof(packet), 0,
      reinterpret_cast<const sockaddr *>(&server_), sizeof(server_));

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));             // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "sendto() failed: %s", std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {   // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "short send: %zd of %zu bytes", sent, sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
    last_send_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
    if (!left_fresh || !right_fresh) {
      repeated_sides_.fetch_add(1, std::memory_order_relaxed);
    }

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "sent both arms | left%s [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]"
      " | right%s [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      left_fresh ? "" : " (repeated)",
      last_left_[0], last_left_[1], last_left_[2], last_left_[3], last_left_[4],
      last_left_[5], last_left_[6],
      right_fresh ? "" : " (repeated)",
      last_right_[0], last_right_[1], last_right_[2], last_right_[3], last_right_[4],
      last_right_[5], last_right_[6]);
    return true;
  }

  // --- end-effector pose -----------------------------------------------------

  void on_ee_pose(ArmSide side, geometry_msgs::msg::PoseStamped::ConstSharedPtr msg)
  {
    if (msg == nullptr) {                                      // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] null ee_pose, dropping", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // Order matches make_ee_pose_msg() in dora_ros2_bridge/main.py.
    const std::vector<double> values = {
      msg->pose.position.x, msg->pose.position.y, msg->pose.position.z,
      msg->pose.orientation.w, msg->pose.orientation.x,
      msg->pose.orientation.y, msg->pose.orientation.z};

    if (!send_pose_packet(side, values)) {
      return;
    }

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[%s] ee_pose sent: pos=[%.3f %.3f %.3f] quat=[%.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), values[0], values[1], values[2],
      values[3], values[4], values[5], values[6]);
  }

  bool send_pose_packet(ArmSide side, const std::vector<double> & values)
  {
    if (!socket_.valid()) {                                    // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] socket not open, cannot send ee_pose", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (!connected_.load(std::memory_order_relaxed)) {         // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] not connected to %s, dropping ee_pose",
        transrecv_udp::to_string(side), server_text_.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const transrecv_udp::PosePacket packet = transrecv_udp::make_pose_packet(side, values);
    const ssize_t sent = send(socket_.get(), &packet, sizeof(packet), 0);

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));               // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                  // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "[%s] send(ee_pose) failed: %s", transrecv_udp::to_string(side), std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {     // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] short ee_pose send: %zd of %zu bytes", transrecv_udp::to_string(side), sent,
        sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    poses_sent_.fetch_add(1, std::memory_order_relaxed);
    last_send_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
    return true;
  }

  // --- Revo2 hand --------------------------------------------------------------

  void on_hand_trajectory(
    ArmSide side, trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg)
  {
    if (msg == nullptr) {                                      // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] null hand trajectory, dropping", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (msg->points.empty()) {                                 // Rule 3: empty guard
      RCLCPP_ERROR(
        get_logger(), "[%s] hand trajectory has no points, dropping",
        transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    const std::vector<double> & commanded = msg->points.back().positions;
    if (commanded.size() < kNumHandJoints) {                   // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] hand trajectory point has %zu positions, need %zu, dropping",
        transrecv_udp::to_string(side), commanded.size(), kNumHandJoints);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    const std::vector<double> positions(commanded.begin(), commanded.begin() + kNumHandJoints);

    if (!send_hand_packet(side, positions)) {
      return;
    }

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[%s] hand sent: [%.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), positions[0], positions[1], positions[2],
      positions[3], positions[4], positions[5]);
  }

  bool send_hand_packet(ArmSide side, const std::vector<double> & positions)
  {
    if (!socket_.valid()) {                                    // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] socket not open, cannot send hand command", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (!connected_.load(std::memory_order_relaxed)) {         // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] not connected to %s, dropping hand command",
        transrecv_udp::to_string(side), server_text_.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const transrecv_udp::HandJointPacket packet = transrecv_udp::make_hand_packet(side, positions);
    const ssize_t sent = send(socket_.get(), &packet, sizeof(packet), 0);

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));               // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                  // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "[%s] send(hand) failed: %s", transrecv_udp::to_string(side), std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {     // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] short hand send: %zd of %zu bytes", transrecv_udp::to_string(side), sent,
        sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    hands_sent_.fetch_add(1, std::memory_order_relaxed);
    last_send_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
    return true;
  }

  // --- VR buttons ------------------------------------------------------------

  /// Mirrors button state to the server, but only when it actually changed.
  ///
  /// /vr_buttons republishes at its source's rate whether or not anything was
  /// pressed, and a button is a step function: resending an unchanged value
  /// says nothing new. Comparing against the last sent value turns a steady
  /// stream into one datagram per press and one per release.
  void on_vr_buttons(sensor_msgs::msg::Joy::ConstSharedPtr msg)
  {
    if (msg == nullptr) {                                    // Rule 3: null guard
      RCLCPP_ERROR(get_logger(), "null Joy message on '%s', dropping", kVrButtonsTopic);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (msg->buttons.empty()) {                              // Rule 3: size guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "Joy message on '%s' has no buttons, dropping", kVrButtonsTopic);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (msg->buttons.size() > transrecv_udp::kMaxButtons) {  // Rule 2: truncating, say so
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "Joy message has %zu buttons, only the first %zu fit in a packet",
        msg->buttons.size(), transrecv_udp::kMaxButtons);
    }

    // Joy carries int32; a button is a flag, so normalise to 0/1 here and the
    // comparison below is then a plain byte compare.
    const std::size_t count =
      std::min(msg->buttons.size(), transrecv_udp::kMaxButtons);
    std::vector<std::uint8_t> buttons(count);
    for (std::size_t i = 0; i < count; ++i) {
      buttons[i] = msg->buttons[i] != 0 ? 1 : 0;
    }

    // Throttled, and logged regardless of whether the state changed -- unlike
    // the "sent" line below, this is here purely so a debugging session can
    // see that /vr_buttons is actually arriving and what it currently reads,
    // even when nothing is being forwarded to the server.
    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "buttons from '%s' (raw, %zu of %zu used): %s", kVrButtonsTopic, count,
      msg->buttons.size(), describe_buttons(buttons).c_str());

    if (buttons == last_buttons_sent_) {                     // unchanged: stay silent
      RCLCPP_DEBUG(get_logger(), "button state unchanged, not sending");
      return;
    }

    if (!send_button_packet(buttons)) {
      // Not recording it as sent: the next message must retry, or a press lost
      // to a down link would never be resent (nothing else repeats it).
      return;
    }

    last_buttons_sent_ = buttons;
    buttons_sent_.fetch_add(1, std::memory_order_relaxed);

    // Not throttled: one line per press or release is exactly the rate the
    // change detection above already limits this to.
    RCLCPP_INFO(
      get_logger(), "buttons changed, sent: %s", describe_buttons(buttons).c_str());
  }

  bool send_button_packet(const std::vector<std::uint8_t> & buttons)
  {
    if (!socket_.valid()) {                                  // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs, "socket not open, cannot send buttons");
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "not connected to %s, dropping button change", server_text_.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const transrecv_udp::ButtonPacket packet = transrecv_udp::make_button_packet(buttons);
    const ssize_t sent = send(socket_.get(), &packet, sizeof(packet), 0);

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));             // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "send(buttons) failed: %s", std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {   // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "short button send: %zd of %zu bytes", sent, sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
    last_send_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
    return true;
  }

  static std::string describe_buttons(const std::vector<std::uint8_t> & buttons)
  {
    std::string text = "[";
    for (std::size_t i = 0; i < buttons.size(); ++i) {
      text += buttons[i] != 0 ? '1' : '0';
      if (i + 1 < buttons.size()) {
        text += ' ';
      }
    }
    return text + "]";
  }

  // --- controller state ------------------------------------------------------

  /// Decodes controller state pushed by the server and republishes it locally.
  /// Runs on the receive thread.
  void handle_joint_datagram(const unsigned char * data, std::size_t length)
  {
    transrecv_udp::DecodedPacket decoded;
    std::string reason;
    if (!transrecv_udp::decode_packet(data, length, decoded, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed joint datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    states_received_.fetch_add(1, std::memory_order_relaxed);
    publish_controller_state(decoded.arm, decoded.positions);
  }

  /// Publishes one arm's controller state locally.
  void publish_controller_state(ArmSide side, const std::vector<double> & positions)
  {
    if (positions.size() != kNumArmJoints) {                 // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] %zu positions, expected %zu, not publishing state",
        transrecv_udp::to_string(side), positions.size(), kNumArmJoints);
      return;
    }

    const auto & publisher =
      side == ArmSide::kLeft ? left_state_pub_ : right_state_pub_;
    if (publisher == nullptr) {                              // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] state publisher is null, not publishing",
        transrecv_udp::to_string(side));
      return;
    }

    control_msgs::msg::JointTrajectoryControllerState message;
    message.header.stamp = now();
    message.joint_names = transrecv_udp::arm_joint_names(side);
    message.feedback.positions = positions;
    publisher->publish(message);

    states_published_.fetch_add(1, std::memory_order_relaxed);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[%s] state published: [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), positions[0], positions[1], positions[2],
      positions[3], positions[4], positions[5], positions[6]);
  }

  // --- health thread ---------------------------------------------------------

  /// One watchdog pass, on its own thread so a stalled callback is still
  /// reported by something that did not stall.
  void check_health()
  {
    if (!socket_.valid()) {                                  // Rule 3: socket sanity
      RCLCPP_ERROR(get_logger(), "health: socket is not open, client is dead");
      return;
    }

    const std::uint64_t sent = packets_sent_.load(std::memory_order_relaxed);
    const std::uint64_t dropped = packets_dropped_.load(std::memory_order_relaxed);
    const std::uint64_t pings = pings_sent_.load(std::memory_order_relaxed);
    const std::uint64_t pongs = pongs_received_.load(std::memory_order_relaxed);
    const std::uint64_t buttons = buttons_sent_.load(std::memory_order_relaxed);
    const std::uint64_t poses = poses_sent_.load(std::memory_order_relaxed);
    const std::uint64_t hands = hands_sent_.load(std::memory_order_relaxed);

    // Appended to every branch below, so the queue state is visible whichever
    // one is taken instead of being repeated in four format strings. A backlog
    // that never drains is the symptom that says the sender thread, not the
    // network, is the problem.
    const std::string queue_text = describe_queues();

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: state-dependent branch
      RCLCPP_WARN(
        get_logger(),
        "health: not connected to %s | pings=%lu pongs=%lu dropped=%lu buttons_sent=%lu "
        "poses_sent=%lu hands_sent=%lu %s",
        server_text_.c_str(), static_cast<unsigned long>(pings),
        static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped),
        static_cast<unsigned long>(buttons), static_cast<unsigned long>(poses),
        static_cast<unsigned long>(hands), queue_text.c_str());
      return;
    }

    if (sent == 0) {
      // last_send_time_ is still its zero value here, so its age is meaningless
      // -- report "never" rather than the seconds since the steady_clock epoch.
      RCLCPP_WARN(
        get_logger(),
        "health: connected to %s but no joint data sent yet "
        "(are the controllers publishing state?) | pongs=%lu dropped=%lu buttons_sent=%lu "
        "poses_sent=%lu hands_sent=%lu %s",
        server_text_.c_str(), static_cast<unsigned long>(pongs),
        static_cast<unsigned long>(dropped), static_cast<unsigned long>(buttons),
        static_cast<unsigned long>(poses), static_cast<unsigned long>(hands),
        queue_text.c_str());
      return;
    }

    const auto since_send =
      std::chrono::steady_clock::now() - last_send_time_.load(std::memory_order_relaxed);

    if (since_send > kSendStaleThreshold) {
      RCLCPP_WARN(
        get_logger(),
        "health: nothing sent for %lds (controller state stalled?) "
        "| sent=%lu dropped=%lu buttons_sent=%lu poses_sent=%lu hands_sent=%lu %s",
        static_cast<long>(
          std::chrono::duration_cast<std::chrono::seconds>(since_send).count()),
        static_cast<unsigned long>(sent), static_cast<unsigned long>(dropped),
        static_cast<unsigned long>(buttons), static_cast<unsigned long>(poses),
        static_cast<unsigned long>(hands), queue_text.c_str());
      return;
    }

    RCLCPP_INFO(
      get_logger(),
      "health: OK | server=%s sent=%lu pongs=%lu dropped=%lu buttons_sent=%lu "
      "poses_sent=%lu hands_sent=%lu %s",
      server_text_.c_str(), static_cast<unsigned long>(sent),
      static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped),
      static_cast<unsigned long>(buttons), static_cast<unsigned long>(poses),
      static_cast<unsigned long>(hands), queue_text.c_str());
  }

  /// Snapshot of both queues, for the health line.
  std::string describe_queues() const
  {
    std::size_t left = 0;
    std::size_t right = 0;
    {
      const std::lock_guard<std::mutex> lock(g_queue_mutex);
      left = g_left_queue.items.size();
      right = g_right_queue.items.size();
    }

    return "queued=" + std::to_string(left) + "/" + std::to_string(right) +
           " (max " + std::to_string(kSendQueueDepth) + ") queue_dropped=" +
           std::to_string(queue_dropped_.load(std::memory_order_relaxed)) +
           " repeated=" + std::to_string(repeated_sides_.load(std::memory_order_relaxed));
  }

  transrecv_udp::UdpSocket & socket_;
  const sockaddr_in server_;
  const std::string server_text_;

  std::atomic<bool> connected_{false};
  std::atomic<std::uint64_t> pings_sent_{0};
  std::atomic<std::uint64_t> pongs_received_{0};
  std::atomic<std::uint64_t> packets_sent_{0};
  std::atomic<std::uint64_t> packets_dropped_{0};
  std::atomic<std::chrono::steady_clock::time_point> last_pong_time_{
    std::chrono::steady_clock::time_point{}};
  std::atomic<std::chrono::steady_clock::time_point> last_send_time_{
    std::chrono::steady_clock::time_point{}};

  std::atomic<std::uint64_t> states_received_{0};
  std::atomic<std::uint64_t> states_published_{0};
  std::atomic<std::uint64_t> buttons_sent_{0};
  std::atomic<std::uint64_t> poses_sent_{0};
  std::atomic<std::uint64_t> hands_sent_{0};

  // Separate from packets_dropped_: a queue overflow means the client could not
  // keep up or the link was down, which is a different problem from a malformed
  // message being rejected.
  std::atomic<std::uint64_t> queue_dropped_{0};

  // Datagrams where at least one side was a repeat. A number that tracks
  // packets_sent_ means one of the two topics has stopped publishing.
  std::atomic<std::uint64_t> repeated_sides_{0};

  // The last positions seen for each arm, repeated into any datagram where that
  // side had nothing queued. Written and read only by the send thread, so no
  // atomic and no lock -- the queue mutex covers the pop, not these.
  ArmPositions last_left_{};
  ArmPositions last_right_{};
  bool have_left_ = false;
  bool have_right_ = false;

  // Touched only by the executor thread (the Joy callback), so no atomic and no
  // lock: the send path for buttons runs entirely inside that callback.
  std::vector<std::uint8_t> last_buttons_sent_;

  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_sub_;
  rclcpp::Subscription<sensor_msgs::msg::Joy>::SharedPtr buttons_sub_;
  rclcpp::Subscription<geometry_msgs::msg::PoseStamped>::SharedPtr left_ee_pose_sub_;
  rclcpp::Subscription<geometry_msgs::msg::PoseStamped>::SharedPtr right_ee_pose_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_hand_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_hand_sub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    left_state_pub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    right_state_pub_;

  std::thread receive_thread_;
  std::atomic<bool> receive_running_{false};

  std::thread send_thread_;
  std::atomic<bool> send_running_{false};

  // Declared last on purpose: members are destroyed in reverse order, so these
  // destructors join their threads before the state those threads read.
  transrecv_udp::PeriodicThread connection_thread_{
    kPingPeriod, get_logger(), [this]() { maintain_connection(); }, "connection thread"};
  transrecv_udp::PeriodicThread health_thread_{
    kHealthCheckPeriod, get_logger(), [this]() { check_health(); }, "health thread"};
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  const rclcpp::Logger logger = rclcpp::get_logger("transrecv_udp_client");

  // rclcpp::init strips ROS args; whatever is left is ours.
  const std::vector<std::string> args = rclcpp::remove_ros_arguments(argc, argv);

  // program, server ip, server port, bind port
  constexpr std::size_t kExpectedArgCount = 4;
  if (args.size() != kExpectedArgCount) {                    // Rule 3 + Rule 2
    RCLCPP_ERROR(
      logger,
      "usage: %s <server_ip> <server_port> <bind_port>   (got %zu arguments)",
      args.empty() ? "client" : args[0].c_str(), args.size() - 1);
    rclcpp::shutdown();
    return 1;
  }

  const std::string server_ip = args[1];
  const std::optional<std::uint16_t> server_port = parse_port(args[2]);
  if (!server_port.has_value()) {                            // Rule 3: range guard
    RCLCPP_ERROR(
      logger, "invalid server port '%s' (expected %u..%u)",
      args[2].c_str(), kMinUserPort, UINT16_MAX);
    rclcpp::shutdown();
    return 1;
  }

  const std::optional<std::uint16_t> bind_port = parse_port(args[3]);
  if (!bind_port.has_value()) {                              // Rule 3: range guard
    RCLCPP_ERROR(
      logger, "invalid bind port '%s' (expected %u..%u)",
      args[3].c_str(), kMinUserPort, UINT16_MAX);
    rclcpp::shutdown();
    return 1;
  }

  // Resolve before binding: a typo in the address should fail before anything
  // holds a port.
  sockaddr_in server{};
  if (!transrecv_udp::resolve_ipv4(server_ip, server_port.value(), server, logger)) {
    rclcpp::shutdown();
    return 1;
  }

  // Bind a known local port on INADDR_ANY: the server was told this port up
  // front, so it has to be the one we actually listen on, but which interface
  // the traffic arrives on is not ours to decide. The server does not have to
  // be up yet -- the connection thread keeps pinging.
  transrecv_udp::UdpSocket client_socket;
  if (!client_socket.bind_any(bind_port.value(), logger)) {
    RCLCPP_ERROR(logger, "cannot bind port %u, exiting", bind_port.value());
    rclcpp::shutdown();
    return 1;
  }

  // Without this the receive thread would park in recv() forever and never see
  // the stop flag, so shutdown would hang.
  if (!client_socket.set_receive_timeout(kReceiveTimeout, logger)) {
    RCLCPP_ERROR(logger, "cannot set the receive timeout, exiting");
    rclcpp::shutdown();
    return 1;
  }

  const std::string server_text = server_ip + ":" + std::to_string(server_port.value());

  int exit_code = 0;
  try {
    auto client = std::make_shared<TrajectoryClient>(client_socket, server, server_text);
    client->start();
    rclcpp::spin(client);
    client->stop();                  // join every thread before the socket closes
  } catch (const std::exception & error) {
    RCLCPP_ERROR(logger, "fatal: %s", error.what());
    exit_code = 1;
  }

  rclcpp::shutdown();
  return exit_code;
}
