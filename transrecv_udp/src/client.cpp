#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <control_msgs/msg/joint_trajectory_controller_state.hpp>
#include <sensor_msgs/msg/joy.hpp>
#include <trajectory_msgs/msg/joint_trajectory.hpp>

#include "transrecv_udp/periodic_thread.hpp"
#include "transrecv_udp/protocol.hpp"
#include "transrecv_udp/udp_socket.hpp"

namespace
{
using transrecv_udp::ArmSide;
using transrecv_udp::kNumArmJoints;

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

constexpr std::size_t kStateQueueDepth = 10;

// Buttons are events, not a stream: a small depth is plenty, and the reliable
// QoS is what stops a press from being dropped locally before we ever see it.
constexpr std::size_t kButtonQueueDepth = 10;

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

constexpr std::chrono::milliseconds kHealthCheckPeriod{5000};
constexpr std::chrono::seconds kSendStaleThreshold{2};

// The send thread times itself and logs on this period. Same cadence as the
// health check on purpose: the two lines land together, so one glance shows both
// what the thread managed and what it carried.
constexpr double kRateReportSeconds = 5.0;

constexpr std::uint16_t kMinUserPort = 1024;

// --- Command batching --------------------------------------------------------
// Arm commands no longer go out from the subscription callback. Each callback
// appends to a vector and this thread drains the whole vector on its own clock,
// so the executor never waits on a syscall and a burst of callbacks costs one
// send() instead of one each.
//
// 2 ms (500 Hz) is the drain period: fast enough that batching adds at most one
// tick of latency to a command, slow enough to absorb a burst into one datagram.
constexpr std::chrono::milliseconds kCommandSendPeriod{2};

// The pending vector has no ceiling. Capping it would mean throwing samples out
// of the middle of a trajectory to stay under the cap, and a missing middle is
// exactly what the arm feels as a jump -- worse than the same poses arriving
// late. It grows instead, so a stalled send thread costs latency and memory,
// never a discontinuity.
//
// It has no reason to grow far: the deadband below keeps a still arm from
// queueing anything at all, and the send thread empties it every tick. Passing
// this mark means the socket really is stuck, which earns a warning -- a
// warning only, nothing is discarded.
constexpr std::size_t kPendingHighWater = 500;

// --- Deadband ----------------------------------------------------------------
// A pose that barely differs from the last one queued carries no new
// instruction, and at teleop rates most poses are exactly that: a still arm
// republishes the same numbers hundreds of times a second. A sample is dropped
// before it reaches the buffer when EVERY joint is within this much of the last
// pose queued for that arm.
//
// Compared against the last pose *queued*, not the last received, on purpose.
// Against the last received, a slow drift of 0.01 per sample would be filtered
// forever and the far end would fall arbitrarily far behind. Against the last
// queued, that drift accumulates until it crosses the threshold and is then
// sent, so the far end is never off by more than the deadband itself.
constexpr double kJointDeadband = 0.02;

// Both arms, indexed by arm_index().
constexpr std::size_t kNumArms = 2;

std::size_t arm_index(ArmSide side)
{
  return side == ArmSide::kLeft ? 0u : 1u;
}

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
/// Subscribes to both arms' controller state and pushes the measured joint
/// positions to the server, which republishes them as commands.
///
/// Arm commands are batched, not sent one per callback. The subscription
/// callbacks only append to a vector; the command thread swaps that vector out
/// on its own clock and sends all of it. That keeps every syscall off the
/// executor thread and turns a burst of callbacks into one datagram instead of
/// one each. Buttons and the ping/pong are unchanged -- both are events, and
/// batching an event only delays it.
///
/// A pose only joins that vector if some joint moved past the deadband, so a
/// still arm puts nothing on the wire at all. Nothing that does join is ever
/// discarded to save room: the buffer is unbounded, because a gap torn out of a
/// trajectory is a jump the arm has to make, and late is better than jumpy.
///
/// Four threads beside the executor, each with one job:
///   command    -- drains the pending command vector every 2 ms and sends it as
///                 MTU-sized batches
///   connection -- pings the server every 500 ms and declares the link down
///                 when the answers stop
///   receive    -- parks in recv() waiting for those answers
///   health     -- periodic self-report, separate so a stalled send path is
///                 still reported by a thread that did not stall
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
      kVrButtonsTopic, rclcpp::QoS(kButtonQueueDepth).reliable(),
      [this](sensor_msgs::msg::Joy::ConstSharedPtr msg) { on_vr_buttons(msg); });

    left_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kLeftControllerStateTopic, qos);
    right_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kRightControllerStateTopic, qos);

    pending_.reserve(kPendingHighWater);
    send_buffer_.reserve(kPendingHighWater);

    RCLCPP_INFO(
      get_logger(), "forwarding '%s' and '%s' to %s",
      kLeftArmCommandTopic, kRightArmCommandTopic, server_text_.c_str());
    RCLCPP_INFO(
      get_logger(),
      "batching arm commands: drain every %ldms, %zu samples (%zu bytes) per datagram, "
      "deadband %.3f, unbounded buffer (warns past %zu)",
      static_cast<long>(kCommandSendPeriod.count()), transrecv_udp::kMaxSamplesPerDatagram,
      sizeof(transrecv_udp::JointBatchPacket), kJointDeadband, kPendingHighWater);
    RCLCPP_INFO(
      get_logger(), "registered publishers for '%s' and '%s' (depth %zu, reliable)",
      kLeftControllerStateTopic, kRightControllerStateTopic, kStateQueueDepth);
    RCLCPP_INFO(
      get_logger(), "mirroring '%s' to %s, on change only",
      kVrButtonsTopic, server_text_.c_str());
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

    command_thread_.start();
    connection_thread_.start();
    health_thread_.start();
  }

  /// Stops every thread and joins them. Safe to call twice.
  void stop()
  {
    health_thread_.stop();
    command_thread_.stop();
    connection_thread_.stop();

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
    unsigned char buffer[sizeof(transrecv_udp::JointPacket) * 2];

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
    transrecv_udp::ArmSample sample;
    sample.arm = side;
    std::copy(commanded.begin(), commanded.begin() + kNumArmJoints, sample.positions.begin());

    if (within_deadband(side, sample)) {                      // Rule 2: nothing moved
      samples_skipped_.fetch_add(1, std::memory_order_relaxed);
      RCLCPP_DEBUG(
        get_logger(), "[%s] every joint within %.3f of the last queued pose, not queueing",
        transrecv_udp::to_string(side), kJointDeadband);
      return;
    }

    // Recorded before the push, and only when the sample is actually kept: this
    // is what the next callback measures its motion against.
    LastPose & last = last_queued_[arm_index(side)];
    last.positions = sample.positions;
    last.has_data = true;

    // Appending, not sending: the socket is the send thread's problem, and this
    // callback is on the executor, which must not be the thread that blocks.
    std::size_t pending = 0;
    {
      std::lock_guard<std::mutex> lock(pending_mutex_);
      pending_.push_back(sample);
      pending = pending_.size();
    }

    samples_queued_.fetch_add(1, std::memory_order_relaxed);

    if (pending > kPendingHighWater) {                       // Rule 2: growing, not dropping
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "%zu samples pending, past the %zu mark (is the send thread starved?) -- "
        "nothing is discarded, they will go out late",
        pending, kPendingHighWater);
    }

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[%s] queued (%zu pending): [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), pending, sample.positions[0], sample.positions[1],
      sample.positions[2], sample.positions[3], sample.positions[4], sample.positions[5],
      sample.positions[6]);
  }

  /// True when `sample` is within kJointDeadband of the last pose queued for the
  /// same arm on every joint, i.e. the arm has not meaningfully moved.
  ///
  /// One joint past the threshold is enough to keep the whole pose: a command is
  /// all seven joints together, and sending six of them is not a thing the wire
  /// format or the arm can do.
  ///
  /// The first sample for an arm always passes -- there is nothing to compare it
  /// against, and the far end has to be told where the arm is at least once.
  ///
  /// Touched only by the executor thread, like last_buttons_sent_: both
  /// subscription callbacks run there, so the state needs no lock.
  bool within_deadband(ArmSide side, const transrecv_udp::ArmSample & sample) const
  {
    const LastPose & last = last_queued_[arm_index(side)];

    if (!last.has_data) {                                    // Rule 2: nothing to compare with
      return false;
    }

    for (std::size_t i = 0; i < kNumArmJoints; ++i) {
      if (std::fabs(last.positions[i] - sample.positions[i]) > kJointDeadband) {
        return false;                    // this joint moved, the pose is new
      }
    }
    return true;
  }

  // --- command send thread ---------------------------------------------------

  /// One tick: take everything the callbacks have collected and put all of it on
  /// the wire, then hand the emptied storage back for the next tick.
  ///
  /// The swap is the whole trick: the lock is held for a pointer exchange, not
  /// for the sends, so a slow socket can never stall a subscription callback.
  void send_pending_commands()
  {
    report_send_rate();

    // Cleared first so the swap hands `pending_` back an empty vector that has
    // already grown to the right capacity -- no allocation on the callback path.
    send_buffer_.clear();
    {
      std::lock_guard<std::mutex> lock(pending_mutex_);
      send_buffer_.swap(pending_);
    }

    if (send_buffer_.empty()) {
      return;                          // nothing arrived this tick, which is normal
    }

    if (!socket_.valid()) {                                  // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "socket not open, dropping %zu queued samples", send_buffer_.size());
      samples_dropped_.fetch_add(send_buffer_.size(), std::memory_order_relaxed);
      return;
    }

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "not connected to %s, dropping %zu queued samples",
        server_text_.c_str(), send_buffer_.size());
      samples_dropped_.fetch_add(send_buffer_.size(), std::memory_order_relaxed);
      return;
    }

    // One datagram per kMaxSamplesPerDatagram samples. Cutting at the MTU rather
    // than sending the vector as a single oversized datagram is what keeps a lost
    // packet costing 24 samples instead of the whole batch.
    std::size_t offset = 0;
    while (offset < send_buffer_.size()) {
      const std::size_t count =
        std::min(transrecv_udp::kMaxSamplesPerDatagram, send_buffer_.size() - offset);

      if (!send_batch(&send_buffer_[offset], count)) {
        // The socket errored or the link went down; the samples still queued
        // behind this chunk are no fresher, so they go too rather than being
        // sent late out of order.
        samples_dropped_.fetch_add(send_buffer_.size() - offset, std::memory_order_relaxed);
        return;
      }
      offset += count;
    }
  }

  /// Times this thread's own cadence and logs it every kRateReportSeconds.
  ///
  /// Measured here rather than by a watchdog because only this thread can see
  /// how late each individual pass was. Two numbers, because one of them lies:
  /// the average rate is what the loop managed overall, the worst gap is the
  /// longest it ever went between passes. 500 Hz for 4.9 s and then a 100 ms
  /// freeze still averages 480 Hz, and the arm feels the 100 ms.
  ///
  /// Called at the top of every pass, from the send thread only, so none of this
  /// state needs synchronisation.
  void report_send_rate()
  {
    const auto now = std::chrono::steady_clock::now();

    if (send_window_started_at_ == std::chrono::steady_clock::time_point{}) {
      // First pass ever: no previous one to measure a gap against.
      send_window_started_at_ = now;
      last_send_tick_ = now;
      return;
    }

    const auto gap = now - last_send_tick_;
    if (gap > worst_send_gap_) {
      worst_send_gap_ = gap;
    }
    last_send_tick_ = now;
    ++send_passes_;

    const double seconds = std::chrono::duration<double>(now - send_window_started_at_).count();
    if (seconds < kRateReportSeconds) {   // also the divide-by-zero guard below
      return;
    }

    RCLCPP_INFO(
      get_logger(), "send thread: %.1f Hz over %.1fs (%lu passes), worst gap %.2f ms",
      static_cast<double>(send_passes_) / seconds, seconds,
      static_cast<unsigned long>(send_passes_),
      std::chrono::duration<double, std::milli>(worst_send_gap_).count());

    send_window_started_at_ = now;
    send_passes_ = 0;
    worst_send_gap_ = std::chrono::steady_clock::duration::zero();
  }

  /// Single send path, so every failure is counted and logged one way.
  bool send_batch(const transrecv_udp::ArmSample * samples, std::size_t count)
  {
    transrecv_udp::JointBatchPacket packet{};
    const std::size_t bytes = transrecv_udp::encode_joint_batch(samples, count, packet);

    if (bytes == 0) {                                        // Rule 3: encoder rejected it
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "cannot encode a batch of %zu samples (max %zu), dropping it",
        count, transrecv_udp::kMaxSamplesPerDatagram);
      return false;
    }

    // send(), not sendto(): the peer is fixed by connect(), which is also what
    // makes ECONNREFUSED reach us here.
    const ssize_t sent = send(socket_.get(), &packet, bytes, 0);

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));             // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "send(batch of %zu) failed: %s", count, std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != bytes) {           // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "short batch send: %zd of %zu bytes", sent, bytes);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
    samples_sent_.fetch_add(count, std::memory_order_relaxed);
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
    const std::uint64_t queued = samples_queued_.load(std::memory_order_relaxed);
    const std::uint64_t samples = samples_sent_.load(std::memory_order_relaxed);
    const std::uint64_t lost = samples_dropped_.load(std::memory_order_relaxed);
    const std::uint64_t skipped = samples_skipped_.load(std::memory_order_relaxed);

    std::size_t pending = 0;
    {
      std::lock_guard<std::mutex> lock(pending_mutex_);
      pending = pending_.size();
    }

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: state-dependent branch
      RCLCPP_WARN(
        get_logger(), "health: not connected to %s | pings=%lu pongs=%lu dropped=%lu",
        server_text_.c_str(), static_cast<unsigned long>(pings),
        static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped));
      return;
    }

    if (sent == 0) {
      // last_send_time_ is still its zero value here, so its age is meaningless
      // -- report "never" rather than the seconds since the steady_clock epoch.
      // queued>0 separates "nothing is publishing commands" from "commands are
      // arriving but nothing reaches the wire", which are different faults.
      RCLCPP_WARN(
        get_logger(),
        "health: connected to %s but no joint data sent yet "
        "(are the controllers publishing state?) | queued=%lu pongs=%lu dropped=%lu",
        server_text_.c_str(), static_cast<unsigned long>(queued),
        static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped));
      return;
    }

    const auto since_send =
      std::chrono::steady_clock::now() - last_send_time_.load(std::memory_order_relaxed);

    if (since_send > kSendStaleThreshold) {
      RCLCPP_WARN(
        get_logger(),
        "health: nothing sent for %lds (controller state stalled?) "
        "| batches=%lu samples=%lu dropped=%lu",
        static_cast<long>(
          std::chrono::duration_cast<std::chrono::seconds>(since_send).count()),
        static_cast<unsigned long>(sent), static_cast<unsigned long>(samples),
        static_cast<unsigned long>(dropped));
      return;
    }

    RCLCPP_INFO(
      get_logger(),
      "health: OK | server=%s batches=%lu samples=%lu/%lu pending=%zu skipped=%lu "
      "lost=%lu pongs=%lu dropped=%lu",
      server_text_.c_str(), static_cast<unsigned long>(sent),
      static_cast<unsigned long>(samples), static_cast<unsigned long>(queued), pending,
      static_cast<unsigned long>(skipped), static_cast<unsigned long>(lost),
      static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped));
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

  /// The newest pose actually queued for one arm, kept only so the next
  /// callback has something to measure its motion against.
  struct LastPose
  {
    std::array<double, kNumArmJoints> positions{};
    bool has_data = false;
  };

  // Touched only by the executor thread (the trajectory callbacks), so no atomic
  // and no lock -- same reasoning as last_buttons_sent_.
  std::array<LastPose, kNumArms> last_queued_;

  // Arm commands waiting for the next send tick. Written by the executor
  // (subscription callbacks), swapped out by the send thread.
  std::mutex pending_mutex_;
  std::vector<transrecv_udp::ArmSample> pending_;

  // Touched only by the send thread: the empty vector handed to pending_ on each
  // swap. Keeping it as a member is what makes the swap allocation-free after
  // the first few ticks -- the capacity comes back around instead of being freed.
  std::vector<transrecv_udp::ArmSample> send_buffer_;

  std::atomic<std::uint64_t> samples_queued_{0};
  std::atomic<std::uint64_t> samples_sent_{0};
  std::atomic<std::uint64_t> samples_dropped_{0};
  std::atomic<std::uint64_t> samples_skipped_{0};

  // Send-thread cadence. Touched only by that thread, hence no atomics.
  std::chrono::steady_clock::time_point send_window_started_at_{};
  std::chrono::steady_clock::time_point last_send_tick_{};
  std::chrono::steady_clock::duration worst_send_gap_{
    std::chrono::steady_clock::duration::zero()};
  std::uint64_t send_passes_ = 0;

  // Touched only by the executor thread (the Joy callback), so no atomic and no
  // lock: the send path for buttons runs entirely inside that callback.
  std::vector<std::uint8_t> last_buttons_sent_;

  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_sub_;
  rclcpp::Subscription<sensor_msgs::msg::Joy>::SharedPtr buttons_sub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    left_state_pub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    right_state_pub_;

  std::thread receive_thread_;
  std::atomic<bool> receive_running_{false};

  // Declared last on purpose: members are destroyed in reverse order, so these
  // destructors join their threads before the state those threads read.
  transrecv_udp::PeriodicThread command_thread_{
    kCommandSendPeriod, get_logger(), [this]() { send_pending_commands(); },
    "command send thread"};
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
