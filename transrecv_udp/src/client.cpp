#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <rclcpp/rclcpp.hpp>
#include <control_msgs/msg/joint_trajectory_controller_state.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/joy.hpp>
#include <trajectory_msgs/msg/joint_trajectory.hpp>

#include "transrecv_udp/image_protocol.hpp"
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

// Revo2 hand commands going into the local hand controllers, mirrored to the
// server; the server's hand controller state comes back on the state topics.
constexpr const char * kLeftHandCommandTopic =
  "/left_revo2_hand_controller/joint_trajectory";
constexpr const char * kRightHandCommandTopic =
  "/right_revo2_hand_controller/joint_trajectory";
constexpr const char * kLeftHandStateTopic =
  "/left_revo2_hand_controller/controller_state";
constexpr const char * kRightHandStateTopic =
  "/right_revo2_hand_controller/controller_state";

// VR face buttons, mirrored to the server. Only the `buttons` array is used;
// `axes` is left alone because the joysticks travel on their own path.
constexpr const char * kVrButtonsTopic = "/vr_buttons";

constexpr std::size_t kStateQueueDepth = 10;

// Head camera frames rebuilt from the server's chunks, republished under the
// same name. Reliable so it matches both reliable and best-effort subscribers;
// depth 1 because only the newest frame matters.
constexpr const char * kHeadCameraTopic = "/cam_head/cam_head/color/image_raw/compressed";
constexpr std::size_t kImageQueueDepth = 1;

// A frame lands as a burst of ~1.4 KB chunks (a 640x480 JPEG is ~70 of them).
// The kernel buffer has to hold the whole burst while the receive thread is
// busy publishing the previous frame; 4 MiB is room for several 1080p frames.
constexpr std::size_t kReceiveBufferBytes = 4u * 1024u * 1024u;

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

/// Which limb a message or datagram belongs to. Arms and hands share every
/// code path; this is what tells them apart in the logs and picks the sizes.
enum class Limb
{
  kArm,
  kHand,
};

const char * to_string(Limb limb)
{
  return limb == Limb::kArm ? "arm" : "hand";
}

std::size_t joint_count(Limb limb)
{
  return limb == Limb::kArm ? kNumArmJoints : transrecv_udp::kNumHandJoints;
}

std::vector<std::string> joint_names(Limb limb, ArmSide side)
{
  return limb == Limb::kArm ?
         transrecv_udp::arm_joint_names(side) : transrecv_udp::hand_joint_names(side);
}
}  // namespace

/// Sending end of the UDP link.
///
/// Subscribes to both arms' controller state and pushes the measured joint
/// positions to the server, which republishes them as commands.
///
/// Three threads beside the executor, each with one job:
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
        on_joint_trajectory(Limb::kArm, ArmSide::kLeft, msg);
      });
    right_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kRightArmCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_joint_trajectory(Limb::kArm, ArmSide::kRight, msg);
      });
    left_hand_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kLeftHandCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_joint_trajectory(Limb::kHand, ArmSide::kLeft, msg);
      });
    right_hand_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kRightHandCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_joint_trajectory(Limb::kHand, ArmSide::kRight, msg);
      });

    buttons_sub_ = create_subscription<sensor_msgs::msg::Joy>(
      kVrButtonsTopic, rclcpp::QoS(kButtonQueueDepth).reliable(),
      [this](sensor_msgs::msg::Joy::ConstSharedPtr msg) { on_vr_buttons(msg); });

    left_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kLeftControllerStateTopic, qos);
    right_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kRightControllerStateTopic, qos);
    left_hand_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kLeftHandStateTopic, qos);
    right_hand_state_pub_ = create_publisher<control_msgs::msg::JointTrajectoryControllerState>(
      kRightHandStateTopic, qos);
    image_pub_ = create_publisher<sensor_msgs::msg::CompressedImage>(
      kHeadCameraTopic, rclcpp::QoS(kImageQueueDepth).reliable());
    RCLCPP_INFO(
      get_logger(), "republishing camera frames from %s on '%s' (depth %zu, reliable)",
      server_text_.c_str(), kHeadCameraTopic, kImageQueueDepth);

    RCLCPP_INFO(
      get_logger(), "forwarding '%s', '%s', '%s' and '%s' to %s",
      kLeftArmCommandTopic, kRightArmCommandTopic, kLeftHandCommandTopic,
      kRightHandCommandTopic, server_text_.c_str());
    RCLCPP_INFO(
      get_logger(),
      "registered publishers for '%s', '%s', '%s' and '%s' (depth %zu, reliable)",
      kLeftControllerStateTopic, kRightControllerStateTopic, kLeftHandStateTopic,
      kRightHandStateTopic, kStateQueueDepth);
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

    connection_thread_.start();
    health_thread_.start();
  }

  /// Stops every thread and joins them. Safe to call twice.
  void stop()
  {
    health_thread_.stop();
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
    // Sized for the largest datagram, an image chunk; recv() would silently
    // truncate anything bigger than the buffer.
    static_assert(
      transrecv_udp::kMaxImageDatagramBytes >= sizeof(transrecv_udp::JointPacket),
      "the receive buffer must fit every packet type");
    std::array<unsigned char, transrecv_udp::kMaxImageDatagramBytes> storage{};
    unsigned char * const buffer = storage.data();
    const std::size_t buffer_bytes = storage.size();

    while (receive_running_.load(std::memory_order_relaxed)) {
      if (!socket_.valid()) {                                // Rule 3: socket sanity
        RCLCPP_ERROR(get_logger(), "receive thread: socket is not open, exiting");
        return;
      }

      const ssize_t received = recv(socket_.get(), buffer, buffer_bytes, 0);

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
    // Image chunks first, by magic: their size varies and could match one of
    // the fixed packet sizes below (see image_protocol.hpp).
    if (transrecv_udp::is_image_chunk(data, length)) {
      handle_image_chunk(data, length);
      return;
    }

    // Everything else is classified by length: each packet type has its own
    // fixed size, so nothing has to be decoded to tell them apart.
    if (length == sizeof(transrecv_udp::JointPacket)) {
      handle_joint_datagram(Limb::kArm, data, length);
      return;
    }

    if (length == sizeof(transrecv_udp::HandPacket)) {
      handle_joint_datagram(Limb::kHand, data, length);
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
    Limb limb, ArmSide side, trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg)
  {
    if (msg == nullptr) {                                    // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s %s] null trajectory, dropping",
        transrecv_udp::to_string(side), to_string(limb));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (msg->points.empty()) {                               // Rule 3: empty guard
      RCLCPP_ERROR(
        get_logger(), "[%s %s] trajectory has no points, dropping",
        transrecv_udp::to_string(side), to_string(limb));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // The bridge publishes one point per message; when there are several, the
    // last one is the target the limb ends up at, which is what the server wants.
    const std::size_t needed = joint_count(limb);
    const std::vector<double> & commanded = msg->points.back().positions;
    if (commanded.size() < needed) {                         // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s %s] trajectory point has %zu positions, need %zu, dropping",
        transrecv_udp::to_string(side), to_string(limb), commanded.size(), needed);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // The message may carry more joints than the wire does; take the first
    // ones, matching what the bridge writes on these same topics.
    const std::vector<double> positions(commanded.begin(), commanded.begin() + needed);

    const bool sent = limb == Limb::kHand ?
      send_packet(limb, side, transrecv_udp::make_hand_packet(side, positions)) :
      send_packet(limb, side, transrecv_udp::make_packet(side, positions));
    if (!sent) {
      return;
    }

    // One call site per limb so each keeps its own throttle.
    if (limb == Limb::kHand) {
      RCLCPP_INFO_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs, "[%s hand] sent: %s",
        transrecv_udp::to_string(side), transrecv_udp::describe_positions(positions).c_str());
    } else {
      RCLCPP_INFO_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs, "[%s arm] sent: %s",
        transrecv_udp::to_string(side), transrecv_udp::describe_positions(positions).c_str());
    }
  }

  /// Single send path for arm and hand packets, so every failure is counted
  /// and logged one way.
  template<typename Packet>
  bool send_packet(Limb limb, ArmSide side, const Packet & packet)
  {
    if (!socket_.valid()) {                                  // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s %s] socket not open, cannot send", transrecv_udp::to_string(side),
        to_string(limb));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s %s] not connected to %s, dropping command",
        transrecv_udp::to_string(side), to_string(limb), server_text_.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    // send(), not sendto(): the peer is fixed by connect(), which is also what
    // makes ECONNREFUSED reach us here.
    const ssize_t sent = send(socket_.get(), &packet, sizeof(packet), 0);

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));             // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "[%s %s] send() failed: %s", transrecv_udp::to_string(side), to_string(limb),
          std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {   // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s %s] short send: %zd of %zu bytes", transrecv_udp::to_string(side),
        to_string(limb), sent, sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
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
  void handle_joint_datagram(Limb limb, const unsigned char * data, std::size_t length)
  {
    transrecv_udp::DecodedPacket decoded;
    std::string reason;
    const bool decoded_ok = limb == Limb::kHand ?
      transrecv_udp::decode_hand_packet(data, length, decoded, reason) :
      transrecv_udp::decode_packet(data, length, decoded, reason);
    if (!decoded_ok) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed %s datagram dropped: %s", to_string(limb), reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    states_received_.fetch_add(1, std::memory_order_relaxed);
    publish_controller_state(limb, decoded.arm, decoded.positions);
  }

  /// Publishes one arm's or hand's controller state locally.
  void publish_controller_state(Limb limb, ArmSide side, const std::vector<double> & positions)
  {
    const std::size_t expected = joint_count(limb);
    if (positions.size() != expected) {                      // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s %s] %zu positions, expected %zu, not publishing state",
        transrecv_udp::to_string(side), to_string(limb), positions.size(), expected);
      return;
    }

    const bool left = side == ArmSide::kLeft;
    const auto & publisher = limb == Limb::kHand ?
      (left ? left_hand_state_pub_ : right_hand_state_pub_) :
      (left ? left_state_pub_ : right_state_pub_);
    if (publisher == nullptr) {                              // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s %s] state publisher is null, not publishing",
        transrecv_udp::to_string(side), to_string(limb));
      return;
    }

    control_msgs::msg::JointTrajectoryControllerState message;
    message.header.stamp = now();
    message.joint_names = joint_names(limb, side);
    message.feedback.positions = positions;
    publisher->publish(message);

    states_published_.fetch_add(1, std::memory_order_relaxed);

    // One call site per limb so each keeps its own throttle.
    if (limb == Limb::kHand) {
      RCLCPP_INFO_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs, "[%s hand] state published: %s",
        transrecv_udp::to_string(side), transrecv_udp::describe_positions(positions).c_str());
    } else {
      RCLCPP_INFO_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs, "[%s arm] state published: %s",
        transrecv_udp::to_string(side), transrecv_udp::describe_positions(positions).c_str());
    }
  }

  // --- head camera -----------------------------------------------------------

  /// Feeds one chunk to the reassembler and publishes the frame it completes.
  /// Runs on the receive thread, the only user of image_reassembler_.
  void handle_image_chunk(const unsigned char * data, std::size_t length)
  {
    transrecv_udp::ImageChunkHeader header{};
    const std::uint8_t * payload = nullptr;
    std::size_t payload_bytes = 0;
    std::string reason;
    if (!transrecv_udp::decode_image_chunk(data, length, header, payload, payload_bytes, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "[image] malformed chunk dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    const std::uint64_t abandoned_before = image_reassembler_.frames_abandoned();
    const transrecv_udp::ImageReassembler::Result result =
      image_reassembler_.add(header, payload, payload_bytes);

    if (image_reassembler_.frames_abandoned() != abandoned_before) {   // Rule 2: lost chunks
      // Mirrored into an atomic: the reassembler is receive-thread only, the
      // health thread reads this copy.
      images_incomplete_.store(
        image_reassembler_.frames_abandoned(), std::memory_order_relaxed);
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[image] frame before %u never completed (chunks lost), %lu incomplete so far",
        header.frame_seq,
        static_cast<unsigned long>(image_reassembler_.frames_abandoned()));
    }

    switch (result) {
      case transrecv_udp::ImageReassembler::Result::kPending:
        return;
      case transrecv_udp::ImageReassembler::Result::kStale:
        RCLCPP_DEBUG(
          get_logger(), "[image] late chunk of frame %u ignored", header.frame_seq);
        return;
      case transrecv_udp::ImageReassembler::Result::kDuplicate:
        RCLCPP_DEBUG(
          get_logger(), "[image] duplicate chunk %u of frame %u ignored",
          static_cast<unsigned>(header.chunk_index), header.frame_seq);
        return;
      case transrecv_udp::ImageReassembler::Result::kInconsistent:
        RCLCPP_WARN_THROTTLE(
          get_logger(), *get_clock(), kLogThrottleMs,
          "[image] chunk %u of frame %u disagrees with the frame in progress, ignored",
          static_cast<unsigned>(header.chunk_index), header.frame_seq);
        packets_dropped_.fetch_add(1, std::memory_order_relaxed);
        return;
      case transrecv_udp::ImageReassembler::Result::kComplete:
        break;
    }

    transrecv_udp::ImageFrame frame;
    if (!transrecv_udp::decode_image_frame(image_reassembler_.frame(), frame, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "[image] frame %u reassembled but undecodable: %s", header.frame_seq, reason.c_str());
      images_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    publish_image(header.frame_seq, std::move(frame));
  }

  void publish_image(std::uint32_t seq, transrecv_udp::ImageFrame frame)
  {
    if (image_pub_ == nullptr) {                             // Rule 3: null guard
      RCLCPP_ERROR(get_logger(), "[image] publisher is null, dropping frame %u", seq);
      images_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    sensor_msgs::msg::CompressedImage message;
    // The camera's own capture stamp, not the time it got here: it is what
    // lines a frame up with the moment it shows. The two machines' clocks are
    // not synchronised, so compare it with server-side stamps, not local ones.
    message.header.stamp.sec = frame.stamp_sec;
    message.header.stamp.nanosec = frame.stamp_nanosec;
    message.header.frame_id = std::move(frame.frame_id);
    message.format = std::move(frame.format);
    message.data = std::move(frame.data);

    const std::size_t bytes = message.data.size();
    image_pub_->publish(std::move(message));
    images_published_.fetch_add(1, std::memory_order_relaxed);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[image] frame %u published: %zu bytes", seq, bytes);
  }

  // --- health thread ---------------------------------------------------------

  /// Image half of the health report, printed whatever state the link is in.
  void report_image_health()
  {
    const std::uint64_t published = images_published_.load(std::memory_order_relaxed);
    const std::uint64_t incomplete = images_incomplete_.load(std::memory_order_relaxed);
    const std::uint64_t dropped = images_dropped_.load(std::memory_order_relaxed);

    if (published == 0) {                                    // Rule 2: nothing yet
      RCLCPP_WARN(
        get_logger(),
        "health: no camera frame rebuilt yet for '%s' | incomplete=%lu undecodable=%lu",
        kHeadCameraTopic, static_cast<unsigned long>(incomplete),
        static_cast<unsigned long>(dropped));
      return;
    }

    RCLCPP_INFO(
      get_logger(), "health: image published=%lu incomplete=%lu undecodable=%lu",
      static_cast<unsigned long>(published), static_cast<unsigned long>(incomplete),
      static_cast<unsigned long>(dropped));
  }

  /// One watchdog pass, on its own thread so a stalled callback is still
  /// reported by something that did not stall.
  void check_health()
  {
    if (!socket_.valid()) {                                  // Rule 3: socket sanity
      RCLCPP_ERROR(get_logger(), "health: socket is not open, client is dead");
      return;
    }

    report_image_health();

    const std::uint64_t sent = packets_sent_.load(std::memory_order_relaxed);
    const std::uint64_t dropped = packets_dropped_.load(std::memory_order_relaxed);
    const std::uint64_t pings = pings_sent_.load(std::memory_order_relaxed);
    const std::uint64_t pongs = pongs_received_.load(std::memory_order_relaxed);

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
      RCLCPP_WARN(
        get_logger(),
        "health: connected to %s but no joint data sent yet "
        "(are the controllers publishing state?) | pongs=%lu dropped=%lu",
        server_text_.c_str(), static_cast<unsigned long>(pongs),
        static_cast<unsigned long>(dropped));
      return;
    }

    const auto since_send =
      std::chrono::steady_clock::now() - last_send_time_.load(std::memory_order_relaxed);

    if (since_send > kSendStaleThreshold) {
      RCLCPP_WARN(
        get_logger(),
        "health: nothing sent for %lds (controller state stalled?) | sent=%lu dropped=%lu",
        static_cast<long>(
          std::chrono::duration_cast<std::chrono::seconds>(since_send).count()),
        static_cast<unsigned long>(sent), static_cast<unsigned long>(dropped));
      return;
    }

    RCLCPP_INFO(
      get_logger(), "health: OK | server=%s sent=%lu pongs=%lu dropped=%lu",
      server_text_.c_str(), static_cast<unsigned long>(sent),
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

  // Touched only by the executor thread (the Joy callback), so no atomic and no
  // lock: the send path for buttons runs entirely inside that callback.
  std::vector<std::uint8_t> last_buttons_sent_;

  // Touched only by the receive thread.
  transrecv_udp::ImageReassembler image_reassembler_;
  // Written by the receive thread, read by the health thread.
  std::atomic<std::uint64_t> images_published_{0};
  std::atomic<std::uint64_t> images_incomplete_{0};
  std::atomic<std::uint64_t> images_dropped_{0};
  rclcpp::Publisher<sensor_msgs::msg::CompressedImage>::SharedPtr image_pub_;

  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_hand_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_hand_sub_;
  rclcpp::Subscription<sensor_msgs::msg::Joy>::SharedPtr buttons_sub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    left_state_pub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    right_state_pub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    left_hand_state_pub_;
  rclcpp::Publisher<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    right_hand_state_pub_;

  std::thread receive_thread_;
  std::atomic<bool> receive_running_{false};

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

  // Not fatal: with the default buffer joint data still flows fine, camera
  // frames just lose chunks more often. set_receive_buffer() says which.
  if (!client_socket.set_receive_buffer(kReceiveBufferBytes, logger)) {
    RCLCPP_WARN(logger, "keeping the default receive buffer, camera frames may drop");
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
