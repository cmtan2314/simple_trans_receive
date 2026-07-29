#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

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
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <sensor_msgs/msg/joy.hpp>
#include <trajectory_msgs/msg/joint_trajectory.hpp>

#include "transrecv_udp/periodic_thread.hpp"
#include "transrecv_udp/protocol.hpp"
#include "transrecv_udp/udp_socket.hpp"

namespace
{
using transrecv_udp::ArmSide;
using transrecv_udp::kNumArmJoints;
using transrecv_udp::kNumHandJoints;

// Joint state arriving from the client is republished here as a command, i.e.
// straight onto the topics the arm controllers consume.
constexpr const char * kLeftArmTrajectoryTopic =
  "/left_joint_trajectory_controller/joint_trajectory";
constexpr const char * kRightArmTrajectoryTopic =
  "/right_joint_trajectory_controller/joint_trajectory";

// Measured state from the local controllers, forwarded to the client.
constexpr const char * kLeftControllerStateTopic =
  "/left_joint_trajectory_controller/controller_state";
constexpr const char * kRightControllerStateTopic =
  "/right_joint_trajectory_controller/controller_state";

// Matches the publisher side of the dora bridge (dora_ros2_bridge/main.py).
constexpr std::size_t kTrajectoryQueueDepth = 10;
// VR face buttons received from the client and republished here.
constexpr const char * kVrButtonsTopic = "/vr_buttons";

// FK end-effector pose received from the client and republished here.
constexpr const char * kLeftEePoseTopic = "/left_ee_pose";
constexpr const char * kRightEePoseTopic = "/right_ee_pose";

// Revo2 hand (gripper) commands received from the client and republished here.
constexpr const char * kLeftHandTrajectoryTopic = "/left_revo2_hand_controller/joint_trajectory";
constexpr const char * kRightHandTrajectoryTopic = "/right_revo2_hand_controller/joint_trajectory";

constexpr std::size_t kStateQueueDepth = 10;
constexpr std::size_t kButtonQueueDepth = 10;
// ee_pose is a high-rate stream like buttons; best_effort matches the source's
// nature (dora_ros2_bridge's qos_best_effort) rather than forcing reliable
// everywhere.
constexpr std::size_t kPoseQueueDepth = 10;

// Both arms, indexed by ArmSide.
constexpr std::size_t kNumArms = 2;

// Traffic is high rate: throttle the per-packet logs so a long run does not
// drown the interesting branches. This one covers the problem paths (malformed
// packets, failed syscalls), which are worth hearing about promptly.
constexpr int kLogThrottleMs = 1000;

// The healthy-path sample line is only a liveness sanity check, so it gets a
// slower cadence of its own -- there is no reason for routine success to be as
// loud as a failure.
//
// Note this throttle is shared by both arms: rcutils keeps the throttle state
// in a static at the call site, and left and right go through the same line.
// So this is one line every 5 s in total, not one per arm, and a stretch
// showing only one side does not mean the other stopped arriving.
constexpr int kDataLogThrottleMs = 5000;

// --- Rate report ------------------------------------------------------------
// Averaging over a fixed number of datagrams rather than a fixed wall-clock
// interval keeps every report built from the same amount of evidence, so the
// numbers stay comparable when the stream speeds up or slows down.
//
// Receive and publish are measured over the same window on purpose: a fixed
// number of publishes follows every accepted datagram, so the two rates keep a
// fixed ratio unless a packet was rejected. The gap is the drop rate, made
// visible without having to diff two counters by hand.
constexpr std::uint64_t kRateReportInterval = 10000;

// How many trajectories one accepted datagram owes. A both-arms packet carries
// the whole robot, so it publishes to both controllers.
constexpr unsigned int kTrajectoriesPerSingleArmPacket = 1;
constexpr unsigned int kTrajectoriesPerDualArmPacket = 2;

// Guards the division below: a window this short means the clock is unusable
// (or 100 datagrams genuinely arrived within a microsecond, which is not real).
constexpr double kMinRateWindowSeconds = 1e-9;

// --- UDP transport ----------------------------------------------------------
// Binding INADDR_ANY accepts a client on any local interface, so only the port
// has to be chosen. Nothing about the client is configured: joint data is
// one-way (client -> server), and the only reply is a pong sent straight back
// to whoever pinged.
constexpr std::uint16_t kMinUserPort = 1024;

// The receive thread parks in a blocking recv(). This timeout is only how long
// it can go without noticing a stop request, so it trades shutdown latency
// against wakeups: 200 ms is imperceptible on exit and idles at 5 wakeups/s.
constexpr std::chrono::milliseconds kReceiveTimeout{200};

// --- Send thread ------------------------------------------------------------
// The controller state subscribed to above is pushed to the client on its own
// thread, on its own clock: the ROS callbacks that supply it and the socket
// that consumes it should not be able to stall each other.
//
// ASSUMPTION: 100 Hz. Nothing in the requirement fixed a rate, so this is a
// starting value, not a derived one -- change kStateSendPeriod if the far end
// wants something else. Sending on a clock rather than per callback also means
// the wire rate stays put when the controller rate wobbles; the cost is that a
// pose can be repeated, or one skipped, when the two clocks disagree.
constexpr std::chrono::milliseconds kStateSendPeriod{10};

// --- Health monitor ---------------------------------------------------------
constexpr std::chrono::milliseconds kHealthCheckPeriod{5000};

// How long without a datagram before the link is called stale.
constexpr std::chrono::seconds kReceiveStaleThreshold{2};

/// Parses the optional port argument. Returns nullopt on a malformed value so
/// the caller can fail loudly instead of silently binding somewhere unintended.
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

std::size_t arm_index(ArmSide side)
{
  return side == ArmSide::kLeft ? 0u : 1u;
}
}  // namespace

/// Receiving end of the UDP link.
///
/// Two threads:
///   receive -- parks in recv(), decodes each datagram and publishes it right
///              there; answers pings with a pong
///   health  -- periodic self-report, on its own thread on purpose: a watchdog
///              that shares a thread with the work it watches cannot report a
///              stall in that work
///
/// Publishing straight from the receive thread means the output rate is exactly
/// the input rate: no buffering, no repeating, and no added latency beyond the
/// decode. If the client stops sending, the topic simply goes quiet.
class TrajectoryServer : public rclcpp::Node
{
public:
  /// `socket_fd` must outlive this node; ownership stays with the caller.
  /// `client` is where the send thread pushes controller state.
  TrajectoryServer(
    int socket_fd, std::uint16_t port, const sockaddr_in & client,
    const std::string & client_text)
  : rclcpp::Node("transrecv_udp_server"),
    socket_fd_(socket_fd),
    port_(port),
    client_(client),
    client_text_(client_text)
  {
    if (socket_fd_ < 0) {                                    // Rule 3: precondition
      RCLCPP_ERROR(get_logger(), "constructed with an invalid socket fd %d", socket_fd_);
      throw std::invalid_argument("TrajectoryServer needs a bound socket");
    }

    const rclcpp::QoS qos = rclcpp::QoS(kTrajectoryQueueDepth).reliable();
    publishers_[arm_index(ArmSide::kLeft)] =
      create_publisher<trajectory_msgs::msg::JointTrajectory>(kLeftArmTrajectoryTopic, qos);
    publishers_[arm_index(ArmSide::kRight)] =
      create_publisher<trajectory_msgs::msg::JointTrajectory>(kRightArmTrajectoryTopic, qos);

    buttons_pub_ = create_publisher<sensor_msgs::msg::Joy>(
      kVrButtonsTopic, rclcpp::QoS(kButtonQueueDepth).reliable());

    const rclcpp::QoS pose_qos = rclcpp::QoS(kPoseQueueDepth).best_effort();
    left_ee_pose_pub_ = create_publisher<geometry_msgs::msg::PoseStamped>(
      kLeftEePoseTopic, pose_qos);
    right_ee_pose_pub_ = create_publisher<geometry_msgs::msg::PoseStamped>(
      kRightEePoseTopic, pose_qos);

    left_hand_pub_ = create_publisher<trajectory_msgs::msg::JointTrajectory>(
      kLeftHandTrajectoryTopic, qos);
    right_hand_pub_ = create_publisher<trajectory_msgs::msg::JointTrajectory>(
      kRightHandTrajectoryTopic, qos);

    const rclcpp::QoS state_qos = rclcpp::QoS(kStateQueueDepth).reliable();
    left_state_sub_ = create_subscription<control_msgs::msg::JointTrajectoryControllerState>(
      kLeftControllerStateTopic, state_qos,
      [this](control_msgs::msg::JointTrajectoryControllerState::ConstSharedPtr msg) {
        on_controller_state(ArmSide::kLeft, msg);
      });
    right_state_sub_ = create_subscription<control_msgs::msg::JointTrajectoryControllerState>(
      kRightControllerStateTopic, state_qos,
      [this](control_msgs::msg::JointTrajectoryControllerState::ConstSharedPtr msg) {
        on_controller_state(ArmSide::kRight, msg);
      });

    RCLCPP_INFO(
      get_logger(), "publishing on '%s' and '%s' (depth %zu, reliable)",
      kLeftArmTrajectoryTopic, kRightArmTrajectoryTopic, kTrajectoryQueueDepth);
    RCLCPP_INFO(
      get_logger(), "publishing on '%s' and '%s' (depth %zu, best_effort)",
      kLeftEePoseTopic, kRightEePoseTopic, kPoseQueueDepth);
    RCLCPP_INFO(
      get_logger(), "publishing on '%s' and '%s' (depth %zu, reliable)",
      kLeftHandTrajectoryTopic, kRightHandTrajectoryTopic, kTrajectoryQueueDepth);
    RCLCPP_INFO(
      get_logger(), "subscribed to '%s' and '%s' (depth %zu, reliable)",
      kLeftControllerStateTopic, kRightControllerStateTopic, kStateQueueDepth);
    RCLCPP_INFO(
      get_logger(), "pushing controller state to %s every %ldms",
      client_text_.c_str(), static_cast<long>(kStateSendPeriod.count()));
    RCLCPP_INFO(get_logger(), "waiting for client datagrams on port %u", port_);
  }

  ~TrajectoryServer() override
  {
    stop();
  }

  /// Starts both threads. Separate from the constructor so `this` is fully
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

    send_thread_.start();
    health_thread_.start();
  }

  /// Stops both threads and joins them. Safe to call twice.
  void stop()
  {
    health_thread_.stop();
    send_thread_.stop();

    if (!receive_thread_.joinable()) {
      return;
    }
    receive_running_.store(false, std::memory_order_relaxed);
    receive_thread_.join();          // wakes within kReceiveTimeout at the latest
    RCLCPP_INFO(get_logger(), "receive thread stopped");
  }

private:
  // --- receive thread --------------------------------------------------------

  /// Parks in recv(), then decodes and publishes in place. Its own thread
  /// rather than an executor timer so it can block instead of poll.
  void run_receive_loop()
  {
    unsigned char buffer[transrecv_udp::kReceiveBufferBytes];

    while (receive_running_.load(std::memory_order_relaxed)) {
      if (socket_fd_ < 0) {                                  // Rule 3: socket sanity
        RCLCPP_ERROR(get_logger(), "receive thread: socket is not open, exiting");
        return;
      }

      // recvfrom, not recv: a ping has to be answered, so the sender's address
      // is needed even though joint data never gets a reply.
      sockaddr_in sender{};
      socklen_t sender_len = sizeof(sender);
      const ssize_t received = recvfrom(
        socket_fd_, buffer, sizeof(buffer), 0,
        reinterpret_cast<sockaddr *>(&sender), &sender_len);

      if (received < 0) {
        // EAGAIN/EWOULDBLOCK is the SO_RCVTIMEO expiring, which is how this
        // loop gets to re-check the stop flag. Not an error.
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
          continue;
        }
        RCLCPP_WARN_THROTTLE(
          get_logger(), *get_clock(), kLogThrottleMs, "recv() failed: %s",
          std::strerror(errno));
        continue;
      }

      if (sender_len != sizeof(sender) || sender.sin_family != AF_INET) {  // Rule 3: sanity
        RCLCPP_WARN_THROTTLE(
          get_logger(), *get_clock(), kLogThrottleMs,
          "ignoring datagram from a non-IPv4 sender");
        packets_dropped_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      handle_datagram(buffer, static_cast<std::size_t>(received), sender);
    }
  }

  void handle_datagram(const unsigned char * data, std::size_t length, const sockaddr_in & sender)
  {
    // Classify by length first: the two packet types have different fixed
    // sizes, so nothing has to be decoded to tell them apart.
    if (length == sizeof(transrecv_udp::ControlPacket)) {
      handle_control(data, length, sender);
      return;
    }

    if (length == sizeof(transrecv_udp::ButtonPacket)) {
      handle_buttons(data, length);
      return;
    }

    if (length == sizeof(transrecv_udp::PosePacket)) {
      handle_ee_pose(data, length);
      return;
    }

    if (length == sizeof(transrecv_udp::HandJointPacket)) {
      handle_hand(data, length);
      return;
    }

    if (length == sizeof(transrecv_udp::DualJointPacket)) {
      handle_dual_joint(data, length);
      return;
    }

    // Single-arm JointPackets are no longer what the client sends -- arm
    // commands arrive as DualJointPacket above. This path stays because the
    // format is still what the server itself sends back as controller state,
    // and rejecting it here would make a loopback setup silently confusing.
    transrecv_udp::DecodedPacket decoded;
    std::string reason;
    if (!transrecv_udp::decode_packet(data, length, decoded, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    packets_received_.fetch_add(1, std::memory_order_relaxed);
    last_receive_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);

    publish_trajectory(decoded.arm, decoded.positions);
    report_rates(kTrajectoriesPerSingleArmPacket);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kDataLogThrottleMs,
      "[%s] received and published: [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(decoded.arm), decoded.positions[0], decoded.positions[1],
      decoded.positions[2], decoded.positions[3], decoded.positions[4],
      decoded.positions[5], decoded.positions[6]);
  }

  /// Decodes one both-arms datagram and publishes a trajectory for each side.
  ///
  /// The two publishes are not atomic -- ROS offers no way to make them so --
  /// but they are back to back off one decoded packet, which is as close to
  /// "both arms from the same instant" as this can get.
  void handle_dual_joint(const unsigned char * data, std::size_t length)
  {
    transrecv_udp::ArmPositions left{};
    transrecv_udp::ArmPositions right{};
    std::string reason;
    if (!transrecv_udp::decode_dual_packet(data, length, left, right, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed both-arms datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    packets_received_.fetch_add(1, std::memory_order_relaxed);
    last_receive_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);

    publish_trajectory(ArmSide::kLeft, std::vector<double>(left.begin(), left.end()));
    publish_trajectory(ArmSide::kRight, std::vector<double>(right.begin(), right.end()));
    report_rates(kTrajectoriesPerDualArmPacket);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kDataLogThrottleMs,
      "both arms received and published | left [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]"
      " | right [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      left[0], left[1], left[2], left[3], left[4], left[5], left[6],
      right[0], right[1], right[2], right[3], right[4], right[5], right[6]);
  }

  /// Averages the receive and publish rates over the last kRateReportInterval
  /// datagrams and logs both, then starts a fresh window.
  ///
  /// `expected` is how many trajectories this datagram should have produced --
  /// two for a both-arms packet, one for a single-arm one. Without it the
  /// shortfall below would be computed against the datagram count and a healthy
  /// both-arms stream would report every window as broken.
  ///
  /// Only ever called from the receive thread, which is also the only writer of
  /// these members, so they need no synchronisation.
  void report_rates(unsigned int expected)
  {
    window_expected_ += expected;
    ++window_received_;
    if (window_received_ < kRateReportInterval) {
      return;
    }

    const auto now = std::chrono::steady_clock::now();

    if (window_started_at_ == std::chrono::steady_clock::time_point{}) {
      // First window ever: it began at an unknown time, so it has no duration
      // to divide by. Start the clock here and report from the next one on.
      window_started_at_ = now;
      window_received_ = 0;
      window_published_ = 0;
      window_expected_ = 0;
      RCLCPP_INFO(
        get_logger(), "rate: first %lu datagrams received, measuring from here",
        static_cast<unsigned long>(kRateReportInterval));
      return;
    }

    const double seconds = std::chrono::duration<double>(now - window_started_at_).count();

    if (seconds < kMinRateWindowSeconds) {                   // Rule 3: divide-by-zero guard
      RCLCPP_WARN(
        get_logger(), "rate: window of %g s is too short to average, skipping this report",
        seconds);
      window_started_at_ = now;
      window_received_ = 0;
      window_published_ = 0;
      window_expected_ = 0;
      return;
    }

    const double receive_hz = static_cast<double>(window_received_) / seconds;
    const double publish_hz = static_cast<double>(window_published_) / seconds;

    // Compared against what the datagrams asked for, not against how many there
    // were: one both-arms datagram owes two trajectories.
    if (window_published_ < window_expected_) {              // Rule 2: the rates disagree
      RCLCPP_WARN(
        get_logger(),
        "rate over %lu datagrams: recv %.1f Hz, push %.1f Hz "
        "(%lu of %lu trajectories not published)",
        static_cast<unsigned long>(kRateReportInterval), receive_hz, publish_hz,
        static_cast<unsigned long>(window_expected_ - window_published_),
        static_cast<unsigned long>(window_expected_));
    } else {
      RCLCPP_INFO(
        get_logger(), "rate over %lu datagrams: recv %.1f Hz, push %.1f Hz",
        static_cast<unsigned long>(kRateReportInterval), receive_hz, publish_hz);
    }

    window_started_at_ = now;
    window_received_ = 0;
    window_published_ = 0;
    window_expected_ = 0;
  }

  /// Republishes VR button state received from the client.
  ///
  /// The client only sends on change, so every datagram that lands here is a
  /// press or a release. That also means this topic goes quiet between events
  /// rather than ticking -- a subscriber must latch what it last saw instead of
  /// expecting a steady stream.
  void handle_buttons(const unsigned char * data, std::size_t length)
  {
    // Counted here, before decode, so this reflects "a button-sized datagram
    // reached the socket" -- independent of whether it then decodes cleanly.
    buttons_received_.fetch_add(1, std::memory_order_relaxed);

    std::vector<std::uint8_t> buttons;
    std::string reason;
    if (!transrecv_udp::decode_button_packet(data, length, buttons, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed button datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (buttons.empty()) {                                   // Rule 3: size guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "button datagram carried no buttons, dropping");
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (buttons_pub_ == nullptr) {                           // Rule 3: null guard
      RCLCPP_ERROR(get_logger(), "button publisher is null, dropping");
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    sensor_msgs::msg::Joy message;
    message.header.stamp = now();
    // axes stays empty: only the button flags travel on this path.
    message.buttons.reserve(buttons.size());
    for (const std::uint8_t value : buttons) {
      message.buttons.push_back(static_cast<std::int32_t>(value));
    }
    buttons_pub_->publish(message);

    buttons_published_.fetch_add(1, std::memory_order_relaxed);

    // Not throttled: the client already limits this to one per press/release.
    std::string text = "[";
    for (std::size_t i = 0; i < buttons.size(); ++i) {
      text += buttons[i] != 0 ? '1' : '0';
      if (i + 1 < buttons.size()) {
        text += ' ';
      }
    }
    RCLCPP_INFO(get_logger(), "buttons published: %s]", text.c_str());
  }

  /// Republishes an FK end-effector pose received from the client.
  void handle_ee_pose(const unsigned char * data, std::size_t length)
  {
    poses_received_.fetch_add(1, std::memory_order_relaxed);

    ArmSide arm = ArmSide::kLeft;
    std::vector<double> values;
    std::string reason;
    if (!transrecv_udp::decode_pose_packet(data, length, arm, values, reason)) {
      RCLCPP_WARN_THROTTLE(                                    // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed pose datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (values.size() != transrecv_udp::kPoseValues) {         // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] decoded %zu pose values, expected %zu, not publishing",
        transrecv_udp::to_string(arm), values.size(), transrecv_udp::kPoseValues);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    const auto & publisher = arm == ArmSide::kLeft ? left_ee_pose_pub_ : right_ee_pose_pub_;
    if (publisher == nullptr) {                                // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] ee_pose publisher is null, dropping", transrecv_udp::to_string(arm));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    geometry_msgs::msg::PoseStamped message;
    message.header.stamp = now();
    message.pose.position.x = values[0];
    message.pose.position.y = values[1];
    message.pose.position.z = values[2];
    message.pose.orientation.w = values[3];
    message.pose.orientation.x = values[4];
    message.pose.orientation.y = values[5];
    message.pose.orientation.z = values[6];
    publisher->publish(message);

    poses_published_.fetch_add(1, std::memory_order_relaxed);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kDataLogThrottleMs,
      "[%s] ee_pose published: pos=[%.3f %.3f %.3f] quat=[%.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(arm), values[0], values[1], values[2],
      values[3], values[4], values[5], values[6]);
  }

  /// Republishes a Revo2 hand (gripper) command received from the client.
  void handle_hand(const unsigned char * data, std::size_t length)
  {
    hands_received_.fetch_add(1, std::memory_order_relaxed);

    ArmSide arm = ArmSide::kLeft;
    std::vector<double> positions;
    std::string reason;
    if (!transrecv_udp::decode_hand_packet(data, length, arm, positions, reason)) {
      RCLCPP_WARN_THROTTLE(                                    // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed hand datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (positions.size() != kNumHandJoints) {                  // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] decoded %zu hand positions, expected %zu, not publishing",
        transrecv_udp::to_string(arm), positions.size(), kNumHandJoints);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    const auto & publisher = arm == ArmSide::kLeft ? left_hand_pub_ : right_hand_pub_;
    if (publisher == nullptr) {                                // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] hand publisher is null, dropping", transrecv_udp::to_string(arm));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    trajectory_msgs::msg::JointTrajectory message;
    // header.stamp stays zero deliberately: joint_trajectory_controller treats
    // a non-zero stamp as an absolute start time, and with time_from_start=0
    // that start time is also the trajectory's end time. Stamping "now" here
    // means it is already in the past by the time the controller processes
    // it, so the controller rejects it ("trajectory ... ends in the past").
    // Zero is the documented "execute immediately" convention.
    message.joint_names = transrecv_udp::hand_joint_names(arm);

    trajectory_msgs::msg::JointTrajectoryPoint point;
    point.positions = positions;
    // time_from_start stays zero: same as the arm command path, "go there now".
    message.points.push_back(point);

    publisher->publish(message);
    hands_published_.fetch_add(1, std::memory_order_relaxed);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kDataLogThrottleMs,
      "[%s] hand published: [%.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(arm), positions[0], positions[1], positions[2],
      positions[3], positions[4], positions[5]);
  }

  /// Answers a client's liveness ping. This reply is the only thing the server
  /// ever sends: it carries no data, it just proves the server is alive, which
  /// is something the client cannot establish on its own over UDP.
  void handle_control(const unsigned char * data, std::size_t length, const sockaddr_in & sender)
  {
    transrecv_udp::ControlType type = transrecv_udp::ControlType::kPing;
    std::string reason;
    if (!transrecv_udp::decode_control_packet(data, length, type, reason)) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: reject, don't guess
        get_logger(), *get_clock(), kLogThrottleMs,
        "malformed control datagram dropped: %s", reason.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (type != transrecv_udp::ControlType::kPing) {         // Rule 2: unexpected but handled
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "ignoring a '%s' control packet, the server only answers pings",
        transrecv_udp::to_string(type));
      return;
    }

    if (socket_fd_ < 0) {                                    // Rule 3: socket sanity
      RCLCPP_ERROR(get_logger(), "cannot answer ping, socket is not open");
      return;
    }

    const transrecv_udp::ControlPacket pong =
      transrecv_udp::make_control_packet(transrecv_udp::ControlType::kPong);
    const ssize_t sent = sendto(
      socket_fd_, &pong, sizeof(pong), 0,
      reinterpret_cast<const sockaddr *>(&sender), sizeof(sender));

    if (sent < 0) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: log the failure path
        get_logger(), *get_clock(), kLogThrottleMs,
        "sendto(pong) failed: %s", std::strerror(errno));
      return;
    }

    if (static_cast<std::size_t>(sent) != sizeof(pong)) {     // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "short pong: %zd of %zu bytes", sent, sizeof(pong));
      return;
    }

    pongs_sent_.fetch_add(1, std::memory_order_relaxed);

    char text[INET_ADDRSTRLEN] = {};
    const char * address =
      inet_ntop(AF_INET, &sender.sin_addr, text, sizeof(text)) != nullptr ? text : "?";
    RCLCPP_DEBUG(get_logger(), "answered ping from %s:%u", address, ntohs(sender.sin_port));
  }

  void publish_trajectory(ArmSide side, const std::vector<double> & positions)
  {
    if (positions.size() != kNumArmJoints) {                 // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] decoded %zu positions, expected %zu, not publishing",
        transrecv_udp::to_string(side), positions.size(), kNumArmJoints);
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    const auto & publisher = publishers_[arm_index(side)];
    if (publisher == nullptr) {                              // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] publisher is null, not publishing",
        transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    trajectory_msgs::msg::JointTrajectory message;
    // header.stamp stays zero deliberately: joint_trajectory_controller treats
    // a non-zero stamp as an absolute start time, and with time_from_start=0
    // that start time is also the trajectory's end time. Stamping "now" here
    // means it is already in the past by the time the controller processes
    // it, so the controller rejects it ("trajectory ... ends in the past").
    // Zero is the documented "execute immediately" convention.
    message.joint_names = transrecv_udp::arm_joint_names(side);

    trajectory_msgs::msg::JointTrajectoryPoint point;
    point.positions = positions;
    // time_from_start stays zero: same as the bridge, "go there now".
    message.points.push_back(point);

    publisher->publish(message);
    messages_published_.fetch_add(1, std::memory_order_relaxed);
    ++window_published_;
  }

  // --- controller state ------------------------------------------------------

  /// Runs on the ROS executor, not the receive thread. Only stores the pose;
  /// the send thread is what puts it on the wire, so a slow socket cannot back
  /// up into the subscription.
  void on_controller_state(
    ArmSide side, control_msgs::msg::JointTrajectoryControllerState::ConstSharedPtr msg)
  {
    if (msg == nullptr) {                                    // Rule 3: null guard
      RCLCPP_ERROR(
        get_logger(), "[%s] null controller state, dropping", transrecv_udp::to_string(side));
      return;
    }

    const std::vector<double> & feedback = msg->feedback.positions;
    if (feedback.size() < kNumArmJoints) {                   // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] feedback has %zu positions, need %zu, dropping",
        transrecv_udp::to_string(side), feedback.size(), kNumArmJoints);
      return;
    }

    // The controller may expose more than the arm joints; take the first seven,
    // matching what the bridge reads on this same topic.
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      ArmState & state = arm_states_[arm_index(side)];
      state.positions.assign(feedback.begin(), feedback.begin() + kNumArmJoints);
      state.has_data = true;
    }

    states_received_.fetch_add(1, std::memory_order_relaxed);

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kDataLogThrottleMs,
      "[%s] controller state: [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), feedback[0], feedback[1], feedback[2],
      feedback[3], feedback[4], feedback[5], feedback[6]);
  }

  // --- send thread -----------------------------------------------------------

  /// One tick: push each arm's newest controller state to the client.
  ///
  /// Runs on its own thread so the socket and the ROS callbacks that feed it
  /// cannot block one another. An arm with no state yet is skipped rather than
  /// sent as zeros -- a zero pose is a valid-looking command, and inventing one
  /// is worse than sending nothing.
  void send_states()
  {
    if (socket_fd_ < 0) {                                    // Rule 3: socket sanity
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "send thread: socket is not open, cannot send");
      return;
    }

    for (std::size_t index = 0; index < kNumArms; ++index) {
      const ArmSide side = index == 0 ? ArmSide::kLeft : ArmSide::kRight;

      std::vector<double> positions;
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        const ArmState & state = arm_states_[index];
        if (!state.has_data) {                               // Rule 2: nothing to send yet
          RCLCPP_WARN_THROTTLE(
            get_logger(), *get_clock(), kLogThrottleMs,
            "[%s] no controller state received yet, nothing to send",
            transrecv_udp::to_string(side));
          continue;
        }
        positions = state.positions;
      }

      send_state(side, positions);
    }
  }

  void send_state(ArmSide side, const std::vector<double> & positions)
  {
    if (positions.size() != kNumArmJoints) {                 // Rule 3: size guard
      RCLCPP_ERROR(
        get_logger(), "[%s] held state has %zu positions, expected %zu, not sending",
        transrecv_udp::to_string(side), positions.size(), kNumArmJoints);
      return;
    }

    const transrecv_udp::JointPacket packet = transrecv_udp::make_packet(side, positions);

    const ssize_t sent = sendto(
      socket_fd_, &packet, sizeof(packet), 0,
      reinterpret_cast<const sockaddr *>(&client_), sizeof(client_));

    if (sent < 0) {
      RCLCPP_WARN_THROTTLE(                                  // Rule 2: log the failure path
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] sendto(%s) failed: %s", transrecv_udp::to_string(side),
        client_text_.c_str(), std::strerror(errno));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {   // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] short send: %zd of %zu bytes", transrecv_udp::to_string(side), sent,
        sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    states_sent_.fetch_add(1, std::memory_order_relaxed);
  }

  // --- health thread ---------------------------------------------------------

  /// One watchdog pass. Its own thread on purpose: if the receive thread
  /// stalls, the report has to come from somewhere that did not.
  void check_health()
  {
    if (socket_fd_ < 0) {                                    // Rule 3: socket sanity
      RCLCPP_ERROR(get_logger(), "health: socket fd is invalid, server is dead");
      return;
    }

    const std::uint64_t received = packets_received_.load(std::memory_order_relaxed);
    const std::uint64_t published = messages_published_.load(std::memory_order_relaxed);
    const std::uint64_t dropped = packets_dropped_.load(std::memory_order_relaxed);
    const std::uint64_t pongs = pongs_sent_.load(std::memory_order_relaxed);
    const std::uint64_t buttons_in = buttons_received_.load(std::memory_order_relaxed);
    const std::uint64_t buttons_out = buttons_published_.load(std::memory_order_relaxed);
    const std::uint64_t poses_in = poses_received_.load(std::memory_order_relaxed);
    const std::uint64_t poses_out = poses_published_.load(std::memory_order_relaxed);
    const std::uint64_t hands_in = hands_received_.load(std::memory_order_relaxed);
    const std::uint64_t hands_out = hands_published_.load(std::memory_order_relaxed);

    if (received == 0) {
      // last_receive_time_ is still its zero value here, so its age is
      // meaningless -- report "never" rather than time since the clock epoch.
      // pongs>0 distinguishes "no client at all" from "client is pinging but
      // sending no joint data", which are very different problems.
      RCLCPP_WARN(
        get_logger(),
        "health: no joint data on port %u yet (client pings answered: %lu) "
        "| published=%lu dropped=%lu buttons_received=%lu buttons_published=%lu "
        "poses_received=%lu poses_published=%lu hands_received=%lu hands_published=%lu",
        port_, static_cast<unsigned long>(pongs), static_cast<unsigned long>(published),
        static_cast<unsigned long>(dropped), static_cast<unsigned long>(buttons_in),
        static_cast<unsigned long>(buttons_out), static_cast<unsigned long>(poses_in),
        static_cast<unsigned long>(poses_out), static_cast<unsigned long>(hands_in),
        static_cast<unsigned long>(hands_out));
      return;
    }

    const auto since_receive =
      std::chrono::steady_clock::now() - last_receive_time_.load(std::memory_order_relaxed);

    if (since_receive > kReceiveStaleThreshold) {
      RCLCPP_WARN(
        get_logger(),
        "health: no datagram for %lds (client stalled?) "
        "| received=%lu published=%lu pongs=%lu dropped=%lu buttons_received=%lu "
        "buttons_published=%lu poses_received=%lu poses_published=%lu hands_received=%lu "
        "hands_published=%lu",
        static_cast<long>(
          std::chrono::duration_cast<std::chrono::seconds>(since_receive).count()),
        static_cast<unsigned long>(received), static_cast<unsigned long>(published),
        static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped),
        static_cast<unsigned long>(buttons_in), static_cast<unsigned long>(buttons_out),
        static_cast<unsigned long>(poses_in), static_cast<unsigned long>(poses_out),
        static_cast<unsigned long>(hands_in), static_cast<unsigned long>(hands_out));
      return;
    }

    RCLCPP_INFO(
      get_logger(),
      "health: OK | port=%u received=%lu published=%lu pongs=%lu dropped=%lu "
      "buttons_received=%lu buttons_published=%lu poses_received=%lu poses_published=%lu "
      "hands_received=%lu hands_published=%lu",
      port_, static_cast<unsigned long>(received), static_cast<unsigned long>(published),
      static_cast<unsigned long>(pongs), static_cast<unsigned long>(dropped),
      static_cast<unsigned long>(buttons_in), static_cast<unsigned long>(buttons_out),
      static_cast<unsigned long>(poses_in), static_cast<unsigned long>(poses_out),
      static_cast<unsigned long>(hands_in), static_cast<unsigned long>(hands_out));
  }

  /// The newest controller state for one arm, waiting to be sent.
  struct ArmState
  {
    std::vector<double> positions;
    bool has_data = false;
  };

  const int socket_fd_;
  const std::uint16_t port_;
  const sockaddr_in client_;
  const std::string client_text_;

  // Written by the ROS executor (state callbacks), read by the send thread.
  std::mutex state_mutex_;
  std::array<ArmState, kNumArms> arm_states_;

  // Written by the receive thread, read by the health thread.
  std::atomic<std::uint64_t> packets_received_{0};
  std::atomic<std::uint64_t> messages_published_{0};
  std::atomic<std::uint64_t> packets_dropped_{0};
  std::atomic<std::uint64_t> pongs_sent_{0};
  std::atomic<std::chrono::steady_clock::time_point> last_receive_time_{
    std::chrono::steady_clock::time_point{}};

  // Rate-report window. Touched only by the receive thread, hence no atomics:
  // publish_trajectory() is called from there too, so window_published_ has a
  // single writer like the others.
  std::chrono::steady_clock::time_point window_started_at_{};
  std::uint64_t window_received_ = 0;
  std::uint64_t window_published_ = 0;
  // Trajectories the window's datagrams owed: two per both-arms packet.
  std::uint64_t window_expected_ = 0;

  // Written by the executor thread (controller state callbacks).
  std::atomic<std::uint64_t> states_received_{0};
  // Written by the send thread.
  std::atomic<std::uint64_t> states_sent_{0};
  // Written by the receive thread.
  std::atomic<std::uint64_t> buttons_received_{0};
  std::atomic<std::uint64_t> buttons_published_{0};
  std::atomic<std::uint64_t> poses_received_{0};
  std::atomic<std::uint64_t> poses_published_{0};
  std::atomic<std::uint64_t> hands_received_{0};
  std::atomic<std::uint64_t> hands_published_{0};

  std::array<rclcpp::Publisher<trajectory_msgs::msg::JointTrajectory>::SharedPtr, kNumArms>
  publishers_;

  rclcpp::Publisher<sensor_msgs::msg::Joy>::SharedPtr buttons_pub_;
  rclcpp::Publisher<geometry_msgs::msg::PoseStamped>::SharedPtr left_ee_pose_pub_;
  rclcpp::Publisher<geometry_msgs::msg::PoseStamped>::SharedPtr right_ee_pose_pub_;
  rclcpp::Publisher<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_hand_pub_;
  rclcpp::Publisher<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_hand_pub_;

  rclcpp::Subscription<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    left_state_sub_;
  rclcpp::Subscription<control_msgs::msg::JointTrajectoryControllerState>::SharedPtr
    right_state_sub_;

  std::thread receive_thread_;
  std::atomic<bool> receive_running_{false};

  // Declared last on purpose: members are destroyed in reverse order, so these
  // destructors join their threads before the state those threads read
  // (arm_states_, counters, socket fd) goes away.
  transrecv_udp::PeriodicThread send_thread_{
    kStateSendPeriod, get_logger(), [this]() { send_states(); }, "send thread"};
  transrecv_udp::PeriodicThread health_thread_{
    kHealthCheckPeriod, get_logger(), [this]() { check_health(); }, "health thread"};
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  const rclcpp::Logger logger = rclcpp::get_logger("transrecv_udp_server");

  // rclcpp::init strips ROS args; whatever is left is ours.
  const std::vector<std::string> args = rclcpp::remove_ros_arguments(argc, argv);

  constexpr std::size_t kExpectedArgCount = 4;  // program, bind port, client ip, client port
  if (args.size() != kExpectedArgCount) {                    // Rule 3 + Rule 2
    RCLCPP_ERROR(
      logger, "usage: %s <bind_port> <client_ip> <client_port>   (got %zu arguments)",
      args.empty() ? "server" : args[0].c_str(), args.size() - 1);
    rclcpp::shutdown();
    return 1;
  }

  const std::optional<std::uint16_t> bind_port = parse_port(args[1]);
  if (!bind_port.has_value()) {                              // Rule 3: range guard
    RCLCPP_ERROR(
      logger, "invalid bind port '%s' (expected %u..%u)",
      args[1].c_str(), kMinUserPort, UINT16_MAX);
    rclcpp::shutdown();
    return 1;
  }
  const std::uint16_t port = bind_port.value();

  const std::string client_ip = args[2];
  const std::optional<std::uint16_t> client_port = parse_port(args[3]);
  if (!client_port.has_value()) {                            // Rule 3: range guard
    RCLCPP_ERROR(
      logger, "invalid client port '%s' (expected %u..%u)",
      args[3].c_str(), kMinUserPort, UINT16_MAX);
    rclcpp::shutdown();
    return 1;
  }

  // Resolve before binding: a typo in the address should fail before anything
  // holds a port.
  sockaddr_in client{};
  if (!transrecv_udp::resolve_ipv4(client_ip, client_port.value(), client, logger)) {
    rclcpp::shutdown();
    return 1;
  }
  const std::string client_text = client_ip + ":" + std::to_string(client_port.value());

  // Bind here, before the node exists: a port clash fails fast and loud instead
  // of surfacing halfway through node construction.
  transrecv_udp::UdpSocket server_socket;
  if (!server_socket.bind_any(port, logger)) {               // Rule 3: bind guard
    RCLCPP_ERROR(logger, "cannot start server on port %u, exiting", port);
    rclcpp::shutdown();
    return 1;
  }

  // Without this the receive thread would park in recv() forever and never see
  // the stop flag, so shutdown would hang.
  if (!server_socket.set_receive_timeout(kReceiveTimeout, logger)) {
    RCLCPP_ERROR(logger, "cannot set the receive timeout, exiting");
    rclcpp::shutdown();
    return 1;
  }

  int exit_code = 0;
  try {
    auto server =
      std::make_shared<TrajectoryServer>(server_socket.get(), port, client, client_text);
    server->start();
    rclcpp::spin(server);
    server->stop();                  // join every thread before the socket closes
  } catch (const std::exception & error) {
    RCLCPP_ERROR(logger, "fatal: %s", error.what());
    exit_code = 1;
  }

  rclcpp::shutdown();
  return exit_code;
}
