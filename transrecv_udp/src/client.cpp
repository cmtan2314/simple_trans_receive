#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

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

constexpr std::size_t kStateQueueDepth = 10;

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
        on_joint_trajectory(ArmSide::kLeft, msg);
      });
    right_sub_ = create_subscription<trajectory_msgs::msg::JointTrajectory>(
      kRightArmCommandTopic, qos,
      [this](trajectory_msgs::msg::JointTrajectory::ConstSharedPtr msg) {
        on_joint_trajectory(ArmSide::kRight, msg);
      });

    RCLCPP_INFO(
      get_logger(), "forwarding '%s' and '%s' to %s",
      kLeftArmCommandTopic, kRightArmCommandTopic, server_text_.c_str());
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
    if (length != sizeof(transrecv_udp::ControlPacket)) {    // Rule 3: size guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "ignoring a %zu byte datagram, the client only expects control packets", length);
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

    if (!send_packet(side, positions)) {
      return;
    }

    RCLCPP_INFO_THROTTLE(
      get_logger(), *get_clock(), kLogThrottleMs,
      "[%s] sent: [%.3f %.3f %.3f %.3f %.3f %.3f %.3f]",
      transrecv_udp::to_string(side), positions[0], positions[1], positions[2],
      positions[3], positions[4], positions[5], positions[6]);
  }

  /// Single send path, so every failure is counted and logged one way.
  bool send_packet(ArmSide side, const std::vector<double> & positions)
  {
    if (!socket_.valid()) {                                  // Rule 3: use-before-init
      RCLCPP_ERROR_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] socket not open, cannot send", transrecv_udp::to_string(side));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (!connected_.load(std::memory_order_relaxed)) {       // Rule 2: waiting on the retry
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] not connected to %s, dropping state",
        transrecv_udp::to_string(side), server_text_.c_str());
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const transrecv_udp::JointPacket packet = transrecv_udp::make_packet(side, positions);

    // send(), not sendto(): the peer is fixed by connect(), which is also what
    // makes ECONNREFUSED reach us here.
    const ssize_t sent = send(socket_.get(), &packet, sizeof(packet), 0);

    if (sent < 0) {
      if (errno == ECONNREFUSED || errno == ENETUNREACH || errno == EHOSTUNREACH) {
        mark_disconnected(std::strerror(errno));             // hand it to the retry thread
      } else {
        RCLCPP_WARN_THROTTLE(                                // Rule 2: log the failure path
          get_logger(), *get_clock(), kLogThrottleMs,
          "[%s] send() failed: %s", transrecv_udp::to_string(side), std::strerror(errno));
      }
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    if (static_cast<std::size_t>(sent) != sizeof(packet)) {   // Rule 3: short-write guard
      RCLCPP_WARN_THROTTLE(
        get_logger(), *get_clock(), kLogThrottleMs,
        "[%s] short send: %zd of %zu bytes", transrecv_udp::to_string(side), sent,
        sizeof(packet));
      packets_dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    packets_sent_.fetch_add(1, std::memory_order_relaxed);
    last_send_time_.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
    return true;
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

  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr left_sub_;
  rclcpp::Subscription<trajectory_msgs::msg::JointTrajectory>::SharedPtr right_sub_;

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

  constexpr std::size_t kExpectedArgCount = 3;  // program, ip, port
  if (args.size() != kExpectedArgCount) {                    // Rule 3 + Rule 2
    RCLCPP_ERROR(
      logger, "usage: %s <server_ip> <server_port>   (got %zu arguments)",
      args.empty() ? "client" : args[0].c_str(), args.size() - 1);
    rclcpp::shutdown();
    return 1;
  }

  const std::string server_ip = args[1];
  const std::optional<std::uint16_t> server_port = parse_port(args[2]);
  if (!server_port.has_value()) {                            // Rule 3: range guard
    RCLCPP_ERROR(
      logger, "invalid port '%s' (expected %u..%u)",
      args[2].c_str(), kMinUserPort, UINT16_MAX);
    rclcpp::shutdown();
    return 1;
  }

  // Open here, before the node exists: a bad address fails fast and loud. The
  // server does not have to be up yet -- the connection thread keeps pinging.
  sockaddr_in server{};
  transrecv_udp::UdpSocket client_socket;
  if (!client_socket.open_to(server_ip, server_port.value(), server, logger)) {
    RCLCPP_ERROR(
      logger, "cannot open a socket for %s:%u, exiting",
      server_ip.c_str(), server_port.value());
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
