#ifndef TRANSRECV_UDP__UDP_SOCKET_HPP_
#define TRANSRECV_UDP__UDP_SOCKET_HPP_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>

#include <rclcpp/rclcpp.hpp>

namespace transrecv_udp
{

/// Fills `out` with an IPv4 address, logging and failing on a malformed one so
/// a typo never turns into datagrams quietly going nowhere.
inline bool resolve_ipv4(
  const std::string & address, std::uint16_t port, sockaddr_in & out,
  const rclcpp::Logger & logger)
{
  out = sockaddr_in{};
  out.sin_family = AF_INET;
  out.sin_port = htons(port);

  const int converted = inet_pton(AF_INET, address.c_str(), &out.sin_addr);
  if (converted != 1) {                             // Rule 3: address guard
    RCLCPP_ERROR(
      logger, "'%s' is not a valid IPv4 address (inet_pton returned %d)",
      address.c_str(), converted);
    return false;
  }
  return true;
}

/// Owns an IPv4 UDP socket and closes it exactly once.
///
/// Both ends create theirs in main(), so a bad port or address fails
/// immediately and loudly instead of halfway through node construction.
class UdpSocket
{
public:
  UdpSocket() = default;

  ~UdpSocket()
  {
    reset();
  }

  UdpSocket(const UdpSocket &) = delete;
  UdpSocket & operator=(const UdpSocket &) = delete;

  UdpSocket(UdpSocket && other) noexcept
  : fd_(other.fd_)
  {
    other.fd_ = -1;
  }

  UdpSocket & operator=(UdpSocket && other) noexcept
  {
    if (this != &other) {
      reset();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  /// Server side: bind INADDR_ANY:port so a client on any local interface is
  /// accepted and only the port has to be chosen.
  ///
  /// The socket stays blocking: the receive thread parks in recv() instead of
  /// spinning, which is the whole reason it is a thread and not a timer. The
  /// timeout below is what still lets it notice a stop request.
  bool bind_any(std::uint16_t port, const rclcpp::Logger & logger)
  {
    if (!open(logger)) {
      return false;
    }

    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons(port);

    if (bind(fd_, reinterpret_cast<const sockaddr *>(&local), sizeof(local)) < 0) {
      RCLCPP_ERROR(                                 // Rule 2: log before failing
        logger, "bind() to 0.0.0.0:%u failed: %s", port, std::strerror(errno));
      reset();
      return false;
    }

    RCLCPP_INFO(logger, "UDP socket bound to 0.0.0.0:%u", port);
    return true;
  }

  /// Caps how long a blocking recv() parks, so the receive thread wakes up
  /// often enough to notice it has been asked to stop.
  bool set_receive_timeout(std::chrono::milliseconds timeout, const rclcpp::Logger & logger)
  {
    if (fd_ < 0) {                                  // Rule 3: use-before-open
      RCLCPP_ERROR(logger, "cannot set receive timeout, socket is not open");
      return false;
    }

    if (timeout <= std::chrono::milliseconds::zero()) {   // Rule 3: range guard
      RCLCPP_ERROR(
        logger, "receive timeout %ldms is not positive, refusing to set it",
        static_cast<long>(timeout.count()));
      return false;
    }

    timeval value{};
    value.tv_sec = timeout.count() / 1000;
    value.tv_usec = (timeout.count() % 1000) * 1000;

    if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value)) < 0) {
      RCLCPP_ERROR(logger, "setsockopt(SO_RCVTIMEO) failed: %s", std::strerror(errno));
      return false;
    }
    return true;
  }

  /// Client side: open a socket and resolve the server address into `peer`.
  /// Nothing is contacted yet -- that is connect_to()'s job.
  ///
  /// Blocking, like the server's: the client also has a thread that parks in
  /// recv() waiting for pongs, and set_receive_timeout() is what lets it notice
  /// a stop request.
  bool open_to(
    const std::string & address, std::uint16_t port, sockaddr_in & peer,
    const rclcpp::Logger & logger)
  {
    if (!open(logger)) {
      return false;
    }

    peer = sockaddr_in{};
    peer.sin_family = AF_INET;
    peer.sin_port = htons(port);

    const int converted = inet_pton(AF_INET, address.c_str(), &peer.sin_addr);
    if (converted != 1) {                           // Rule 3: address guard
      RCLCPP_ERROR(
        logger, "'%s' is not a valid IPv4 address (inet_pton returned %d)",
        address.c_str(), converted);
      reset();
      return false;
    }

    RCLCPP_INFO(logger, "UDP socket ready, server is %s:%u", address.c_str(), port);
    return true;
  }

  /// Fixes the default peer so send() can be used and, more importantly, so the
  /// kernel reports an ICMP port-unreachable back to us as ECONNREFUSED. That
  /// error is the only "is the server there?" signal UDP offers.
  ///
  /// It is not a handshake: connect() on a datagram socket sends nothing, and
  /// on a network that filters ICMP no error ever comes back. "Connected" here
  /// means "no error so far", not "the server answered".
  bool connect_to(const sockaddr_in & peer, const rclcpp::Logger & logger)
  {
    if (fd_ < 0) {                                  // Rule 3: use-before-open
      RCLCPP_ERROR(logger, "cannot connect, socket is not open");
      return false;
    }

    if (connect(fd_, reinterpret_cast<const sockaddr *>(&peer), sizeof(peer)) < 0) {
      RCLCPP_WARN(logger, "connect() failed: %s", std::strerror(errno));
      return false;
    }
    return true;
  }

  int get() const
  {
    return fd_;
  }

  bool valid() const
  {
    return fd_ >= 0;
  }

private:
  bool open(const rclcpp::Logger & logger, bool non_blocking = false)
  {
    reset();

    const int type = SOCK_DGRAM | (non_blocking ? SOCK_NONBLOCK : 0);
    fd_ = socket(AF_INET, type, 0);
    if (fd_ < 0) {                                  // Rule 3: syscall guard
      RCLCPP_ERROR(logger, "socket() failed: %s", std::strerror(errno));
      return false;
    }
    return true;
  }

  void reset()
  {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

  int fd_ = -1;
};

}  // namespace transrecv_udp

#endif  // TRANSRECV_UDP__UDP_SOCKET_HPP_
