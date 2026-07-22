#ifndef TRANSRECV_UDP__PROTOCOL_HPP_
#define TRANSRECV_UDP__PROTOCOL_HPP_

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace transrecv_udp
{

/// Wire protocol for the one-way UDP link.
///
///   client: reads measured joint state -> sends here
///   server: receives -> republishes as a JointTrajectory command
///
/// Joint data only ever flows client -> server. The one thing that travels the
/// other way is a liveness reply: the client pings, the server pongs, and the
/// client treats itself as connected only while pongs keep coming back.
///
/// That reply is what makes the check real. UDP's connect() sends nothing and
/// cannot fail, and the ICMP port-unreachable it would otherwise rely on is
/// routinely filtered, so "no error yet" is not evidence anybody is listening.
/// An answer from the far end is.
///
/// The two packet types have different fixed sizes, so a datagram is classified
/// by length before anything is decoded, and a short or oversized one is
/// rejected on size alone.

// Each arm is a 7-DOF OpenArm. Single source of truth for every buffer size.
constexpr std::size_t kNumArmJoints = 7;

// Marks our datagrams so a stray packet on the port is never decoded as a pose.
constexpr std::uint32_t kPacketMagic = 0x4F41524Du;  // "OARM"

/// Which arm a packet refers to. Sent as a uint32 in network byte order.
enum class ArmSide : std::uint32_t
{
  kLeft = 0,
  kRight = 1,
};

inline const char * to_string(ArmSide side)
{
  return side == ArmSide::kLeft ? "left" : "right";
}

/// The datagram layout.
///
/// `magic` and `arm` are network byte order; `positions` are raw IEEE-754
/// doubles in host order, so this assumes both ends are little-endian (x86 /
/// aarch64 here). Fixed size, no padding: the static_assert locks it.
#pragma pack(push, 1)
struct JointPacket
{
  std::uint32_t magic;
  std::uint32_t arm;
  double positions[kNumArmJoints];
};
#pragma pack(pop)

constexpr std::size_t kJointPacketBytes =
  sizeof(std::uint32_t) * 2 + sizeof(double) * kNumArmJoints;
static_assert(
  sizeof(JointPacket) == kJointPacketBytes,
  "JointPacket must be tightly packed for the wire format");

/// Fills a packet ready to send. `positions` must hold exactly kNumArmJoints
/// entries; the caller checks that (and logs) before calling.
inline JointPacket make_packet(ArmSide arm, const std::vector<double> & positions)
{
  JointPacket packet{};
  packet.magic = htonl(kPacketMagic);
  packet.arm = htonl(static_cast<std::uint32_t>(arm));

  if (positions.size() == kNumArmJoints) {          // Rule 3: never copy a wrong size
    std::memcpy(packet.positions, positions.data(), sizeof(packet.positions));
  }
  return packet;
}

/// The liveness exchange. Deliberately a different size from JointPacket so the
/// receiver can tell the two apart by length alone.
enum class ControlType : std::uint32_t
{
  kPing = 0,   ///< client -> server, "are you there?"
  kPong = 1,   ///< server -> client, "yes"
};

inline const char * to_string(ControlType type)
{
  return type == ControlType::kPing ? "ping" : "pong";
}

#pragma pack(push, 1)
struct ControlPacket
{
  std::uint32_t magic;
  std::uint32_t type;
};
#pragma pack(pop)

constexpr std::size_t kControlPacketBytes = sizeof(std::uint32_t) * 2;
static_assert(
  sizeof(ControlPacket) == kControlPacketBytes,
  "ControlPacket must be tightly packed for the wire format");
static_assert(
  sizeof(ControlPacket) != sizeof(JointPacket),
  "the two packet types must differ in size, that is how they are told apart");

inline ControlPacket make_control_packet(ControlType type)
{
  ControlPacket packet{};
  packet.magic = htonl(kPacketMagic);
  packet.type = htonl(static_cast<std::uint32_t>(type));
  return packet;
}

/// Validates and decodes a control datagram. Returns false (with `reason` for
/// the caller to log) on wrong size, bad magic or unknown type.
inline bool decode_control_packet(
  const void * data, std::size_t length, ControlType & out, std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(ControlPacket)) {            // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(ControlPacket));
    return false;
  }

  ControlPacket packet{};
  std::memcpy(&packet, data, sizeof(packet));

  if (ntohl(packet.magic) != kPacketMagic) {        // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::uint32_t type = ntohl(packet.type);
  if (type > static_cast<std::uint32_t>(ControlType::kPong)) {   // Rule 3: enum range
    reason = "unknown control type " + std::to_string(type);
    return false;
  }

  out = static_cast<ControlType>(type);
  return true;
}

/// Result of decoding a received joint datagram.
struct DecodedPacket
{
  ArmSide arm = ArmSide::kLeft;
  std::vector<double> positions;
};

/// Validates and decodes a datagram. Returns false (with `reason` filled in for
/// the caller to log) on any malformed input -- wrong size, bad magic, unknown
/// arm. Never partially fills `out`.
inline bool decode_packet(
  const void * data, std::size_t length, DecodedPacket & out, std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(JointPacket)) {              // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(JointPacket));
    return false;
  }

  JointPacket packet{};
  std::memcpy(&packet, data, sizeof(packet));

  const std::uint32_t magic = ntohl(packet.magic);
  if (magic != kPacketMagic) {                      // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::uint32_t arm = ntohl(packet.arm);
  if (arm > static_cast<std::uint32_t>(ArmSide::kRight)) {   // Rule 3: enum range
    reason = "unknown arm " + std::to_string(arm);
    return false;
  }

  out.arm = static_cast<ArmSide>(arm);
  out.positions.assign(packet.positions, packet.positions + kNumArmJoints);
  return true;
}

// Joint names used by the dora bridge (dora_ros2_bridge/main.py), so a
// trajectory published by the server addresses the same joints.
inline std::vector<std::string> arm_joint_names(ArmSide side)
{
  const std::string prefix = side == ArmSide::kLeft ? "openarm_left_joint" : "openarm_right_joint";
  std::vector<std::string> names;
  names.reserve(kNumArmJoints);
  for (std::size_t i = 0; i < kNumArmJoints; ++i) {
    names.push_back(prefix + std::to_string(i + 1));
  }
  return names;
}

}  // namespace transrecv_udp

#endif  // TRANSRECV_UDP__PROTOCOL_HPP_
