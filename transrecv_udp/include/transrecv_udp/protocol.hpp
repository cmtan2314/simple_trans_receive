#ifndef TRANSRECV_UDP__PROTOCOL_HPP_
#define TRANSRECV_UDP__PROTOCOL_HPP_

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

namespace transrecv_udp
{

/// Wire protocol for the UDP link.
///
///   client -> server: arm + hand trajectory commands, VR buttons, pings
///   server -> client: arm + hand controller state, pongs
///
/// The client pings, the server pongs, and the client treats itself as
/// connected only while pongs keep coming back.
///
/// That reply is what makes the check real. UDP's connect() sends nothing and
/// cannot fail, and the ICMP port-unreachable it would otherwise rely on is
/// routinely filtered, so "no error yet" is not evidence anybody is listening.
/// An answer from the far end is.
///
/// Every packet type has a different fixed size, so a datagram is classified by
/// length before anything is decoded, and a wrong-sized one is rejected on size
/// alone: ControlPacket 8, ButtonPacket 12, HandPacket 56, JointPacket 64. The
/// static_asserts below enforce that they stay distinct.

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

namespace detail
{
/// Number of joints a sided packet type carries, read off its array so the
/// count is never restated by hand.
template<typename Packet>
constexpr std::size_t joint_count_of()
{
  return std::extent<decltype(Packet::positions)>::value;
}

/// Shared by every "side + positions" packet (arm and hand). `positions` must
/// hold exactly joint_count_of<Packet>() entries; the caller checks that (and
/// logs) before calling.
template<typename Packet>
inline Packet make_sided_packet(ArmSide side, const std::vector<double> & positions)
{
  Packet packet{};
  packet.magic = htonl(kPacketMagic);
  packet.arm = htonl(static_cast<std::uint32_t>(side));

  if (positions.size() == joint_count_of<Packet>()) {   // Rule 3: never copy a wrong size
    std::memcpy(packet.positions, positions.data(), sizeof(packet.positions));
  }
  return packet;
}
}  // namespace detail

/// Fills a packet ready to send. `positions` must hold exactly kNumArmJoints
/// entries; the caller checks that (and logs) before calling.
inline JointPacket make_packet(ArmSide arm, const std::vector<double> & positions)
{
  return detail::make_sided_packet<JointPacket>(arm, positions);
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

// --- VR buttons -------------------------------------------------------------
// Room for the four face buttons the VR receiver knows about (a, b, x, y),
// even though only a and b are wired up today -- a fixed-size packet cannot be
// grown later without breaking both ends, so the space is reserved up front.
constexpr std::size_t kMaxButtons = 4;

/// Button state, sent only when it changes.
///
/// `count` says how many entries of `buttons` are meaningful; the rest are
/// zero. Each entry is 0 or 1, matching sensor_msgs/Joy's `buttons` array
/// (which is int32, but a button is a flag, so one byte carries it).
#pragma pack(push, 1)
struct ButtonPacket
{
  std::uint32_t magic;
  std::uint32_t count;
  std::uint8_t buttons[kMaxButtons];
};
#pragma pack(pop)

constexpr std::size_t kButtonPacketBytes =
  sizeof(std::uint32_t) * 2 + sizeof(std::uint8_t) * kMaxButtons;
static_assert(
  sizeof(ButtonPacket) == kButtonPacketBytes,
  "ButtonPacket must be tightly packed for the wire format");
static_assert(
  sizeof(ButtonPacket) != sizeof(JointPacket) &&
  sizeof(ButtonPacket) != sizeof(ControlPacket),
  "every packet type must have a distinct size, that is how they are told apart");

/// Packs button flags. Anything past kMaxButtons is dropped -- the caller
/// checks and logs that before calling.
inline ButtonPacket make_button_packet(const std::vector<std::uint8_t> & buttons)
{
  ButtonPacket packet{};
  packet.magic = htonl(kPacketMagic);

  const std::size_t count = buttons.size() < kMaxButtons ? buttons.size() : kMaxButtons;
  packet.count = htonl(static_cast<std::uint32_t>(count));
  for (std::size_t i = 0; i < count; ++i) {
    // Normalise to 0/1 so a stray value can never reach the far end as-is.
    packet.buttons[i] = buttons[i] != 0 ? 1 : 0;
  }
  return packet;
}

/// Validates and decodes a button datagram. Returns false (with `reason` for
/// the caller to log) on wrong size, bad magic or an impossible count.
inline bool decode_button_packet(
  const void * data, std::size_t length, std::vector<std::uint8_t> & out, std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(ButtonPacket)) {             // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(ButtonPacket));
    return false;
  }

  ButtonPacket packet{};
  std::memcpy(&packet, data, sizeof(packet));

  if (ntohl(packet.magic) != kPacketMagic) {        // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::uint32_t count = ntohl(packet.count);
  if (count > kMaxButtons) {                        // Rule 3: range guard
    reason = "count " + std::to_string(count) + " > " + std::to_string(kMaxButtons);
    return false;
  }

  out.assign(packet.buttons, packet.buttons + count);
  return true;
}

/// Result of decoding a received joint datagram.
struct DecodedPacket
{
  ArmSide arm = ArmSide::kLeft;
  std::vector<double> positions;
};

namespace detail
{
/// Validates and decodes any "side + positions" datagram. Returns false (with
/// `reason` filled in for the caller to log) on any malformed input -- wrong
/// size, bad magic, unknown side. Never partially fills `out`.
template<typename Packet>
inline bool decode_sided_packet(
  const void * data, std::size_t length, DecodedPacket & out, std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(Packet)) {                   // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(Packet));
    return false;
  }

  Packet packet{};
  std::memcpy(&packet, data, sizeof(packet));

  const std::uint32_t magic = ntohl(packet.magic);
  if (magic != kPacketMagic) {                      // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::uint32_t arm = ntohl(packet.arm);
  if (arm > static_cast<std::uint32_t>(ArmSide::kRight)) {   // Rule 3: enum range
    reason = "unknown side " + std::to_string(arm);
    return false;
  }

  out.arm = static_cast<ArmSide>(arm);
  out.positions.assign(packet.positions, packet.positions + joint_count_of<Packet>());
  return true;
}
}  // namespace detail

/// Validates and decodes an arm datagram. See detail::decode_sided_packet.
inline bool decode_packet(
  const void * data, std::size_t length, DecodedPacket & out, std::string & reason)
{
  return detail::decode_sided_packet<JointPacket>(data, length, out, reason);
}

// --- Revo2 hands ------------------------------------------------------------
// Each <side>_revo2_hand_controller drives six joints: thumb metacarpal, thumb
// proximal, then index/middle/ring/pinky proximal. The distal joints are mimic
// joints and are not part of the controller, so they never travel.
constexpr std::size_t kNumHandJoints = 6;

/// Same layout as JointPacket, six positions instead of seven. `arm` carries
/// the hand's side as an ArmSide value (the name is shared so both packet
/// types go through the same encode/decode template).
#pragma pack(push, 1)
struct HandPacket
{
  std::uint32_t magic;
  std::uint32_t arm;
  double positions[kNumHandJoints];
};
#pragma pack(pop)

constexpr std::size_t kHandPacketBytes =
  sizeof(std::uint32_t) * 2 + sizeof(double) * kNumHandJoints;
static_assert(
  sizeof(HandPacket) == kHandPacketBytes,
  "HandPacket must be tightly packed for the wire format");
static_assert(
  sizeof(HandPacket) != sizeof(JointPacket) &&
  sizeof(HandPacket) != sizeof(ControlPacket) &&
  sizeof(HandPacket) != sizeof(ButtonPacket),
  "every packet type must have a distinct size, that is how they are told apart");

/// Fills a hand packet ready to send. `positions` must hold exactly
/// kNumHandJoints entries; the caller checks that (and logs) before calling.
inline HandPacket make_hand_packet(ArmSide side, const std::vector<double> & positions)
{
  return detail::make_sided_packet<HandPacket>(side, positions);
}

/// Validates and decodes a hand datagram. See detail::decode_sided_packet.
inline bool decode_hand_packet(
  const void * data, std::size_t length, DecodedPacket & out, std::string & reason)
{
  return detail::decode_sided_packet<HandPacket>(data, length, out, reason);
}

/// Joint names of <side>_revo2_hand_controller, in controller order
/// (brainco_hand_ros2 revo2_<side>_controllers.yaml).
inline std::vector<std::string> hand_joint_names(ArmSide side)
{
  static constexpr const char * kFingerJoints[kNumHandJoints] = {
    "thumb_metacarpal_joint", "thumb_proximal_joint", "index_proximal_joint",
    "middle_proximal_joint", "ring_proximal_joint", "pinky_proximal_joint"};

  const std::string prefix = side == ArmSide::kLeft ? "left_" : "right_";
  std::vector<std::string> names;
  names.reserve(kNumHandJoints);
  for (const char * joint : kFingerJoints) {
    names.push_back(prefix + joint);
  }
  return names;
}

/// "[0.000 0.123 ...]" for log lines, whatever the joint count.
inline std::string describe_positions(const std::vector<double> & positions)
{
  constexpr std::size_t kValueChars = 16;           // "-123.456 " with headroom
  std::string text = "[";
  text.reserve(positions.size() * kValueChars);
  char value[kValueChars] = {};
  for (std::size_t i = 0; i < positions.size(); ++i) {
    std::snprintf(value, sizeof(value), i == 0 ? "%.3f" : " %.3f", positions[i]);
    text += value;
  }
  return text + "]";
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
