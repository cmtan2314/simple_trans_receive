#ifndef TRANSRECV_UDP__PROTOCOL_HPP_
#define TRANSRECV_UDP__PROTOCOL_HPP_

#include <arpa/inet.h>

#include <array>
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
/// Every packet type has a different fixed size, so a datagram is classified by
/// length before anything is decoded, and a wrong-sized one is rejected on size
/// alone: ControlPacket 8, ButtonPacket 12, PosePacket 36, HandJointPacket 56,
/// JointPacket 64, DualJointPacket 116. The static_asserts below enforce that
/// they stay distinct.

// Each arm is a 7-DOF OpenArm. Single source of truth for every buffer size.
constexpr std::size_t kNumArmJoints = 7;

/// One arm's joint positions. Fixed size and trivially copyable, so it can sit
/// in a queue or be cached without allocating.
using ArmPositions = std::array<double, kNumArmJoints>;

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

// --- Both arms in one datagram ----------------------------------------------
// The client sends arm commands as this, not as two JointPackets.
//
// Two separate datagrams for one instant of a two-armed robot can be reordered,
// spaced apart, or have exactly one of them lost -- and the far end has no way
// to tell that it is acting on a half-updated pose. One datagram carrying both
// sides makes that impossible to express: the arms either both move or neither
// does. It also halves the packet rate.
//
// There is no `arm` field, because a packet is never about one arm.
#pragma pack(push, 1)
struct DualJointPacket
{
  std::uint32_t magic;
  double left[kNumArmJoints];
  double right[kNumArmJoints];
};
#pragma pack(pop)

constexpr std::size_t kDualJointPacketBytes =
  sizeof(std::uint32_t) + sizeof(double) * kNumArmJoints * 2;
static_assert(
  sizeof(DualJointPacket) == kDualJointPacketBytes,
  "DualJointPacket must be tightly packed for the wire format");
static_assert(
  sizeof(DualJointPacket) != sizeof(JointPacket),
  "the two packet types must differ in size, that is how they are told apart");

/// Fills a packet with both arms at once. Neither side is optional: the sender
/// repeats its last known value for an arm that has nothing new, so that the
/// packet always describes the whole robot.
inline DualJointPacket make_dual_packet(const ArmPositions & left, const ArmPositions & right)
{
  DualJointPacket packet{};
  packet.magic = htonl(kPacketMagic);
  std::memcpy(packet.left, left.data(), sizeof(packet.left));
  std::memcpy(packet.right, right.data(), sizeof(packet.right));
  return packet;
}

/// Validates and decodes a both-arms datagram. Returns false (with `reason` for
/// the caller to log) on wrong size or bad magic. Never partially fills `out`.
inline bool decode_dual_packet(
  const void * data, std::size_t length, ArmPositions & left_out, ArmPositions & right_out,
  std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(DualJointPacket)) {          // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(DualJointPacket));
    return false;
  }

  DualJointPacket packet{};
  std::memcpy(&packet, data, sizeof(packet));

  if (ntohl(packet.magic) != kPacketMagic) {        // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  std::memcpy(left_out.data(), packet.left, sizeof(packet.left));
  std::memcpy(right_out.data(), packet.right, sizeof(packet.right));
  return true;
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

// --- End-effector pose -------------------------------------------------------
// FK-derived pose for one arm, forwarded from ee_pose_{left,right}
// (dora_ros2_bridge/main.py) to /left_ee_pose or /right_ee_pose.
constexpr std::size_t kPoseValues = 7;  // px, py, pz, qw, qx, qy, qz

#pragma pack(push, 1)
struct PosePacket
{
  std::uint32_t magic;
  std::uint32_t arm;
  // float, not double: the source (extract_pose() in main.py) is float32-only,
  // so double would add wire bytes with no real precision. It also keeps this
  // size distinct from JointPacket's, which otherwise carries the same 7
  // numbers per arm side and would collide with it.
  float values[kPoseValues];
};
#pragma pack(pop)

constexpr std::size_t kPosePacketBytes =
  sizeof(std::uint32_t) * 2 + sizeof(float) * kPoseValues;
static_assert(
  sizeof(PosePacket) == kPosePacketBytes,
  "PosePacket must be tightly packed for the wire format");
static_assert(
  sizeof(PosePacket) != sizeof(ControlPacket) &&
  sizeof(PosePacket) != sizeof(ButtonPacket) &&
  sizeof(PosePacket) != sizeof(JointPacket),
  "every packet type must have a distinct size, that is how they are told apart");

/// Fills a pose packet. `values` must hold exactly kPoseValues entries
/// (px,py,pz,qw,qx,qy,qz); the caller checks that (and logs) before calling.
inline PosePacket make_pose_packet(ArmSide arm, const std::vector<double> & values)
{
  PosePacket packet{};
  packet.magic = htonl(kPacketMagic);
  packet.arm = htonl(static_cast<std::uint32_t>(arm));

  if (values.size() == kPoseValues) {               // Rule 3: never copy a wrong size
    for (std::size_t i = 0; i < kPoseValues; ++i) {
      packet.values[i] = static_cast<float>(values[i]);
    }
  }
  return packet;
}

/// Validates and decodes a pose datagram. Returns false (with `reason` for the
/// caller to log) on wrong size, bad magic or unknown arm.
inline bool decode_pose_packet(
  const void * data, std::size_t length, ArmSide & arm_out, std::vector<double> & out,
  std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(PosePacket)) {               // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(PosePacket));
    return false;
  }

  PosePacket packet{};
  std::memcpy(&packet, data, sizeof(packet));

  if (ntohl(packet.magic) != kPacketMagic) {        // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::uint32_t arm = ntohl(packet.arm);
  if (arm > static_cast<std::uint32_t>(ArmSide::kRight)) {   // Rule 3: enum range
    reason = "unknown arm " + std::to_string(arm);
    return false;
  }

  arm_out = static_cast<ArmSide>(arm);
  out.assign(packet.values, packet.values + kPoseValues);
  return true;
}

// --- Revo2 hand (gripper) joint trajectory -----------------------------------
// 6 driven joints per hand (thumb metacarpal/proximal + 4 finger proximals),
// forwarded from {left,right}_revo2_hand_controller/joint_trajectory
// (dora_ros2_bridge/main.py) to the same-named topic on the server.
constexpr std::size_t kNumHandJoints = 6;

#pragma pack(push, 1)
struct HandJointPacket
{
  std::uint32_t magic;
  std::uint32_t arm;
  double positions[kNumHandJoints];
};
#pragma pack(pop)

constexpr std::size_t kHandJointPacketBytes =
  sizeof(std::uint32_t) * 2 + sizeof(double) * kNumHandJoints;
static_assert(
  sizeof(HandJointPacket) == kHandJointPacketBytes,
  "HandJointPacket must be tightly packed for the wire format");
static_assert(
  sizeof(HandJointPacket) != sizeof(ControlPacket) &&
  sizeof(HandJointPacket) != sizeof(ButtonPacket) &&
  sizeof(HandJointPacket) != sizeof(PosePacket) &&
  sizeof(HandJointPacket) != sizeof(JointPacket),
  "every packet type must have a distinct size, that is how they are told apart");

/// Fills a hand packet. `positions` must hold exactly kNumHandJoints entries;
/// the caller checks that (and logs) before calling.
inline HandJointPacket make_hand_packet(ArmSide arm, const std::vector<double> & positions)
{
  HandJointPacket packet{};
  packet.magic = htonl(kPacketMagic);
  packet.arm = htonl(static_cast<std::uint32_t>(arm));

  if (positions.size() == kNumHandJoints) {         // Rule 3: never copy a wrong size
    std::memcpy(packet.positions, positions.data(), sizeof(packet.positions));
  }
  return packet;
}

/// Validates and decodes a hand datagram. Returns false (with `reason` for the
/// caller to log) on wrong size, bad magic or unknown arm.
inline bool decode_hand_packet(
  const void * data, std::size_t length, ArmSide & arm_out, std::vector<double> & out,
  std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length != sizeof(HandJointPacket)) {          // Rule 3: size guard
    reason = "size " + std::to_string(length) + " != " + std::to_string(sizeof(HandJointPacket));
    return false;
  }

  HandJointPacket packet{};
  std::memcpy(&packet, data, sizeof(packet));

  if (ntohl(packet.magic) != kPacketMagic) {        // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::uint32_t arm = ntohl(packet.arm);
  if (arm > static_cast<std::uint32_t>(ArmSide::kRight)) {   // Rule 3: enum range
    reason = "unknown arm " + std::to_string(arm);
    return false;
  }

  arm_out = static_cast<ArmSide>(arm);
  out.assign(packet.positions, packet.positions + kNumHandJoints);
  return true;
}

// Joint names for the Revo2 hand, matching HAND_FINGERS/NAMES_L_HAND/
// NAMES_R_HAND in dora_ros2_bridge/main.py.
inline std::vector<std::string> hand_joint_names(ArmSide side)
{
  static const char * const kFingers[kNumHandJoints] = {
    "thumb_metacarpal", "thumb_proximal", "index_proximal",
    "middle_proximal", "ring_proximal", "pinky_proximal"};

  const std::string prefix = side == ArmSide::kLeft ? "left_" : "right_";
  std::vector<std::string> names;
  names.reserve(kNumHandJoints);
  for (const char * finger : kFingers) {
    names.push_back(prefix + finger + "_joint");
  }
  return names;
}

// --- Receive buffer ----------------------------------------------------------
// DualJointPacket is declared before the types below it, so its distinctness
// from them can only be checked here, once every type exists.
static_assert(
  sizeof(DualJointPacket) != sizeof(ControlPacket) &&
  sizeof(DualJointPacket) != sizeof(ButtonPacket) &&
  sizeof(DualJointPacket) != sizeof(PosePacket) &&
  sizeof(DualJointPacket) != sizeof(HandJointPacket),
  "every packet type must have a distinct size, that is how they are told apart");

constexpr std::size_t kMaxPacketBytes = sizeof(DualJointPacket);
static_assert(
  kMaxPacketBytes >= sizeof(JointPacket) &&
  kMaxPacketBytes >= sizeof(HandJointPacket) &&
  kMaxPacketBytes >= sizeof(PosePacket) &&
  kMaxPacketBytes >= sizeof(ButtonPacket) &&
  kMaxPacketBytes >= sizeof(ControlPacket),
  "kMaxPacketBytes must name the largest packet type, or recv() would truncate it");

/// Size for any recv() buffer on this protocol.
///
/// One byte larger than the largest packet on purpose: recv() silently
/// truncates a datagram to the buffer, so a buffer of exactly kMaxPacketBytes
/// would hand an oversized datagram back at exactly the length of a valid
/// packet, and it would be decoded as one. The extra byte makes an oversized
/// datagram come back at a length no type has, so it is rejected on size alone.
constexpr std::size_t kReceiveBufferBytes = kMaxPacketBytes + 1;

}  // namespace transrecv_udp

#endif  // TRANSRECV_UDP__PROTOCOL_HPP_
