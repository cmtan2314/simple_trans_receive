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
/// A datagram is classified by length before anything is decoded, and one whose
/// length matches no type is rejected on size alone: ControlPacket 8,
/// ButtonPacket 12, JointPacket 64. The static_asserts below enforce that those
/// stay distinct.
///
/// JointBatchPacket is the one variable-length type: 8 + 60n bytes for n samples,
/// n in 1..kMaxSamplesPerDatagram. That series never lands on 8, 12 or 64, so
/// length still identifies a datagram unambiguously -- see is_joint_batch_size().

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

// --- Batched joint stream (client -> server) --------------------------------
// One callback used to be one datagram, which puts a send() syscall on the ROS
// executor thread for every sample the controllers produce. Samples are
// collected into a vector instead, and one tick of a dedicated thread puts the
// whole vector on the wire.
//
// A batch is deliberately capped well below the Ethernet MTU. Handing IP a
// 30 kB datagram (the 500 samples the client will buffer) means ~21 fragments,
// and UDP loses the entire datagram if any single fragment goes missing -- so
// fragmenting multiplies the loss rate by the fragment count. At 1% link loss a
// 21-fragment batch is lost 19% of the time. 24 samples is 1448 bytes, one
// unfragmented packet, so a loss costs 24 samples and nothing more.
constexpr std::size_t kMaxSamplesPerDatagram = 24;

/// One arm pose in flight. std::array, not std::vector: a batch holds hundreds
/// of these and a heap allocation per sample is exactly the cost being removed.
struct ArmSample
{
  ArmSide arm = ArmSide::kLeft;
  std::array<double, kNumArmJoints> positions{};
};

/// Wire layout of one sample. `arm` is network byte order; `positions` are raw
/// doubles in host order, the same little-endian assumption JointPacket makes.
#pragma pack(push, 1)
struct JointSample
{
  std::uint32_t arm;
  double positions[kNumArmJoints];
};

struct JointBatchHeader
{
  std::uint32_t magic;
  std::uint32_t count;   ///< samples that follow, 1..kMaxSamplesPerDatagram
};

/// The send buffer: header plus room for a full batch. Only the first
/// `count` samples are ever transmitted, so the datagram is 8 + 60*count bytes.
struct JointBatchPacket
{
  JointBatchHeader header;
  JointSample samples[kMaxSamplesPerDatagram];
};
#pragma pack(pop)

static_assert(
  sizeof(JointSample) == sizeof(std::uint32_t) + sizeof(double) * kNumArmJoints,
  "JointSample must be tightly packed for the wire format");
static_assert(
  sizeof(JointBatchHeader) == sizeof(std::uint32_t) * 2,
  "JointBatchHeader must be tightly packed for the wire format");
static_assert(
  sizeof(JointBatchPacket) ==
  sizeof(JointBatchHeader) + sizeof(JointSample) * kMaxSamplesPerDatagram,
  "JointBatchPacket must be tightly packed for the wire format");
static_assert(
  sizeof(JointBatchPacket) <= 1472,
  "a batch must fit one 1500-byte MTU packet, fragmentation multiplies the loss rate");

// A batch is 8 + 60n bytes with n >= 1. These assert that no such length can
// collide with a fixed-size type, which is what keeps classification by length
// unambiguous. ControlPacket would collide at n = 0, hence the n >= 1 rule.
static_assert(
  (sizeof(JointPacket) - sizeof(JointBatchHeader)) % sizeof(JointSample) != 0,
  "a batch length must never equal a JointPacket length");
static_assert(
  (sizeof(ButtonPacket) - sizeof(JointBatchHeader)) % sizeof(JointSample) != 0,
  "a batch length must never equal a ButtonPacket length");

/// True if `length` could be a batch. Cheap enough to sit in the classify path,
/// and it rejects a truncated read: the receive buffer is one sample larger than
/// a full batch, so a datagram that overflowed it fails the upper bound here.
inline bool is_joint_batch_size(std::size_t length)
{
  if (length < sizeof(JointBatchHeader) + sizeof(JointSample)) {   // Rule 3: n >= 1
    return false;
  }
  if (length > sizeof(JointBatchPacket)) {                         // Rule 3: n <= max
    return false;
  }
  return (length - sizeof(JointBatchHeader)) % sizeof(JointSample) == 0;
}

/// Packs up to kMaxSamplesPerDatagram samples from `samples` into `out` and
/// returns the byte count to send, or 0 if the request is out of range (the
/// caller logs that). `count` is clamped by the caller, not silently truncated
/// here, so a caller bug is visible rather than quietly halving the batch.
inline std::size_t encode_joint_batch(
  const ArmSample * samples, std::size_t count, JointBatchPacket & out)
{
  if (samples == nullptr) {                         // Rule 3: null guard
    return 0;
  }

  if (count == 0 || count > kMaxSamplesPerDatagram) {   // Rule 3: range guard
    return 0;
  }

  out.header.magic = htonl(kPacketMagic);
  out.header.count = htonl(static_cast<std::uint32_t>(count));

  for (std::size_t i = 0; i < count; ++i) {
    out.samples[i].arm = htonl(static_cast<std::uint32_t>(samples[i].arm));
    std::memcpy(
      out.samples[i].positions, samples[i].positions.data(), sizeof(out.samples[i].positions));
  }

  return sizeof(JointBatchHeader) + sizeof(JointSample) * count;
}

/// Reads one sample out of a batch datagram. The caller must already have
/// established that the datagram holds at least `index + 1` of them --
/// is_joint_batch_size() plus the header's count is what proves that.
inline JointSample joint_sample_at(const void * data, std::size_t index)
{
  const unsigned char * const bytes = static_cast<const unsigned char *>(data);
  const std::size_t offset = sizeof(JointBatchHeader) + index * sizeof(JointSample);

  // memcpy rather than a cast: the buffer has no alignment guarantee, and a
  // misaligned load of a double is undefined behaviour, not merely slow.
  JointSample sample{};
  std::memcpy(&sample, bytes + offset, sizeof(sample));
  return sample;
}

/// Validates a batch datagram and APPENDS its samples to `out`, so the receiver
/// can decode straight onto the vector it is accumulating into.
///
/// Every field is checked before a single sample is appended: on failure `out`
/// is left exactly as it was, never half-extended with a partly-valid batch.
inline bool decode_joint_batch(
  const void * data, std::size_t length, std::vector<ArmSample> & out, std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (!is_joint_batch_size(length)) {               // Rule 3: size guard
    reason = "size " + std::to_string(length) + " is not a whole batch";
    return false;
  }

  JointBatchHeader header{};
  std::memcpy(&header, data, sizeof(header));

  if (ntohl(header.magic) != kPacketMagic) {        // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  const std::size_t samples_in_length = (length - sizeof(JointBatchHeader)) / sizeof(JointSample);
  const std::uint32_t count = ntohl(header.count);

  if (count != samples_in_length) {                 // Rule 3: header vs length
    reason = "count " + std::to_string(count) + " but " + std::to_string(samples_in_length) +
      " samples fit the datagram";
    return false;
  }

  // First pass validates, second pass appends. Splitting them is what keeps the
  // "never partially fills out" promise when a later sample turns out to be bad.
  for (std::size_t i = 0; i < count; ++i) {
    const std::uint32_t arm = ntohl(joint_sample_at(data, i).arm);

    if (arm > static_cast<std::uint32_t>(ArmSide::kRight)) {    // Rule 3: enum range
      reason = "sample " + std::to_string(i) + " has unknown arm " + std::to_string(arm);
      return false;
    }
  }

  out.reserve(out.size() + count);
  for (std::size_t i = 0; i < count; ++i) {
    const JointSample sample = joint_sample_at(data, i);

    ArmSample decoded;
    decoded.arm = static_cast<ArmSide>(ntohl(sample.arm));
    std::memcpy(decoded.positions.data(), sample.positions, sizeof(sample.positions));
    out.push_back(decoded);
  }

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
