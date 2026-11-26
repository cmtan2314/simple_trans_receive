#ifndef TRANSRECV_UDP__IMAGE_PROTOCOL_HPP_
#define TRANSRECV_UDP__IMAGE_PROTOCOL_HPP_

#include <arpa/inet.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace transrecv_udp
{

/// Compressed camera frames, server -> client.
///
/// A JPEG frame (tens to hundreds of KB) does not fit in one datagram, and a
/// datagram bigger than the path MTU gets IP-fragmented, where losing any one
/// fragment silently loses the whole datagram. So each frame is serialised into
/// one blob, cut into chunks that each fit in a single Ethernet frame, and
/// reassembled on the far side.
///
/// A frame with a missing chunk is dropped whole. There is no retransmission:
/// by the time a resend arrived the next frame would already be on its way,
/// and a late camera frame is worth nothing.
///
/// Chunks are told apart from the fixed-size packets (protocol.hpp) by their
/// magic, not their length: a chunk's size varies (the last one is short) and
/// could land on any of the fixed sizes. Every fixed-size packet starts with
/// kPacketMagic at the same offset, so checking kImageMagic first is safe.

constexpr std::uint32_t kImageMagic = 0x4F494D47u;  // "OIMG"

// 1500-byte Ethernet MTU - 20 IPv4 - 8 UDP = 1472 bytes per unfragmented
// datagram. Stay under it with headroom for VLAN tags / tunnels.
constexpr std::size_t kImageChunkPayloadBytes = 1400;

// Upper bound on one serialised frame. A 1080p JPEG is well under 1 MiB; this
// only exists so a corrupt header cannot make the receiver allocate gigabytes.
constexpr std::size_t kMaxImageFrameBytes = 8u * 1024u * 1024u;

/// Every chunk starts with this. All fields network byte order on the wire.
#pragma pack(push, 1)
struct ImageChunkHeader
{
  std::uint32_t magic;
  std::uint32_t frame_seq;     ///< increments per frame, wraps
  std::uint32_t frame_bytes;   ///< size of the whole serialised frame
  std::uint16_t chunk_index;
  std::uint16_t chunk_count;
};
#pragma pack(pop)

constexpr std::size_t kImageChunkHeaderBytes =
  sizeof(std::uint32_t) * 3 + sizeof(std::uint16_t) * 2;
static_assert(
  sizeof(ImageChunkHeader) == kImageChunkHeaderBytes,
  "ImageChunkHeader must be tightly packed for the wire format");

constexpr std::size_t kMaxImageDatagramBytes = kImageChunkHeaderBytes + kImageChunkPayloadBytes;

/// Buffer that holds any one chunk datagram.
using ImageDatagram = std::array<std::uint8_t, kMaxImageDatagramBytes>;

constexpr std::size_t image_chunk_count(std::size_t frame_bytes)
{
  return (frame_bytes + kImageChunkPayloadBytes - 1) / kImageChunkPayloadBytes;
}

static_assert(
  image_chunk_count(kMaxImageFrameBytes) <= UINT16_MAX,
  "the largest frame's chunk count must fit in the uint16 chunk_count field");

// --- frame blob -------------------------------------------------------------
// What gets chunked: a small fixed meta block, then the format string, the
// frame_id string and the compressed bytes, back to back.

#pragma pack(push, 1)
struct ImageFrameMeta
{
  std::int32_t stamp_sec;
  std::uint32_t stamp_nanosec;
  std::uint16_t format_bytes;
  std::uint16_t frame_id_bytes;
};
#pragma pack(pop)

constexpr std::size_t kImageFrameMetaBytes =
  sizeof(std::int32_t) + sizeof(std::uint32_t) + sizeof(std::uint16_t) * 2;
static_assert(
  sizeof(ImageFrameMeta) == kImageFrameMetaBytes,
  "ImageFrameMeta must be tightly packed for the wire format");

/// A decoded frame, everything a sensor_msgs/CompressedImage needs.
struct ImageFrame
{
  std::int32_t stamp_sec = 0;
  std::uint32_t stamp_nanosec = 0;
  std::string format;
  std::string frame_id;
  std::vector<std::uint8_t> data;
};

/// Serialises one frame into `blob` (reused across calls to avoid
/// reallocating per frame). Returns false with `reason` on input that cannot
/// be represented on the wire.
inline bool encode_image_frame(
  std::int32_t stamp_sec, std::uint32_t stamp_nanosec, const std::string & format,
  const std::string & frame_id, const std::uint8_t * data, std::size_t data_bytes,
  std::vector<std::uint8_t> & blob, std::string & reason)
{
  if (data == nullptr && data_bytes > 0) {          // Rule 3: null guard
    reason = "null image data";
    return false;
  }

  if (data_bytes == 0) {                            // Rule 3: empty guard
    reason = "empty image data";
    return false;
  }

  if (format.size() > UINT16_MAX || frame_id.size() > UINT16_MAX) {   // Rule 3: field range
    reason = "format or frame_id longer than " + std::to_string(UINT16_MAX) + " bytes";
    return false;
  }

  const std::size_t total =
    kImageFrameMetaBytes + format.size() + frame_id.size() + data_bytes;
  if (total > kMaxImageFrameBytes) {                // Rule 3: size guard
    reason = "frame of " + std::to_string(total) + " bytes > limit " +
      std::to_string(kMaxImageFrameBytes);
    return false;
  }

  ImageFrameMeta meta{};
  meta.stamp_sec = static_cast<std::int32_t>(htonl(static_cast<std::uint32_t>(stamp_sec)));
  meta.stamp_nanosec = htonl(stamp_nanosec);
  meta.format_bytes = htons(static_cast<std::uint16_t>(format.size()));
  meta.frame_id_bytes = htons(static_cast<std::uint16_t>(frame_id.size()));

  blob.resize(total);
  std::uint8_t * cursor = blob.data();
  std::memcpy(cursor, &meta, sizeof(meta));
  cursor += sizeof(meta);
  std::memcpy(cursor, format.data(), format.size());
  cursor += format.size();
  std::memcpy(cursor, frame_id.data(), frame_id.size());
  cursor += frame_id.size();
  std::memcpy(cursor, data, data_bytes);
  return true;
}

/// Inverse of encode_image_frame. Never partially fills `out`.
inline bool decode_image_frame(
  const std::vector<std::uint8_t> & blob, ImageFrame & out, std::string & reason)
{
  if (blob.size() < kImageFrameMetaBytes) {         // Rule 3: size guard
    reason = "frame of " + std::to_string(blob.size()) + " bytes is shorter than its meta block";
    return false;
  }

  ImageFrameMeta meta{};
  std::memcpy(&meta, blob.data(), sizeof(meta));

  const std::size_t format_bytes = ntohs(meta.format_bytes);
  const std::size_t frame_id_bytes = ntohs(meta.frame_id_bytes);
  const std::size_t header_bytes = kImageFrameMetaBytes + format_bytes + frame_id_bytes;
  if (header_bytes >= blob.size()) {                // Rule 3: lengths must leave image data
    reason = "string lengths (" + std::to_string(format_bytes) + " + " +
      std::to_string(frame_id_bytes) + ") leave no image data in a " +
      std::to_string(blob.size()) + " byte frame";
    return false;
  }

  const char * strings = reinterpret_cast<const char *>(blob.data() + kImageFrameMetaBytes);
  ImageFrame frame;
  frame.stamp_sec = static_cast<std::int32_t>(ntohl(static_cast<std::uint32_t>(meta.stamp_sec)));
  frame.stamp_nanosec = ntohl(meta.stamp_nanosec);
  frame.format.assign(strings, format_bytes);
  frame.frame_id.assign(strings + format_bytes, frame_id_bytes);
  frame.data.assign(blob.begin() + static_cast<std::ptrdiff_t>(header_bytes), blob.end());

  out = std::move(frame);
  return true;
}

// --- chunks -----------------------------------------------------------------

/// True when the datagram claims to be an image chunk. Only the magic is
/// checked here; decode_image_chunk() does the real validation.
inline bool is_image_chunk(const void * data, std::size_t length)
{
  if (data == nullptr || length < kImageChunkHeaderBytes) {   // Rule 3: null + size guard
    return false;
  }
  std::uint32_t magic = 0;
  std::memcpy(&magic, data, sizeof(magic));
  return ntohl(magic) == kImageMagic;
}

/// Writes chunk `index` of `blob` into `datagram`. Returns the datagram's size,
/// or 0 when `index` is out of range or the blob cannot be chunked.
inline std::size_t make_image_chunk(
  std::uint32_t frame_seq, const std::vector<std::uint8_t> & blob, std::size_t index,
  ImageDatagram & datagram)
{
  const std::size_t chunk_count = image_chunk_count(blob.size());
  if (blob.empty() || blob.size() > kMaxImageFrameBytes || index >= chunk_count) {  // Rule 3
    return 0;
  }

  const std::size_t offset = index * kImageChunkPayloadBytes;
  const std::size_t remaining = blob.size() - offset;
  const std::size_t payload_bytes =
    remaining < kImageChunkPayloadBytes ? remaining : kImageChunkPayloadBytes;

  ImageChunkHeader header{};
  header.magic = htonl(kImageMagic);
  header.frame_seq = htonl(frame_seq);
  header.frame_bytes = htonl(static_cast<std::uint32_t>(blob.size()));
  header.chunk_index = htons(static_cast<std::uint16_t>(index));
  header.chunk_count = htons(static_cast<std::uint16_t>(chunk_count));

  std::memcpy(datagram.data(), &header, sizeof(header));
  std::memcpy(datagram.data() + sizeof(header), blob.data() + offset, payload_bytes);
  return sizeof(header) + payload_bytes;
}

/// Validates a chunk datagram. On success `header` is in host byte order and
/// `payload` points into `data`. Returns false with `reason` on any field that
/// does not agree with the others.
inline bool decode_image_chunk(
  const void * data, std::size_t length, ImageChunkHeader & header,
  const std::uint8_t * & payload, std::size_t & payload_bytes, std::string & reason)
{
  if (data == nullptr) {                            // Rule 3: null guard
    reason = "null buffer";
    return false;
  }

  if (length <= kImageChunkHeaderBytes || length > kMaxImageDatagramBytes) {   // Rule 3
    reason = "chunk size " + std::to_string(length) + " outside (" +
      std::to_string(kImageChunkHeaderBytes) + ", " + std::to_string(kMaxImageDatagramBytes) + "]";
    return false;
  }

  ImageChunkHeader wire{};
  std::memcpy(&wire, data, sizeof(wire));
  if (ntohl(wire.magic) != kImageMagic) {           // Rule 3: not ours
    reason = "bad magic";
    return false;
  }

  ImageChunkHeader host{};
  host.magic = kImageMagic;
  host.frame_seq = ntohl(wire.frame_seq);
  host.frame_bytes = ntohl(wire.frame_bytes);
  host.chunk_index = ntohs(wire.chunk_index);
  host.chunk_count = ntohs(wire.chunk_count);

  if (host.frame_bytes == 0 || host.frame_bytes > kMaxImageFrameBytes) {   // Rule 3: range
    reason = "frame size " + std::to_string(host.frame_bytes) + " outside (0, " +
      std::to_string(kMaxImageFrameBytes) + "]";
    return false;
  }

  if (host.chunk_count != image_chunk_count(host.frame_bytes)) {   // Rule 3: consistency
    reason = "chunk count " + std::to_string(host.chunk_count) + " does not match frame size " +
      std::to_string(host.frame_bytes);
    return false;
  }

  if (host.chunk_index >= host.chunk_count) {       // Rule 3: index range
    reason = "chunk index " + std::to_string(host.chunk_index) + " >= count " +
      std::to_string(host.chunk_count);
    return false;
  }

  // Every chunk is full except the last, so the exact size is known up front.
  const std::size_t offset = static_cast<std::size_t>(host.chunk_index) * kImageChunkPayloadBytes;
  const std::size_t remaining = host.frame_bytes - offset;
  const std::size_t expected =
    remaining < kImageChunkPayloadBytes ? remaining : kImageChunkPayloadBytes;
  const std::size_t received = length - kImageChunkHeaderBytes;
  if (received != expected) {                       // Rule 3: payload size guard
    reason = "chunk " + std::to_string(host.chunk_index) + " carries " +
      std::to_string(received) + " bytes, expected " + std::to_string(expected);
    return false;
  }

  header = host;
  payload = static_cast<const std::uint8_t *>(data) + kImageChunkHeaderBytes;
  payload_bytes = received;
  return true;
}

/// Rebuilds frames from chunks. Holds one frame in progress at a time: a chunk
/// of a newer frame abandons the current one, a chunk of an older frame is
/// ignored. Not thread-safe; feed it from one thread.
class ImageReassembler
{
public:
  enum class Result
  {
    kPending,       ///< chunk stored, frame not complete yet
    kComplete,      ///< this chunk completed the frame; read it with frame()
    kStale,         ///< chunk of an older (or already completed) frame, ignored
    kDuplicate,     ///< chunk already received for this frame, ignored
    kInconsistent,  ///< same frame number, different frame size: ignored
  };

  /// `header` and `payload` must come from a successful decode_image_chunk().
  Result add(const ImageChunkHeader & header, const std::uint8_t * payload, std::size_t payload_bytes)
  {
    if (payload == nullptr || payload_bytes == 0) {   // Rule 3: null + empty guard
      return Result::kInconsistent;
    }

    if (active_) {
      // Signed distance, so the comparison survives the sequence wrapping.
      const auto distance = static_cast<std::int32_t>(header.frame_seq - seq_);
      if (distance < 0 || (distance == 0 && completed_)) {
        return Result::kStale;
      }
      if (distance == 0 && header.frame_bytes != frame_bytes_) {
        return Result::kInconsistent;
      }
      if (distance > 0) {
        if (!completed_) {
          ++frames_abandoned_;
        }
        begin(header);
      }
    } else {
      begin(header);
    }

    const std::size_t index = header.chunk_index;
    if (index >= have_.size()) {                      // Rule 3: index guard
      return Result::kInconsistent;
    }
    if (have_[index]) {
      return Result::kDuplicate;
    }

    const std::size_t offset = index * kImageChunkPayloadBytes;
    if (offset + payload_bytes > blob_.size()) {      // Rule 3: never write past the blob
      return Result::kInconsistent;
    }
    std::memcpy(blob_.data() + offset, payload, payload_bytes);
    have_[index] = true;
    ++received_;

    if (received_ < have_.size()) {
      return Result::kPending;
    }
    completed_ = true;
    return Result::kComplete;
  }

  /// The completed frame's blob. Valid after add() returned kComplete, until
  /// the next add().
  const std::vector<std::uint8_t> & frame() const
  {
    return blob_;
  }

  std::uint32_t frame_seq() const
  {
    return seq_;
  }

  /// Frames that were still missing chunks when a newer frame started.
  std::uint64_t frames_abandoned() const
  {
    return frames_abandoned_;
  }

private:
  void begin(const ImageChunkHeader & header)
  {
    active_ = true;
    completed_ = false;
    seq_ = header.frame_seq;
    frame_bytes_ = header.frame_bytes;
    received_ = 0;
    have_.assign(header.chunk_count, false);
    blob_.resize(header.frame_bytes);
  }

  bool active_ = false;
  bool completed_ = false;
  std::uint32_t seq_ = 0;
  std::uint32_t frame_bytes_ = 0;
  std::size_t received_ = 0;
  std::vector<bool> have_;
  std::vector<std::uint8_t> blob_;
  std::uint64_t frames_abandoned_ = 0;
};

}  // namespace transrecv_udp

#endif  // TRANSRECV_UDP__IMAGE_PROTOCOL_HPP_
