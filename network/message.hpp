#pragma once

#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "socket_utils.hpp"

enum class MsgType : uint8_t {
    PUT_REPL = 1,
    ACK = 2,
    CLIENT_PUT = 3,
    CLIENT_GET = 4,
    CLIENT_GET_REPLY = 5,
    CLIENT_PUT_REPLY = 6,
    CLIENT_HEALTH = 7,
    CLIENT_HEALTH_REPLY = 8,
    PUT_REPL_BATCH = 9,
    PREFIX_QUERY = 10,
    PREFIX_REPLY = 11,
    RECOVERY_SUMMARY_REQUEST = 12,
    RECOVERY_SUMMARY_REPLY = 13,
    RECOVERY_FETCH_REQUEST = 14,
    RECOVERY_FETCH_REPLY = 15,
    RECOVERY_INSTALL_BATCH = 16,
    TERM_ADVANCE = 17,
    TERM_ADVANCE_REPLY = 18
};

enum class OperationStatus : uint8_t {
    OK = 0,
    NOT_FOUND = 1,
    NOT_LEADER = 2,
    NO_QUORUM = 3,
    OUT_OF_SPACE = 4,
    INVALID_REQUEST = 5,
    STORAGE_ERROR = 6,
    SHUTTING_DOWN = 7,
    OUT_OF_ORDER = 8,
    INDEX_FULL = 9,
    RECOVERING = 10
};

inline const char* status_name(OperationStatus status) {
    switch (status) {
        case OperationStatus::OK: return "OK";
        case OperationStatus::NOT_FOUND: return "NOT_FOUND";
        case OperationStatus::NOT_LEADER: return "NOT_LEADER";
        case OperationStatus::NO_QUORUM: return "NO_QUORUM";
        case OperationStatus::OUT_OF_SPACE: return "OUT_OF_SPACE";
        case OperationStatus::INVALID_REQUEST: return "INVALID_REQUEST";
        case OperationStatus::STORAGE_ERROR: return "STORAGE_ERROR";
        case OperationStatus::SHUTTING_DOWN: return "SHUTTING_DOWN";
        case OperationStatus::OUT_OF_ORDER: return "OUT_OF_ORDER";
        case OperationStatus::INDEX_FULL: return "INDEX_FULL";
        case OperationStatus::RECOVERING: return "RECOVERING";
    }
    return "UNKNOWN";
}

struct ReplicationEntry {
    uint64_t sequence{0};       // Sequence in the owning worker's stream.
    uint64_t incarnation{0};    // Version of this key.
    uint64_t segment_index{0};  // Leader's physical segment coordinate.
    uint64_t object_index{0};   // Leader's position inside that segment.
    std::string key;
    std::vector<uint8_t> value;
    uint64_t term{0};           // Included by recovery records.
    uint64_t worker_id{0};      // Included by recovery records.
};

struct WorkerProgressWire {
    uint64_t worker_id{0}; // Stream described by this summary.
    uint64_t term{0};      // Latest term with a contiguous prefix.
    uint64_t sequence{0};  // Highest contiguous sequence in that term.
};


struct NetMessage {
    MsgType type{MsgType::ACK};
    OperationStatus status{OperationStatus::OK};
    uint64_t term{0};
    uint64_t seq{0};
    uint64_t incarnation{0};
    uint64_t worker_id{0};
    uint64_t segment_index{0};
    uint64_t object_index{0};
    std::string key;
    std::vector<uint8_t> value;
    std::vector<ReplicationEntry> entries;
    std::vector<WorkerProgressWire> worker_progress;
};

constexpr uint32_t kProtocolMagic = 0x444b5633U;
constexpr uint16_t kProtocolVersion = 3;         // Reject incompatible peers.
constexpr uint32_t kMaxWireKeySize = 63;
constexpr uint32_t kMaxWireValueSize = 1024U * 1024U;
constexpr uint32_t kMaxWireBatchEntries = 16;
constexpr uint32_t kMaxWireRecoveryBatchEntries = 256;
constexpr size_t kMaxWireRecoveryBatchBytes = 1024U * 1024U;
constexpr uint32_t kMaxWireWorkerProgress = 64;
constexpr uint32_t kMaxWireFrameSize = 20U * 1024U * 1024U;

inline uint64_t host_to_network_u64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (static_cast<uint64_t>(htonl(static_cast<uint32_t>(value))) << 32U) |
           htonl(static_cast<uint32_t>(value >> 32U));
#else
    return value;
#endif
}

inline uint64_t network_to_host_u64(uint64_t value) {
    return host_to_network_u64(value);
}

inline bool valid_message_type(uint8_t type) {
    return type >= static_cast<uint8_t>(MsgType::PUT_REPL) &&
           type <= static_cast<uint8_t>(MsgType::TERM_ADVANCE_REPLY);
}

namespace message_detail {

inline void append_u32(std::vector<uint8_t>& bytes, uint32_t value) {
    const uint32_t wire = htonl(value);
    const auto* begin = reinterpret_cast<const uint8_t*>(&wire);
    bytes.insert(bytes.end(), begin, begin + sizeof(wire));
}

inline void append_u64(std::vector<uint8_t>& bytes, uint64_t value) {
    const uint64_t wire = host_to_network_u64(value);
    const auto* begin = reinterpret_cast<const uint8_t*>(&wire);
    bytes.insert(bytes.end(), begin, begin + sizeof(wire));
}

inline void append_bytes(std::vector<uint8_t>& bytes, const void* data,
                         size_t size) {
    if (size == 0) return;
    const auto* begin = static_cast<const uint8_t*>(data);
    bytes.insert(bytes.end(), begin, begin + size);
}

inline bool read_u32(const std::vector<uint8_t>& bytes, size_t& cursor,
                     uint32_t& value) {
    if (cursor > bytes.size() || bytes.size() - cursor < sizeof(uint32_t)) {
        return false;
    }
    uint32_t wire = 0;
    std::memcpy(&wire, bytes.data() + cursor, sizeof(wire));
    cursor += sizeof(wire);
    value = ntohl(wire);
    return true;
}

inline bool read_u64(const std::vector<uint8_t>& bytes, size_t& cursor,
                     uint64_t& value) {
    if (cursor > bytes.size() || bytes.size() - cursor < sizeof(uint64_t)) {
        return false;
    }
    uint64_t wire = 0;
    std::memcpy(&wire, bytes.data() + cursor, sizeof(wire));
    cursor += sizeof(wire);
    value = network_to_host_u64(wire);
    return true;
}

inline bool read_blob(const std::vector<uint8_t>& bytes, size_t& cursor,
                      uint32_t size, std::string& destination) {
    if (cursor > bytes.size() || bytes.size() - cursor < size) return false;
    destination.assign(reinterpret_cast<const char*>(bytes.data() + cursor),
                       size);
    cursor += size;
    return true;
}

inline bool read_blob(const std::vector<uint8_t>& bytes, size_t& cursor,
                      uint32_t size, std::vector<uint8_t>& destination) {
    if (cursor > bytes.size() || bytes.size() - cursor < size) return false;
    destination.assign(bytes.begin() + static_cast<std::ptrdiff_t>(cursor),
                       bytes.begin() +
                           static_cast<std::ptrdiff_t>(cursor + size));
    cursor += size;
    return true;
}

inline bool valid_entry(const ReplicationEntry& entry) {
    return entry.sequence > 0 && !entry.key.empty() &&
           entry.key.size() <= kMaxWireKeySize &&
           entry.value.size() <= kMaxWireValueSize;
}

inline bool valid_recovery_entry(const ReplicationEntry& entry) {
    return valid_entry(entry) && entry.term > 0 &&
           entry.worker_id < kMaxWireWorkerProgress;
}

// Validate both size limits and which variable-length fields a message type is
// allowed to carry. This protects allocation and protocol state on both ends.
inline bool valid_shape(const NetMessage& message) {
    if (!valid_message_type(static_cast<uint8_t>(message.type)) ||
        static_cast<uint8_t>(message.status) >
            static_cast<uint8_t>(OperationStatus::RECOVERING) ||
        message.key.size() > kMaxWireKeySize ||
        message.value.size() > kMaxWireValueSize) {
        return false;
    }
    const bool carries_recovery_entries =
        message.type == MsgType::RECOVERY_FETCH_REPLY ||
        message.type == MsgType::RECOVERY_INSTALL_BATCH;
    const bool carries_entries =
        message.type == MsgType::PUT_REPL_BATCH || carries_recovery_entries;
    if (!carries_entries && !message.entries.empty()) return false;
    if (message.type != MsgType::RECOVERY_SUMMARY_REPLY &&
        !message.worker_progress.empty()) {
        return false;
    }
    if (message.worker_progress.size() > kMaxWireWorkerProgress) return false;
    if (message.type == MsgType::RECOVERY_SUMMARY_REPLY) {
        for (const WorkerProgressWire& progress : message.worker_progress) {
            if (progress.worker_id >= kMaxWireWorkerProgress ||
                (progress.term == 0 && progress.sequence != 0)) {
                return false;
            }
        }
    }
    if (!carries_entries) return true;
    const size_t entry_limit = carries_recovery_entries
                                   ? kMaxWireRecoveryBatchEntries
                                   : kMaxWireBatchEntries;
    if (!message.key.empty() || !message.value.empty() ||
        message.entries.size() > entry_limit) {
        return false;
    }
    if (message.type == MsgType::RECOVERY_FETCH_REPLY) {
        for (const ReplicationEntry& entry : message.entries) {
            if (!valid_recovery_entry(entry)) return false;
        }
        return true;
    }
    if (message.entries.empty()) return false;
    if (message.type == MsgType::RECOVERY_INSTALL_BATCH) {
        for (const ReplicationEntry& entry : message.entries) {
            if (!valid_recovery_entry(entry)) return false;
        }
        return true;
    }
    if (message.seq == 0) return false;
    // Live replication batches must describe one gap-free worker prefix.
    uint64_t expected = message.seq;
    for (size_t index = 0; index < message.entries.size(); ++index) {
        const ReplicationEntry& entry = message.entries[index];
        if (!valid_entry(entry) || entry.sequence != expected) return false;
        if (expected == std::numeric_limits<uint64_t>::max() &&
            index + 1 < message.entries.size()) {
            return false;
        }
        ++expected;
    }
    return true;
}

// Encode the type-independent payload in a fixed field order. The small frame
// header containing magic/version/type/status is written by send_message.
inline bool encode_payload(const NetMessage& message,
                           std::vector<uint8_t>& payload) {
    if (!valid_shape(message)) return false;
    append_u64(payload, message.term);
    append_u64(payload, message.seq);
    append_u64(payload, message.incarnation);
    append_u64(payload, message.worker_id);
    append_u64(payload, message.segment_index);
    append_u64(payload, message.object_index);
    append_u32(payload, static_cast<uint32_t>(message.key.size()));
    append_u32(payload, static_cast<uint32_t>(message.value.size()));
    append_bytes(payload, message.key.data(), message.key.size());
    append_bytes(payload, message.value.data(), message.value.size());
    append_u32(payload, static_cast<uint32_t>(message.entries.size()));
    for (const ReplicationEntry& entry : message.entries) {
        append_u64(payload, entry.sequence);
        append_u64(payload, entry.incarnation);
        append_u64(payload, entry.segment_index);
        append_u64(payload, entry.object_index);
        append_u32(payload, static_cast<uint32_t>(entry.key.size()));
        append_u32(payload, static_cast<uint32_t>(entry.value.size()));
        append_bytes(payload, entry.key.data(), entry.key.size());
        append_bytes(payload, entry.value.data(), entry.value.size());
        append_u64(payload, entry.term);
        append_u64(payload, entry.worker_id);
        if (payload.size() > kMaxWireFrameSize) return false;
    }
    append_u32(payload,
               static_cast<uint32_t>(message.worker_progress.size()));
    for (const WorkerProgressWire& progress : message.worker_progress) {
        append_u64(payload, progress.worker_id);
        append_u64(payload, progress.term);
        append_u64(payload, progress.sequence);
    }
    return payload.size() <= kMaxWireFrameSize;
}

// Decode with a checked cursor and reject trailing bytes. A fully consumed but
// semantically invalid payload is still rejected by valid_shape at the end.
inline bool decode_payload(const std::vector<uint8_t>& payload,
                           NetMessage& message) {
    size_t cursor = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    uint32_t entry_count = 0;
    uint32_t progress_count = 0;
    if (!read_u64(payload, cursor, message.term) ||
        !read_u64(payload, cursor, message.seq) ||
        !read_u64(payload, cursor, message.incarnation) ||
        !read_u64(payload, cursor, message.worker_id) ||
        !read_u64(payload, cursor, message.segment_index) ||
        !read_u64(payload, cursor, message.object_index) ||
        !read_u32(payload, cursor, key_size) ||
        !read_u32(payload, cursor, value_size) ||
        key_size > kMaxWireKeySize || value_size > kMaxWireValueSize ||
        !read_blob(payload, cursor, key_size, message.key) ||
        !read_blob(payload, cursor, value_size, message.value) ||
        !read_u32(payload, cursor, entry_count)) {
        return false;
    }

    const bool recovery_entries =
        message.type == MsgType::RECOVERY_FETCH_REPLY ||
        message.type == MsgType::RECOVERY_INSTALL_BATCH;
    const uint32_t entry_limit = recovery_entries
                                     ? kMaxWireRecoveryBatchEntries
                                     : kMaxWireBatchEntries;
    if (entry_count > entry_limit) return false;

    message.entries.reserve(entry_count);
    for (uint32_t index = 0; index < entry_count; ++index) {
        ReplicationEntry entry;
        if (!read_u64(payload, cursor, entry.sequence) ||
            !read_u64(payload, cursor, entry.incarnation) ||
            !read_u64(payload, cursor, entry.segment_index) ||
            !read_u64(payload, cursor, entry.object_index) ||
            !read_u32(payload, cursor, key_size) ||
            !read_u32(payload, cursor, value_size) ||
            key_size > kMaxWireKeySize ||
            value_size > kMaxWireValueSize ||
            !read_blob(payload, cursor, key_size, entry.key) ||
            !read_blob(payload, cursor, value_size, entry.value) ||
            !read_u64(payload, cursor, entry.term) ||
            !read_u64(payload, cursor, entry.worker_id)) {
            return false;
        }
        message.entries.push_back(std::move(entry));
    }
    if (!read_u32(payload, cursor, progress_count) ||
        progress_count > kMaxWireWorkerProgress) {
        return false;
    }
    message.worker_progress.reserve(progress_count);
    for (uint32_t index = 0; index < progress_count; ++index) {
        WorkerProgressWire progress;
        if (!read_u64(payload, cursor, progress.worker_id) ||
            !read_u64(payload, cursor, progress.term) ||
            !read_u64(payload, cursor, progress.sequence)) {
            return false;
        }
        message.worker_progress.push_back(progress);
    }
    return cursor == payload.size() && valid_shape(message);
}

}  // namespace message_detail

// Write one complete, versioned frame. send_all handles short socket writes.
inline bool send_message(int socket, const NetMessage& message) {
    std::vector<uint8_t> payload;
    if (!message_detail::encode_payload(message, payload)) return false;

    const uint32_t magic = htonl(kProtocolMagic);
    const uint16_t version = htons(kProtocolVersion);
    const uint8_t type = static_cast<uint8_t>(message.type);
    const uint8_t status = static_cast<uint8_t>(message.status);
    const uint32_t payload_size = htonl(static_cast<uint32_t>(payload.size()));
    return send_all(socket, &magic, sizeof(magic)) &&
           send_all(socket, &version, sizeof(version)) &&
           send_all(socket, &type, sizeof(type)) &&
           send_all(socket, &status, sizeof(status)) &&
           send_all(socket, &payload_size, sizeof(payload_size)) &&
           send_all(socket, payload.data(), payload.size());
}

// Read and validate the frame header before allocating its declared payload.
inline bool recv_message(int socket, NetMessage& message) {
    uint32_t magic = 0;
    uint16_t version = 0;
    uint8_t type = 0;
    uint8_t status = 0;
    uint32_t payload_size = 0;
    if (!recv_all(socket, &magic, sizeof(magic)) ||
        !recv_all(socket, &version, sizeof(version)) ||
        !recv_all(socket, &type, sizeof(type)) ||
        !recv_all(socket, &status, sizeof(status)) ||
        !recv_all(socket, &payload_size, sizeof(payload_size))) {
        return false;
    }
    payload_size = ntohl(payload_size);
    if (ntohl(magic) != kProtocolMagic || ntohs(version) != kProtocolVersion ||
        !valid_message_type(type) ||
        status > static_cast<uint8_t>(OperationStatus::RECOVERING) ||
        payload_size > kMaxWireFrameSize) {
        return false;
    }

    std::vector<uint8_t> payload(payload_size);
    if (!recv_all(socket, payload.data(), payload.size())) return false;
    message = NetMessage{};
    message.type = static_cast<MsgType>(type);
    message.status = static_cast<OperationStatus>(status);
    return message_detail::decode_payload(payload, message);
}
