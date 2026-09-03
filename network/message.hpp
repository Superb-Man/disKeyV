#pragma once

#include <arpa/inet.h>
#include <cstdint>
#include <string>
#include <vector>

#include "socket_utils.hpp"

enum class MsgType : uint8_t {
    PUT_REPL = 1,
    ACK = 2,
    CLIENT_PUT = 3,
    CLIENT_GET = 4,
    CLIENT_GET_REPLY = 5,
    CLIENT_PUT_REPLY = 6
};

enum class OperationStatus : uint8_t {
    OK = 0,
    NOT_FOUND = 1,
    NOT_LEADER = 2,
    NO_QUORUM = 3,
    OUT_OF_SPACE = 4,
    INVALID_REQUEST = 5,
    STORAGE_ERROR = 6,
    SHUTTING_DOWN = 7
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
    }
    return "UNKNOWN";
}

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
};

constexpr uint32_t kProtocolMagic = 0x444b5631U;
constexpr uint16_t kProtocolVersion = 1;
constexpr uint32_t kMaxWireKeySize = 63;
constexpr uint32_t kMaxWireValueSize = 1024U * 1024U;

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
           type <= static_cast<uint8_t>(MsgType::CLIENT_PUT_REPLY);
}

inline bool send_u64(int sock, uint64_t value) {
    const uint64_t wire = host_to_network_u64(value);
    return send_all(sock, &wire, sizeof(wire));
}

inline bool recv_u64(int sock, uint64_t& value) {
    uint64_t wire = 0;
    if (!recv_all(sock, &wire, sizeof(wire))) return false;
    value = network_to_host_u64(wire);
    return true;
}

inline bool send_message(int sock, const NetMessage& msg) {
    if (msg.key.size() > kMaxWireKeySize ||
        msg.value.size() > kMaxWireValueSize) {
        return false;
    }

    const uint32_t magic = htonl(kProtocolMagic);
    const uint16_t version = htons(kProtocolVersion);
    const uint8_t type = static_cast<uint8_t>(msg.type);
    const uint8_t status = static_cast<uint8_t>(msg.status);
    const uint32_t key_size = htonl(static_cast<uint32_t>(msg.key.size()));
    const uint32_t value_size = htonl(static_cast<uint32_t>(msg.value.size()));

    return send_all(sock, &magic, sizeof(magic)) &&
           send_all(sock, &version, sizeof(version)) &&
           send_all(sock, &type, sizeof(type)) &&
           send_all(sock, &status, sizeof(status)) &&
           send_u64(sock, msg.term) &&
           send_u64(sock, msg.seq) &&
           send_u64(sock, msg.incarnation) &&
           send_u64(sock, msg.worker_id) &&
           send_u64(sock, msg.segment_index) &&
           send_u64(sock, msg.object_index) &&
           send_all(sock, &key_size, sizeof(key_size)) &&
           send_all(sock, &value_size, sizeof(value_size)) &&
           send_all(sock, msg.key.data(), msg.key.size()) &&
           send_all(sock, msg.value.data(), msg.value.size());
}

inline bool recv_message(int sock, NetMessage& msg) {
    uint32_t magic = 0;
    uint16_t version = 0;
    uint8_t type = 0;
    uint8_t status = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;

    if (!recv_all(sock, &magic, sizeof(magic)) ||
        !recv_all(sock, &version, sizeof(version)) ||
        !recv_all(sock, &type, sizeof(type)) ||
        !recv_all(sock, &status, sizeof(status))) {
        return false;
    }
    if (ntohl(magic) != kProtocolMagic || ntohs(version) != kProtocolVersion ||
        !valid_message_type(type) ||
        status > static_cast<uint8_t>(OperationStatus::SHUTTING_DOWN)) {
        return false;
    }

    msg = NetMessage{};
    msg.type = static_cast<MsgType>(type);
    msg.status = static_cast<OperationStatus>(status);
    if (!recv_u64(sock, msg.term) ||
        !recv_u64(sock, msg.seq) ||
        !recv_u64(sock, msg.incarnation) ||
        !recv_u64(sock, msg.worker_id) ||
        !recv_u64(sock, msg.segment_index) ||
        !recv_u64(sock, msg.object_index) ||
        !recv_all(sock, &key_size, sizeof(key_size)) ||
        !recv_all(sock, &value_size, sizeof(value_size))) {
        return false;
    }

    key_size = ntohl(key_size);
    value_size = ntohl(value_size);
    if (key_size > kMaxWireKeySize || value_size > kMaxWireValueSize) {
        return false;
    }

    msg.key.resize(key_size);
    msg.value.resize(value_size);
    return recv_all(sock, msg.key.data(), msg.key.size()) &&
           recv_all(sock, msg.value.data(), msg.value.size());
}
