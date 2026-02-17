#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "socket_utils.hpp"

// In network/message.hpp
struct TxKeyValue {
    std::string key;
    std::vector<uint8_t> value;

    // Default constructor
    TxKeyValue() = default;

    // Constructor from pair
    TxKeyValue(const std::pair<std::string, std::vector<uint8_t>>& p)
        : key(p.first), value(p.second) {}

    // ✅ NEW: Constructor from key and value
    TxKeyValue(std::string k, std::vector<uint8_t> v)
        : key(std::move(k)), value(std::move(v)) {}

    // Implicit conversion to pair
    operator std::pair<std::string, std::vector<uint8_t>>() const {
        return {key, value};
    }
};

enum class MsgType : uint8_t {
    PUT_REPL = 1,
    ACK      = 2,

    CLIENT_PUT = 3,
    CLIENT_GET = 4,
    CLIENT_GET_REPLY = 5,
    CLIENT_PUT_REPLY = 6,

    TX_PREPARE = 10,
    TX_COMMIT = 11,
    TX_ABORT = 12,
    TX_PREPARE_OK = 13,
    TX_PREPARE_FAIL = 14,
    CLIENT_TX_PUT = 15,
    CLIENT_TX_PUT_REPLY = 16
};


struct NetMessage {
    MsgType type;
    uint64_t term;
    uint64_t seq;
    uint64_t incarnation;
    std::string key;
    std::vector<uint8_t> value;

    uint64_t tx_id = 0;
    std::vector<TxKeyValue> kv_pairs;
};

inline bool send_message(int sock, const NetMessage& msg) {
    uint8_t type = (uint8_t)msg.type;
    if (!send_all(sock, &type, 1)) return false;
    if (!send_all(sock, &msg.term, 8)) return false;
    if (!send_all(sock, &msg.seq, 8)) return false;
    if (!send_all(sock, &msg.incarnation, 8)) return false;

    uint32_t ksz = msg.key.size();
    uint32_t vsz = msg.value.size();
    if (!send_all(sock, &ksz, 4)) return false;
    if (!send_all(sock, &vsz, 4)) return false;
    if (!send_all(sock, msg.key.data(), ksz)) return false;
    if (!send_all(sock, msg.value.data(), vsz)) return false;

    // Send tx_id for ALL transaction messages
    if (msg.type == MsgType::TX_PREPARE || 
        msg.type == MsgType::TX_COMMIT || 
        msg.type == MsgType::TX_ABORT ||
        msg.type == MsgType::CLIENT_TX_PUT) {
        if (!send_all(sock, &msg.tx_id, 8)) return false;
        
        // Only TX_PREPARE and CLIENT_TX_PUT have kv_pairs
        if (msg.type == MsgType::TX_PREPARE || msg.type == MsgType::CLIENT_TX_PUT) {
            uint32_t kv_count = msg.kv_pairs.size();
            if (!send_all(sock, &kv_count, 4)) return false;
            for (const auto& kv : msg.kv_pairs) {
                uint32_t ksize = kv.key.size();
                uint32_t vsize = kv.value.size();
                if (!send_all(sock, &ksize, 4)) return false;
                if (!send_all(sock, &vsize, 4)) return false;
                if (!send_all(sock, kv.key.data(), ksize)) return false;
                if (!send_all(sock, kv.value.data(), vsize)) return false;
            }
        }
    }
    return true;
}

inline bool recv_message(int sock, NetMessage& msg) {
    uint8_t type;
    if (!recv_all(sock, &type, 1)) return false;
    msg.type = (MsgType)type;
    if (!recv_all(sock, &msg.term, 8)) return false;
    if (!recv_all(sock, &msg.seq, 8)) return false;
    if (!recv_all(sock, &msg.incarnation, 8)) return false;

    uint32_t ksz, vsz;
    if (!recv_all(sock, &ksz, 4)) return false;
    if (!recv_all(sock, &vsz, 4)) return false;
    msg.key.resize(ksz);
    msg.value.resize(vsz);
    if (!recv_all(sock, (void*)msg.key.data(), ksz)) return false;
    if (!recv_all(sock, msg.value.data(), vsz)) return false;

    // Receive tx_id for ALL transaction messages
    if (msg.type == MsgType::TX_PREPARE || 
        msg.type == MsgType::TX_COMMIT || 
        msg.type == MsgType::TX_ABORT ||
        msg.type == MsgType::CLIENT_TX_PUT) {
        if (!recv_all(sock, &msg.tx_id, 8)) return false;
        
        // Only TX_PREPARE and CLIENT_TX_PUT have kv_pairs
        if (msg.type == MsgType::TX_PREPARE || msg.type == MsgType::CLIENT_TX_PUT) {
            uint32_t kv_count;
            if (!recv_all(sock, &kv_count, 4)) return false;
            msg.kv_pairs.clear();
            for (uint32_t i = 0; i < kv_count; i++) {
                uint32_t ksize, vsize;
                if (!recv_all(sock, &ksize, 4)) return false;
                if (!recv_all(sock, &vsize, 4)) return false;
                std::string key(ksize, '\0');
                std::vector<uint8_t> value(vsize);
                if (!recv_all(sock, (void*)key.data(), ksize)) return false;
                if (!recv_all(sock, value.data(), vsize)) return false;
                msg.kv_pairs.emplace_back(key, value);
            }
        }
    }
    return true;
}