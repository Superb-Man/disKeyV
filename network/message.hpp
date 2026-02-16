#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "socket_utils.hpp"

enum class MsgType : uint8_t {
    PUT_REPL = 1,
    ACK      = 2,

    CLIENT_PUT = 3,
    CLIENT_GET = 4,
    CLIENT_GET_REPLY = 5,
    CLIENT_PUT_REPLY = 6
};


struct NetMessage {
    MsgType type;
    uint64_t term;
    uint64_t seq;
    uint64_t incarnation;
    std::string key;
    std::vector<uint8_t> value;
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

    return true;
}
