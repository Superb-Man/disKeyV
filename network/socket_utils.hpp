#pragma once
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <iostream>
#include <cstdint>

inline int create_server(int port) {
    if (port < 0 || port > 65535) return -1;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    int opt = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(sock);
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
        listen(sock, 128) < 0) {
        close(sock);
        return -1;
    }

    return sock;
}

// Timeouts bound later protocol reads and writes, not connect().
inline int connect_to(const std::string& ip, int port) {
    if (port <= 0 || port > 65535) return -1;
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    addrinfo* addresses = nullptr;
    const std::string service = std::to_string(port);
    if (getaddrinfo(ip.c_str(), service.c_str(), &hints, &addresses) != 0) {
        return -1;
    }

    int sock = -1;
    for (addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        sock = socket(address->ai_family, address->ai_socktype,
                      address->ai_protocol);
        if (sock < 0) continue;
        if (connect(sock, address->ai_addr, address->ai_addrlen) == 0) break;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(addresses);
    if (sock < 0) return -1;

    timeval timeout{};
    timeout.tv_sec = 2;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    return sock;
}

// TCP may complete only part of a buffer, so callers use these exact-length
// helpers instead of assuming one send/recv maps to one protocol field.
inline bool send_all(int sock, const void* buf, size_t len) {
    size_t total = 0;
    const char* data = (const char*)buf;

    while (total < len) {
#ifdef MSG_NOSIGNAL
        ssize_t s = send(sock, data + total, len - total, MSG_NOSIGNAL);
#else
        ssize_t s = send(sock, data + total, len - total, 0);
#endif
        if (s <= 0) return false;
        total += s;
    }
    return true;
}

inline bool recv_all(int sock, void* buf, size_t len) {
    size_t total = 0;
    char* data = (char*)buf;

    while (total < len) {
        ssize_t r = recv(sock, data + total, len - total, 0);
        if (r <= 0) return false;
        total += r;
    }
    return true;
}
