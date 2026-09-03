#pragma once
#include <arpa/inet.h>
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

inline int connect_to(const std::string& ip, int port) {
    if (port <= 0 || port > 65535) return -1;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
        close(sock);
        return -1;
    }

    if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    timeval timeout{};
    timeout.tv_sec = 2;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    return sock;
}

inline bool send_all(int sock, const void* buf, size_t len) {
    size_t total = 0;
    const char* data = (const char*)buf;

    while (total < len) {
        ssize_t s = send(sock, data + total, len - total, 0);
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
