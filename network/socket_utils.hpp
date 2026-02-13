#pragma once
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <iostream>

inline int create_server(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bind(sock, (sockaddr*)&addr, sizeof(addr));
    listen(sock, 16);

    return sock;
}

inline int connect_to(const std::string& ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0)
        return -1;

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
