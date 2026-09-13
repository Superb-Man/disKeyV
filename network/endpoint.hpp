#pragma once

#include <stdexcept>
#include <string>
#include <utility>

struct PeerEndpoint {
    PeerEndpoint(int endpoint_port)
        : host("127.0.0.1"), port(endpoint_port) {
        validate();
    }

    PeerEndpoint(std::string endpoint_host, int endpoint_port)
        : host(std::move(endpoint_host)), port(endpoint_port) {
        validate();
    }

    static PeerEndpoint parse(const std::string& text) {
        const size_t separator = text.rfind(':');
        if (separator == std::string::npos) {
            return PeerEndpoint(parse_port(text));
        }
        if (separator == 0 || separator + 1 >= text.size()) {
            throw std::invalid_argument("peer must be PORT or HOST:PORT");
        }
        return PeerEndpoint(text.substr(0, separator),
                            parse_port(text.substr(separator + 1)));
    }

    std::string identity() const { return host + ":" + std::to_string(port); }

    std::string host;
    int port;

private:
    static int parse_port(const std::string& text) {
        size_t consumed = 0;
        int parsed = 0;
        try {
            parsed = std::stoi(text, &consumed);
        } catch (const std::exception&) {
            throw std::invalid_argument("invalid peer port: " + text);
        }
        if (consumed != text.size()) {
            throw std::invalid_argument("invalid peer port: " + text);
        }
        if (parsed <= 0 || parsed > 65535) {
            throw std::invalid_argument("peer port is outside 1..65535");
        }
        return parsed;
    }

    void validate() const {
        if (host.empty()) throw std::invalid_argument("peer host is empty");
        if (port <= 0 || port > 65535) {
            throw std::invalid_argument("peer port is outside 1..65535");
        }
    }
};
