#include <iostream>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>
#include "logging/log.hpp"
#include "replica/replica.hpp"

namespace {
// Signal handlers only set this async-signal-safe flag. Replica teardown stays
// in the normal control flow where locks, joins, and logging are safe.
volatile std::sig_atomic_t shutdown_requested = 0;

void request_shutdown(int) {
    shutdown_requested = 1;
}

uint64_t replica_id_from_environment(uint64_t fallback) {
    const char* configured = std::getenv("DISKEYV_REPLICA_ID");
    if (configured == nullptr) return fallback;
    size_t consumed = 0;
    const uint64_t replica_id = std::stoull(configured, &consumed);
    if (consumed != std::string(configured).size()) {
        throw std::invalid_argument("DISKEYV_REPLICA_ID must be an integer");
    }
    return replica_id;
}

int worker_count_from_environment(int fallback) {
    const char* configured = std::getenv("DISKEYV_WORKERS");
    if (configured == nullptr) return fallback;
    size_t consumed = 0;
    const int workers = std::stoi(configured, &consumed);
    if (consumed != std::string(configured).size() || workers <= 0 ||
        workers > 64) {
        throw std::invalid_argument("DISKEYV_WORKERS must be within 1..64");
    }
    return workers;
}
}

int main(int argc, char** argv) {

    std::signal(SIGINT, request_shutdown);
    std::signal(SIGTERM, request_shutdown);
    std::signal(SIGPIPE, SIG_IGN);

    if (argc < 3) {
        std::cout << "Usage:\n";
        std::cout << "Leader  : ./node leader <port> <peer_host:port> ...\n";
        std::cout << "Recover : ./node recover <port> <peer_host:port> ...\n";
        std::cout << "Follower: ./node follower <port>\n";
        return 0;
    }

    std::string mode = argv[1];
    int port = std::stoi(argv[2]);

    if (mode == "leader" || mode == "recover") {

        std::vector<PeerEndpoint> peers;

        for (int i = 3; i < argc; i++)
            peers.push_back(PeerEndpoint::parse(argv[i]));

        Replica leader(
            replica_id_from_environment(1),
            worker_count_from_environment(4),
            Role::LEADER,
            port,
            peers,
            32,
            1024,
            4096,
            mode == "recover"
        );
        while (!shutdown_requested) sleep(1);
        leader.shutdown();

    } else if (mode == "follower") {

        Replica follower(
            replica_id_from_environment(2),
            worker_count_from_environment(2),
            Role::FOLLOWER,
            port,
            {}
        );

        while (!shutdown_requested) sleep(1);
        follower.shutdown();
    } else {
        DISKEYV_ERROR("PROCESS", "unknown-role=" << mode);
        std::cerr << "Unknown role: " << mode << "\n";
        return 1;
    }

    return 0;
}
