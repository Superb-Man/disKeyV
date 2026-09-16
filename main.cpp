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

bool election_enabled_from_environment() {
    const char* configured = std::getenv("DISKEYV_ELECTION_ENABLED");
    if (configured == nullptr || std::string(configured) == "1" ||
        std::string(configured) == "true") {
        return true;
    }
    if (std::string(configured) == "0" ||
        std::string(configured) == "false") {
        return false;
    }
    throw std::invalid_argument(
        "DISKEYV_ELECTION_ENABLED must be true, false, 1, or 0");
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
        std::cout << "Follower: ./node follower <port> <peer_host:port> ...\n";
        std::cout << "\nEvery node must list every other voting replica.\n";
        return 0;
    }

    const std::string mode = argv[1];
    const int port = std::stoi(argv[2]);
    if (port <= 0 || port > 65535) {
        throw std::invalid_argument("port must be within 1..65535");
    }

    if (mode != "leader" && mode != "recover" && mode != "follower") {
        DISKEYV_ERROR("PROCESS", "unknown-role=" << mode);
        std::cerr << "Unknown role: " << mode << "\n";
        return 1;
    }

    std::vector<PeerEndpoint> peers;
    for (int i = 3; i < argc; ++i) {
        peers.push_back(PeerEndpoint::parse(argv[i]));
    }

    // The command-line role only controls startup behavior. A preferred leader
    // campaigns immediately; followers wait for a heartbeat timeout. After
    // startup, every process can vote, fail over, and become the sole leader.
    const bool preferred_leader = mode == "leader" || mode == "recover";
    const bool recover = mode == "recover";
    const uint64_t fallback_replica_id = preferred_leader ? 1 : 2;
    const int fallback_workers = preferred_leader ? 4 : 2;

    Replica replica(
        replica_id_from_environment(fallback_replica_id),
        worker_count_from_environment(fallback_workers),
        preferred_leader ? Role::LEADER : Role::FOLLOWER,
        port,
        peers,
        32,
        1024,
        4096,
        recover,
        election_enabled_from_environment()
    );

    while (!shutdown_requested) sleep(1);
    replica.shutdown();

    return 0;
}
