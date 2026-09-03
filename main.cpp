#include <iostream>
#include <csignal>
#include <unistd.h>
#include "replica/replica.hpp"

namespace {
volatile std::sig_atomic_t shutdown_requested = 0;

void request_shutdown(int) {
    shutdown_requested = 1;
}
}

int main(int argc, char** argv) {

    std::signal(SIGINT, request_shutdown);
    std::signal(SIGTERM, request_shutdown);
    std::signal(SIGPIPE, SIG_IGN);

    if (argc < 3) {
        std::cout << "Usage:\n";
        std::cout << "Leader  : ./node leader <port> <peer_port1> <peer_port2> ...\n";
        std::cout << "Follower: ./node follower <port>\n";
        return 0;
    }

    std::string mode = argv[1];
    int port = std::stoi(argv[2]);

    if (mode == "leader") {

        std::vector<int> peers;

        for (int i = 3; i < argc; i++)
            peers.push_back(std::stoi(argv[i]));

        Replica leader(
            1,
            4,
            Role::LEADER,
            port,
            peers
        );
        while (!shutdown_requested) sleep(1);
        leader.shutdown();

    } else if (mode == "follower") {

        Replica follower(
            2,
            2,
            Role::FOLLOWER,
            port,
            {}
        );

        while (!shutdown_requested) sleep(1);
        follower.shutdown();
    } else {
        std::cerr << "Unknown role: " << mode << "\n";
        return 1;
    }

    return 0;
}
