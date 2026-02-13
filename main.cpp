#include <iostream>
#include <unistd.h>
#include "replica/replica.hpp"

int main(int argc, char** argv) {

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

        leader.submit_put("key", {1,2,3});
        leader.submit_put("key", {4,5,6});

        usleep(500000);

        leader.shutdown();

        ObjectEntry* e = leader.get("key");
        if (e) {
            std::cout << "SUCCESS\n";
            std::cout << "Term = " << e->term_id << "\n";
            std::cout << "Seq  = " << e->seq_num << "\n";
            std::cout << "Inc  = " << e->incarnation << "\n";
        }

    } else if (mode == "follower") {

        Replica follower(
            2,
            2,
            Role::FOLLOWER,
            port,
            {}
        );

        while (true)
            sleep(10);
    }

    return 0;
}