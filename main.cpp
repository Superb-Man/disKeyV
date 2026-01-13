#include <iostream>
#include <unistd.h>
#include "replica/replica.hpp"

int main() {
    std::cout << "🚀 Starting LoLKV...\n";
    Replica leader(1, 4); // replica_id=1, 4 workers

    leader.submit_put("key", {1,2,3});
    leader.submit_put("key", {4,5,6});

    usleep(100000); // 100ms

    // Critical: Wait for all requests to finish
    leader.shutdown();
    ObjectEntry* e = leader.get("key");
    if (e) {
        std::cout << "SUCCESS:\n";
        std::cout << "Term = " << e->term_id << "\n";
        std::cout << "Seq  = " << e->seq_num << "\n";
        std::cout << "Inc  = " << e->incarnation << "\n";
        std::cout << "Size = " << e->value.size() << "\n";
    } else {
        std::cout << "FAILED: Key not found!\n";
    }
    return 0;
}