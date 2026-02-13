#include <iostream>
#include <unistd.h>
#include "replica/replica.hpp"

int main() {

    std::cout << "🚀 Starting LoLKV...\n";

    // 3 total replicas (1 leader + 2 followers)
    Replica leader(1, 4, 3);

    leader.submit_put("key", {1,2,3});
    leader.submit_put("key", {4,5,6});

    // allow worker/apply threads to run
    usleep(200000); // 200ms

    leader.shutdown();

    ObjectEntry* e = leader.get("key");

    if (e) {
        std::cout << "✅ SUCCESS:\n";
        std::cout << "Term = " << e->term_id << "\n";
        std::cout << "Seq  = " << e->seq_num << "\n";
        std::cout << "Inc  = " << e->incarnation << "\n";
        std::cout << "Size = " << e->value.size() << "\n";
    } else {
        std::cout << "❌ FAILED: Key not found!\n";
    }

    return 0;
}
