#pragma once
#include <atomic>
#include <vector>
#include <string>

struct IncarnationTable {
    // Hashed counters provide a cheap monotonic version source. Colliding keys
    // may skip incarnation values, but they can never move backwards.
    std::vector<std::atomic<uint64_t>> table;

    IncarnationTable(size_t sz) : table(sz) {
        for (auto& t : table) t.store(0);
    }

    uint64_t next(const std::string& key) {
        size_t h = std::hash<std::string>{}(key);
        // The returned incarnation starts at one; zero means "not assigned".
        return table[h % table.size()].fetch_add(1) + 1;
    }
};
