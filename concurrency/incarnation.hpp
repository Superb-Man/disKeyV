#pragma once
#include <atomic>
#include <vector>
#include <string>

struct IncarnationTable {
    std::vector<std::atomic<uint64_t>> table;

    IncarnationTable(size_t sz) : table(sz) {
        for (auto& t : table) t.store(0);
    }

    uint64_t next(const std::string& key) {
        size_t h = std::hash<std::string>{}(key);
        return table[h % table.size()].fetch_add(1) + 1;
    }
};
