#pragma once
#include <cstdint>
#include <vector>
#include <cstring>

struct ObjectEntry {
    uint64_t term_id;
    uint64_t seq_num;
    uint64_t incarnation;

    char key[64];
    std::vector<uint8_t> value;

    ObjectEntry(uint64_t t, uint64_t s, uint64_t i,
                const std::string& k,
                const std::vector<uint8_t>& v)
        : term_id(t), seq_num(s), incarnation(i), value(v) {
        std::memset(key, 0, 64);
        std::memcpy(key, k.data(), std::min(k.size(), size_t(63)));
    }
};
