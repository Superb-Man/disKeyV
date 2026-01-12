#pragma once
#include <vector>
#include <cstring>
#include <string>

struct ObjectEntry {
    uint64_t term_id;
    uint64_t seq_num;
    uint64_t incarnation;

    char key[64];
    std::vector<uint8_t> value;

        ObjectEntry() : term_id(0), seq_num(0), incarnation(0) {
        std::memset(key, 0, sizeof(key));
    }
    

    ObjectEntry(const ObjectEntry& other)
        : term_id(other.term_id), seq_num(other.seq_num),
          incarnation(other.incarnation), value(other.value) {
        std::memcpy(key, other.key, sizeof(key));
    }

    ObjectEntry(uint64_t t, uint64_t s, uint64_t i,
                const std::string& k,
                const std::vector<uint8_t>& v)
        : term_id(t), seq_num(s), incarnation(i), value(v) {
        std::memset(key, 0, sizeof(key));
        std::memcpy(key, k.c_str(), std::min(k.size(), sizeof(key) - 1));
    }

    ObjectEntry& operator=(const ObjectEntry& other) {
        if (this != &other) {
            term_id = other.term_id;
            seq_num = other.seq_num;
            incarnation = other.incarnation;
            value = other.value;
            std::memcpy(key, other.key, sizeof(key));
        }
        return *this;
    }
};
