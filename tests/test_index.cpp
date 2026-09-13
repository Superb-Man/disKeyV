#include <cstdlib>
#include <atomic>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "index/hash_table.hpp"
#include "replica/replica.hpp"

namespace {

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

uint64_t append(SegmentStore& store, size_t segment_index,
                const std::string& key, uint64_t incarnation,
                uint64_t term = 1) {
    ObjectEntry entry(term, incarnation, incarnation, key,
                      {static_cast<uint8_t>(incarnation)});
    return store.segments[segment_index]->append(entry);
}

void test_term_then_incarnation_ordering() {
    SegmentStore store(1, 3);
    check(store.acquire_free_segment(0, 1) == 0, "acquire ordering segment");
    check(append(store, 0, "versioned", 5, 2) == 0,
          "append initial ordered version");
    check(append(store, 0, "versioned", 999, 1) == 1,
          "append older-term version");
    check(append(store, 0, "versioned", 1, 3) == 2,
          "append newer-term version");

    HashTable index(2);
    check(index.apply(store, 0, 0) == IndexApplyResult::APPLIED,
          "publish initial ordered version");
    check(index.apply(store, 0, 1) == IndexApplyResult::SUPERSEDED,
          "older term loses despite a higher incarnation");
    check(index.apply(store, 0, 2) == IndexApplyResult::APPLIED,
          "newer term wins despite a lower incarnation");
    ObjectEntry* result = index.get(store, "versioned");
    check(result != nullptr && result->term_id == 3 && result->incarnation == 1,
          "index retains greatest term/incarnation tuple");
}

void test_segment_zero_object_zero_is_representable() {
    SegmentStore store(1, 2);
    check(store.acquire_free_segment(0, 1) == 0, "acquire segment zero");
    check(append(store, 0, "first", 1) == 0, "append object zero");

    HashTable index(4);
    check(index.apply(store, 0, 0) == IndexApplyResult::APPLIED,
          "publish segment zero/object zero");
    ObjectEntry* result = index.get(store, "first");
    check(result != nullptr && result->incarnation == 1,
          "resolve segment zero/object zero");
}

void test_linear_probe_collision() {
    constexpr size_t index_capacity = 2;
    const std::string first = "collision-0";
    const size_t bucket =
        std::hash<std::string>{}(first) % index_capacity;
    std::string second;
    for (size_t candidate = 1; candidate < 10000; ++candidate) {
        const std::string key = "collision-" + std::to_string(candidate);
        if (std::hash<std::string>{}(key) % index_capacity == bucket) {
            second = key;
            break;
        }
    }
    check(!second.empty(), "find deterministic collision");

    SegmentStore store(1, 2);
    check(store.acquire_free_segment(0, 1) == 0, "acquire collision segment");
    check(append(store, 0, first, 1) == 0, "append first collision key");
    check(append(store, 0, second, 2) == 1, "append second collision key");

    HashTable index(index_capacity);
    check(index.apply(store, 0, 0) == IndexApplyResult::APPLIED,
          "publish first collision key");
    check(index.apply(store, 0, 1) == IndexApplyResult::APPLIED,
          "linear probe publishes second collision key");
    check(index.get(store, first) != nullptr &&
              index.get(store, second) != nullptr,
          "both colliding keys remain readable");
}

void test_full_table_and_existing_key_replacement() {
    SegmentStore store(1, 3);
    check(store.acquire_free_segment(0, 1) == 0, "acquire full-table segment");
    check(append(store, 0, "a", 1) == 0, "append first key");
    check(append(store, 0, "b", 1) == 1, "append second key");
    check(append(store, 0, "a", 2) == 2, "append replacement key");

    HashTable index(1);
    check(index.apply(store, 0, 0) == IndexApplyResult::APPLIED,
          "fill the only index slot");
    check(index.apply(store, 0, 1) == IndexApplyResult::FULL,
          "report full table for a new key");
    check(index.apply(store, 0, 2) == IndexApplyResult::APPLIED,
          "replace an existing key even when the table is full");
    ObjectEntry* result = index.get(store, "a");
    check(result != nullptr && result->incarnation == 2,
          "replacement points to the newer version");
}

void test_concurrent_same_key_publication() {
    constexpr size_t version_count = 64;
    SegmentStore store(1, version_count);
    check(store.acquire_free_segment(0, 1) == 0,
          "acquire concurrent publication segment");
    for (size_t version = 1; version <= version_count; ++version) {
        check(append(store, 0, "shared", version) == version - 1,
              "append concurrent version");
    }

    HashTable index(8);
    std::atomic<bool> publication_complete{false};
    std::atomic<bool> reader_observed_valid_data{true};
    std::vector<std::thread> readers;
    for (size_t reader = 0; reader < 4; ++reader) {
        readers.emplace_back([&index, &store, &publication_complete,
                              &reader_observed_valid_data] {
            while (!publication_complete.load(std::memory_order_acquire)) {
                ObjectEntry* entry = index.get(store, "shared");
                if (entry != nullptr &&
                    (std::string(entry->key) != "shared" ||
                     entry->incarnation == 0 ||
                     entry->incarnation > version_count)) {
                    reader_observed_valid_data.store(false,
                                                     std::memory_order_release);
                }
            }
        });
    }
    std::vector<std::thread> publishers;
    publishers.reserve(version_count);
    for (size_t version = 1; version <= version_count; ++version) {
        publishers.emplace_back([&index, &store, version] {
            const IndexApplyResult result = index.apply(store, 0, version - 1);
            check(index_apply_succeeded(result),
                  "concurrent same-key publication result");
        });
    }
    for (auto& publisher : publishers) publisher.join();
    publication_complete.store(true, std::memory_order_release);
    for (auto& reader : readers) reader.join();

    ObjectEntry* result = index.get(store, "shared");
    check(reader_observed_valid_data.load(std::memory_order_acquire) &&
              result != nullptr && result->incarnation == version_count,
          "concurrent publication converges on the newest incarnation");
}

void test_client_observes_index_exhaustion() {
    Replica leader(1, 1, Role::LEADER, 0, {}, 1, 4, 1);
    check(leader.submit_put("first", {1}) == OperationStatus::OK,
          "first key fits in the index");
    check(leader.submit_put("second", {2}) == OperationStatus::INDEX_FULL,
          "client receives INDEX_FULL for an unpublishable committed key");
    check(leader.get("first") != nullptr && leader.get("second") == nullptr,
          "index exhaustion does not overwrite another key");
}

}  // namespace

int main() {
    test_segment_zero_object_zero_is_representable();
    test_term_then_incarnation_ordering();
    test_linear_probe_collision();
    test_full_table_and_existing_key_replacement();
    test_concurrent_same_key_publication();
    test_client_observes_index_exhaustion();
    std::cout << "All index tests passed\n";
    return 0;
}
