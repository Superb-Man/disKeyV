#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <memory>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "network/message.hpp"
#include "replica/replica.hpp"

namespace {

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

NetMessage exchange_direct(Replica& replica, const NetMessage& request) {
    int sockets[2] = {-1, -1};
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0,
          "socketpair creation");
    check(send_message(sockets[0], request), "request serialization");
    replica.handle_connection(sockets[1]);
    NetMessage reply;
    check(recv_message(sockets[0], reply), "reply serialization");
    close(sockets[0]);
    return reply;
}

NetMessage exchange_tcp(int port, const NetMessage& request) {
    const int socket = connect_to("127.0.0.1", port);
    check(socket >= 0, "TCP connection");
    check(send_message(socket, request), "TCP request send");
    NetMessage reply;
    check(recv_message(socket, reply), "TCP reply receive");
    close(socket);
    return reply;
}

void store_replica_entry(Replica& follower, uint64_t worker_id,
                         uint64_t sequence, uint64_t incarnation,
                         uint64_t segment_index, uint64_t object_index,
                         const std::string& key, uint8_t value) {
    NetMessage message;
    message.type = MsgType::PUT_REPL;
    message.term = 1;
    message.seq = sequence;
    message.incarnation = incarnation;
    message.worker_id = worker_id;
    message.segment_index = segment_index;
    message.object_index = object_index;
    message.key = key;
    message.value = {value};
    const NetMessage reply = exchange_tcp(follower.listening_port(), message);
    check(reply.status == OperationStatus::OK && reply.seq == sequence,
          "seed follower recovery record");
}

int unused_tcp_port() {
    const int socket = create_server(0);
    check(socket >= 0, "temporary listening socket");
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    check(getsockname(socket, reinterpret_cast<sockaddr*>(&address), &length) ==
              0,
          "temporary port lookup");
    const int port = static_cast<int>(ntohs(address.sin_port));
    close(socket);
    return port;
}

int bound_tcp_port(int socket) {
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    check(getsockname(socket, reinterpret_cast<sockaddr*>(&address), &length) ==
              0,
          "listening port lookup");
    return static_cast<int>(ntohs(address.sin_port));
}

void test_replication_session_pipelines_batches() {
    const int server = create_server(0);
    check(server >= 0, "pipeline peer listener");
    const int port = bound_tcp_port(server);
    std::atomic<bool> received_window{false};

    std::thread peer([&] {
        const int client = accept(server, nullptr, nullptr);
        if (client < 0) return;
        NetMessage query;
        if (!recv_message(client, query) ||
            query.type != MsgType::PREFIX_QUERY) {
            close(client);
            return;
        }
        NetMessage prefix;
        prefix.type = MsgType::PREFIX_REPLY;
        prefix.status = OperationStatus::OK;
        prefix.term = query.term;
        prefix.worker_id = query.worker_id;
        if (!send_message(client, prefix)) {
            close(client);
            return;
        }

        std::vector<NetMessage> batches(3);
        for (NetMessage& batch : batches) {
            if (!recv_message(client, batch) ||
                batch.type != MsgType::PUT_REPL_BATCH) {
                close(client);
                return;
            }
        }
        received_window.store(true, std::memory_order_release);
        for (const NetMessage& batch : batches) {
            NetMessage ack;
            ack.type = MsgType::ACK;
            ack.status = OperationStatus::OK;
            ack.term = batch.term;
            ack.worker_id = batch.worker_id;
            ack.seq = batch.entries.back().sequence;
            if (!send_message(client, ack)) break;
        }
        close(client);
    });

    std::vector<ReplicationEntry> history;
    for (uint64_t sequence = 1; sequence <= 40; ++sequence) {
        ReplicationEntry entry;
        entry.sequence = sequence;
        entry.incarnation = sequence;
        entry.segment_index = 0;
        entry.object_index = sequence - 1;
        entry.key = "pipeline-" + std::to_string(sequence);
        entry.value = {static_cast<uint8_t>(sequence)};
        history.push_back(std::move(entry));
    }

    ReplicationSession session(PeerEndpoint("127.0.0.1", port));
    const bool replicated = session.replicate_to(
        1, 0, history.size(),
        [&](size_t first, size_t last) {
            NetMessage batch;
            batch.type = MsgType::PUT_REPL_BATCH;
            batch.term = 1;
            batch.worker_id = 0;
            batch.seq = history[first].sequence;
            const size_t end = std::min(last, first + kMaxWireBatchEntries);
            batch.entries.assign(history.begin() +
                                     static_cast<std::ptrdiff_t>(first),
                                 history.begin() +
                                     static_cast<std::ptrdiff_t>(end));
            return batch;
        });
    peer.join();
    close(server);
    check(replicated && session.highest_acked == history.size() &&
              received_window.load(std::memory_order_acquire),
          "replication session sends a bounded multi-batch window before ACKs");
}

void test_client_completes_at_early_quorum() {
    const int delayed_server = create_server(0);
    check(delayed_server >= 0, "delayed follower listener");
    const int delayed_port = bound_tcp_port(delayed_server);
    std::thread delayed_peer([&] {
        const int client = accept(delayed_server, nullptr, nullptr);
        if (client < 0) return;
        NetMessage query;
        if (!recv_message(client, query)) {
            close(client);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        NetMessage prefix;
        prefix.type = MsgType::PREFIX_REPLY;
        prefix.status = OperationStatus::OK;
        prefix.term = query.term;
        prefix.worker_id = query.worker_id;
        if (!send_message(client, prefix)) {
            close(client);
            return;
        }
        NetMessage batch;
        if (recv_message(client, batch)) {
            NetMessage ack;
            ack.type = MsgType::ACK;
            ack.status = OperationStatus::OK;
            ack.term = batch.term;
            ack.worker_id = batch.worker_id;
            ack.seq = batch.entries.back().sequence;
            send_message(client, ack);
        }
        close(client);
    });

    Replica healthy(2, 1, Role::FOLLOWER, 0, {}, 4, 8);
    {
        Replica leader(
            1, 1, Role::LEADER, 0,
            {PeerEndpoint("127.0.0.1", delayed_port),
             PeerEndpoint("127.0.0.1", healthy.listening_port())},
            4, 8);
        const auto started = std::chrono::steady_clock::now();
        const OperationStatus result = leader.submit_put("early-quorum", {9});
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started);
        check(result == OperationStatus::OK && elapsed.count() < 1000,
              "client completes before a delayed non-quorum follower ACK");
        check(healthy.get("early-quorum") != nullptr,
              "early completion still requires one follower plus the leader");
    }
    delayed_peer.join();
    close(delayed_server);
}

void test_message_defaults_and_round_trip() {
    NetMessage message;
    check(message.term == 0 && message.seq == 0 && message.incarnation == 0,
          "message metadata is zero-initialized");

    message.type = MsgType::PUT_REPL;
    message.status = OperationStatus::NO_QUORUM;
    message.term = 9;
    message.seq = 17;
    message.incarnation = 4;
    message.worker_id = 2;
    message.segment_index = 3;
    message.object_index = 8;
    message.key = "round-trip";
    message.value = {1, 2, 3};

    int sockets[2] = {-1, -1};
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0,
          "protocol socketpair creation");
    if (!send_message(sockets[0], message)) {
        std::cerr << "protocol send errno: " << std::strerror(errno) << '\n';
        check(false, "protocol send");
    }
    NetMessage received;
    check(recv_message(sockets[1], received), "protocol receive");
    close(sockets[0]);
    close(sockets[1]);

    check(received.type == message.type && received.status == message.status &&
              received.term == message.term && received.seq == message.seq &&
              received.worker_id == message.worker_id &&
              received.segment_index == message.segment_index &&
              received.object_index == message.object_index &&
              received.key == message.key && received.value == message.value,
          "protocol fields survive a round trip");
}

void test_batch_frame_round_trip() {
    NetMessage batch;
    batch.type = MsgType::PUT_REPL_BATCH;
    batch.term = 3;
    batch.seq = 10;
    batch.worker_id = 2;
    batch.entries = {
        ReplicationEntry{10, 7, 1, 4, "first", {1, 2}},
        ReplicationEntry{11, 8, 1, 5, "second", {3, 4, 5}}};

    int sockets[2] = {-1, -1};
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0,
          "batch socketpair creation");
    check(send_message(sockets[0], batch), "batch frame send");
    NetMessage received;
    check(recv_message(sockets[1], received), "batch frame receive");
    close(sockets[0]);
    close(sockets[1]);

    check(received.type == MsgType::PUT_REPL_BATCH &&
              received.term == batch.term && received.seq == batch.seq &&
              received.worker_id == batch.worker_id &&
              received.entries.size() == 2 &&
              received.entries[0].sequence == 10 &&
              received.entries[0].key == "first" &&
              received.entries[0].value == std::vector<uint8_t>({1, 2}) &&
              received.entries[1].sequence == 11 &&
              received.entries[1].segment_index == 1 &&
              received.entries[1].object_index == 5,
          "framed batch preserves its contiguous range and objects");
}

void test_endpoint_parsing() {
    const PeerEndpoint local = PeerEndpoint::parse("5001");
    check(local.host == "127.0.0.1" && local.port == 5001,
          "bare peer ports default to loopback");
    const PeerEndpoint container = PeerEndpoint::parse("follower1:5000");
    check(container.host == "follower1" && container.port == 5000,
          "container DNS peer endpoints are parsed");

    bool rejected = false;
    try {
        static_cast<void>(PeerEndpoint::parse("follower1:not-a-port"));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    check(rejected, "invalid peer endpoints are rejected");
}

void test_health_check() {
    Replica replica(1, 1, Role::LEADER, 0, {});
    NetMessage request;
    request.type = MsgType::CLIENT_HEALTH;
    const NetMessage reply = exchange_tcp(replica.listening_port(), request);
    check(reply.type == MsgType::CLIENT_HEALTH_REPLY &&
              reply.status == OperationStatus::OK,
          "listening replica answers health checks");
}

void test_no_false_success_without_quorum() {
    Replica leader(1, 1, Role::LEADER, 0, {1}, 2, 4);
    NetMessage request;
    request.type = MsgType::CLIENT_PUT;
    request.key = "key";
    request.value = {7};
    const NetMessage reply = exchange_tcp(leader.listening_port(), request);
    check(reply.type == MsgType::CLIENT_PUT_REPLY &&
              reply.status == OperationStatus::NO_QUORUM,
          "PUT reports NO_QUORUM when its follower is unavailable");
    check(leader.get("key") == nullptr,
          "failed PUT is not published in the leader index");

    NetMessage get_request;
    get_request.type = MsgType::CLIENT_GET;
    get_request.key = "key";
    const NetMessage get_reply = exchange_tcp(leader.listening_port(), get_request);
    check(get_reply.status == OperationStatus::NOT_FOUND &&
              get_reply.term == 0 && get_reply.seq == 0 &&
              get_reply.incarnation == 0 && get_reply.value.empty(),
          "missing-key replies contain initialized metadata");
}

void test_success_requires_replication_and_publication() {
    Replica follower1(2, 1, Role::FOLLOWER, 0, {}, 4, 8);
    Replica follower2(3, 1, Role::FOLLOWER, 0, {}, 4, 8);
    Replica leader(1, 1, Role::LEADER, 0,
                   {PeerEndpoint("localhost", follower1.listening_port()),
                    PeerEndpoint("localhost", follower2.listening_port())},
                   4, 8);

    NetMessage request;
    request.type = MsgType::CLIENT_PUT;
    request.key = "replicated";
    request.value = {4, 2};
    const NetMessage reply = exchange_tcp(leader.listening_port(), request);
    check(reply.type == MsgType::CLIENT_PUT_REPLY &&
              reply.status == OperationStatus::OK,
          "three-replica PUT succeeds");
    ObjectEntry* leader_entry = leader.get("replicated");
    ObjectEntry* follower1_entry = follower1.get("replicated");
    ObjectEntry* follower2_entry = follower2.get("replicated");
    check(leader_entry != nullptr && follower1_entry != nullptr &&
              follower2_entry != nullptr,
          "successful PUT is published on all reachable replicas");
    check(leader_entry->value == std::vector<uint8_t>({4, 2}) &&
              follower1_entry->value == leader_entry->value &&
              follower2_entry->value == leader_entry->value,
          "replicated values match");

    request.key = "second";
    request.value = {8};
    check(exchange_tcp(leader.listening_port(), request).status ==
              OperationStatus::OK,
          "second PUT succeeds over existing replication sessions");
    request.key = "third";
    request.value = {9};
    check(exchange_tcp(leader.listening_port(), request).status ==
              OperationStatus::OK,
          "third PUT succeeds over existing replication sessions");
    check(follower1.accepted_connection_count() == 1 &&
              follower2.accepted_connection_count() == 1,
          "each worker reuses one persistent connection per follower");
}

void test_contiguous_prefix_acknowledgements() {
    Replica follower(2, 1, Role::FOLLOWER, 0, {}, 2, 4);

    NetMessage message;
    message.type = MsgType::PUT_REPL;
    message.term = 1;
    message.incarnation = 1;
    message.worker_id = 7;
    message.segment_index = 0;
    message.key = "ordered";
    message.value = {1};

    message.seq = 2;
    message.object_index = 1;
    NetMessage reply = exchange_direct(follower, message);
    check(reply.status == OperationStatus::OUT_OF_ORDER && reply.seq == 0,
          "a follower rejects a sequence gap and reports its watermark");

    message.seq = 1;
    message.object_index = 0;
    reply = exchange_direct(follower, message);
    check(reply.status == OperationStatus::OK && reply.seq == 1,
          "the first contiguous entry advances the watermark");

    reply = exchange_direct(follower, message);
    check(reply.status == OperationStatus::OK && reply.seq == 1,
          "an identical retry is idempotently acknowledged");

    message.value = {99};
    reply = exchange_direct(follower, message);
    check(reply.status == OperationStatus::INVALID_REQUEST && reply.seq == 1,
          "a conflicting duplicate is not acknowledged");

    message.seq = 2;
    message.object_index = 1;
    message.incarnation = 2;
    message.value = {2};
    reply = exchange_direct(follower, message);
    check(reply.status == OperationStatus::OK && reply.seq == 2,
          "filling the next sequence advances the cumulative watermark");
}

void test_batch_acknowledgement_and_prefix_query() {
    Replica follower(2, 1, Role::FOLLOWER, 0, {}, 2, 4);

    NetMessage batch;
    batch.type = MsgType::PUT_REPL_BATCH;
    batch.term = 1;
    batch.seq = 1;
    batch.worker_id = 3;
    batch.entries = {
        ReplicationEntry{1, 1, 0, 0, "batch-one", {1}},
        ReplicationEntry{2, 1, 0, 1, "batch-two", {2}}};
    const NetMessage reply = exchange_direct(follower, batch);
    check(reply.status == OperationStatus::OK && reply.seq == 2,
          "one ACK covers a fully stored contiguous batch");
    check(follower.received_batch_count() == 1 &&
              follower.largest_batch_count() == 2,
          "follower records the bounded batch operation");
    check(follower.get("batch-one") != nullptr &&
              follower.get("batch-two") != nullptr,
          "every object in an acknowledged batch is published");

    NetMessage query;
    query.type = MsgType::PREFIX_QUERY;
    query.term = 1;
    query.worker_id = 3;
    const NetMessage progress = exchange_direct(follower, query);
    check(progress.type == MsgType::PREFIX_REPLY &&
              progress.status == OperationStatus::OK && progress.seq == 2,
          "prefix query returns the follower's stored worker prefix");
}

void test_partial_batch_acknowledges_only_stored_prefix() {
    Replica follower(2, 1, Role::FOLLOWER, 0, {}, 1, 1);

    NetMessage batch;
    batch.type = MsgType::PUT_REPL_BATCH;
    batch.term = 1;
    batch.seq = 1;
    batch.worker_id = 0;
    batch.entries = {
        ReplicationEntry{1, 1, 0, 0, "stored", {1}},
        ReplicationEntry{2, 1, 0, 1, "not-stored", {2}}};
    const NetMessage reply = exchange_direct(follower, batch);
    check(reply.status == OperationStatus::STORAGE_ERROR && reply.seq == 1,
          "partial batch ACK exposes only its completely stored prefix");
    check(follower.get("stored") != nullptr &&
              follower.get("not-stored") == nullptr,
          "failed suffix is never made visible");
}

void test_late_follower_and_restart_prefix_repair() {
    Replica follower1(2, 1, Role::FOLLOWER, 0, {}, 16, 32, 128);
    const int repair_port = unused_tcp_port();
    Replica leader(
        1, 1, Role::LEADER, 0,
        {PeerEndpoint("127.0.0.1", follower1.listening_port()),
         PeerEndpoint("127.0.0.1", repair_port)},
        16, 32, 128);

    for (uint8_t value = 1; value <= 3; ++value) {
        check(leader.submit_put("before-" + std::to_string(value), {value}) ==
                  OperationStatus::OK,
              "writes retain quorum while one follower is absent");
    }

    auto late_follower = std::make_unique<Replica>(
        3, 1, Role::FOLLOWER, repair_port, std::vector<PeerEndpoint>{},
        16, 32, 128);
    check(leader.submit_put("catch-up-trigger", {4}) == OperationStatus::OK,
          "write succeeds while the late follower catches up");
    for (uint8_t value = 1; value <= 3; ++value) {
        ObjectEntry* entry =
            late_follower->get("before-" + std::to_string(value));
        check(entry != nullptr && entry->value == std::vector<uint8_t>({value}),
              "late follower receives the missing SegmentStore prefix");
    }
    check(late_follower->largest_batch_count() >= 4,
          "late follower repair uses a multi-object batch");

    late_follower->shutdown();
    late_follower.reset();
    auto restarted_follower = std::make_unique<Replica>(
        3, 1, Role::FOLLOWER, repair_port, std::vector<PeerEndpoint>{},
        16, 32, 128);
    check(leader.submit_put("restart-trigger", {5}) == OperationStatus::OK,
          "session reconnects and repairs a restarted follower");
    check(restarted_follower->get("before-1") != nullptr &&
              restarted_follower->get("catch-up-trigger") != nullptr &&
              restarted_follower->get("restart-trigger") != nullptr,
          "restarted follower is rebuilt through the complete worker prefix");
    check(restarted_follower->largest_batch_count() >= 5,
          "restart repair remains bounded and batched");
}

void test_lolkv_per_worker_recovery_consolidation() {
    Replica follower1(2, 1, Role::FOLLOWER, 0, {}, 8, 8, 64);
    Replica follower2(3, 1, Role::FOLLOWER, 0, {}, 8, 8, 64);

    store_replica_entry(follower1, 0, 1, 1, 0, 0, "alpha", 1);
    store_replica_entry(follower1, 0, 2, 1, 0, 1, "shared", 10);
    store_replica_entry(follower1, 1, 1, 1, 1, 0, "beta", 2);

    store_replica_entry(follower2, 0, 1, 1, 0, 0, "alpha", 1);
    store_replica_entry(follower2, 1, 1, 1, 1, 0, "beta", 2);
    store_replica_entry(follower2, 1, 2, 2, 1, 1, "shared", 20);

    Replica recovered(
        1, 2, Role::LEADER, 0,
        {PeerEndpoint("127.0.0.1", follower1.listening_port()),
         PeerEndpoint("127.0.0.1", follower2.listening_port())},
        8, 8, 64, true);

    check(recovered.rs.current_term.load(std::memory_order_acquire) == 2 &&
              follower1.rs.current_term.load(std::memory_order_acquire) == 2 &&
              follower2.rs.current_term.load(std::memory_order_acquire) == 2,
          "recovery fences the old leader before serving in a new term");
    check(recovered.get("alpha") != nullptr &&
              recovered.get("beta") != nullptr,
          "leader reconstructs independent worker prefixes");
    ObjectEntry* shared = recovered.get("shared");
    check(shared != nullptr && shared->value == std::vector<uint8_t>({20}) &&
              shared->incarnation == 2,
          "reconstructed index selects the newest same-key incarnation");

    check(follower1.get("shared") != nullptr &&
              follower1.get("shared")->value ==
                  std::vector<uint8_t>({20}) &&
              follower2.get("shared") != nullptr,
          "recovery completes and repairs different follower suffixes");

    NetMessage stale;
    stale.type = MsgType::PUT_REPL;
    stale.term = 1;
    stale.seq = 3;
    stale.incarnation = 3;
    stale.worker_id = 0;
    stale.segment_index = 0;
    stale.object_index = 2;
    stale.key = "stale";
    stale.value = {9};
    check(exchange_tcp(follower1.listening_port(), stale).status ==
              OperationStatus::INVALID_REQUEST,
          "fenced follower rejects an old-term leader write");

    check(recovered.submit_put("after-recovery", {7}) == OperationStatus::OK,
          "recovered leader serves writes only after consolidation");
    ObjectEntry* fresh = recovered.get("after-recovery");
    check(fresh != nullptr && fresh->term_id == 2 && fresh->seq_num == 1,
          "new-term worker sequence starts at one after recovery");
}

void test_recovery_requires_every_configured_survivor() {
    Replica follower(2, 1, Role::FOLLOWER, 0, {}, 4, 4, 16);
    const int unavailable_port = unused_tcp_port();
    bool rejected = false;
    try {
        Replica unavailable_recovery(
            1, 1, Role::LEADER, 0,
            {PeerEndpoint("127.0.0.1", follower.listening_port()),
             PeerEndpoint("127.0.0.1", unavailable_port)},
            4, 4, 16, true);
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    check(rejected &&
              follower.rs.current_term.load(std::memory_order_acquire) == 1,
          "empty static leader does not serve from an incomplete recovery set");
}

void test_recovery_across_multiple_terms() {
    Replica follower1(2, 1, Role::FOLLOWER, 0, {}, 8, 8, 64);
    Replica follower2(3, 1, Role::FOLLOWER, 0, {}, 8, 8, 64);
    store_replica_entry(follower1, 0, 1, 1, 0, 0, "term-one", 1);
    store_replica_entry(follower2, 0, 1, 1, 0, 0, "term-one", 1);
    const std::vector<PeerEndpoint> peers = {
        PeerEndpoint("127.0.0.1", follower1.listening_port()),
        PeerEndpoint("127.0.0.1", follower2.listening_port())};

    auto term_two = std::make_unique<Replica>(
        1, 1, Role::LEADER, 0, peers, 8, 8, 64, true);
    check(term_two->submit_put("term-two", {2}) == OperationStatus::OK,
          "first recovered term commits a new operation");
    term_two->shutdown();
    term_two.reset();

    Replica term_three(1, 1, Role::LEADER, 0, peers, 8, 8, 64, true);
    check(term_three.rs.current_term.load(std::memory_order_acquire) == 3 &&
              term_three.get("term-one") != nullptr &&
              term_three.get("term-two") != nullptr,
          "later recovery reconstructs complete histories across terms");
    check(term_three.submit_put("term-three", {3}) == OperationStatus::OK,
          "third term starts only after repeated consolidation completes");
    ObjectEntry* newest = term_three.get("term-three");
    check(newest != nullptr && newest->term_id == 3 && newest->seq_num == 1,
          "worker sequence numbers reset independently in each new term");
}

void test_follower_never_acks_failed_storage() {
    Replica follower(2, 1, Role::FOLLOWER, 0, {}, 1, 1);

    NetMessage first;
    first.type = MsgType::PUT_REPL;
    first.term = 1;
    first.seq = 1;
    first.incarnation = 1;
    first.worker_id = 0;
    first.segment_index = 0;
    first.object_index = 0;
    first.key = "hot";
    first.value = {1};
    check(exchange_direct(follower, first).status == OperationStatus::OK,
          "follower ACKs a stored object");

    NetMessage overflow = first;
    overflow.seq = 2;
    overflow.incarnation = 2;
    overflow.object_index = 1;
    overflow.value = {2};
    check(exchange_direct(follower, overflow).status ==
              OperationStatus::STORAGE_ERROR,
          "follower rejects an object it cannot store");

    ObjectEntry* visible = follower.get("hot");
    check(visible != nullptr && visible->value == std::vector<uint8_t>({1}),
          "failed replicated write is not published");
}

void test_input_limits() {
    Replica leader(1, 1, Role::LEADER, 0, {}, 2, 2);
    check(leader.submit_put(std::string(64, 'x'), {1}) ==
              OperationStatus::INVALID_REQUEST,
          "long keys are rejected instead of truncated");
    check(leader.submit_put("", {1}) == OperationStatus::INVALID_REQUEST,
          "empty keys are rejected");
}

}  // namespace

int main() {
    test_message_defaults_and_round_trip();
    test_batch_frame_round_trip();
    test_endpoint_parsing();
    test_health_check();
    test_replication_session_pipelines_batches();
    test_client_completes_at_early_quorum();
    test_no_false_success_without_quorum();
    test_success_requires_replication_and_publication();
    test_contiguous_prefix_acknowledgements();
    test_batch_acknowledgement_and_prefix_query();
    test_partial_batch_acknowledges_only_stored_prefix();
    test_late_follower_and_restart_prefix_repair();
    test_lolkv_per_worker_recovery_consolidation();
    test_recovery_requires_every_configured_survivor();
    test_recovery_across_multiple_terms();
    test_follower_never_acks_failed_storage();
    test_input_limits();
    std::cout << "All correctness tests passed\n";
    return 0;
}
