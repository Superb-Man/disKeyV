#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
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
    Replica follower1(2, 1, Role::FOLLOWER, 0, {}, 4, 4);
    Replica follower2(3, 1, Role::FOLLOWER, 0, {}, 4, 4);
    Replica leader(1, 1, Role::LEADER, 0,
                   {follower1.listening_port(), follower2.listening_port()}, 4, 4);

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
    test_no_false_success_without_quorum();
    test_success_requires_replication_and_publication();
    test_follower_never_acks_failed_storage();
    test_input_limits();
    std::cout << "All correctness tests passed\n";
    return 0;
}
