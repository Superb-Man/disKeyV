#include <algorithm>
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <vector>
#include "network/socket_utils.hpp"
#include "network/message.hpp"

std::vector<uint8_t> parse_value(const std::string& val_str) {
    std::vector<uint8_t> result;
    std::stringstream ss(val_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        try {
            int num = std::stoi(item);
            result.push_back(static_cast<uint8_t>(num));
        } catch (...) {
            continue;
        }
    }
    return result;
}

bool parse_u64(const char* text, uint64_t maximum, uint64_t& result) {
    try {
        size_t parsed = 0;
        const std::string input(text);
        if (input.empty() || input.front() == '-') return false;
        const unsigned long long value = std::stoull(input, &parsed);
        if (parsed != input.size() || value > maximum) return false;
        result = static_cast<uint64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<uint8_t> deterministic_value(uint64_t record_index,
                                         size_t value_size) {
    std::vector<uint8_t> value(value_size);
    for (size_t byte_index = 0; byte_index < value.size(); ++byte_index) {
        const uint64_t pattern =
            record_index * 131U + static_cast<uint64_t>(byte_index) * 17U;
        value[byte_index] = static_cast<uint8_t>(pattern % 251U);
    }
    return value;
}

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    if (argc < 3) {
        std::cout << "Usage:\n";
        std::cout << " PUT: ./cl put <port> <key> <value_csv>\n";
        std::cout << " GET: ./cl get <port> <key>\n";
        std::cout << " HEALTH: ./cl health <port>\n";
        std::cout << " LOAD: ./cl load <port> <key_prefix> <count> <value_size>\n";
        std::cout << " HOTLOAD: ./cl hotload <port> <key> <count> "
                     "<value_size> <seed>\n";
        std::cout << " VERIFY: ./cl verify <port> <key_prefix> <count> "
                     "<value_size> <term>\n";
        std::cout << "Set DISKEYV_HOST to connect to a non-local server.\n";
        std::cout << "Example: ./cl put 5000 mykey 1,2,3\n";
        return 1;
    }

    std::string op = argv[1];
    int port = std::stoi(argv[2]);

    const bool valid_arguments =
        (op == "put" && argc >= 5) || (op == "get" && argc >= 4) ||
        (op == "health" && argc >= 3) || (op == "load" && argc >= 6) ||
        (op == "hotload" && argc >= 7) ||
        (op == "verify" && argc >= 7);
    if (!valid_arguments) {
        std::cerr << "Invalid or incomplete operation: " << op << "\n";
        return 1;
    }

    const char* configured_host = std::getenv("DISKEYV_HOST");
    const std::string host = configured_host == nullptr
                                 ? "127.0.0.1"
                                 : configured_host;
    int sock = connect_to(host, port);
    if (sock < 0) {
        std::cerr << "Failed to connect to " << host << ':' << port << "\n";
        return 1;
    }

    NetMessage msg;

    if (op == "put") {
        std::string key = argv[3];
        std::string value_str = argv[4];
        std::vector<uint8_t> value = parse_value(value_str);

        msg.type = MsgType::CLIENT_PUT;
        msg.key = key;
        msg.value = value;

        if (!send_message(sock, msg)) {
            std::cerr << "Failed to send PUT request\n";
            close(sock);
            return 1;
        }

        NetMessage reply;
        if (!recv_message(sock, reply)) {
            std::cerr << "Failed to receive PUT reply\n";
            close(sock);
            return 1;
        }

        if (reply.type != MsgType::CLIENT_PUT_REPLY ||
            reply.status != OperationStatus::OK) {
            std::cerr << "PUT failed: " << status_name(reply.status) << "\n";
            close(sock);
            return 2;
        }
        std::cout << "PUT completed successfully\n";

    } else if (op == "get") {
        std::string key = argv[3];
        msg.type = MsgType::CLIENT_GET;
        msg.key = key;

        if (!send_message(sock, msg)) {
            std::cerr << "Failed to send GET request\n";
            close(sock);
            return 1;
        }

        NetMessage reply;
        if (!recv_message(sock, reply)) {
            std::cerr << "Failed to receive GET reply\n";
            close(sock);
            return 1;
        }

        if (reply.type != MsgType::CLIENT_GET_REPLY ||
            reply.status != OperationStatus::OK) {
            std::cerr << "GET failed: " << status_name(reply.status) << "\n";
            close(sock);
            return reply.status == OperationStatus::NOT_FOUND ? 3 : 2;
        }

        std::cout << "GET reply:\n";
        std::cout << "Term: " << reply.term << "\n";
        std::cout << "Seq : " << reply.seq << "\n";
        std::cout << "Inc : " << reply.incarnation << "\n";
        std::cout << "Value size: " << reply.value.size() << "\n";
        if (!reply.value.empty()) {
            std::cout << "Value: ";
            for (size_t i = 0; i < reply.value.size(); ++i) {
                if (i > 0) std::cout << ",";
                std::cout << static_cast<int>(reply.value[i]);
            }
            std::cout << "\n";
        }

    } else if (op == "load" || op == "hotload" || op == "verify") {
        const std::string prefix = argv[3];
        uint64_t count = 0;
        uint64_t value_size = 0;
        uint64_t expected_term = 0;
        uint64_t value_seed = 0;
        constexpr uint64_t kMaxBulkRecords = 1000000;
        if (!parse_u64(argv[4], kMaxBulkRecords, count) || count == 0 ||
            !parse_u64(argv[5], kMaxWireValueSize, value_size) ||
            (op == "verify" &&
             (!parse_u64(argv[6], std::numeric_limits<uint64_t>::max(),
                         expected_term) ||
              expected_term == 0)) ||
            (op == "hotload" &&
             !parse_u64(argv[6], std::numeric_limits<uint64_t>::max(), value_seed))) {
            std::cerr << "Invalid bulk count, value size, or term\n";
            close(sock);
            return 1;
        }

        constexpr uint64_t kPipelineWindow = 32;
        for (uint64_t first = 0; first < count;) {
            const uint64_t in_window = std::min(kPipelineWindow, count - first);
            for (uint64_t offset = 0; offset < in_window; ++offset) {
                const uint64_t record_index = first + offset;
                const std::string key = op == "hotload" ? prefix : prefix + std::to_string(record_index);

                if (key.empty() || key.size() > kMaxWireKeySize) {
                    std::cerr << "Generated key exceeds wire limit at record "
                              << record_index << "\n";
                    close(sock);
                    return 1;
                }
                NetMessage request;
                request.type = op == "verify" ? MsgType::CLIENT_GET : MsgType::CLIENT_PUT;
                request.key = key;
                if (op != "verify") {
                    request.value = deterministic_value(
                        value_seed + record_index,
                        static_cast<size_t>(value_size)
                    );
                }
                if (!send_message(sock, request)) {
                    std::cerr << "Bulk send failed at record " << record_index << "\n";
                    close(sock);
                    return 2;
                }
            }

            for (uint64_t offset = 0; offset < in_window; ++offset) {
                const uint64_t record_index = first + offset;
                NetMessage reply;
                if (!recv_message(sock, reply)) {
                    std::cerr << "Bulk receive failed at record "<< record_index << "\n";
                    close(sock);
                    return 2;
                }
                if (op != "verify") {
                    if (reply.type != MsgType::CLIENT_PUT_REPLY ||
                        reply.status != OperationStatus::OK) {
                        std::cerr << "Bulk PUT failed at record "
                                  << record_index << ": "
                                  << status_name(reply.status) << "\n";
                        close(sock);
                        return 2;
                    }
                } else {
                    const std::vector<uint8_t> expected = deterministic_value(
                        record_index, static_cast<size_t>(value_size)
                    );
                    
                    if (reply.type != MsgType::CLIENT_GET_REPLY ||
                        reply.status != OperationStatus::OK ||
                        reply.term != expected_term ||
                        reply.value != expected) {
                        std::cerr << "Bulk verification failed at record "
                                  << record_index << "\n";
                        close(sock);
                        return 2;
                    }
                }
            }
            first += in_window;
        }
        std::cout << (op == "verify" ? "Verified " : "Loaded ") << count
                  << " records with prefix " << prefix << "\n";

    } else if (op == "health") {
        msg.type = MsgType::CLIENT_HEALTH;
        if (!send_message(sock, msg)) {
            std::cerr << "Failed to send health request\n";
            close(sock);
            return 1;
        }
        NetMessage reply;
        if (!recv_message(sock, reply) ||
            reply.type != MsgType::CLIENT_HEALTH_REPLY ||
            reply.status != OperationStatus::OK) {
            std::cerr << "Health check failed\n";
            close(sock);
            return 2;
        }
        std::cout << "healthy\n";
    }

    close(sock);
    return 0;
}
