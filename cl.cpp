#include <iostream>
#include <sstream>
#include <vector>
#include "network/socket_utils.hpp"
#include "network/message.hpp"

std::vector<uint8_t> parse_value(const std::string& val_str) {
    std::vector<uint8_t> result;
    std::stringstream ss(val_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        try {
            // Convert string to integer, then cast to uint8_t
            int num = std::stoi(item);
            result.push_back(static_cast<uint8_t>(num));
        } catch (...) {
            // Skip invalid numbers
            continue;
        }
    }
    return result;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cout << "Usage:\n";
        std::cout << " PUT: ./cl put <port> <key> <value_csv>\n";
        std::cout << " GET: ./cl get <port> <key>\n";
        std::cout << "Example: ./cl put 5000 mykey 1,2,3\n";
        return 1;
    }

    std::string op = argv[1];
    int port = std::stoi(argv[2]);
    std::string key = argv[3];

    int sock = connect_to("127.0.0.1", port);
    if (sock < 0) {
        std::cerr << "Failed to connect to port " << port << "\n";
        return 1;
    }

    NetMessage msg;

    if (op == "put") {
        if (argc < 5) {
            std::cerr << "Error: PUT requires value (e.g., 1,2,3)\n";
            close(sock);
            return 1;
        }
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

        std::cout << "PUT completed successfully\n";

    } else if (op == "get") {
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

    } else {
        std::cerr << "Unknown operation: " << op << "\n";
        close(sock);
        return 1;
    }

    close(sock);
    return 0;
}