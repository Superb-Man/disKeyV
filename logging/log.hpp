#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <functional>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/syscall.h>
#include <unistd.h>

namespace diskeyv_log {

enum class Level : uint8_t {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    OFF = 4
};

inline const char* level_name(Level level) {
    switch (level) {
        case Level::DEBUG: return "D";
        case Level::INFO: return "I";
        case Level::WARN: return "W";
        case Level::ERROR: return "E";
        case Level::OFF: return "O";
    }
    return "?";
}

// Parse once so disabled debug sites pay only an enum comparison afterward.
inline Level configured_level() {
    static const Level configured = [] {
        const char* value = std::getenv("DISKEYV_LOG_LEVEL");
        if (value == nullptr) return Level::INFO;
        const std::string level(value);
        if (level == "DEBUG" || level == "D") return Level::DEBUG;
        if (level == "INFO" || level == "I") return Level::INFO;
        if (level == "WARN" || level == "W") return Level::WARN;
        if (level == "ERROR" || level == "E") return Level::ERROR;
        if (level == "OFF" || level == "O") return Level::OFF;
        return Level::INFO;
    }();
    return configured;
}

inline bool enabled(Level level) {
    return level >= configured_level() && configured_level() != Level::OFF;
}

inline const char* basename(const char* path) {
    const char* name = path;
    for (const char* cursor = path; *cursor != '\0'; ++cursor) {
        if (*cursor == '/') name = cursor + 1;
    }
    return name;
}

// Format one logcat-style record and issue it as one write. The process-local
// mutex prevents worker threads from interleaving their records.
inline void emit(Level level, const char* tag, const std::string& message,
                 const char* source_file, int source_line) {
    if (!enabled(level)) return;

    const auto now = std::chrono::system_clock::now();
    const auto milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) %
        1000;
    const std::time_t current_time = std::chrono::system_clock::to_time_t(now);
    std::tm local_time{};
    localtime_r(&current_time, &local_time);

    std::ostringstream line;
    line << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S") << '.'
         << std::setfill('0') << std::setw(3) << milliseconds.count() << ' '
         << level_name(level) << '/' << tag << '(' << getpid() << ':'
         << ::syscall(SYS_gettid) << ") "
         << message << " [" << basename(source_file) << ':' << source_line
         << "]\n";
    const std::string output = line.str();

    static std::mutex write_mutex;
    std::lock_guard<std::mutex> lock(write_mutex);
    // A shared append-only file lets several local replicas feed one follower
    // terminal; stderr remains the natural path for Docker/Podman collection.
    const char* configured_file = std::getenv("DISKEYV_LOG_FILE");
    if (configured_file == nullptr || configured_file[0] == '\0') {
        static_cast<void>(
            ::write(STDERR_FILENO, output.data(), output.size()));
        return;
    }

    const int descriptor =
        ::open(configured_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (descriptor < 0) {
        static_cast<void>(
            ::write(STDERR_FILENO, output.data(), output.size()));
        return;
    }
    static_cast<void>(::write(descriptor, output.data(), output.size()));
    close(descriptor);
}

}  // namespace diskeyv_log

#define DISKEYV_LOG(level, tag, expression)                                  \
    do {                                                                      \
        if (::diskeyv_log::enabled(level)) {                                  \
            std::ostringstream diskeyv_log_stream;                            \
            diskeyv_log_stream << expression;                                \
            ::diskeyv_log::emit(level, tag, diskeyv_log_stream.str(),         \
                                __FILE__, __LINE__);                           \
        }                                                                     \
    } while (false)

#define DISKEYV_DEBUG(tag, expression)                                        \
    DISKEYV_LOG(::diskeyv_log::Level::DEBUG, tag, expression)
#define DISKEYV_INFO(tag, expression)                                         \
    DISKEYV_LOG(::diskeyv_log::Level::INFO, tag, expression)
#define DISKEYV_WARN(tag, expression)                                         \
    DISKEYV_LOG(::diskeyv_log::Level::WARN, tag, expression)
#define DISKEYV_ERROR(tag, expression)                                        \
    DISKEYV_LOG(::diskeyv_log::Level::ERROR, tag, expression)
