#pragma once

#include <cassert>
#include <chrono>
#include <string>

namespace pond::common {

enum class LogLevel {
    Debug,    // More detailed information for debugging than Verbose
    Verbose,  // Detailed information for debugging
    Status,   // General operational information
    Warning,  // Potential issues that don't affect core functionality
    Error     // Serious issues that affect functionality
};

class Logger {
public:
    static Logger& instance();

    // Initialize logger with log file path
    bool init(const std::string& log_file);

    // Varadic logging functions
    template <typename... Args>
    void log(LogLevel level, const char* file, int line, const char* format, Args&&... args);

    template <typename... Args>
    void verbose(const char* file, int line, const char* format, Args&&... args);

    template <typename... Args>
    void debug(const char* file, int line, const char* format, Args&&... args);

    template <typename... Args>
    void status(const char* file, int line, const char* format, Args&&... args);

    template <typename... Args>
    void warning(const char* file, int line, const char* format, Args&&... args);

    template <typename... Args>
    void error(const char* file, int line, const char* format, Args&&... args);

    // Set minimum log level
    void setLogLevel(LogLevel level) { min_level_ = level; }

    // Close logger
    void close();

private:
    Logger() = default;
    ~Logger();
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    std::string formatTime() const;
    std::string levelToString(LogLevel level) const;

    template <typename... Args>
    std::string formatMessage(const char* format, Args&&... args) const;

    FILE* log_file_{nullptr};
    LogLevel min_level_{LogLevel::Status};
    std::string file_path_;
};

// Convenience macros
#define LOG_DEBUG(...) pond::common::Logger::instance().debug(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_VERBOSE(...) pond::common::Logger::instance().verbose(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATUS(...) pond::common::Logger::instance().status(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...) pond::common::Logger::instance().status(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARNING(...) pond::common::Logger::instance().warning(__FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) pond::common::Logger::instance().error(__FILE__, __LINE__, __VA_ARGS__)

#define LOG_CHECK(condition, ...)                                   \
    if (!(condition)) {                                             \
        LOG_ERROR(__FILE__, __LINE__, "Check failed: " #condition); \
        assert(false);                                              \
    }

// Allow gtest to access private members of class.
#define FRIEND_TEST(test_case_name, test_name) friend class test_case_name##_##test_name##_Test

}  // namespace pond::common

#include "log.inl"