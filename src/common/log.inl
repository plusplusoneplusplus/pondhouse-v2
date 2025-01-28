#pragma once

#include <cstdio>
#include <memory>

namespace pond::common {

template <typename... Args>
void Logger::log(LogLevel level, const char* file, int line, const char* format, Args&&... args) {
    if (level < min_level_ || !log_file_) {
        return;
    }

    std::string message = formatMessage(format, std::forward<Args>(args)...);
    std::string timestamp = formatTime();
    std::string level_str = levelToString(level);

    // Format: [TIME][LEVEL] Message
    char buffer[4096];
    snprintf(buffer,
             sizeof(buffer),
             "[%s][%s] [%s::%d] %s\n",
             timestamp.c_str(),
             level_str.c_str(),
             file,
             line,
             message.c_str());

    // Write to file
    fputs(buffer, log_file_);
    fflush(log_file_);

    // Also write errors to stderr
    if (level == LogLevel::Error) {
        fputs(buffer, stderr);
        fflush(stderr);
    } else if (level == LogLevel::Status) {
        fputs(buffer, stdout);
        fflush(stdout);
    }
}

template <typename... Args>
void Logger::verbose(const char* file, int line, const char* format, Args&&... args) {
    log(LogLevel::Verbose, file, line, format, std::forward<Args>(args)...);
}

template <typename... Args>
void Logger::status(const char* file, int line, const char* format, Args&&... args) {
    log(LogLevel::Status, file, line, format, std::forward<Args>(args)...);
}

template <typename... Args>
void Logger::warning(const char* file, int line, const char* format, Args&&... args) {
    log(LogLevel::Warning, file, line, format, std::forward<Args>(args)...);
}

template <typename... Args>
void Logger::error(const char* file, int line, const char* format, Args&&... args) {
    log(LogLevel::Error, file, line, format, std::forward<Args>(args)...);
}

template <typename... Args>
std::string Logger::formatMessage(const char* format, Args&&... args) const {
    if constexpr (sizeof...(Args) == 0) {
        return format;
    } else {
        char buffer[4096];
        int len = snprintf(buffer, sizeof(buffer), format, std::forward<Args>(args)...);
        if (len < 0 || len >= static_cast<int>(sizeof(buffer))) {
            throw std::runtime_error("Formatted message is too long");
        }
        return std::string(buffer);
    }
}

}  // namespace pond::common