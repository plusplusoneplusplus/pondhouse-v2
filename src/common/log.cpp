#include "log.h"

#include <filesystem>
#include <iomanip>
#include <sstream>

namespace pond::common {

Logger& Logger::instance() {
    static Logger instance;
    return instance;
}

bool Logger::init(const std::string& log_file) {
    if (log_file_) {
        close();
    }

    // Create directory if it doesn't exist
    std::filesystem::path log_path(log_file);
    std::string log_dir = log_path.parent_path().string();
    if (log_dir != "" && !std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
    }

    log_file_ = fopen(log_file.c_str(), "a");
    if (!log_file_) {
        return false;
    }

    file_path_ = log_file;

    LOG_STATUS("========== START LOGGING ==========");
    LOG_STATUS("System time: %s", formatTime().c_str());
    LOG_STATUS("Log file: %s", log_file.c_str());
    LOG_STATUS("==================================");

    return true;
}

Logger::~Logger() {
    close();
}

void Logger::close() {
    if (log_file_) {
        fclose(log_file_);
        log_file_ = nullptr;
    }
}

std::string Logger::formatTime() const {
    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    std::stringstream ss;
    ss << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
    return ss.str();
}

std::string Logger::levelToString(LogLevel level) const {
    switch (level) {
        case LogLevel::Verbose:
            return "VERBOSE";
        case LogLevel::Status:
            return "STATUS";
        case LogLevel::Warning:
            return "WARNING";
        case LogLevel::Error:
            return "ERROR";
        default:
            return "UNKNOWN";
    }
}

}  // namespace pond::common