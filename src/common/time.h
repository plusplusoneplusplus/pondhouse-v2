#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "log.h"

namespace pond::common {

using Timestamp = int64_t;     // microseconds since epoch, 1970-01-01 00:00:00
using TimeInterval = int64_t;  // microseconds

inline Timestamp now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline TimeInterval GetTimeInterval(Timestamp start, Timestamp end) {
    return end - start;
}

// Convert a timestamp to a string in the format YYYY/MM/DD HH:MM:SS.UUUUUU, in UTC
// Example: 2024/01/01 00:00:00.000000
inline std::string TimestampToString(Timestamp timestamp) {
    // Convert timestamp to system time
    auto time_point = std::chrono::system_clock::time_point(std::chrono::microseconds(timestamp));
    auto time = std::chrono::system_clock::to_time_t(time_point);

    // Get microseconds part
    auto microseconds = timestamp % 1000000LL;

    // Format the date/time string using UTC
    char buffer[64];  // Increased buffer size to be safe
    auto tm = std::gmtime(&time);
    snprintf(buffer,
             sizeof(buffer),
             "%04d/%02d/%02d %02d:%02d:%02d.%06" PRId64,  // Using PRId64 for 64-bit integer
             tm->tm_year + 1900,
             tm->tm_mon + 1,
             tm->tm_mday,
             tm->tm_hour,
             tm->tm_min,
             tm->tm_sec,
             static_cast<int64_t>(microseconds));
    return std::string(buffer);
}

inline std::string TimeIntervalToString(TimeInterval interval) {
    // Convert microseconds to larger units
    auto days = interval / (1000000LL * 60 * 60 * 24);
    interval %= (1000000LL * 60 * 60 * 24);

    auto hours = interval / (1000000LL * 60 * 60);
    interval %= (1000000LL * 60 * 60);

    auto minutes = interval / (1000000LL * 60);
    interval %= (1000000LL * 60);

    auto seconds = interval / 1000000LL;
    auto microseconds = interval % 1000000LL;

    std::string result;
    if (days > 0) {
        result += std::to_string(days) + "d";
    }
    if (hours > 0) {
        result += std::to_string(hours) + "h";
    }
    if (minutes > 0) {
        result += std::to_string(minutes) + "m";
    }
    if (seconds > 0 || microseconds > 0) {
        result += std::to_string(seconds);
        if (microseconds > 0) {
            result += "." + std::to_string(microseconds);
        }
        result += "s";
    }
    return result.empty() ? "0s" : result;
}

/**
 * HybridTime represents a microsecond-precision timestamp.
 * Format: [physical_time_bits:64]
 * - Physical time: microseconds since epoch (max ~584,942 years)
 */
class HybridTime {
public:
    static constexpr uint64_t INVALID_TIME = 0;
    static constexpr uint64_t MIN_TIME = 1;
    static constexpr uint64_t MAX_TIME = UINT64_MAX;

    HybridTime() : time_(INVALID_TIME) {}
    explicit HybridTime(uint64_t time) : time_(time) {}

    // Accessors
    uint64_t physical_time() const { return time_; }
    uint64_t encoded() const { return time_; }

    // Comparison operators
    bool operator==(const HybridTime& other) const { return time_ == other.time_; }
    bool operator!=(const HybridTime& other) const { return time_ != other.time_; }
    bool operator<(const HybridTime& other) const { return time_ < other.time_; }
    bool operator<=(const HybridTime& other) const { return time_ <= other.time_; }
    bool operator>(const HybridTime& other) const { return time_ > other.time_; }
    bool operator>=(const HybridTime& other) const { return time_ >= other.time_; }

private:
    uint64_t time_;  // Physical time in microseconds
};

/**
 * HybridTimeManager provides timestamp allocation using steady_clock for tracking time deltas.
 *
 * When wall clock time goes backwards or stays the same, the manager uses the steady_clock
 * delta to increment the physical time, ensuring strict monotonicity.
 *
 * When the physical time increment is too large, an error will be thrown.
 */
class HybridTimeManager {
public:
    static constexpr uint64_t MAX_PHYSICAL_INCREMENT = std::chrono::microseconds(300 * 1000 * 1000).count();

    static HybridTimeManager& Instance() {
        static HybridTimeManager instance;
        return instance;
    }

    // Get next hybrid timestamp (strictly monotonic)
    HybridTime Next() {
        auto current_time = now();
        return NextWithTime(current_time);
    }

    // Test-only: Get next timestamp with fixed physical time
    HybridTime NextWithTime(uint64_t fixed_time) {
        auto last_steady = last_steady_;

        while (true) {
            auto steady_now = std::chrono::steady_clock::now();
            auto steady_delta = std::chrono::duration_cast<std::chrono::microseconds>(steady_now - last_steady).count();

            uint64_t current_encoded = current_.load(std::memory_order_seq_cst);
            uint64_t new_physical = fixed_time;

            // Check for too large increment
            if (new_physical > (current_encoded + MAX_PHYSICAL_INCREMENT)) {
                LOG_ERROR("Physical time increment too large, from %llu to %llu, delta=%llu",
                          current_encoded,
                          new_physical,
                          new_physical - current_encoded);

                throw std::runtime_error("Physical time increment too large, from " + std::to_string(current_encoded)
                                         + " to " + std::to_string(new_physical));
            }

            // If time went backwards or stayed the same, use steady clock delta
            if (new_physical <= current_encoded) {
                new_physical = current_encoded + std::max(static_cast<uint64_t>(1), static_cast<uint64_t>(steady_delta));
            }

            assert(new_physical > current_encoded);

            if (current_.compare_exchange_strong(
                    current_encoded, new_physical, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
                last_steady_ = steady_now;
                return HybridTime(new_physical);
            }
            // CAS failed, retry with new current value
        }
    }

    // Get current hybrid timestamp without incrementing
    HybridTime Current() const { return HybridTime(current_.load(std::memory_order_seq_cst)); }

private:
    HybridTimeManager() : current_(now()), last_steady_(std::chrono::steady_clock::now()) {}

    // Use sequential consistency for timestamp operations to ensure total ordering
    std::atomic<uint64_t> current_;
    std::chrono::steady_clock::time_point last_steady_;
};

inline HybridTime GetNextHybridTime() {
    return HybridTimeManager::Instance().Next();
}

inline HybridTime InvalidHybridTime() {
    return HybridTime(HybridTime::INVALID_TIME);
}

inline HybridTime MinHybridTime() {
    return HybridTime(HybridTime::MIN_TIME);
}

inline HybridTime MaxHybridTime() {
    return HybridTime(HybridTime::MAX_TIME);
}

}  // namespace pond::common