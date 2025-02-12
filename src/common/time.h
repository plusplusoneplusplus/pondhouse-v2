#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "log.h"

namespace pond::common {

using Timestamp = int64_t;

inline Timestamp now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

/**
 * HybridTime combines physical wall clock time with a logical counter.
 * Format: [physical_time_bits:48][logical_counter:16]
 * - Physical time: microseconds since epoch (max ~8925 years)
 * - Logical counter: sequence number within the same microsecond
 */
class HybridTime {
public:
    static constexpr uint64_t PHYSICAL_BITS = 48;
    static constexpr uint64_t LOGICAL_BITS = 16;
    static constexpr uint64_t MAX_LOGICAL = (1ULL << LOGICAL_BITS) - 1;
    static constexpr uint64_t PHYSICAL_MASK = (1ULL << PHYSICAL_BITS) - 1;
    static constexpr uint64_t INVALID_TIME = 0;

    HybridTime() : time_(INVALID_TIME) {}
    explicit HybridTime(uint64_t encoded_time) : time_(encoded_time) {}
    HybridTime(uint64_t physical, uint16_t logical) { time_ = ((physical & PHYSICAL_MASK) << LOGICAL_BITS) | logical; }

    // Accessors
    uint64_t physical_time() const { return time_ >> LOGICAL_BITS; }
    uint16_t logical_counter() const { return time_ & MAX_LOGICAL; }
    uint64_t encoded() const { return time_; }

    // Comparison operators
    bool operator==(const HybridTime& other) const { return time_ == other.time_; }
    bool operator!=(const HybridTime& other) const { return time_ != other.time_; }
    bool operator<(const HybridTime& other) const { return time_ < other.time_; }
    bool operator<=(const HybridTime& other) const { return time_ <= other.time_; }
    bool operator>(const HybridTime& other) const { return time_ > other.time_; }
    bool operator>=(const HybridTime& other) const { return time_ >= other.time_; }

private:
    uint64_t time_;  // Combined physical and logical time
};

/**
 * HybridTimeManager provides timestamp allocation combining wall clock time
 * with logical counters for strict ordering.
 *
 * When wall clock time goes backwards, the returned timestamp will have the same physical time as the last returned
 * timestamp and the logical counter will be incremented.
 *
 * When the logical counter overflows, the physical time will be incremented.
 *
 * When the physical time increment is too large, an error will be thrown.
 *
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
        while (true) {
            uint64_t current_encoded = current_.load(std::memory_order_seq_cst);
            HybridTime current(current_encoded);

            uint64_t new_physical = fixed_time;
            uint16_t new_logical = 0;

            // If in same microsecond, increment logical counter
            if (new_physical == current.physical_time()) {
                new_logical = current.logical_counter() + 1;
                if (new_logical == MAX_LOGICAL) {
                    // If logical counter would overflow, move to next microsecond
                    new_physical++;
                    new_logical = 0;
                }
            } else if (new_physical < current.physical_time()) {
                // Clock went backwards, increment from last timestamp
                new_physical = current.physical_time();
                new_logical = current.logical_counter() + 1;
                if (new_logical == MAX_LOGICAL) {
                    new_physical++;
                    new_logical = 0;
                }
            } else if (new_physical > (current.physical_time() + MAX_PHYSICAL_INCREMENT)) {
                LOG_ERROR("Physical time increment too large, from %llu to %llu, delta=%llu",
                          current.physical_time(),
                          new_physical,
                          new_physical - current.physical_time());

                throw std::runtime_error("Physical time increment too large, from "
                                         + std::to_string(current.physical_time()) + " to "
                                         + std::to_string(new_physical));
            }

            HybridTime new_time(new_physical, new_logical);

            if (current_.compare_exchange_strong(
                    current_encoded, new_time.encoded(), std::memory_order_seq_cst, std::memory_order_seq_cst)) {
                return new_time;
            }
            // CAS failed, retry with new current value
        }
    }

    // Get current hybrid timestamp without incrementing
    HybridTime Current() const { return HybridTime(current_.load(std::memory_order_seq_cst)); }

private:
    HybridTimeManager()
        : current_(HybridTime(now(), 0).encoded()) {}  // Start from (1,0), (0,0) reserved for INVALID_TIME

    // Use sequential consistency for timestamp operations to ensure total ordering
    std::atomic<uint64_t> current_;

    static constexpr uint16_t MAX_LOGICAL = HybridTime::MAX_LOGICAL;
};

}  // namespace pond::common