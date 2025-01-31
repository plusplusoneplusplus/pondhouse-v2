#pragma once

#include <chrono>
#include <cstdint>

namespace pond::common {

using Timestamp = int64_t;

inline Timestamp now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace pond::common