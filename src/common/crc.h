#pragma once

#include <cstdint>
#include <string>

namespace pond::common {

// CRC32 implementation using the IEEE polynomial
class CRC32 {
public:
    // Initialize the CRC32 lookup table
    static void Initialize();

    // Calculate CRC32 for a buffer
    static uint32_t Calculate(const uint8_t* data, size_t length);

    // Calculate CRC32 for a string
    static uint32_t Calculate(const std::string& data);

private:
    static uint32_t table_[256];
    static bool initialized_;
};

// Helper function to directly calculate CRC32
inline uint32_t Crc32(const uint8_t* data, size_t length) {
    return CRC32::Calculate(data, length);
}

inline uint32_t Crc32(const std::string& data) {
    return CRC32::Calculate(data);
}

}  // namespace pond::common
