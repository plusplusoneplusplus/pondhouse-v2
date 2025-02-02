#include "common/crc.h"

namespace pond::common {

uint32_t CRC32::table_[256];
bool CRC32::initialized_ = false;

void CRC32::Initialize() {
    if (initialized_) {
        return;
    }

    // IEEE polynomial: 0xEDB88320 (bit-reversed 0x04C11DB7)
    const uint32_t polynomial = 0xEDB88320;

    // Generate lookup table
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ polynomial;
            } else {
                crc >>= 1;
            }
        }
        table_[i] = crc;
    }

    initialized_ = true;
}

uint32_t CRC32::Calculate(const uint8_t* data, size_t length) {
    if (!initialized_) {
        Initialize();
    }

    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < length; i++) {
        uint8_t index = (crc ^ data[i]) & 0xFF;
        crc = (crc >> 8) ^ table_[index];
    }

    return ~crc;  // Final XOR
}

uint32_t CRC32::Calculate(const std::string& data) {
    return Calculate(reinterpret_cast<const uint8_t*>(data.data()), data.length());
}

}  // namespace pond::common
