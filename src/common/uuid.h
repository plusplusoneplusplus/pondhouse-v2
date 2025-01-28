#pragma once

#include <array>
#include <cstdint>
#include <random>
#include <string>

namespace pond::common {

class UUID {
public:
    UUID() = default;
    UUID(uint64_t high, uint64_t low) {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(uuid_.data());
        ptr[0] = high;
        ptr[1] = low;
    }
    UUID(const UUID& other) = default;
    UUID(UUID&& other) = default;
    UUID& operator=(const UUID& other) = default;
    UUID& operator=(UUID&& other) = default;
    ~UUID() = default;

    bool operator==(const UUID& other) const { return uuid_ == other.uuid_; }

    friend std::ostream& operator<<(std::ostream& os, const UUID& uuid) { return os << uuid.toString(); }

    void generate() {
        static thread_local std::random_device rd;
        static thread_local std::mt19937_64 gen(rd());
        static thread_local std::uniform_int_distribution<uint64_t> dis;

        uint64_t* ptr = reinterpret_cast<uint64_t*>(uuid_.data());
        ptr[0] = dis(gen);
        ptr[1] = dis(gen);

        // Set version to 4 (random)
        uuid_[6] = (uuid_[6] & 0x0F) | 0x40;
        // Set variant to RFC4122
        uuid_[8] = (uuid_[8] & 0x3F) | 0x80;
    }

    std::string toString() const {
        static const char hex[] = "0123456789abcdef";
        std::string result(36, '-');
        int i = 0;

        for (int j = 0; j < 16; j++) {
            if (j == 4 || j == 6 || j == 8 || j == 10) {
                i++;
            }
            result[i++] = hex[(uuid_[j] >> 4) & 0x0F];
            result[i++] = hex[uuid_[j] & 0x0F];
        }

        return result;
    }

    const uint8_t* data() const { return uuid_.data(); }

    size_t size() const { return uuid_.size(); }

    void parse(const std::string& str) { std::memcpy(uuid_.data(), str.data(), 16); }

public:
    static UUID newUUID() {
        UUID uuid;
        uuid.generate();
        return uuid;
    }

    static UUID fromString(const std::string& str) {
        UUID uuid;
        uuid.parse(str);
        return uuid;
    }

private:
    std::array<uint8_t, 16> uuid_;
};
}  // namespace pond::common