#include "kv_entry.h"

#include <cstring>

#include "common/log.h"

using namespace pond::common;

namespace pond::kv {

// format:
// | LSN (8B) | TS (8B) | Type (4B) | Key Size (4B) | Key (key_size) | Value Size (4B) | Value (value_size) |
DataChunk KvEntry::Serialize() const {
    DataChunk data;
    uint32_t key_size = static_cast<uint32_t>(key.size());
    uint32_t value_size = static_cast<uint32_t>(value.Size());

    data.Reserve(sizeof(lsn_) + sizeof(ts) + sizeof(type) + sizeof(key_size) + key_size + sizeof(value_size)
                 + value_size);

    Serialize(data);
    return data;
}

void KvEntry::Serialize(DataChunk& data) const {
    uint32_t key_size = static_cast<uint32_t>(key.size());
    uint32_t value_size = static_cast<uint32_t>(value.Size());

    data.Append(reinterpret_cast<const uint8_t*>(&lsn_), sizeof(lsn_));
    data.Append(reinterpret_cast<const uint8_t*>(&ts), sizeof(ts));
    data.Append(reinterpret_cast<const uint8_t*>(&type), sizeof(type));
    data.Append(reinterpret_cast<const uint8_t*>(&key_size), sizeof(key_size));
    data.Append(reinterpret_cast<const uint8_t*>(key.data()), key_size);
    data.Append(reinterpret_cast<const uint8_t*>(&value_size), sizeof(value_size));
    data.Append(value.Data(), value_size);
}

bool KvEntry::Deserialize(const DataChunk& data) {
    if (data.Size() < sizeof(lsn_) + sizeof(ts) + sizeof(type) + sizeof(uint32_t) + sizeof(uint32_t)) {
        return false;
    }

    const uint8_t* ptr = data.Data();
    std::memcpy(&lsn_, ptr, sizeof(lsn_));
    ptr += sizeof(lsn_);
    std::memcpy(&ts, ptr, sizeof(ts));
    ptr += sizeof(ts);
    std::memcpy(&type, ptr, sizeof(type));
    ptr += sizeof(type);

    uint32_t key_size;
    std::memcpy(&key_size, ptr, sizeof(key_size));
    ptr += sizeof(key_size);

    key = std::string(reinterpret_cast<const char*>(ptr), key_size);
    ptr += key_size;

    uint32_t value_size;
    std::memcpy(&value_size, ptr, sizeof(value_size));
    ptr += sizeof(value_size);

    value = DataChunk(ptr, value_size);
    return true;
}

}  // namespace pond::kv