#include "kv/record.h"

#include <cstring>

namespace pond::kv {

common::DataChunk Record::PackValue(int32_t value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(&value), sizeof(value));
}

common::DataChunk Record::PackValue(int64_t value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(&value), sizeof(value));
}

common::DataChunk Record::PackValue(float value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(&value), sizeof(value));
}

common::DataChunk Record::PackValue(double value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(&value), sizeof(value));
}

common::DataChunk Record::PackValue(bool value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(&value), sizeof(value));
}

common::DataChunk Record::PackValue(const std::string& value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

common::DataChunk Record::PackValue(const char* value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(value), strlen(value));
}

common::DataChunk Record::PackValue(const common::DataChunk& value) {
    return value;
}

common::DataChunk Record::PackValue(const common::UUID& value) {
    return common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

template <>
common::Result<int32_t> Record::UnpackValue(const common::DataChunk& data) {
    if (data.Size() != sizeof(int32_t)) {
        return common::Result<int32_t>::failure(common::ErrorCode::InvalidArgument, "Invalid data size for int32");
    }
    int32_t value;
    std::memcpy(&value, data.Data(), sizeof(value));
    return common::Result<int32_t>::success(value);
}

template <>
common::Result<int64_t> Record::UnpackValue(const common::DataChunk& data) {
    if (data.Size() != sizeof(int64_t)) {
        return common::Result<int64_t>::failure(common::ErrorCode::InvalidArgument, "Invalid data size for int64");
    }
    int64_t value;
    std::memcpy(&value, data.Data(), sizeof(value));
    return common::Result<int64_t>::success(value);
}

template <>
common::Result<float> Record::UnpackValue(const common::DataChunk& data) {
    if (data.Size() != sizeof(float)) {
        return common::Result<float>::failure(common::ErrorCode::InvalidArgument, "Invalid data size for float");
    }
    float value;
    std::memcpy(&value, data.Data(), sizeof(value));
    return common::Result<float>::success(value);
}

template <>
common::Result<double> Record::UnpackValue(const common::DataChunk& data) {
    if (data.Size() != sizeof(double)) {
        return common::Result<double>::failure(common::ErrorCode::InvalidArgument, "Invalid data size for double");
    }
    double value;
    std::memcpy(&value, data.Data(), sizeof(value));
    return common::Result<double>::success(value);
}

template <>
common::Result<bool> Record::UnpackValue(const common::DataChunk& data) {
    if (data.Size() != sizeof(bool)) {
        return common::Result<bool>::failure(common::ErrorCode::InvalidArgument, "Invalid data size for boolean");
    }
    bool value;
    std::memcpy(&value, data.Data(), sizeof(value));
    return common::Result<bool>::success(value);
}

template <>
common::Result<std::string> Record::UnpackValue(const common::DataChunk& data) {
    return common::Result<std::string>::success(std::string(reinterpret_cast<const char*>(data.Data()), data.Size()));
}

template <>
common::Result<common::DataChunk> Record::UnpackValue(const common::DataChunk& data) {
    return common::Result<common::DataChunk>::success(data);
}

template <>
common::Result<common::UUID> Record::UnpackValue(const common::DataChunk& data) {
    return common::Result<common::UUID>::success(common::UUID::FromString(data.ToString()));
}

common::DataChunk Record::Serialize() const {
    // Format:
    // | num_columns (4B) | null_bitmap | value_lengths | values |

    size_t num_columns = schema_->NumColumns();
    std::vector<uint8_t> null_bitmap((num_columns + 7) / 8, 0);
    std::vector<uint32_t> value_lengths(num_columns, 0);
    size_t total_values_size = 0;

    // Calculate sizes and build null bitmap
    for (size_t i = 0; i < num_columns; i++) {
        if (values_[i].has_value()) {
            null_bitmap[i / 8] |= (1 << (i % 8));
            value_lengths[i] = values_[i]->Size();
            total_values_size += value_lengths[i];
        }
    }

    // Calculate total size and create buffer
    size_t total_size = sizeof(uint32_t) +                // num_columns
                        null_bitmap.size() +              // null bitmap
                        num_columns * sizeof(uint32_t) +  // value lengths
                        total_values_size;                // actual values

    std::vector<uint8_t> buffer(total_size);
    size_t offset = 0;

    // Write num_columns
    uint32_t cols = static_cast<uint32_t>(num_columns);
    std::memcpy(buffer.data() + offset, &cols, sizeof(cols));
    offset += sizeof(cols);

    // Write null bitmap
    std::memcpy(buffer.data() + offset, null_bitmap.data(), null_bitmap.size());
    offset += null_bitmap.size();

    // Write value lengths
    std::memcpy(buffer.data() + offset, value_lengths.data(), value_lengths.size() * sizeof(uint32_t));
    offset += value_lengths.size() * sizeof(uint32_t);

    // Write values
    for (size_t i = 0; i < num_columns; i++) {
        if (values_[i].has_value()) {
            std::memcpy(buffer.data() + offset, values_[i]->Data(), values_[i]->Size());
            offset += values_[i]->Size();
        }
    }

    return common::DataChunk(buffer.data(), buffer.size());
}

common::Result<std::unique_ptr<Record>> Record::Deserialize(const common::DataChunk& data,
                                                            std::shared_ptr<common::Schema> schema) {
    const uint8_t* ptr = data.Data();
    size_t remaining = data.Size();

    if (remaining < sizeof(uint32_t)) {
        return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::InvalidArgument,
                                                                "Invalid data size");
    }

    // Read num_columns
    uint32_t num_columns;
    std::memcpy(&num_columns, ptr, sizeof(num_columns));
    ptr += sizeof(num_columns);
    remaining -= sizeof(num_columns);

    if (num_columns != schema->NumColumns()) {
        return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::InvalidOperation,
                                                                "Schema mismatch" + std::to_string(num_columns) +
                                                                    " != " + std::to_string(schema->NumColumns()));
    }

    // Read null bitmap
    size_t bitmap_size = (num_columns + 7) / 8;
    if (remaining < bitmap_size) {
        return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::InvalidArgument,
                                                                "Invalid data size");
    }
    std::vector<uint8_t> null_bitmap(ptr, ptr + bitmap_size);
    ptr += bitmap_size;
    remaining -= bitmap_size;

    // Read value lengths
    if (remaining < num_columns * sizeof(uint32_t)) {
        return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::InvalidArgument,
                                                                "Invalid data size");
    }
    std::vector<uint32_t> value_lengths(num_columns);
    std::memcpy(value_lengths.data(), ptr, num_columns * sizeof(uint32_t));
    ptr += num_columns * sizeof(uint32_t);
    remaining -= num_columns * sizeof(uint32_t);

    // Create record and read values
    auto record = std::make_unique<Record>(schema);
    for (size_t i = 0; i < num_columns; i++) {
        if (null_bitmap[i / 8] & (1 << (i % 8))) {
            if (remaining < value_lengths[i]) {
                return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::InvalidArgument,
                                                                        "Invalid data size");
            }
            record->values_[i] = common::DataChunk(ptr, value_lengths[i]);
            ptr += value_lengths[i];
            remaining -= value_lengths[i];
        }
    }

    return common::Result<std::unique_ptr<Record>>::success(std::move(record));
}

}  // namespace pond::kv
