#include "common/memory_stream.h"

#include "common/error.h"

namespace pond::common {

// MemoryInputStream implementation
MemoryInputStream::MemoryInputStream(const DataChunkPtr& data) : data_(data), position_(0) {}

std::unique_ptr<MemoryInputStream> MemoryInputStream::Create(const DataChunkPtr& data) {
    if (!data) {
        return nullptr;
    }
    return std::unique_ptr<MemoryInputStream>(new MemoryInputStream(data));
}

std::unique_ptr<MemoryInputStream> MemoryInputStream::Create(const void* data, size_t size) {
    if (!data && size > 0) {
        return nullptr;
    }
    auto chunk = std::make_shared<DataChunk>(static_cast<const uint8_t*>(data), size);
    return Create(chunk);
}

Result<DataChunkPtr> MemoryInputStream::Read(size_t length) {
    if (position_ >= data_->Size()) {
        // End of stream
        return Result<DataChunkPtr>::success(std::make_shared<DataChunk>());
    }

    // Calculate how many bytes we can actually read
    size_t available = data_->Size() - position_;
    size_t to_read = std::min(length, available);

    // Create a new chunk with the requested data
    auto result = std::make_shared<DataChunk>(data_->Data() + position_, to_read);
    position_ += to_read;

    return Result<DataChunkPtr>::success(result);
}

Result<size_t> MemoryInputStream::Size() const {
    return Result<size_t>::success(data_->Size());
}

Result<bool> MemoryInputStream::Seek(size_t position) {
    if (position > data_->Size()) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "Position beyond end of stream");
    }

    position_ = position;
    return Result<bool>::success(true);
}

// MemoryOutputStream implementation
MemoryOutputStream::MemoryOutputStream(size_t initial_capacity) {
    data_ = std::make_shared<DataChunk>(initial_capacity);
    data_->Resize(0);  // Set actual size to 0 while keeping the capacity
}

std::unique_ptr<MemoryOutputStream> MemoryOutputStream::Create(size_t initial_capacity) {
    return std::unique_ptr<MemoryOutputStream>(new MemoryOutputStream(initial_capacity));
}

Result<size_t> MemoryOutputStream::Write(const DataChunkPtr& data) {
    if (!data) {
        return Result<size_t>::failure(ErrorCode::InvalidArgument, "Data pointer is null");
    }

    size_t old_size = data_->Size();
    size_t new_size = old_size + data->Size();

    // Resize our buffer to accommodate the new data
    data_->Resize(new_size);

    // Copy the new data at the end
    std::memcpy(data_->Data() + old_size, data->Data(), data->Size());

    return Result<size_t>::success(data->Size());
}

Result<size_t> MemoryOutputStream::Write(const void* data, size_t size) {
    if (!data && size > 0) {
        return Result<size_t>::failure(ErrorCode::InvalidArgument, "Data pointer is null");
    }

    size_t old_size = data_->Size();
    size_t new_size = old_size + size;

    // Resize our buffer to accommodate the new data
    data_->Resize(new_size);

    // Copy the new data at the end
    std::memcpy(data_->Data() + old_size, data, size);

    return Result<size_t>::success(size);
}

}  // namespace pond::common