#include "append_only_input_stream.h"

#include <arrow/buffer.h>

#include "common/log.h"
#include "common/types.h"

namespace pond::format {

AppendOnlyInputStream::AppendOnlyInputStream(std::shared_ptr<common::IAppendOnlyFileSystem> fs, common::FileHandle handle)
    : fs_(fs), handle_(handle) {
    auto size_result = fs_->size(handle_);
    if (size_result.ok()) {
        size_ = size_result.value();
    } else {
        size_ = 0;
    }
    position_ = 0;
}

arrow::Status AppendOnlyInputStream::Close() {
    return arrow::Status::OK();
}

bool AppendOnlyInputStream::closed() const {
    return false;
}

arrow::Result<int64_t> AppendOnlyInputStream::Tell() const {
    return position_;
}

arrow::Result<int64_t> AppendOnlyInputStream::GetSize() {
    return size_;
}

arrow::Status AppendOnlyInputStream::Seek(int64_t position) {
    if (position < 0 || position > size_) {
        LOG_ERROR("Invalid seek position: %zu", position);
        return arrow::Status::Invalid("Invalid seek position");
    }
    position_ = position;
    return arrow::Status::OK();
}

arrow::Result<int64_t> AppendOnlyInputStream::Read(int64_t nbytes, void* out) {
    if (position_ >= size_) {
        return 0;
    }

    auto read_result = fs_->read(handle_, position_, nbytes);
    if (!read_result.ok()) {
        LOG_ERROR("Failed to read from file: %s", read_result.error().c_str());
        return arrow::Status::IOError("Failed to read from file");
    }

    const auto& data = read_result.value();
    int64_t bytes_to_read = std::min(static_cast<int64_t>(data.size()), nbytes);
    std::memcpy(out, data.data(), bytes_to_read);
    position_ += bytes_to_read;
    return bytes_to_read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> AppendOnlyInputStream::Read(int64_t nbytes) {
    if (position_ >= size_) {
        return arrow::Status::OK();
    }

    auto read_result = fs_->read(handle_, position_, nbytes);
    if (!read_result.ok()) {
        LOG_ERROR("Failed to read from file: %s", read_result.error().c_str());
        return arrow::Status::IOError("Failed to read from file");
    }

    const auto& data = read_result.value();
    auto buffer = arrow::Buffer::FromString(std::string(reinterpret_cast<const char*>(data.data()), data.size()));
    position_ += data.size();
    return buffer;
}

}  // namespace pond::format