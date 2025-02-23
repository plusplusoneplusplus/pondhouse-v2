#include "common/filesystem_stream.h"

#include "common/error.h"
#include "common/log.h"

namespace pond::common {

// FileSystemInputStream implementation
FileSystemInputStream::FileSystemInputStream(std::shared_ptr<IAppendOnlyFileSystem> fs, FileHandle handle)
    : fs_(std::move(fs)), handle_(handle), position_(0) {
    auto size_result = fs_->Size(handle_);
    if (!size_result.ok()) {
        LOG_ERROR("Failed to get size of file: %s", size_result.error().message().c_str());
        return;
    }

    size_ = size_result.value();
}

FileSystemInputStream::~FileSystemInputStream() {
    if (handle_ != INVALID_HANDLE) {
        fs_->CloseFile(handle_);
    }
}

Result<std::unique_ptr<FileSystemInputStream>> FileSystemInputStream::Create(std::shared_ptr<IAppendOnlyFileSystem> fs,
                                                                             const std::string& path) {
    if (!fs) {
        return Result<std::unique_ptr<FileSystemInputStream>>::failure(ErrorCode::InvalidArgument,
                                                                       "Filesystem is null");
    }

    auto result = fs->OpenFile(path, false);
    if (!result.ok()) {
        return Result<std::unique_ptr<FileSystemInputStream>>::failure(result.error());
    }

    return Result<std::unique_ptr<FileSystemInputStream>>::success(
        std::unique_ptr<FileSystemInputStream>(new FileSystemInputStream(fs, result.value())));
}

Result<DataChunkPtr> FileSystemInputStream::Read(size_t length) {
    if (position_ >= size_) {
        return Result<DataChunkPtr>::success(std::make_shared<DataChunk>());
    }

    auto result = fs_->Read(handle_, position_, std::min(length, size_ - position_));
    if (!result.ok()) {
        return Result<DataChunkPtr>::failure(result.error());
    }
    position_ += result.value().Size();
    return Result<DataChunkPtr>::success(std::make_shared<DataChunk>(std::move(result.value())));
}

Result<size_t> FileSystemInputStream::Read(void* data, size_t size) {
    if (position_ >= size_) {
        return Result<size_t>::success(0);
    }

    auto result = fs_->Read(handle_, position_, std::min(size, size_ - position_));
    if (!result.ok()) {
        return Result<size_t>::failure(result.error());
    }
    if (result.value().Size() == 0) {
        return Result<size_t>::success(0);
    }

    position_ += result.value().Size();
    std::memcpy(data, result.value().Data(), result.value().Size());
    return Result<size_t>::success(result.value().Size());
}

Result<DataChunkPtr> FileSystemInputStream::ReadAt(size_t offset, size_t length) {
    if (offset >= size_) {
        return Result<DataChunkPtr>::success(std::make_shared<DataChunk>());
    }

    auto result = fs_->Read(handle_, offset, std::min(length, size_ - offset));
    if (!result.ok()) {
        return Result<DataChunkPtr>::failure(result.error());
    }
    return Result<DataChunkPtr>::success(std::make_shared<DataChunk>(std::move(result.value())));
}

Result<size_t> FileSystemInputStream::Size() const {
    return Result<size_t>::success(size_);
}

Result<bool> FileSystemInputStream::UpdateSize(size_t size) {
    auto current_size = fs_->Size(handle_);
    if (!current_size.ok()) {
        return Result<bool>::failure(current_size.error());
    }

    if (size > current_size.value()) {
        return Result<bool>::failure(ErrorCode::InvalidArgument,
                                     "Cannot expand file size through FileSystemInputStream");
    }

    size_ = size;
    if (position_ > size_) {
        position_ = size_;
    }

    return Result<bool>::success(true);
}

Result<bool> FileSystemInputStream::Seek(size_t position) {
    if (position > size_) {
        return Result<bool>::failure(ErrorCode::EndOfStream, "Position beyond end of file");
    }

    position_ = position;

    return Result<bool>::success(true);
}

// FileSystemOutputStream implementation
FileSystemOutputStream::FileSystemOutputStream(std::shared_ptr<IAppendOnlyFileSystem> fs, FileHandle handle)
    : fs_(std::move(fs)), handle_(handle), position_(0) {}

FileSystemOutputStream::~FileSystemOutputStream() {
    if (handle_ != INVALID_HANDLE) {
        fs_->CloseFile(handle_);
    }
}

Result<std::unique_ptr<FileSystemOutputStream>> FileSystemOutputStream::Create(
    std::shared_ptr<IAppendOnlyFileSystem> fs, const std::string& path, bool create_if_not_exists) {
    if (!fs) {
        return Result<std::unique_ptr<FileSystemOutputStream>>::failure(ErrorCode::InvalidArgument,
                                                                        "Filesystem is null");
    }

    auto result = fs->OpenFile(path, create_if_not_exists);
    if (!result.ok()) {
        return Result<std::unique_ptr<FileSystemOutputStream>>::failure(result.error());
    }

    return Result<std::unique_ptr<FileSystemOutputStream>>::success(
        std::unique_ptr<FileSystemOutputStream>(new FileSystemOutputStream(fs, result.value())));
}

Result<size_t> FileSystemOutputStream::Write(const DataChunkPtr& data) {
    if (!data) {
        return Result<size_t>::failure(ErrorCode::InvalidArgument, "Data pointer is null");
    }

    auto result = fs_->Append(handle_, *data);
    if (!result.ok()) {
        return Result<size_t>::failure(result.error());
    }

    position_ = result.value().offset_ + result.value().length_;
    return Result<size_t>::success(data->Size());
}

Result<size_t> FileSystemOutputStream::Write(const void* data, size_t size) {
    if (!data && size > 0) {
        return Result<size_t>::failure(ErrorCode::InvalidArgument, "Data pointer is null");
    }

    // Create a DataChunk from the raw data
    auto chunk = std::make_shared<DataChunk>(static_cast<const uint8_t*>(data), size);
    return Write(chunk);
}

}  // namespace pond::common