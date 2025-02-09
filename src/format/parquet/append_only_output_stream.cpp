#include "append_only_output_stream.h"

#include <iomanip>
#include <sstream>

#include <arrow/buffer.h>

#include "common/types.h"
#include "common/log.h"

namespace pond::format {

AppendOnlyOutputStream::AppendOnlyOutputStream(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                               common::FileHandle handle)
    : fs_(fs), handle_(handle), position_(0), closed_(false) {
    LOG_VERBOSE("Created AppendOnlyOutputStream");
}

arrow::Status AppendOnlyOutputStream::Close() {
    if (closed_) {
        LOG_VERBOSE("Stream already closed");
        return arrow::Status::OK();
    }

    LOG_VERBOSE("Closing stream at position %zu", position_);

    // Ensure all data is flushed
    auto status = Flush();
    if (!status.ok()) {
        LOG_ERROR("Failed to flush stream: %s", status.ToString().c_str());
        return status;
    }

    closed_ = true;
    LOG_VERBOSE("Stream closed successfully");
    return arrow::Status::OK();
}

bool AppendOnlyOutputStream::closed() const {
    return closed_;
}

arrow::Result<int64_t> AppendOnlyOutputStream::Tell() const {
    LOG_VERBOSE("Current position: %zu", position_);
    return position_;
}

arrow::Status AppendOnlyOutputStream::Write(const void* data, int64_t nbytes) {
    if (closed_) {
        return arrow::Status::IOError("Stream is closed");
    }

    if (nbytes <= 0) {
        LOG_VERBOSE("Zero bytes write request");
        return arrow::Status::OK();
    }

    common::DataChunk chunk(static_cast<const uint8_t*>(data), nbytes);
    auto result = fs_->Append(handle_, chunk);
    if (!result.ok()) {
        LOG_ERROR("Failed to write to file: %s", result.error().c_str());
        return arrow::Status::IOError("Failed to write to file: " + result.error().message());
    }

    position_ += nbytes;

    LOG_VERBOSE("Write completed, length: %zu, new position: %zu", nbytes, position_);

    if (is_debug_) {
        // Print data as hex
        std::stringstream hex_stream;
        hex_stream << std::hex << std::setfill('0');
        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        for (int64_t i = 0; i < nbytes; i++) {
            hex_stream << std::setw(2) << static_cast<int>(bytes[i]);
        }
        LOG_VERBOSE("Data hex dump: %s", hex_stream.str().c_str());
    }

    return arrow::Status::OK();
}

arrow::Status AppendOnlyOutputStream::Write(const std::shared_ptr<arrow::Buffer>& data) {
    LOG_VERBOSE("Writing buffer of size %zu", data->size());
    return Write(data->data(), data->size());
}

arrow::Status AppendOnlyOutputStream::Flush() {
    if (closed_) {
        LOG_ERROR("Cannot flush closed stream");
        return arrow::Status::Invalid("Cannot flush closed stream");
    }
    LOG_VERBOSE("Flushing stream at position %zu", position_);
    return arrow::Status::OK();
}

}  // namespace pond::format