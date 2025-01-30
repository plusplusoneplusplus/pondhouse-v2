#pragma once

#include <memory>

#include <arrow/io/interfaces.h>

#include "common/append_only_fs.h"

namespace pond::format {

// Custom output stream implementation that uses our filesystem
class AppendOnlyOutputStream : public arrow::io::OutputStream {
public:
    AppendOnlyOutputStream(std::shared_ptr<common::IAppendOnlyFileSystem> fs, common::FileHandle handle);
    ~AppendOnlyOutputStream() override = default;

    arrow::Status Close() override;
    bool closed() const override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;
    arrow::Status Flush() override;

    void setDebugMode(bool enable) { is_debug_ = enable; }

private:
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    common::FileHandle handle_;
    int64_t position_;
    bool closed_;
    bool is_debug_ = false;
};

}  // namespace pond::format