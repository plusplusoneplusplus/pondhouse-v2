#pragma once

#include <memory>

#include <arrow/io/interfaces.h>

#include "common/append_only_fs.h"

namespace pond::format {

// Custom input stream implementation that uses our filesystem
class AppendOnlyInputStream : public arrow::io::RandomAccessFile {
public:
    AppendOnlyInputStream(std::shared_ptr<common::IAppendOnlyFileSystem> fs, common::FileHandle handle);
    ~AppendOnlyInputStream() override = default;

    arrow::Status Close() override;
    bool closed() const override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

private:
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    common::FileHandle handle_;
    int64_t size_;
    int64_t position_;
};

}  // namespace pond::format