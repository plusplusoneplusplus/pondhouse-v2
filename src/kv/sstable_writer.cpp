#include "sstable_writer.h"

#include <cassert>
#include <vector>

#include "common/error.h"
#include "common/log.h"

using namespace pond::common;

namespace pond::kv {

class SSTableWriter::Impl {
public:
    Impl(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
        : fs_(std::move(fs)), path_(path), data_builder_(), index_builder_(), metadata_builder_() {
        // Initialize file header
        header_.version = 1;
        header_.flags = 0;
    }

    ~Impl() {
        if (file_handle_ != common::INVALID_HANDLE) {
            fs_->CloseFile(file_handle_);
        }
    }

    common::Result<bool> Add(const std::string& key, const common::DataChunk& value, common::HybridTime version) {
        if (finished_) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "Writer is already finished");
        }

        if (version == common::InvalidHybridTime()) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidArgument, "Invalid version");
        }

        if (!file_handle_) {
            // Open file on first write
            auto result = fs_->OpenFile(path_, true);
            if (!result.ok()) {
                return common::Result<bool>::failure(result.error());
            }
            file_handle_ = result.value();

            // Write file header
            if (filter_builder_) {
                header_.flags |= FileHeader::kHasFilter;
            }
            auto header_data = header_.Serialize();
            auto append_result = fs_->Append(file_handle_, common::DataChunk(header_data.data(), header_data.size()));
            if (!append_result.ok()) {
                return common::Result<bool>::failure(append_result.error());
            }
            current_offset_ = FileHeader::kHeaderSize;
        }

        // Validate key order
        if (!last_key_.empty()) {
            if (key < last_key_) {
                return common::Result<bool>::failure(common::ErrorCode::InvalidArgument,
                                                     "Keys must be added in sorted order");
            }
            if (key == last_key_ && version >= last_version_) {
                return common::Result<bool>::failure(common::ErrorCode::InvalidArgument,
                                                     "Versions of the same key must be added in descending order");
            }
        }

        // Try to add to current data block
        if (!data_builder_.Add(key, version, value)) {
            // Current block is full, flush it
            auto flush_result = FlushDataBlock();
            if (!flush_result.ok()) {
                return common::Result<bool>::failure(flush_result.error());
            }

            // Try again with empty block
            if (!data_builder_.Add(key, version, value)) {
                return common::Result<bool>::failure(common::ErrorCode::InvalidArgument, "Entry too large for block");
            }
        }

        // Update metadata
        metadata_builder_.UpdateStats(key, value);

        // Update state
        last_key_ = key;
        last_version_ = version;
        if (smallest_key_.empty()) {
            smallest_key_ = key;
        }
        largest_key_ = key;
        num_entries_++;

        // Add to bloom filter if enabled
        if (filter_builder_) {
            filter_builder_->AddKeys({key});
            metadata_builder_.SetFilterType(1, filter_builder_->GetFalsePositiveRate());
        }

        return common::Result<bool>::success(true);
    }

    common::Result<bool> Finish() {
        if (finished_) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "Writer is already finished");
        }

        if (!file_handle_) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "No data was written");
        }

        // Flush final data block if not empty
        if (!data_builder_.Empty()) {
            auto flush_result = FlushDataBlock();
            if (!flush_result.ok()) {
                return common::Result<bool>::failure(flush_result.error());
            }
        }

        // Write filter block if enabled
        uint64_t filter_offset = 0;
        uint64_t filter_size = 0;
        if (filter_builder_) {
            filter_offset = current_offset_;
            auto filter_data = filter_builder_->Finish();
            filter_size = filter_data.size();
            auto append_result = fs_->Append(file_handle_, common::DataChunk(filter_data.data(), filter_data.size()));
            if (!append_result.ok()) {
                return common::Result<bool>::failure(append_result.error());
            }
            current_offset_ += filter_data.size();
        }

        // Write index block
        uint64_t index_offset = current_offset_;
        auto index_data = index_builder_.Finish();
        uint64_t index_size = index_data.size();
        auto append_result = fs_->Append(file_handle_, common::DataChunk(index_data.data(), index_data.size()));
        if (!append_result.ok()) {
            return common::Result<bool>::failure(append_result.error());
        }
        current_offset_ += index_data.size();

        // Write metadata block
        uint64_t metadata_offset = current_offset_;
        auto metadata_data = metadata_builder_.Finish();
        uint64_t metadata_size = metadata_data.size();
        append_result = fs_->Append(file_handle_, common::DataChunk(metadata_data.data(), metadata_data.size()));
        if (!append_result.ok()) {
            return common::Result<bool>::failure(append_result.error());
        }
        current_offset_ += metadata_data.size();

        // Write footer
        Footer footer;
        footer.index_block_offset = index_offset;
        footer.filter_block_offset = filter_offset;
        footer.metadata_block_offset = metadata_offset;
        footer.index_block_size = index_size;
        footer.filter_block_size = filter_size;
        footer.metadata_block_size = metadata_size;
        auto footer_data = footer.Serialize();
        append_result = fs_->Append(file_handle_, common::DataChunk(footer_data.data(), footer_data.size()));
        if (!append_result.ok()) {
            return common::Result<bool>::failure(append_result.error());
        }

        // Close file
        auto close_result = fs_->CloseFile(file_handle_);
        if (!close_result.ok()) {
            return common::Result<bool>::failure(close_result.error());
        }
        file_handle_ = common::INVALID_HANDLE;
        finished_ = true;

        return common::Result<bool>::success(true);
    }

    void EnableFilter(size_t expected_keys, double false_positive_rate) {
        if (!filter_builder_ && !finished_ && num_entries_ == 0) {
            filter_builder_ = std::make_unique<FilterBlockBuilder>(expected_keys, false_positive_rate);
            metadata_builder_.SetFilterType(1, false_positive_rate);
        }
    }

    size_t GetMemoryUsage() const {
        // Approximate memory usage:
        // - Data block builder
        // - Index block builder
        // - Filter block builder (if enabled)
        // - Internal buffers
        size_t usage = data_builder_.GetMemoryUsage() + index_builder_.GetMemoryUsage();
        if (filter_builder_) {
            usage += filter_builder_->GetMemoryUsage();
        }
        return usage + kEstimatedBufferSize;
    }

    size_t GetFileSize() const { return current_offset_; }

private:
    static constexpr size_t kEstimatedBufferSize = 64 * 1024;  // 64KB for internal buffers
    common::Result<bool> FlushDataBlock() {
        auto block_data = data_builder_.Finish();
        if (block_data.empty()) {
            return common::Result<bool>::success(true);
        }

        // Append block to file
        auto append_result = fs_->Append(file_handle_, common::DataChunk(block_data.data(), block_data.size()));
        if (!append_result.ok()) {
            return common::Result<bool>::failure(append_result.error());
        }

        // Add to index
        index_builder_.AddEntry(last_key_, current_offset_, block_data.size(), data_builder_.GetKeys().size());
        current_offset_ += block_data.size();

        // Reset data builder
        data_builder_.Reset();

        return common::Result<bool>::success(true);
    }

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string path_;
    common::FileHandle file_handle_{common::INVALID_HANDLE};
    uint64_t current_offset_{0};
    bool finished_{false};

    // File components
    FileHeader header_;
    DataBlockBuilder data_builder_;
    IndexBlockBuilder index_builder_;
    std::unique_ptr<FilterBlockBuilder> filter_builder_;
    MetadataBlockBuilder metadata_builder_;

    // State tracking
    std::string last_key_;
    common::HybridTime last_version_;
    std::string smallest_key_;
    std::string largest_key_;
    size_t num_entries_{0};
};

SSTableWriter::SSTableWriter(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
    : impl_(std::make_unique<Impl>(std::move(fs), path)) {}

SSTableWriter::~SSTableWriter() = default;

common::Result<bool> SSTableWriter::Add(const std::string& key,
                                        const common::DataChunk& value,
                                        common::HybridTime version) {
    return impl_->Add(key, value, version);
}

common::Result<bool> SSTableWriter::Finish() {
    return impl_->Finish();
}

void SSTableWriter::EnableFilter(size_t expected_keys, double false_positive_rate) {
    impl_->EnableFilter(expected_keys, false_positive_rate);
}

size_t SSTableWriter::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

size_t SSTableWriter::GetFileSize() const {
    return impl_->GetFileSize();
}

}  // namespace pond::kv