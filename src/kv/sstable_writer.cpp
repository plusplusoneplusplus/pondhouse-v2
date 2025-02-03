#include "sstable_writer.h"

#include <cassert>
#include <vector>

#include "common/error.h"
#include "common/log.h"

namespace pond::kv {

class SSTableWriter::Impl {
public:
    Impl(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
        : fs_(std::move(fs)), path_(path), data_builder_(), index_builder_() {
        // Initialize file header
        header_.version = 1;
        header_.flags = 0;
    }

    ~Impl() {
        if (file_handle_ != common::INVALID_HANDLE) {
            fs_->closeFile(file_handle_);
        }
    }

    common::Result<bool> Add(const std::string& key, const common::DataChunk& value) {
        if (finished_) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "Writer is already finished");
        }

        if (!file_handle_) {
            // Open file on first write
            auto result = fs_->openFile(path_, true);
            if (!result.ok()) {
                return common::Result<bool>::failure(result.error());
            }
            file_handle_ = result.value();

            // Write file header
            if (filter_builder_) {
                header_.flags |= FileHeader::kHasFilter;
            }
            auto header_data = header_.Serialize();
            auto append_result = fs_->append(file_handle_, common::DataChunk(header_data.data(), header_data.size()));
            if (!append_result.ok()) {
                return common::Result<bool>::failure(append_result.error());
            }
            current_offset_ = FileHeader::kHeaderSize;
        }

        // Validate key order
        if (!last_key_.empty() && key <= last_key_) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidArgument,
                                                 "Keys must be added in sorted order");
        }

        // Try to add to current data block
        if (!data_builder_.Add(key, value)) {
            // Current block is full, flush it
            auto flush_result = FlushDataBlock();
            if (!flush_result.ok()) {
                return common::Result<bool>::failure(flush_result.error());
            }

            // Try again with empty block
            if (!data_builder_.Add(key, value)) {
                return common::Result<bool>::failure(common::ErrorCode::InvalidArgument, "Entry too large for block");
            }
        }

        // Update state
        last_key_ = key;
        if (smallest_key_.empty()) {
            smallest_key_ = key;
        }
        largest_key_ = key;
        num_entries_++;

        // Add to bloom filter if enabled
        if (filter_builder_) {
            filter_builder_->AddKeys({key});
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
        if (filter_builder_) {
            filter_offset = current_offset_;
            auto filter_data = filter_builder_->Finish();
            auto append_result = fs_->append(file_handle_, common::DataChunk(filter_data.data(), filter_data.size()));
            if (!append_result.ok()) {
                return common::Result<bool>::failure(append_result.error());
            }
            current_offset_ += filter_data.size();
        }

        // Write index block
        uint64_t index_offset = current_offset_;
        auto index_data = index_builder_.Finish();
        auto append_result = fs_->append(file_handle_, common::DataChunk(index_data.data(), index_data.size()));
        if (!append_result.ok()) {
            return common::Result<bool>::failure(append_result.error());
        }
        current_offset_ += index_data.size();

        // Write footer
        Footer footer;
        footer.index_block_offset = index_offset;
        footer.filter_block_offset = filter_offset;
        footer.metadata_block_offset = 0;  // No metadata block for now
        auto footer_data = footer.Serialize();
        append_result = fs_->append(file_handle_, common::DataChunk(footer_data.data(), footer_data.size()));
        if (!append_result.ok()) {
            return common::Result<bool>::failure(append_result.error());
        }

        // Close file
        auto close_result = fs_->closeFile(file_handle_);
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
        }
    }

private:
    common::Result<bool> FlushDataBlock() {
        auto block_data = data_builder_.Finish();
        if (block_data.empty()) {
            return common::Result<bool>::success(true);
        }

        // Append block to file
        auto append_result = fs_->append(file_handle_, common::DataChunk(block_data.data(), block_data.size()));
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

    // State tracking
    std::string last_key_;
    std::string smallest_key_;
    std::string largest_key_;
    size_t num_entries_{0};
};

SSTableWriter::SSTableWriter(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
    : impl_(std::make_unique<Impl>(std::move(fs), path)) {}

SSTableWriter::~SSTableWriter() = default;

common::Result<bool> SSTableWriter::Add(const std::string& key, const common::DataChunk& value) {
    return impl_->Add(key, value);
}

common::Result<bool> SSTableWriter::Finish() {
    return impl_->Finish();
}

void SSTableWriter::EnableFilter(size_t expected_keys, double false_positive_rate) {
    impl_->EnableFilter(expected_keys, false_positive_rate);
}

}  // namespace pond::kv