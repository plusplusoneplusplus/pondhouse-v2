#include "sstable_reader.h"

#include <cassert>
#include <vector>

#include "common/error.h"
#include "common/log.h"

namespace pond::kv {

class SSTableReader::Impl {
public:
    Impl(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
        : fs_(std::move(fs)), path_(path), file_size_(0), num_entries_(0) {}

    ~Impl() {
        if (file_handle_ != common::INVALID_HANDLE) {
            fs_->closeFile(file_handle_);
        }
    }

    common::Result<bool> Open() {
        // Open file
        auto result = fs_->openFile(path_, false);
        if (!result.ok()) {
            return common::Result<bool>::failure(result.error());
        }
        file_handle_ = result.value();

        // Get file size
        auto size_result = fs_->size(file_handle_);
        if (!size_result.ok()) {
            return common::Result<bool>::failure(size_result.error());
        }
        file_size_ = size_result.value();

        if (file_size_ < FileHeader::kHeaderSize + Footer::kFooterSize) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "File too small");
        }

        // Read and validate header
        auto header_result = fs_->read(file_handle_, 0, FileHeader::kHeaderSize);
        if (!header_result.ok()) {
            return common::Result<bool>::failure(header_result.error());
        }

        if (!header_.Deserialize(header_result.value().data(), FileHeader::kHeaderSize)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid header");
        }

        // Read and validate footer
        auto footer_result = fs_->read(file_handle_, file_size_ - Footer::kFooterSize, Footer::kFooterSize);
        if (!footer_result.ok()) {
            return common::Result<bool>::failure(footer_result.error());
        }

        if (!footer_.Deserialize(footer_result.value().data(), Footer::kFooterSize)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid footer");
        }

        // Read index block
        auto index_size = file_size_ - footer_.index_block_offset - Footer::kFooterSize;
        auto index_result = fs_->read(file_handle_, footer_.index_block_offset, index_size);
        if (!index_result.ok()) {
            return common::Result<bool>::failure(index_result.error());
        }

        // Parse index entries
        auto index_data = index_result.value();
        size_t pos = 0;
        while (pos + IndexBlockEntry::kHeaderSize <= index_data.size() - BlockFooter::kFooterSize) {
            IndexBlockEntry entry;
            if (!entry.DeserializeHeader(index_data.data() + pos, IndexBlockEntry::kHeaderSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid index entry");
            }

            if (pos + IndexBlockEntry::kHeaderSize + entry.key_length > index_data.size() - BlockFooter::kFooterSize) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid,
                                                     "Invalid index entry key length");
            }

            std::string largest_key(
                reinterpret_cast<const char*>(index_data.data() + pos + IndexBlockEntry::kHeaderSize),
                entry.key_length);
            index_entries_.push_back({largest_key, entry.block_offset, entry.block_size, entry.entry_count});
            num_entries_ += entry.entry_count;

            pos += IndexBlockEntry::kHeaderSize + entry.key_length;
        }

        if (index_entries_.empty()) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "No index entries found");
        }

        // Read first data block to get the smallest key
        auto first_block_result = fs_->read(file_handle_, index_entries_[0].offset, index_entries_[0].size);
        if (!first_block_result.ok()) {
            return common::Result<bool>::failure(first_block_result.error());
        }

        auto first_block_data = first_block_result.value();
        DataBlockEntry first_entry;
        if (!first_entry.DeserializeHeader(first_block_data.data(), DataBlockEntry::kHeaderSize)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid first data entry");
        }

        smallest_key_ =
            std::string(reinterpret_cast<const char*>(first_block_data.data() + DataBlockEntry::kHeaderSize),
                        first_entry.key_length);
        largest_key_ = index_entries_.back().largest_key;

        // Read bloom filter if present
        if (header_.HasFilter() && footer_.filter_block_offset > 0) {
            auto filter_size = footer_.index_block_offset - footer_.filter_block_offset;
            auto filter_result = fs_->read(file_handle_, footer_.filter_block_offset, filter_size);
            if (!filter_result.ok()) {
                return common::Result<bool>::failure(filter_result.error());
            }

            auto filter_data = filter_result.value();
            FilterBlockFooter filter_footer;
            if (!filter_footer.Deserialize(filter_data.data() + filter_data.size() - FilterBlockFooter::kFooterSize,
                                           FilterBlockFooter::kFooterSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid filter footer");
            }

            auto filter_result_2 =
                common::BloomFilter::deserialize(common::DataChunk(filter_data.data(), filter_footer.filter_size));
            if (!filter_result_2.ok()) {
                return common::Result<bool>::failure(filter_result_2.error());
            }
            filter_ = std::make_unique<common::BloomFilter>(std::move(filter_result_2.value()));
        }

        return common::Result<bool>::success(true);
    }

    common::Result<common::DataChunk> Get(const std::string& key) {
        if (file_handle_ == common::INVALID_HANDLE) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::InvalidOperation, "Reader not opened");
        }

        // Check key range
        if (key < smallest_key_ || key > largest_key_) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
        }

        // Check bloom filter if present
        if (filter_
            && !filter_->mightContain(common::DataChunk(reinterpret_cast<const uint8_t*>(key.data()), key.size()))) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
        }

        // Find data block containing key
        auto it = std::upper_bound(
            index_entries_.begin(), index_entries_.end(), key, [](const std::string& k, const IndexEntry& e) {
                return k <= e.largest_key;
            });
        if (it == index_entries_.end()) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
        }

        // Read data block
        auto block_result = fs_->read(file_handle_, it->offset, it->size);
        if (!block_result.ok()) {
            return common::Result<common::DataChunk>::failure(block_result.error());
        }

        // Parse data block entries
        auto block_data = block_result.value();
        size_t pos = 0;

        // Iterate through data block entries, can be optimized to binary search if needed
        while (pos + DataBlockEntry::kHeaderSize <= block_data.size() - BlockFooter::kFooterSize) {
            DataBlockEntry entry;
            if (!entry.DeserializeHeader(block_data.data() + pos, DataBlockEntry::kHeaderSize)) {
                return common::Result<common::DataChunk>::failure(common::ErrorCode::SSTableInvalid,
                                                                  "Invalid data entry");
            }

            if (pos + DataBlockEntry::kHeaderSize + entry.key_length + entry.value_length
                > block_data.size() - BlockFooter::kFooterSize) {
                return common::Result<common::DataChunk>::failure(common::ErrorCode::SSTableInvalid,
                                                                  "Invalid data entry lengths");
            }

            std::string current_key(
                reinterpret_cast<const char*>(block_data.data() + pos + DataBlockEntry::kHeaderSize), entry.key_length);

            if (current_key == key) {
                // Found the key
                const uint8_t* value_ptr = block_data.data() + pos + DataBlockEntry::kHeaderSize + entry.key_length;
                return common::Result<common::DataChunk>::success(common::DataChunk(value_ptr, entry.value_length));
            } else if (current_key > key) {
                // Key not found (we've gone past it)
                break;
            }

            pos += DataBlockEntry::kHeaderSize + entry.key_length + entry.value_length;
        }

        // Key not found
        return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
    }

    common::Result<bool> MayContain(const std::string& key) {
        if (file_handle_ == common::INVALID_HANDLE) {
            return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "Reader not opened");
        }

        // Check key range
        if (key < smallest_key_ || key > largest_key_) {
            return common::Result<bool>::success(false);
        }

        // Check bloom filter if present
        if (filter_) {
            return common::Result<bool>::success(
                filter_->mightContain(common::DataChunk(reinterpret_cast<const uint8_t*>(key.data()), key.size())));
        }

        // No bloom filter, have to assume it might be present
        return common::Result<bool>::success(true);
    }

    size_t GetEntryCount() const { return num_entries_; }
    size_t GetFileSize() const { return file_size_; }
    const std::string& GetSmallestKey() const { return smallest_key_; }
    const std::string& GetLargestKey() const { return largest_key_; }

private:
    struct IndexEntry {
        std::string largest_key;
        uint64_t offset;
        uint32_t size;
        uint32_t entry_count;
    };

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string path_;
    common::FileHandle file_handle_{common::INVALID_HANDLE};
    size_t file_size_;
    size_t num_entries_;

    // File components
    FileHeader header_;
    Footer footer_;
    std::vector<IndexEntry> index_entries_;
    std::unique_ptr<common::BloomFilter> filter_;

    // Key range
    std::string smallest_key_;
    std::string largest_key_;
};

SSTableReader::SSTableReader(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
    : impl_(std::make_unique<Impl>(std::move(fs), path)) {}

SSTableReader::~SSTableReader() = default;

common::Result<bool> SSTableReader::Open() {
    return impl_->Open();
}

common::Result<common::DataChunk> SSTableReader::Get(const std::string& key) {
    return impl_->Get(key);
}

common::Result<bool> SSTableReader::MayContain(const std::string& key) {
    return impl_->MayContain(key);
}

size_t SSTableReader::GetEntryCount() const {
    return impl_->GetEntryCount();
}

size_t SSTableReader::GetFileSize() const {
    return impl_->GetFileSize();
}

const std::string& SSTableReader::GetSmallestKey() const {
    return impl_->GetSmallestKey();
}

const std::string& SSTableReader::GetLargestKey() const {
    return impl_->GetLargestKey();
}

}  // namespace pond::kv