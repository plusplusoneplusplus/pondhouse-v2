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

        // Read metadata block if present
        if (footer_.metadata_block_offset > 0) {
            // Read metadata block
            auto metadata_size = footer_.metadata_block_size;
            auto read_result = fs_->read(file_handle_, footer_.metadata_block_offset, metadata_size);
            if (!read_result.ok()) {
                return common::Result<bool>::failure(read_result.error());
            }
            auto metadata_data = read_result.value();

            MetadataBlockFooter metadata_footer;
            // Parse metadata block
            if (!metadata_footer.Deserialize(
                    metadata_data.data() + metadata_data.size() - MetadataBlockFooter::kFooterSize,
                    MetadataBlockFooter::kFooterSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata block");
            }

            // Verify metadata block checksum
            if (common::Crc32(metadata_data.data(), metadata_data.size() - MetadataBlockFooter::kFooterSize)
                != metadata_footer.checksum) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata block");
            }

            // Parse metadata sections

            if (!stats_.Deserialize(metadata_data.data(), metadata_footer.stats_size)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata stats");
            }

            if (!properties_.Deserialize(metadata_data.data() + metadata_footer.stats_size,
                                         metadata_footer.props_size)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata props");
            }
        }

        // Read filter block if present
        if (footer_.filter_block_offset > 0) {
            auto filter_size = footer_.filter_block_size;
            auto read_result = fs_->read(file_handle_, footer_.filter_block_offset, filter_size);
            if (!read_result.ok()) {
                return common::Result<bool>::failure(read_result.error());
            }
            filter_data_ = read_result.value();
        }

        // Read index block
        auto index_size = footer_.index_block_size;
        auto read_result = fs_->read(file_handle_, footer_.index_block_offset, index_size);
        if (!read_result.ok()) {
            return common::Result<bool>::failure(read_result.error());
        }
        index_data_ = read_result.value();

        // Parse index entries
        size_t pos = 0;
        while (pos + IndexBlockEntry::kHeaderSize <= index_data_.size() - BlockFooter::kFooterSize) {
            IndexBlockEntry entry;
            if (!entry.DeserializeHeader(index_data_.data() + pos, IndexBlockEntry::kHeaderSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid index entry");
            }

            if (pos + IndexBlockEntry::kHeaderSize + entry.key_length > index_data_.size() - BlockFooter::kFooterSize) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid,
                                                     "Invalid index entry key length");
            }

            std::string largest_key(
                reinterpret_cast<const char*>(index_data_.data() + pos + IndexBlockEntry::kHeaderSize),
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
                common::BloomFilter::Deserialize(common::DataChunk(filter_data.data(), filter_footer.filter_size));
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

    common::Result<Metadata> GetMetadata() const {
        if (file_handle_ == common::INVALID_HANDLE) {
            return common::Result<Metadata>::failure(common::ErrorCode::InvalidOperation, "Reader not opened");
        }

        if (footer_.metadata_block_offset == 0) {
            return common::Result<Metadata>::failure(common::ErrorCode::NotFound, "No metadata block present");
        }

        Metadata metadata;
        metadata.stats = stats_;
        metadata.props = properties_;
        return common::Result<Metadata>::success(metadata);
    }

    common::Result<std::unique_ptr<common::BloomFilter>> GetBloomFilter() const {
        if (!header_.HasFilter()) {
            return common::Result<std::unique_ptr<common::BloomFilter>>::failure(
                common::ErrorCode::NotFound, "SSTable does not have a bloom filter");
        }

        // Read filter block
        auto filter_data = fs_->read(file_handle_, footer_.filter_block_offset, footer_.filter_block_size);
        if (!filter_data.ok()) {
            return common::Result<std::unique_ptr<common::BloomFilter>>::failure(filter_data.error());
        }

        // Deserialize filter
        auto filter_result = common::BloomFilter::Deserialize(filter_data.value());
        if (!filter_result.ok()) {
            return common::Result<std::unique_ptr<common::BloomFilter>>::failure(common::ErrorCode::InvalidArgument,
                                                                                 "Failed to deserialize bloom filter");
        }

        return common::Result<std::unique_ptr<common::BloomFilter>>::success(
            std::make_unique<common::BloomFilter>(std::move(filter_result).value()));
    }

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

    // Add metadata members
    MetadataStats stats_;
    MetadataProperties properties_;

    // Metadata block data
    common::DataChunk filter_data_;
    common::DataChunk index_data_;
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

common::Result<SSTableReader::Metadata> SSTableReader::GetMetadata() const {
    return impl_->GetMetadata();
}

common::Result<std::unique_ptr<common::BloomFilter>> SSTableReader::GetBloomFilter() const {
    if (!impl_) {
        return common::Result<std::unique_ptr<common::BloomFilter>>::failure(common::ErrorCode::InvalidOperation,
                                                                             "Reader not initialized");
    }
    return impl_->GetBloomFilter();
}

class SSTableReader::Iterator::Impl {
public:
    explicit Impl(SSTableReader* reader) : reader_(reader), valid_(false) {}

    bool Valid() const { return valid_; }

    const std::string& key() const {
        assert(valid_);
        return current_key_;
    }

    const common::DataChunk& value() const {
        assert(valid_);
        return current_value_;
    }

    void Next() {
        assert(valid_);

        // If we're at the end of the current block, move to next block
        if (block_pos_ + DataBlockEntry::kHeaderSize + current_entry_.key_length + current_entry_.value_length
            >= current_block_.size() - BlockFooter::kFooterSize) {
            if (current_block_idx_ + 1 >= reader_->impl_->index_entries_.size()) {
                valid_ = false;
                return;
            }
            LoadBlock(++current_block_idx_);
            block_pos_ = 0;
        } else {
            // Move to next entry in current block
            block_pos_ += DataBlockEntry::kHeaderSize + current_entry_.key_length + current_entry_.value_length;
        }

        ParseCurrentEntry();
    }

    void SeekToFirst() {
        if (reader_->impl_->index_entries_.empty()) {
            valid_ = false;
            return;
        }

        current_block_idx_ = 0;
        LoadBlock(current_block_idx_);
        block_pos_ = 0;
        ParseCurrentEntry();
    }

    void Seek(const std::string& target) {
        // Find the block that may contain the target key
        auto it = std::upper_bound(
            reader_->impl_->index_entries_.begin(),
            reader_->impl_->index_entries_.end(),
            target,
            [](const std::string& k, const SSTableReader::Impl::IndexEntry& e) { return k <= e.largest_key; });

        if (it == reader_->impl_->index_entries_.end()) {
            valid_ = false;
            return;
        }

        current_block_idx_ = std::distance(reader_->impl_->index_entries_.begin(), it);
        LoadBlock(current_block_idx_);
        block_pos_ = 0;

        // Scan through entries in the block until we find the first key >= target
        while (block_pos_ + DataBlockEntry::kHeaderSize <= current_block_.size() - BlockFooter::kFooterSize) {
            if (!ParseCurrentEntry()) {
                valid_ = false;
                return;
            }

            if (current_key_ >= target) {
                return;
            }

            block_pos_ += DataBlockEntry::kHeaderSize + current_entry_.key_length + current_entry_.value_length;
        }

        valid_ = false;
    }

private:
    bool LoadBlock(size_t block_idx) {
        const auto& index_entry = reader_->impl_->index_entries_[block_idx];
        auto block_result =
            reader_->impl_->fs_->read(reader_->impl_->file_handle_, index_entry.offset, index_entry.size);
        if (!block_result.ok()) {
            valid_ = false;
            return false;
        }
        current_block_ = block_result.value();
        return true;
    }

    bool ParseCurrentEntry() {
        if (block_pos_ + DataBlockEntry::kHeaderSize > current_block_.size() - BlockFooter::kFooterSize) {
            valid_ = false;
            return false;
        }

        if (!current_entry_.DeserializeHeader(current_block_.data() + block_pos_, DataBlockEntry::kHeaderSize)) {
            valid_ = false;
            return false;
        }

        if (block_pos_ + DataBlockEntry::kHeaderSize + current_entry_.key_length + current_entry_.value_length
            > current_block_.size() - BlockFooter::kFooterSize) {
            valid_ = false;
            return false;
        }

        const char* key_ptr =
            reinterpret_cast<const char*>(current_block_.data() + block_pos_ + DataBlockEntry::kHeaderSize);
        current_key_ = std::string(key_ptr, current_entry_.key_length);

        const uint8_t* value_ptr =
            current_block_.data() + block_pos_ + DataBlockEntry::kHeaderSize + current_entry_.key_length;
        current_value_ = common::DataChunk(value_ptr, current_entry_.value_length);

        valid_ = true;
        return true;
    }

    SSTableReader* reader_;
    bool valid_;
    size_t current_block_idx_{0};
    common::DataChunk current_block_;
    size_t block_pos_{0};
    DataBlockEntry current_entry_;
    std::string current_key_;
    common::DataChunk current_value_;
};

// Iterator implementation
SSTableReader::Iterator::Iterator(SSTableReader* reader) : impl_(std::make_unique<Impl>(reader)) {}

SSTableReader::Iterator::~Iterator() = default;

SSTableReader::Iterator::Iterator(const Iterator& other) : impl_(std::make_unique<Impl>(*other.impl_)) {}

SSTableReader::Iterator& SSTableReader::Iterator::operator=(const Iterator& other) {
    if (this != &other) {
        impl_ = std::make_unique<Impl>(*other.impl_);
    }
    return *this;
}

bool SSTableReader::Iterator::Valid() const {
    return impl_->Valid();
}

const std::string& SSTableReader::Iterator::key() const {
    return impl_->key();
}

const common::DataChunk& SSTableReader::Iterator::value() const {
    return impl_->value();
}

void SSTableReader::Iterator::Next() {
    impl_->Next();
}

void SSTableReader::Iterator::SeekToFirst() {
    impl_->SeekToFirst();
}

void SSTableReader::Iterator::Seek(const std::string& target) {
    impl_->Seek(target);
}

// Add NewIterator method to SSTableReader
std::unique_ptr<SSTableReader::Iterator> SSTableReader::NewIterator() {
    return std::make_unique<Iterator>(this);
}

SSTableReader::Iterator SSTableReader::begin() {
    auto iter = Iterator(this);
    iter.SeekToFirst();
    return iter;
}

SSTableReader::Iterator SSTableReader::end() {
    return Iterator(this);  // Creates an invalid iterator representing the end
}

}  // namespace pond::kv