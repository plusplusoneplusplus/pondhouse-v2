#include "sstable_reader.h"

#include <cassert>
#include <vector>

#include "common/error.h"
#include "common/log.h"

namespace pond::format {

class SSTableReader::Impl {
public:
    Impl(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
        : fs_(std::move(fs)), path_(path), file_size_(0), num_entries_(0) {}

    ~Impl() {
        if (file_handle_ != common::INVALID_HANDLE) {
            fs_->CloseFile(file_handle_);
        }
    }

    common::Result<bool> Open() {
        // Open file
        auto result = fs_->OpenFile(path_, false);
        if (!result.ok()) {
            return common::Result<bool>::failure(result.error());
        }
        file_handle_ = result.value();

        // Get file size
        auto size_result = fs_->Size(file_handle_);
        if (!size_result.ok()) {
            return common::Result<bool>::failure(size_result.error());
        }
        file_size_ = size_result.value();

        if (file_size_ < FileHeader::kHeaderSize + Footer::kFooterSize) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "File too small");
        }

        // Read and validate header
        auto header_result = ReadBlock(0, FileHeader::kHeaderSize);
        if (!header_result.ok()) {
            return common::Result<bool>::failure(header_result.error());
        }

        if (!header_.Deserialize(header_result.value().Data(), FileHeader::kHeaderSize)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid header");
        }

        // Read and validate footer
        auto footer_result = ReadBlock(file_size_ - Footer::kFooterSize, Footer::kFooterSize);
        if (!footer_result.ok()) {
            return common::Result<bool>::failure(footer_result.error());
        }

        if (!footer_.Deserialize(footer_result.value().Data(), Footer::kFooterSize)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid footer");
        }

        // Read metadata block if present
        if (footer_.metadata_block_offset > 0) {
            // Read metadata block
            auto metadata_size = footer_.metadata_block_size;
            auto read_result = ReadBlock(footer_.metadata_block_offset, metadata_size);
            if (!read_result.ok()) {
                return common::Result<bool>::failure(read_result.error());
            }
            auto metadata_data = read_result.value();

            MetadataBlockFooter metadata_footer;
            // Parse metadata block
            if (!metadata_footer.Deserialize(
                    metadata_data.Data() + metadata_data.Size() - MetadataBlockFooter::kFooterSize,
                    MetadataBlockFooter::kFooterSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata block");
            }

            // Verify metadata block checksum
            if (common::Crc32(metadata_data.Data(), metadata_data.Size() - MetadataBlockFooter::kFooterSize)
                != metadata_footer.checksum) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata block");
            }

            // Parse metadata sections
            if (!stats_.Deserialize(metadata_data.Data(), metadata_footer.stats_size)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata stats");
            }

            if (!properties_.Deserialize(metadata_data.Data() + metadata_footer.stats_size,
                                         metadata_footer.props_size)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid metadata props");
            }
        }

        // Read filter block if present
        if (footer_.filter_block_offset > 0) {
            auto filter_size = footer_.filter_block_size;
            auto read_result = ReadBlock(footer_.filter_block_offset, filter_size);
            if (!read_result.ok()) {
                return common::Result<bool>::failure(read_result.error());
            }
            filter_data_ = read_result.value();
        }

        // Read index block
        auto index_size = footer_.index_block_size;
        auto read_result = ReadBlock(footer_.index_block_offset, index_size);
        if (!read_result.ok()) {
            return common::Result<bool>::failure(read_result.error());
        }
        index_data_ = read_result.value();

        // Parse index entries
        size_t pos = 0;
        while (pos + IndexBlockEntry::kHeaderSize <= index_data_.Size() - BlockFooter::kFooterSize) {
            IndexBlockEntry entry;
            if (!entry.DeserializeHeader(index_data_.Data() + pos, IndexBlockEntry::kHeaderSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid index entry");
            }

            if (pos + IndexBlockEntry::kHeaderSize + entry.key_length > index_data_.Size() - BlockFooter::kFooterSize) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid,
                                                     "Invalid index entry key length");
            }

            std::string largest_key(
                reinterpret_cast<const char*>(index_data_.Data() + pos + IndexBlockEntry::kHeaderSize),
                entry.key_length);
            index_entries_.push_back({largest_key, entry.block_offset, entry.block_size, entry.entry_count});
            num_entries_ += entry.entry_count;

            pos += IndexBlockEntry::kHeaderSize + entry.key_length;
        }

        if (index_entries_.empty()) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "No index entries found");
        }

        // Read first data block to get the smallest key
        auto first_block_result = ReadBlock(index_entries_[0].offset, index_entries_[0].size);
        if (!first_block_result.ok()) {
            return common::Result<bool>::failure(first_block_result.error());
        }

        auto first_block_data = first_block_result.value();
        DataBlockEntry first_entry;
        if (!first_entry.DeserializeHeader(first_block_data.Data(), DataBlockEntry::kHeaderSize)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid first data entry");
        }

        InternalKey smallest;
        if (!smallest.Deserialize(first_block_data.Data() + DataBlockEntry::kHeaderSize, first_entry.key_length)) {
            return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid first data entry");
        }

        smallest_key_ = smallest.user_key();
        largest_key_ = index_entries_.back().largest_key;

        // Read bloom filter if present
        if (header_.HasFilter() && footer_.filter_block_offset > 0) {
            auto filter_size = footer_.index_block_offset - footer_.filter_block_offset;
            auto filter_result = ReadBlock(footer_.filter_block_offset, filter_size);
            if (!filter_result.ok()) {
                return common::Result<bool>::failure(filter_result.error());
            }

            auto filter_data = filter_result.value();
            FilterBlockFooter filter_footer;
            if (!filter_footer.Deserialize(filter_data.Data() + filter_data.Size() - FilterBlockFooter::kFooterSize,
                                           FilterBlockFooter::kFooterSize)) {
                return common::Result<bool>::failure(common::ErrorCode::SSTableInvalid, "Invalid filter footer");
            }

            auto filter_result_2 =
                common::BloomFilter::Deserialize(common::DataChunk(filter_data.Data(), filter_footer.filter_size));
            if (!filter_result_2.ok()) {
                return common::Result<bool>::failure(filter_result_2.error());
            }
            filter_ = std::make_unique<common::BloomFilter>(std::move(filter_result_2.value()));
        }

        opened_ = true;
        return common::Result<bool>::success(true);
    }

    common::Result<common::DataChunk> Get(const std::string& key,
                                          common::HybridTime version = common::MaxHybridTime()) {
        if (file_handle_ == common::INVALID_HANDLE) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::InvalidOperation, "Reader not opened");
        }

        // Check key range
        if (key < smallest_key_ || key > largest_key_) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
        }

        // Check bloom filter if present
        if (filter_
            && !filter_->MightContain(common::DataChunk(reinterpret_cast<const uint8_t*>(key.data()), key.size()))) {
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
        auto block_result = ReadBlock(it->offset, it->size);
        if (!block_result.ok()) {
            return common::Result<common::DataChunk>::failure(block_result.error());
        }

        // Parse data block entries
        auto block_data = block_result.value();
        size_t pos = 0;

        // Track the best matching version found so far
        common::DataChunk best_value;

        // Iterate through data block entries
        while (pos + DataBlockEntry::kHeaderSize <= block_data.Size() - BlockFooter::kFooterSize) {
            DataBlockEntry entry;
            if (!entry.DeserializeHeader(block_data.Data() + pos, DataBlockEntry::kHeaderSize)) {
                return common::Result<common::DataChunk>::failure(common::ErrorCode::SSTableInvalid,
                                                                  "Invalid data entry");
            }

            if (pos + DataBlockEntry::kHeaderSize + entry.key_length + entry.value_length
                > block_data.Size() - BlockFooter::kFooterSize) {
                return common::Result<common::DataChunk>::failure(common::ErrorCode::SSTableInvalid,
                                                                  "Invalid data entry lengths");
            }

            // Deserialize internal key
            InternalKey current_key;
            if (!current_key.Deserialize(block_data.Data() + pos + DataBlockEntry::kHeaderSize, entry.key_length)) {
                return common::Result<common::DataChunk>::failure(common::ErrorCode::SSTableInvalid,
                                                                  "Invalid internal key");
            }

            if (current_key.user_key() == key) {
                // Found matching key
                if (current_key.version() <= version) {
                    if (entry.IsTombstone()) {
                        return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
                    }

                    // If no specific version requested or exact version match, return immediately
                    const uint8_t* value_ptr = block_data.Data() + pos + DataBlockEntry::kHeaderSize + entry.key_length;
                    return common::Result<common::DataChunk>::success(common::DataChunk(value_ptr, entry.value_length));
                }
            } else if (current_key.user_key() > key) {
                // Gone past the key we're looking for
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
                filter_->MightContain(common::DataChunk(reinterpret_cast<const uint8_t*>(key.data()), key.size())));
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
        if (!opened_) {
            return common::Result<std::unique_ptr<common::BloomFilter>>::failure(common::ErrorCode::InvalidOperation,
                                                                                 "Reader not opened");
        }

        if (!filter_) {
            return common::Result<std::unique_ptr<common::BloomFilter>>::failure(common::ErrorCode::NotFound,
                                                                                 "No bloom filter present");
        }

        return common::Result<std::unique_ptr<common::BloomFilter>>::success(
            std::make_unique<common::BloomFilter>(*filter_));
    }

    bool IsOpen() const { return opened_; }

    size_t GetMemoryUsage() const {
        // Approximate memory usage:
        // - Index entries (strings + metadata)
        // - Filter data
        // - Index data
        // - Bloom filter (if present)
        // - Internal buffers
        size_t usage = 0;

        // Index entries
        for (const auto& entry : index_entries_) {
            usage += entry.largest_key.size() + sizeof(IndexEntry);
        }

        // Filter and index data
        usage += filter_data_.Size() + index_data_.Size();

        // Bloom filter memory usage is approximately the same as its data size
        if (filter_) {
            usage += filter_data_.Size();
        }

        return usage + kEstimatedBufferSize;
    }

    size_t GetBytesRead() const { return bytes_read_.load(std::memory_order_relaxed); }

    struct IndexEntry {
        std::string largest_key;
        uint64_t offset;
        uint32_t size;
        uint32_t entry_count;
    };

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string path_;
    bool opened_{false};
    common::FileHandle file_handle_{common::INVALID_HANDLE};
    size_t file_size_;
    size_t num_entries_;
    std::atomic<size_t> bytes_read_{0};  // Track total bytes read from disk

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

    common::Result<common::DataChunk> ReadBlock(uint64_t offset, uint32_t size) {
        if (file_handle_ == common::INVALID_HANDLE) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::InvalidOperation,
                                                              "File handle is invalid");
        }

        auto result = fs_->Read(file_handle_, offset, size);
        if (result.ok()) {
            bytes_read_ += size;  // Track bytes read
        }
        return result;
    }

private:
    static constexpr size_t kEstimatedBufferSize = 32 * 1024;  // 32KB for internal buffers
};

SSTableReader::SSTableReader(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path)
    : impl_(std::make_unique<Impl>(std::move(fs), path)) {}

SSTableReader::~SSTableReader() = default;

common::Result<bool> SSTableReader::Open() {
    return impl_->Open();
}

common::Result<common::DataChunk> SSTableReader::Get(const std::string& key,
                                                     common::HybridTime version /*= common::MaxHybridTime()*/) {
    return impl_->Get(key, version);
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

size_t SSTableReader::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

size_t SSTableReader::GetBytesRead() const {
    return impl_->GetBytesRead();
}

class SSTableReader::Iterator::Impl {
public:
    // Define VersionInfo struct as a member of Impl
    struct VersionInfo {
        size_t block_idx;
        size_t block_pos;
        common::DataChunk value;
        common::HybridTime version;
    };

    Impl(SSTableReader* reader, common::HybridTime read_time, common::IteratorMode mode, bool seek_to_first = true)
        : reader_(reader),
          read_time_(read_time),
          mode_(mode),
          valid_(false),
          current_block_idx_(0),
          block_pos_(0),
          current_version_idx_(0) {
        if (seek_to_first) {
            SeekToFirst();
        }
    }

    ~Impl() = default;

    bool Valid() const { return valid_; }

    void SeekToFirst() {
        if (!reader_->impl_->IsOpen()) {
            valid_ = false;
            return;
        }

        current_block_idx_ = 0;
        block_pos_ = 0;
        valid_ = LoadBlock(0) && ParseCurrentEntry();
        AdvanceToNextValidEntry();
    }

    void Seek(const std::string& target) {
        if (!reader_->impl_->IsOpen()) {
            valid_ = false;
            return;
        }

        valid_ = false;

        // Binary search through index entries
        auto it = std::upper_bound(
            reader_->impl_->index_entries_.begin(),
            reader_->impl_->index_entries_.end(),
            target,
            [](const std::string& k, const SSTableReader::Impl::IndexEntry& e) { return k < e.largest_key; });

        if (it != reader_->impl_->index_entries_.begin()) {
            --it;
        }

        current_block_idx_ = std::distance(reader_->impl_->index_entries_.begin(), it);
        if (!LoadBlock(current_block_idx_)) {
            return;
        }

        // Linear scan within block
        block_pos_ = 0;
        while (ParseCurrentEntry()) {
            if (current_internal_key_.user_key() >= target) {
                valid_ = true;
                AdvanceToNextValidEntry();
                return;
            }
            AdvanceToNextBlockPosition();
        }

        // If we didn't find the key in current block, try next block
        if (current_block_idx_ + 1 < reader_->impl_->index_entries_.size()) {
            current_block_idx_++;
            block_pos_ = 0;
            if (LoadBlock(current_block_idx_) && ParseCurrentEntry()) {
                valid_ = true;
                AdvanceToNextValidEntry();
                return;
            }
        }
        valid_ = false;
    }

    bool Next() {
        if (!valid_) {
            return false;
        }

        if (common::CheckIteratorMode(mode_, common::IteratorMode::IncludeAllVersions)
            && current_version_idx_ < visible_versions_.size() - 1) {
            // If we're iterating all versions and have more versions of current key
            current_version_idx_++;
            RestoreVersion(current_version_idx_);
            return true;
        }

        // Save current key to detect key changes
        const std::string current_key = current_internal_key_.user_key();

        // Skip all remaining versions of the current key since we've processed them
        while (true) {
            AdvanceToNextBlockPosition();

            // Try to parse the next entry
            if (!ParseCurrentEntry()) {
                // If we can't parse the next entry in current block, try next block
                if (current_block_idx_ + 1 < reader_->impl_->index_entries_.size()) {
                    current_block_idx_++;
                    block_pos_ = 0;
                    if (!LoadBlock(current_block_idx_)) {
                        valid_ = false;
                        return false;
                    }
                    if (!ParseCurrentEntry()) {
                        valid_ = false;
                        return false;
                    }
                } else {
                    valid_ = false;
                    return false;
                }
            }

            // If we've moved to a different key, stop skipping
            if (current_internal_key_.user_key() != current_key) {
                break;
            }
        }

        // Now we're at a new key, find its valid version(s)
        valid_ = true;
        current_version_idx_ = 0;
        visible_versions_.clear();
        AdvanceToNextValidEntry();
        return valid_;
    }

    void AdvanceToNextBlockPosition() {
        block_pos_ += DataBlockEntry::kHeaderSize + current_entry_.key_length + current_entry_.value_length;
    }

    const std::string& key() const { return current_internal_key_.user_key(); }

    const common::DataChunk& value() const { return current_entry_value_; }

    common::HybridTime version() const {
        if (!valid_) {
            return common::InvalidHybridTime();
        }

        return current_internal_key_.version();
    }

    bool IsTombstone() const { return current_entry_.IsTombstone(); }

private:
    bool LoadBlock(size_t block_idx) {
        const auto& index_entry = reader_->impl_->index_entries_[block_idx];
        auto block_result = reader_->impl_->ReadBlock(index_entry.offset, index_entry.size);

        if (!block_result.ok()) {
            valid_ = false;
            return false;
        }
        current_block_ = block_result.value();

        // Verify block footer
        BlockFooter footer;
        if (current_block_.Size() < BlockFooter::kFooterSize) {
            valid_ = false;
            return false;
        }

        const uint8_t* footer_data = current_block_.Data() + current_block_.Size() - BlockFooter::kFooterSize;
        if (!footer.Deserialize(footer_data, BlockFooter::kFooterSize)) {
            valid_ = false;
            return false;
        }

        // Validate block size matches footer
        if (current_block_.Size() != footer.block_size) {
            valid_ = false;
            return false;
        }

        // Verify checksum (exclude footer itself)
        const uint32_t computed_crc =
            common::Crc32(current_block_.Data(), current_block_.Size() - BlockFooter::kFooterSize);
        if (computed_crc != footer.checksum) {
            valid_ = false;
            return false;
        }

        return true;
    }

    bool ParseCurrentEntry() {
        const size_t block_data_size = current_block_.Size() - BlockFooter::kFooterSize;

        if (block_pos_ + DataBlockEntry::kHeaderSize > block_data_size) {
            return false;
        }

        // Deserialize header
        if (!current_entry_.DeserializeHeader(current_block_.Data() + block_pos_, DataBlockEntry::kHeaderSize)) {
            return false;
        }

        // Validate entry fits in block
        const size_t entry_size = DataBlockEntry::kHeaderSize + current_entry_.key_length + current_entry_.value_length;
        if (block_pos_ + entry_size > block_data_size) {
            return false;
        }

        // Deserialize internal key
        if (!current_internal_key_.Deserialize(current_block_.Data() + block_pos_ + DataBlockEntry::kHeaderSize,
                                               current_entry_.key_length)) {
            return false;
        }

        // Copy value data
        const uint8_t* value_ptr =
            current_block_.Data() + block_pos_ + DataBlockEntry::kHeaderSize + current_entry_.key_length;
        current_entry_value_ = common::DataChunk(value_ptr, current_entry_.value_length);

        return true;
    }

    void RestoreVersion(size_t version_idx) {
        const auto& version_info = visible_versions_[version_idx];
        if (current_block_idx_ != version_info.block_idx) {
            current_block_idx_ = version_info.block_idx;
            LoadBlock(current_block_idx_);
        }
        block_pos_ = version_info.block_pos;
        current_entry_value_ = version_info.value;
        ParseCurrentEntry();
    }

    void AdvanceToNextValidEntry() {
        const std::string current_key = current_internal_key_.user_key();
        bool found_valid_version = false;
        visible_versions_.clear();

        // Keep track of our starting position in case we need to revert
        const size_t start_block_idx = current_block_idx_;
        const size_t start_block_pos = block_pos_;

        while (true) {
            // Check if current version is visible
            if (current_internal_key_.version() <= read_time_) {
                found_valid_version = true;
                visible_versions_.push_back(
                    VersionInfo{current_block_idx_, block_pos_, current_entry_value_, current_internal_key_.version()});
            }

            // Try to advance to next entry
            AdvanceToNextBlockPosition();
            if (!ParseCurrentEntry() || current_internal_key_.user_key() != current_key) {
                // We've either hit the end of the block or found a different key
                if (found_valid_version) {
                    // Sort versions by timestamp (newest first)
                    std::sort(visible_versions_.begin(), visible_versions_.end(), [](const auto& a, const auto& b) {
                        return a.version > b.version;
                    });

                    // If we only want latest version, clear all but the first
                    if (!common::CheckIteratorMode(mode_, common::IteratorMode::IncludeAllVersions)
                        && !visible_versions_.empty()) {
                        visible_versions_.resize(1);
                    }

                    // Restore to the first version position
                    current_version_idx_ = 0;
                    RestoreVersion(0);
                    valid_ = true;
                    return;
                }

                // If we hit end of block, try next block
                if (!ParseCurrentEntry() && current_block_idx_ + 1 < reader_->impl_->index_entries_.size()) {
                    current_block_idx_++;
                    block_pos_ = 0;
                    if (!LoadBlock(current_block_idx_) || !ParseCurrentEntry()
                        || current_internal_key_.user_key() != current_key) {
                        break;  // Either failed to load block or found different key
                    }
                    continue;  // Continue checking versions in next block
                }
                break;  // No more versions to check
            }
        }

        // If we get here without finding a valid version, restore to start position and mark as invalid
        if (!found_valid_version) {
            if (current_block_idx_ != start_block_idx) {
                current_block_idx_ = start_block_idx;
                LoadBlock(current_block_idx_);
            }
            block_pos_ = start_block_pos;
            ParseCurrentEntry();
            valid_ = false;
        }
    }

    SSTableReader* reader_;
    common::HybridTime read_time_;
    common::IteratorMode mode_;
    bool valid_;
    size_t current_block_idx_;
    common::DataChunk current_block_;
    size_t block_pos_;
    DataBlockEntry current_entry_;
    InternalKey current_internal_key_;
    common::DataChunk current_entry_value_;
    std::vector<VersionInfo> visible_versions_;
    size_t current_version_idx_;
};

// Iterator implementation
SSTableReader::Iterator::Iterator(SSTableReader* reader,
                                  common::HybridTime read_time,
                                  common::IteratorMode mode,
                                  bool seek_to_first)
    : common::SnapshotIterator<std::string, common::DataChunk>(read_time, common::IteratorMode::Default),
      impl_(std::make_unique<Impl>(reader, read_time, mode, seek_to_first)) {}

SSTableReader::Iterator::~Iterator() = default;

SSTableReader::Iterator::Iterator(const Iterator& other)
    : common::SnapshotIterator<std::string, common::DataChunk>(other.read_time_, common::IteratorMode::Default),
      impl_(std::make_unique<Impl>(*other.impl_)) {}

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

common::HybridTime SSTableReader::Iterator::version() const {
    return impl_->version();
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

bool SSTableReader::Iterator::IsTombstone() const {
    return impl_->IsTombstone();
}

std::shared_ptr<SSTableReader::Iterator> SSTableReader::NewIterator(common::HybridTime read_time,
                                                                    common::IteratorMode mode) {
    return std::make_shared<Iterator>(this, read_time, mode, true);
}

SSTableReader::Iterator SSTableReader::begin(common::HybridTime read_time, common::IteratorMode mode) {
    return Iterator(this, read_time, mode, true);
}

SSTableReader::Iterator SSTableReader::end() {
    return Iterator(this, common::MaxHybridTime(), common::IteratorMode::Default, false);
}

}  // namespace pond::format