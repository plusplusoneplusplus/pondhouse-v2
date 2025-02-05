#include "sstable_format.h"

#include <cassert>
#include <cstring>

#include "common/crc.h"

namespace pond::kv {

std::vector<uint8_t> FilterBlockFooter::Serialize() const {
    std::vector<uint8_t> result(kFooterSize);
    uint8_t* ptr = result.data();

    // Write all fields in little-endian order
    *reinterpret_cast<uint32_t*>(ptr) = util::HostToLittleEndian32(filter_size);
    ptr += sizeof(uint32_t);
    *reinterpret_cast<uint32_t*>(ptr) = util::HostToLittleEndian32(num_keys);
    ptr += sizeof(uint32_t);
    *reinterpret_cast<uint32_t*>(ptr) = util::HostToLittleEndian32(checksum);
    ptr += sizeof(uint32_t);
    *reinterpret_cast<uint32_t*>(ptr) = util::HostToLittleEndian32(reserved);

    return result;
}

bool FilterBlockFooter::Deserialize(const uint8_t* data, size_t size) {
    if (size < kFooterSize) {
        return false;
    }

    // Read all fields in little-endian order
    filter_size = util::LittleEndianToHost32(*reinterpret_cast<const uint32_t*>(data));
    data += sizeof(uint32_t);
    num_keys = util::LittleEndianToHost32(*reinterpret_cast<const uint32_t*>(data));
    data += sizeof(uint32_t);
    checksum = util::LittleEndianToHost32(*reinterpret_cast<const uint32_t*>(data));
    data += sizeof(uint32_t);
    reserved = util::LittleEndianToHost32(*reinterpret_cast<const uint32_t*>(data));

    return true;
}

std::vector<uint8_t> FileHeader::Serialize() const {
    std::vector<uint8_t> buffer(kHeaderSize);
    uint8_t* ptr = buffer.data();

    // Write magic number
    uint64_t le_magic = util::HostToLittleEndian64(magic_number);
    std::memcpy(ptr, &le_magic, sizeof(le_magic));
    ptr += sizeof(le_magic);

    // Write version
    uint32_t le_version = util::HostToLittleEndian32(version);
    std::memcpy(ptr, &le_version, sizeof(le_version));
    ptr += sizeof(le_version);

    // Write flags
    uint32_t le_flags = util::HostToLittleEndian32(flags);
    std::memcpy(ptr, &le_flags, sizeof(le_flags));
    ptr += sizeof(le_flags);

    // Write reserved
    std::memcpy(ptr, reserved, sizeof(reserved));

    return buffer;
}

bool FileHeader::Deserialize(const uint8_t* data, size_t size) {
    if (size < kHeaderSize)
        return false;

    const uint8_t* ptr = data;

    // Read magic number
    uint64_t le_magic;
    std::memcpy(&le_magic, ptr, sizeof(le_magic));
    magic_number = util::LittleEndianToHost64(le_magic);
    ptr += sizeof(le_magic);

    // Read version
    uint32_t le_version;
    std::memcpy(&le_version, ptr, sizeof(le_version));
    version = util::LittleEndianToHost32(le_version);
    ptr += sizeof(le_version);

    // Read flags
    uint32_t le_flags;
    std::memcpy(&le_flags, ptr, sizeof(le_flags));
    flags = util::LittleEndianToHost32(le_flags);
    ptr += sizeof(le_flags);

    // Read reserved
    std::memcpy(reserved, ptr, sizeof(reserved));

    return IsValid();
}

std::vector<uint8_t> Footer::Serialize() const {
    std::vector<uint8_t> buffer(kFooterSize);
    uint8_t* ptr = buffer.data();

    // Write block offsets
    uint64_t le_index_offset = util::HostToLittleEndian64(index_block_offset);
    std::memcpy(ptr, &le_index_offset, sizeof(le_index_offset));
    ptr += sizeof(le_index_offset);

    uint64_t le_filter_offset = util::HostToLittleEndian64(filter_block_offset);
    std::memcpy(ptr, &le_filter_offset, sizeof(le_filter_offset));
    ptr += sizeof(le_filter_offset);

    uint64_t le_metadata_offset = util::HostToLittleEndian64(metadata_block_offset);
    std::memcpy(ptr, &le_metadata_offset, sizeof(le_metadata_offset));
    ptr += sizeof(le_metadata_offset);

    // Write block sizes
    uint64_t le_index_size = util::HostToLittleEndian64(index_block_size);
    std::memcpy(ptr, &le_index_size, sizeof(le_index_size));
    ptr += sizeof(le_index_size);

    uint64_t le_filter_size = util::HostToLittleEndian64(filter_block_size);
    std::memcpy(ptr, &le_filter_size, sizeof(le_filter_size));
    ptr += sizeof(le_filter_size);

    uint64_t le_metadata_size = util::HostToLittleEndian64(metadata_block_size);
    std::memcpy(ptr, &le_metadata_size, sizeof(le_metadata_size));
    ptr += sizeof(le_metadata_size);

    // Write padding
    std::memcpy(ptr, padding, sizeof(padding));
    ptr += sizeof(padding);

    // Write magic number
    uint64_t le_magic = util::HostToLittleEndian64(magic_number);
    std::memcpy(ptr, &le_magic, sizeof(le_magic));

    return buffer;
}

bool Footer::Deserialize(const uint8_t* data, size_t size) {
    if (size < kFooterSize)
        return false;

    const uint8_t* ptr = data;

    // Read block offsets
    uint64_t le_index_offset;
    std::memcpy(&le_index_offset, ptr, sizeof(le_index_offset));
    index_block_offset = util::LittleEndianToHost64(le_index_offset);
    ptr += sizeof(le_index_offset);

    uint64_t le_filter_offset;
    std::memcpy(&le_filter_offset, ptr, sizeof(le_filter_offset));
    filter_block_offset = util::LittleEndianToHost64(le_filter_offset);
    ptr += sizeof(le_filter_offset);

    uint64_t le_metadata_offset;
    std::memcpy(&le_metadata_offset, ptr, sizeof(le_metadata_offset));
    metadata_block_offset = util::LittleEndianToHost64(le_metadata_offset);
    ptr += sizeof(le_metadata_offset);

    // Read block sizes
    uint64_t le_index_size;
    std::memcpy(&le_index_size, ptr, sizeof(le_index_size));
    index_block_size = util::LittleEndianToHost64(le_index_size);
    ptr += sizeof(le_index_size);

    uint64_t le_filter_size;
    std::memcpy(&le_filter_size, ptr, sizeof(le_filter_size));
    filter_block_size = util::LittleEndianToHost64(le_filter_size);
    ptr += sizeof(le_filter_size);

    uint64_t le_metadata_size;
    std::memcpy(&le_metadata_size, ptr, sizeof(le_metadata_size));
    metadata_block_size = util::LittleEndianToHost64(le_metadata_size);
    ptr += sizeof(le_metadata_size);

    // Read padding
    std::memcpy(padding, ptr, sizeof(padding));
    ptr += sizeof(padding);

    // Read magic number
    uint64_t le_magic;
    std::memcpy(&le_magic, ptr, sizeof(le_magic));
    magic_number = util::LittleEndianToHost64(le_magic);

    return IsValid();
}

std::vector<uint8_t> BlockFooter::Serialize() const {
    std::vector<uint8_t> buffer(kFooterSize);
    uint8_t* ptr = buffer.data();

    uint32_t le_count = util::HostToLittleEndian32(entry_count);
    std::memcpy(ptr, &le_count, sizeof(le_count));
    ptr += sizeof(le_count);

    uint32_t le_size = util::HostToLittleEndian32(block_size);
    std::memcpy(ptr, &le_size, sizeof(le_size));
    ptr += sizeof(le_size);

    uint32_t le_checksum = util::HostToLittleEndian32(checksum);
    std::memcpy(ptr, &le_checksum, sizeof(le_checksum));
    ptr += sizeof(le_checksum);

    uint32_t le_compression = util::HostToLittleEndian32(compression_type);
    std::memcpy(ptr, &le_compression, sizeof(le_compression));

    return buffer;
}

bool BlockFooter::Deserialize(const uint8_t* data, size_t size) {
    if (size < kFooterSize)
        return false;

    const uint8_t* ptr = data;

    uint32_t le_count;
    std::memcpy(&le_count, ptr, sizeof(le_count));
    entry_count = util::LittleEndianToHost32(le_count);
    ptr += sizeof(le_count);

    uint32_t le_size;
    std::memcpy(&le_size, ptr, sizeof(le_size));
    block_size = util::LittleEndianToHost32(le_size);
    ptr += sizeof(le_size);

    uint32_t le_checksum;
    std::memcpy(&le_checksum, ptr, sizeof(le_checksum));
    checksum = util::LittleEndianToHost32(le_checksum);
    ptr += sizeof(le_checksum);

    uint32_t le_compression;
    std::memcpy(&le_compression, ptr, sizeof(le_compression));
    compression_type = util::LittleEndianToHost32(le_compression);

    return true;
}

std::vector<uint8_t> DataBlockEntry::SerializeHeader() const {
    std::vector<uint8_t> buffer(kHeaderSize);
    uint8_t* ptr = buffer.data();

    uint32_t le_key_len = util::HostToLittleEndian32(key_length);
    std::memcpy(ptr, &le_key_len, sizeof(le_key_len));
    ptr += sizeof(le_key_len);

    uint32_t le_value_len = util::HostToLittleEndian32(value_length);
    std::memcpy(ptr, &le_value_len, sizeof(le_value_len));

    return buffer;
}

bool DataBlockEntry::DeserializeHeader(const uint8_t* data, size_t size) {
    if (size < kHeaderSize)
        return false;

    const uint8_t* ptr = data;

    uint32_t le_key_len;
    std::memcpy(&le_key_len, ptr, sizeof(le_key_len));
    key_length = util::LittleEndianToHost32(le_key_len);
    ptr += sizeof(le_key_len);

    uint32_t le_value_len;
    std::memcpy(&le_value_len, ptr, sizeof(le_value_len));
    value_length = util::LittleEndianToHost32(le_value_len);

    return true;
}

std::vector<uint8_t> DataBlockEntry::Serialize(const std::string& key, const common::DataChunk& value) const {
    assert(key.size() == key_length);
    assert(value.size() == value_length);

    std::vector<uint8_t> buffer;
    buffer.reserve(kHeaderSize + key_length + value_length);

    // Add header
    auto header = SerializeHeader();
    buffer.insert(buffer.end(), header.begin(), header.end());

    // Add key
    buffer.insert(buffer.end(), key.begin(), key.end());

    // Add value
    buffer.insert(buffer.end(), value.data(), value.data() + value.size());

    return buffer;
}

std::vector<uint8_t> IndexBlockEntry::SerializeHeader() const {
    std::vector<uint8_t> buffer(kHeaderSize);
    uint8_t* ptr = buffer.data();

    uint32_t le_key_len = util::HostToLittleEndian32(key_length);
    std::memcpy(ptr, &le_key_len, sizeof(le_key_len));
    ptr += sizeof(le_key_len);

    uint64_t le_offset = util::HostToLittleEndian64(block_offset);
    std::memcpy(ptr, &le_offset, sizeof(le_offset));
    ptr += sizeof(le_offset);

    uint32_t le_size = util::HostToLittleEndian32(block_size);
    std::memcpy(ptr, &le_size, sizeof(le_size));
    ptr += sizeof(le_size);

    uint32_t le_count = util::HostToLittleEndian32(entry_count);
    std::memcpy(ptr, &le_count, sizeof(le_count));

    return buffer;
}

bool IndexBlockEntry::DeserializeHeader(const uint8_t* data, size_t size) {
    if (size < kHeaderSize)
        return false;

    const uint8_t* ptr = data;

    uint32_t le_key_len;
    std::memcpy(&le_key_len, ptr, sizeof(le_key_len));
    key_length = util::LittleEndianToHost32(le_key_len);
    ptr += sizeof(le_key_len);

    uint64_t le_offset;
    std::memcpy(&le_offset, ptr, sizeof(le_offset));
    block_offset = util::LittleEndianToHost64(le_offset);
    ptr += sizeof(le_offset);

    uint32_t le_size;
    std::memcpy(&le_size, ptr, sizeof(le_size));
    block_size = util::LittleEndianToHost32(le_size);
    ptr += sizeof(le_size);

    uint32_t le_count;
    std::memcpy(&le_count, ptr, sizeof(le_count));
    entry_count = util::LittleEndianToHost32(le_count);

    return true;
}

std::vector<uint8_t> IndexBlockEntry::Serialize(const std::string& largest_key) const {
    assert(largest_key.size() == key_length);

    std::vector<uint8_t> buffer;
    buffer.reserve(kHeaderSize + key_length);

    // Add header
    auto header = SerializeHeader();
    buffer.insert(buffer.end(), header.begin(), header.end());

    // Add key
    buffer.insert(buffer.end(), largest_key.begin(), largest_key.end());

    return buffer;
}

bool DataBlockBuilder::Add(const std::string& key, const common::DataChunk& value) {
    size_t entry_size = DataBlockEntry::kHeaderSize + key.size() + value.size();
    if (!Empty() && current_size_ + entry_size > kTargetBlockSize) {
        return false;
    }

    entries_.push_back({key, value});
    current_size_ += entry_size;
    return true;
}

std::vector<uint8_t> DataBlockBuilder::Finish() {
    if (Empty()) {
        return {};
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(current_size_ + BlockFooter::kFooterSize);

    // Write all entries
    for (const auto& entry : entries_) {
        DataBlockEntry header;
        header.key_length = entry.key.size();
        header.value_length = entry.value.size();

        auto serialized = header.Serialize(entry.key, entry.value);
        buffer.insert(buffer.end(), serialized.begin(), serialized.end());
    }

    // Calculate checksum of data portion
    uint32_t checksum = common::Crc32(buffer.data(), buffer.size());

    // Add footer
    BlockFooter footer;
    footer.entry_count = entries_.size();
    footer.block_size = buffer.size() + BlockFooter::kFooterSize;
    footer.checksum = checksum;
    footer.compression_type = 0;  // No compression

    auto footer_data = footer.Serialize();
    buffer.insert(buffer.end(), footer_data.begin(), footer_data.end());

    return buffer;
}

void DataBlockBuilder::Reset() {
    entries_.clear();
    current_size_ = 0;
}

void IndexBlockBuilder::AddEntry(const std::string& largest_key,
                                 uint64_t block_offset,
                                 uint32_t block_size,
                                 uint32_t entry_count) {
    entries_.push_back({largest_key, block_offset, block_size, entry_count});
}

std::vector<uint8_t> IndexBlockBuilder::Finish() {
    if (Empty()) {
        return {};
    }

    std::vector<uint8_t> buffer;
    size_t total_size = 0;

    // Calculate total size needed
    for (const auto& entry : entries_) {
        total_size += IndexBlockEntry::kHeaderSize + entry.largest_key.size();
    }
    buffer.reserve(total_size + BlockFooter::kFooterSize);

    // Write all entries
    for (const auto& entry : entries_) {
        IndexBlockEntry header;
        header.key_length = entry.largest_key.size();
        header.block_offset = entry.block_offset;
        header.block_size = entry.block_size;
        header.entry_count = entry.entry_count;

        auto serialized = header.Serialize(entry.largest_key);
        buffer.insert(buffer.end(), serialized.begin(), serialized.end());
    }

    // Calculate checksum of data portion
    uint32_t checksum = common::Crc32(buffer.data(), buffer.size());

    // Add footer
    BlockFooter footer;
    footer.entry_count = entries_.size();
    footer.block_size = buffer.size() + BlockFooter::kFooterSize;
    footer.checksum = checksum;
    footer.compression_type = 0;  // No compression

    auto footer_data = footer.Serialize();
    buffer.insert(buffer.end(), footer_data.begin(), footer_data.end());

    return buffer;
}

void IndexBlockBuilder::Reset() {
    entries_.clear();
}

std::vector<uint8_t> MetadataStats::Serialize() const {
    // Calculate size needed
    size_t size = sizeof(uint64_t) * 3 +  // key_count, total_key_size, total_value_size
                  sizeof(uint32_t) * 2 +  // smallest_key length, largest_key length
                  smallest_key.size() + largest_key.size();

    std::vector<uint8_t> buffer(size);
    uint8_t* ptr = buffer.data();

    // Write counts and sizes
    uint64_t le_key_count = util::HostToLittleEndian64(key_count);
    std::memcpy(ptr, &le_key_count, sizeof(le_key_count));
    ptr += sizeof(le_key_count);

    uint64_t le_key_size = util::HostToLittleEndian64(total_key_size);
    std::memcpy(ptr, &le_key_size, sizeof(le_key_size));
    ptr += sizeof(le_key_size);

    uint64_t le_value_size = util::HostToLittleEndian64(total_value_size);
    std::memcpy(ptr, &le_value_size, sizeof(le_value_size));
    ptr += sizeof(le_value_size);

    // Write smallest key
    uint32_t smallest_len = smallest_key.size();
    uint32_t le_smallest_len = util::HostToLittleEndian32(smallest_len);
    std::memcpy(ptr, &le_smallest_len, sizeof(le_smallest_len));
    ptr += sizeof(le_smallest_len);
    std::memcpy(ptr, smallest_key.data(), smallest_len);
    ptr += smallest_len;

    // Write largest key
    uint32_t largest_len = largest_key.size();
    uint32_t le_largest_len = util::HostToLittleEndian32(largest_len);
    std::memcpy(ptr, &le_largest_len, sizeof(le_largest_len));
    ptr += sizeof(le_largest_len);
    std::memcpy(ptr, largest_key.data(), largest_len);

    return buffer;
}

bool MetadataStats::Deserialize(const uint8_t* data, size_t size) {
    if (size < sizeof(uint64_t) * 3 + sizeof(uint32_t) * 2) {
        return false;
    }

    const uint8_t* ptr = data;

    // Read counts and sizes
    uint64_t le_key_count;
    std::memcpy(&le_key_count, ptr, sizeof(le_key_count));
    key_count = util::LittleEndianToHost64(le_key_count);
    ptr += sizeof(le_key_count);

    uint64_t le_key_size;
    std::memcpy(&le_key_size, ptr, sizeof(le_key_size));
    total_key_size = util::LittleEndianToHost64(le_key_size);
    ptr += sizeof(le_key_size);

    uint64_t le_value_size;
    std::memcpy(&le_value_size, ptr, sizeof(le_value_size));
    total_value_size = util::LittleEndianToHost64(le_value_size);
    ptr += sizeof(le_value_size);

    // Read smallest key
    uint32_t smallest_len;
    std::memcpy(&smallest_len, ptr, sizeof(smallest_len));
    smallest_len = util::LittleEndianToHost32(smallest_len);
    ptr += sizeof(smallest_len);

    if (ptr + smallest_len > data + size) {
        return false;
    }
    smallest_key.assign(reinterpret_cast<const char*>(ptr), smallest_len);
    ptr += smallest_len;

    // Read largest key
    uint32_t largest_len;
    std::memcpy(&largest_len, ptr, sizeof(largest_len));
    largest_len = util::LittleEndianToHost32(largest_len);
    ptr += sizeof(largest_len);

    if (ptr + largest_len > data + size) {
        return false;
    }
    largest_key.assign(reinterpret_cast<const char*>(ptr), largest_len);

    return true;
}

std::vector<uint8_t> MetadataProperties::Serialize() const {
    std::vector<uint8_t> buffer(sizeof(uint64_t) + sizeof(uint32_t) * 3 + sizeof(double));
    uint8_t* ptr = buffer.data();

    // Write creation time
    uint64_t le_time = util::HostToLittleEndian64(creation_time);
    std::memcpy(ptr, &le_time, sizeof(le_time));
    ptr += sizeof(le_time);

    // Write compression type
    uint32_t le_compression = util::HostToLittleEndian32(compression_type);
    std::memcpy(ptr, &le_compression, sizeof(le_compression));
    ptr += sizeof(le_compression);

    // Write index type
    uint32_t le_index = util::HostToLittleEndian32(index_type);
    std::memcpy(ptr, &le_index, sizeof(le_index));
    ptr += sizeof(le_index);

    // Write filter type
    uint32_t le_filter = util::HostToLittleEndian32(filter_type);
    std::memcpy(ptr, &le_filter, sizeof(le_filter));
    ptr += sizeof(le_filter);

    // Write filter false positive rate
    std::memcpy(ptr, &filter_fp_rate, sizeof(filter_fp_rate));

    return buffer;
}

bool MetadataProperties::Deserialize(const uint8_t* data, size_t size) {
    if (size < sizeof(uint64_t) + sizeof(uint32_t) * 3 + sizeof(double)) {
        return false;
    }

    const uint8_t* ptr = data;

    // Read creation time
    uint64_t le_time;
    std::memcpy(&le_time, ptr, sizeof(le_time));
    creation_time = util::LittleEndianToHost64(le_time);
    ptr += sizeof(le_time);

    // Read compression type
    uint32_t le_compression;
    std::memcpy(&le_compression, ptr, sizeof(le_compression));
    compression_type = util::LittleEndianToHost32(le_compression);
    ptr += sizeof(le_compression);

    // Read index type
    uint32_t le_index;
    std::memcpy(&le_index, ptr, sizeof(le_index));
    index_type = util::LittleEndianToHost32(le_index);
    ptr += sizeof(le_index);

    // Read filter type
    uint32_t le_filter;
    std::memcpy(&le_filter, ptr, sizeof(le_filter));
    filter_type = util::LittleEndianToHost32(le_filter);
    ptr += sizeof(le_filter);

    // Read filter false positive rate
    std::memcpy(&filter_fp_rate, ptr, sizeof(filter_fp_rate));

    return true;
}

std::vector<uint8_t> MetadataBlockFooter::Serialize() const {
    std::vector<uint8_t> buffer(kFooterSize);
    uint8_t* ptr = buffer.data();

    uint32_t le_stats_size = util::HostToLittleEndian32(stats_size);
    std::memcpy(ptr, &le_stats_size, sizeof(le_stats_size));
    ptr += sizeof(le_stats_size);

    uint32_t le_props_size = util::HostToLittleEndian32(props_size);
    std::memcpy(ptr, &le_props_size, sizeof(le_props_size));
    ptr += sizeof(le_props_size);

    uint32_t le_checksum = util::HostToLittleEndian32(checksum);
    std::memcpy(ptr, &le_checksum, sizeof(le_checksum));
    ptr += sizeof(le_checksum);

    uint32_t le_reserved = util::HostToLittleEndian32(reserved);
    std::memcpy(ptr, &le_reserved, sizeof(le_reserved));

    return buffer;
}

bool MetadataBlockFooter::Deserialize(const uint8_t* data, size_t size) {
    if (size < kFooterSize) {
        return false;
    }

    const uint8_t* ptr = data;

    uint32_t le_stats_size;
    std::memcpy(&le_stats_size, ptr, sizeof(le_stats_size));
    stats_size = util::LittleEndianToHost32(le_stats_size);
    ptr += sizeof(le_stats_size);

    uint32_t le_props_size;
    std::memcpy(&le_props_size, ptr, sizeof(le_props_size));
    props_size = util::LittleEndianToHost32(le_props_size);
    ptr += sizeof(le_props_size);

    uint32_t le_checksum;
    std::memcpy(&le_checksum, ptr, sizeof(le_checksum));
    checksum = util::LittleEndianToHost32(le_checksum);
    ptr += sizeof(le_checksum);

    uint32_t le_reserved;
    std::memcpy(&le_reserved, ptr, sizeof(le_reserved));
    reserved = util::LittleEndianToHost32(le_reserved);

    return true;
}

}  // namespace pond::kv
