#include "kv/sstable.h"

#include <algorithm>
#include <cstring>
#include <sstream>

#include "common/crc.h"
#include "common/time.h"

namespace pond::kv {

SSTable::SSTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& table_name)
    : fs_(std::move(fs)), table_name_(table_name), index_tree_(std::make_unique<IndexTree>()) {}

common::Result<void> SSTable::Write(const std::vector<KvEntry>& entries) {
    if (entries.empty()) {
        return common::Result<void>::success();
    }

    // Open or create the file
    auto handle_result = fs_->openFile(table_name_, true);
    if (!handle_result.ok()) {
        return common::Result<void>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Sort entries by key
    std::vector<KvEntry> sorted_entries = entries;
    std::sort(
        sorted_entries.begin(), sorted_entries.end(), [](const KvEntry& a, const KvEntry& b) { return a.key < b.key; });

    // Write entries and build index
    index_tree_->Clear();
    for (const auto& entry : sorted_entries) {
        // Write entry to file
        auto write_result = WriteEntry(entry);
        if (!write_result.ok()) {
            return common::Result<void>::failure(write_result.error());
        }

        // Add to index
        auto insert_result = index_tree_->Insert(entry.key, write_result.value());
        if (!insert_result.ok()) {
            return common::Result<void>::failure(insert_result.error());
        }
    }

    // Write metadata block
    MetaBlock meta;
    meta.num_entries = entries.size();
    meta.smallest_key = sorted_entries.front().key;
    meta.largest_key = sorted_entries.back().key;
    meta.create_timestamp = common::now();
    meta.checksum = common::crc32(reinterpret_cast<const uint8_t*>(&meta), sizeof(meta) - sizeof(meta.checksum));

    auto meta_result = WriteMetaBlock(meta);
    if (!meta_result.ok()) {
        return common::Result<void>::failure(meta_result.error());
    }

    // Write index tree
    auto index_result = WriteIndex();
    if (!index_result.ok()) {
        return common::Result<void>::failure(index_result.error());
    }

    // Write footer
    Footer footer;
    footer.root_handle = index_result.value();
    footer.meta_handle = meta_result.value();
    footer.checksum =
        common::crc32(reinterpret_cast<const uint8_t*>(&footer), sizeof(footer) - sizeof(footer.checksum));

    return WriteFooter(footer);
}

common::Result<BlockHandle> SSTable::WriteNode(const Node& node) {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<BlockHandle>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Get current position as node offset
    auto size_result = fs_->size(file_handle);
    if (!size_result.ok()) {
        return common::Result<BlockHandle>::failure(size_result.error());
    }

    // Serialize node
    auto serialized = node.Serialize();
    if (!serialized.ok()) {
        return common::Result<BlockHandle>::failure(serialized.error());
    }

    // Create block handle
    BlockHandle handle;
    handle.offset = size_result.value();
    handle.size = serialized.value().size();
    handle.checksum =
        common::crc32(reinterpret_cast<const uint8_t*>(serialized.value().data()), serialized.value().size());

    // Write node
    common::DataChunk data(reinterpret_cast<const uint8_t*>(serialized.value().data()), serialized.value().size());
    auto append_result = fs_->append(file_handle, data);
    if (!append_result.ok()) {
        return common::Result<BlockHandle>::failure(append_result.error());
    }

    return common::Result<BlockHandle>::success(handle);
}

common::Result<BlockHandle> SSTable::BuildBPlusTree(const std::vector<KvEntry>& sorted_entries) {
    std::vector<Node> leaf_nodes;
    std::vector<std::string> current_keys;
    std::vector<BlockHandle> current_children;
    size_t current_size = 0;

    // Create leaf nodes
    for (const auto& entry : sorted_entries) {
        if (current_size + entry.size() > BLOCK_SIZE || current_keys.size() >= 2 * MIN_KEYS_PER_NODE) {
            // Write current leaf node
            Node leaf_node;
            leaf_node.type = NodeType::LEAF;
            leaf_node.keys = std::move(current_keys);
            leaf_node.children = std::move(current_children);
            leaf_nodes.push_back(std::move(leaf_node));

            current_keys.clear();
            current_children.clear();
            current_size = 0;
        }

        // Add entry to current leaf node
        current_keys.push_back(entry.key());
        auto leaf_result = WriteLeafBlock({entry});
        if (!leaf_result.ok()) {
            return common::Result<BlockHandle>::failure(leaf_result.error());
        }
        current_children.push_back(leaf_result.value());
        current_size += entry.size();
    }

    // Write last leaf node if not empty
    if (!current_keys.empty()) {
        Node leaf_node;
        leaf_node.type = NodeType::LEAF;
        leaf_node.keys = std::move(current_keys);
        leaf_node.children = std::move(current_children);
        leaf_nodes.push_back(std::move(leaf_node));
    }

    // Link leaf nodes
    for (size_t i = 0; i < leaf_nodes.size() - 1; i++) {
        auto next_result = WriteNode(leaf_nodes[i + 1]);
        if (!next_result.ok()) {
            return common::Result<BlockHandle>::failure(next_result.error());
        }
        leaf_nodes[i].next = next_result.value();
    }

    // Build internal nodes bottom-up
    std::vector<Node> current_level = std::move(leaf_nodes);
    while (current_level.size() > 1) {
        std::vector<Node> next_level;
        std::vector<std::string> parent_keys;
        std::vector<BlockHandle> parent_children;

        for (const auto& node : current_level) {
            if (parent_keys.size() >= 2 * MIN_KEYS_PER_NODE) {
                // Write current internal node
                Node internal_node;
                internal_node.type = NodeType::INTERNAL;
                internal_node.keys = std::move(parent_keys);
                internal_node.children = std::move(parent_children);
                next_level.push_back(std::move(internal_node));

                parent_keys.clear();
                parent_children.clear();
            }

            // Add node to current internal node
            if (!parent_children.empty()) {
                parent_keys.push_back(node.keys.front());
            }
            auto node_result = WriteNode(node);
            if (!node_result.ok()) {
                return common::Result<BlockHandle>::failure(node_result.error());
            }
            parent_children.push_back(node_result.value());
        }

        // Write last internal node if not empty
        if (!parent_keys.empty() || !parent_children.empty()) {
            Node internal_node;
            internal_node.type = NodeType::INTERNAL;
            internal_node.keys = std::move(parent_keys);
            internal_node.children = std::move(parent_children);
            next_level.push_back(std::move(internal_node));
        }

        current_level = std::move(next_level);
    }

    // Write root node
    if (current_level.empty()) {
        return common::Result<BlockHandle>::failure(common::ErrorCode::Unknown, "Failed to build B+ tree");
    }

    return WriteNode(current_level[0]);
}

common::Result<BlockHandle> SSTable::WriteLeafBlock(const std::vector<KvEntry>& entries) {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<BlockHandle>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Get current position
    auto size_result = fs_->size(file_handle);
    if (!size_result.ok()) {
        return common::Result<BlockHandle>::failure(size_result.error());
    }

    // Serialize entries
    std::stringstream ss;
    uint32_t num_entries = entries.size();
    ss.write(reinterpret_cast<const char*>(&num_entries), sizeof(num_entries));

    for (const auto& entry : entries) {
        common::DataChunk data_chunk;
        entry.serialize(data_chunk);
        uint32_t entry_size = data_chunk.size();
        ss.write(reinterpret_cast<const char*>(&entry_size), sizeof(entry_size));
        ss.write(reinterpret_cast<const char*>(data_chunk.data()), entry_size);
    }

    std::string serialized = ss.str();

    // Create block handle
    BlockHandle handle;
    handle.offset = size_result.value();
    handle.size = serialized.size();
    handle.checksum = common::crc32(reinterpret_cast<const uint8_t*>(serialized.data()), serialized.size());

    // Write block
    common::DataChunk data(reinterpret_cast<const uint8_t*>(serialized.data()), serialized.size());
    auto append_result = fs_->append(file_handle, data);
    if (!append_result.ok()) {
        return common::Result<BlockHandle>::failure(append_result.error());
    }

    return common::Result<BlockHandle>::success(handle);
}

common::Result<std::vector<KvEntry>> SSTable::ReadLeafBlock(const BlockHandle& handle) const {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<std::vector<KvEntry>>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Read block data
    auto read_result = fs_->read(file_handle, handle.offset, handle.size);
    if (!read_result.ok()) {
        return common::Result<std::vector<KvEntry>>::failure(read_result.error());
    }

    // Verify checksum
    uint32_t computed_checksum = common::crc32(read_result.value().data(), read_result.value().size());
    if (computed_checksum != handle.checksum) {
        return common::Result<std::vector<KvEntry>>::failure(common::ErrorCode::FileCorrupted,
                                                             "Block checksum mismatch");
    }

    // Deserialize entries
    std::vector<KvEntry> entries;
    const char* data = reinterpret_cast<const char*>(read_result.value().data());
    size_t pos = 0;

    uint32_t num_entries;
    std::memcpy(&num_entries, data + pos, sizeof(num_entries));
    pos += sizeof(num_entries);

    for (uint32_t i = 0; i < num_entries; i++) {
        uint32_t entry_size;
        std::memcpy(&entry_size, data + pos, sizeof(entry_size));
        pos += sizeof(entry_size);

        KvEntry entry;
        common::DataChunk entry_data(reinterpret_cast<const uint8_t*>(data + pos), entry_size);
        if (!entry.deserialize(entry_data)) {
            return common::Result<std::vector<KvEntry>>::failure(common::ErrorCode::DeserializationError,
                                                                 "Failed to deserialize KvEntry");
        }
        entries.push_back(std::move(entry));
        pos += entry_size;
    }

    return common::Result<std::vector<KvEntry>>::success(std::move(entries));
}

common::Result<void> SSTable::WriteFooter(const Footer& footer) {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<void>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    common::DataChunk data(reinterpret_cast<const uint8_t*>(footer.ToString().data()), footer.ToString().size());
    auto append_result = fs_->append(file_handle, data);
    if (!append_result.ok()) {
        return common::Result<void>::failure(append_result.error());
    }

    return common::Result<void>::success();
}

std::string SSTable::BlockHandle::ToString() const {
    std::string result;
    result.resize(sizeof(offset) + sizeof(size));
    memcpy(&result[0], &offset, sizeof(offset));
    memcpy(&result[sizeof(offset)], &size, sizeof(size));
    return result;
}

SSTable::BlockHandle SSTable::BlockHandle::FromString(const std::string& str) {
    BlockHandle handle;
    memcpy(&handle.offset, &str[0], sizeof(handle.offset));
    memcpy(&handle.size, &str[sizeof(handle.offset)], sizeof(handle.size));
    return handle;
}

std::string SSTable::Footer::ToString() const {
    std::string result;
    result.resize(sizeof(Footer));
    memcpy(&result[0], this, sizeof(Footer));
    return result;
}

SSTable::Footer SSTable::Footer::FromString(const std::string& str) {
    Footer footer;
    memcpy(&footer, &str[0], sizeof(Footer));
    return footer;
}

common::Result<Node> SSTable::ReadNode(const BlockHandle& handle) const {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<Node>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Read node data
    auto read_result = fs_->read(file_handle, handle.offset, handle.size);
    if (!read_result.ok()) {
        return common::Result<Node>::failure(read_result.error());
    }

    // Verify checksum
    uint32_t computed_checksum = common::crc32(read_result.value().data(), read_result.value().size());
    if (computed_checksum != handle.checksum) {
        return common::Result<Node>::failure(common::ErrorCode::FileCorrupted, "Node checksum mismatch");
    }

    return Node::Deserialize(
        std::string(reinterpret_cast<const char*>(read_result.value().data()), read_result.value().size()));
}

common::Result<std::unique_ptr<KvEntry>> SSTable::Get(const std::string& key) const {
    // Read footer to get root node location
    auto footer_result = ReadFooter();
    if (!footer_result.ok()) {
        return common::Result<std::unique_ptr<KvEntry>>::failure(footer_result.error());
    }

    // Load index tree if not loaded
    if (index_tree_->Empty()) {
        auto load_result = LoadIndex(footer_result.value().root_handle);
        if (!load_result.ok()) {
            return common::Result<std::unique_ptr<KvEntry>>::failure(load_result.error());
        }
    }

    // Find block handle in index
    auto handle_result = index_tree_->Get(key);
    if (!handle_result.ok()) {
        return common::Result<std::unique_ptr<KvEntry>>::failure(handle_result.error());
    }

    if (!handle_result.value().has_value()) {
        return common::Result<std::unique_ptr<KvEntry>>::success(nullptr);
    }

    // Read entry from block
    auto entry_result = ReadEntry(handle_result.value().value());
    if (!entry_result.ok()) {
        return common::Result<std::unique_ptr<KvEntry>>::failure(entry_result.error());
    }

    return common::Result<std::unique_ptr<KvEntry>>::success(std::make_unique<KvEntry>(entry_result.value()));
}

common::Result<std::vector<KvEntry>> SSTable::GetRange(const std::string& start_key,
                                                       const std::string& end_key,
                                                       size_t limit) const {
    std::vector<KvEntry> results;

    // Find leaf node containing start_key
    auto leaf_result = FindLeaf(start_key);
    if (!leaf_result.ok()) {
        return common::Result<std::vector<KvEntry>>::failure(leaf_result.error());
    }

    // Scan through leaf nodes
    BlockHandle current = leaf_result.value();
    while (current.offset != 0 && results.size() < limit) {
        // Read current leaf node
        auto node_result = ReadNode(current);
        if (!node_result.ok()) {
            return common::Result<std::vector<KvEntry>>::failure(node_result.error());
        }
        auto& node = node_result.value();

        // Find starting position in leaf node
        auto start_it = std::lower_bound(node.keys.begin(), node.keys.end(), start_key);

        // Process entries in this leaf
        for (auto it = start_it; it != node.keys.end() && results.size() < limit; ++it) {
            // Stop if we've passed end_key
            if (*it > end_key) {
                return common::Result<std::vector<KvEntry>>::success(std::move(results));
            }

            // Read entry
            size_t idx = it - node.keys.begin();
            auto entries_result = ReadLeafBlock(node.children[idx]);
            if (!entries_result.ok()) {
                return common::Result<std::vector<KvEntry>>::failure(entries_result.error());
            }

            if (!entries_result.value().empty()) {
                results.push_back(entries_result.value()[0]);
            }
        }

        // Move to next leaf node
        current = node.next;
    }

    return common::Result<std::vector<KvEntry>>::success(std::move(results));
}

// Helper functions for the new implementation
common::Result<KvEntry> SSTable::ReadEntry(const BlockHandle& handle) const {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<KvEntry>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Read entry data
    auto read_result = fs_->read(file_handle, handle.offset, handle.size);
    if (!read_result.ok()) {
        return common::Result<KvEntry>::failure(read_result.error());
    }

    // Verify checksum
    uint32_t computed_checksum = common::crc32(read_result.value().data(), read_result.value().size());
    if (computed_checksum != handle.checksum) {
        return common::Result<KvEntry>::failure(common::ErrorCode::FileCorrupted, "Entry checksum mismatch");
    }

    return KvEntry::Deserialize(read_result.value());
}

common::Result<BlockHandle> SSTable::WriteEntry(const KvEntry& entry) {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<BlockHandle>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Get current position as entry offset
    auto size_result = fs_->size(file_handle);
    if (!size_result.ok()) {
        return common::Result<BlockHandle>::failure(size_result.error());
    }

    // Serialize entry
    auto serialized = entry.Serialize();
    if (!serialized.ok()) {
        return common::Result<BlockHandle>::failure(serialized.error());
    }

    // Create block handle
    BlockHandle handle;
    handle.offset = size_result.value();
    handle.size = serialized.value().size();
    handle.checksum = common::crc32(serialized.value().data(), serialized.value().size());

    // Write entry
    auto append_result = fs_->append(file_handle, serialized.value());
    if (!append_result.ok()) {
        return common::Result<BlockHandle>::failure(append_result.error());
    }

    return common::Result<BlockHandle>::success(handle);
}

common::Result<BlockHandle> SSTable::WriteIndex() const {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<BlockHandle>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Get current position as index offset
    auto size_result = fs_->size(file_handle);
    if (!size_result.ok()) {
        return common::Result<BlockHandle>::failure(size_result.error());
    }

    // TODO: Implement index serialization
    // This should serialize the B+ tree structure to disk
    return common::Result<BlockHandle>::failure(common::ErrorCode::NotImplemented,
                                                "Index serialization not implemented");
}

common::Result<void> SSTable::LoadIndex(const BlockHandle& handle) const {
    auto handle_result = fs_->openFile(table_name_);
    if (!handle_result.ok()) {
        return common::Result<void>::failure(handle_result.error());
    }
    auto file_handle = handle_result.value();

    // Read index data
    auto read_result = fs_->read(file_handle, handle.offset, handle.size);
    if (!read_result.ok()) {
        return common::Result<void>::failure(read_result.error());
    }

    // Verify checksum
    uint32_t computed_checksum = common::crc32(read_result.value().data(), read_result.value().size());
    if (computed_checksum != handle.checksum) {
        return common::Result<void>::failure(common::ErrorCode::FileCorrupted, "Index checksum mismatch");
    }

    // TODO: Implement index deserialization
    // This should deserialize the B+ tree structure from disk
    return common::Result<void>::failure(common::ErrorCode::NotImplemented, "Index deserialization not implemented");
}

}  // namespace pond::kv

}  // namespace pond::kv
