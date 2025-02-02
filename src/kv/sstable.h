#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/bplus_tree.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "kv/kv_entry.h"
#include "kv/record.h"

namespace pond::kv {

class SSTable {
public:
    // Constants for SSTable configuration
    static constexpr size_t BLOCK_SIZE = 4 * 1024;                // 4KB
    static constexpr size_t TARGET_FILE_SIZE = 10 * 1024 * 1024;  // 10MB
    static constexpr size_t MAX_KEY_SIZE = 1024;                  // 1KB
    static constexpr uint32_t MAGIC_NUMBER = 0x53535442;          // "SSTB"
    static constexpr uint16_t CURRENT_VERSION = 1;

    struct BlockHandle {
        uint64_t offset;
        uint32_t size;
        uint32_t checksum;

        std::string ToString() const;
        static BlockHandle FromString(const std::string& str);
    };

    struct Footer {
        BlockHandle root_handle;  // Points to root node
        BlockHandle meta_handle;  // Points to metadata block
        uint32_t magic = MAGIC_NUMBER;
        uint16_t version = CURRENT_VERSION;
        uint32_t checksum;

        std::string ToString() const;
        static Footer FromString(const std::string& str);
    };

    struct MetaBlock {
        uint64_t num_entries;
        std::string smallest_key;
        std::string largest_key;
        uint64_t create_timestamp;
        uint32_t checksum;

        std::string ToString() const;
        static MetaBlock FromString(const std::string& str);
    };

    explicit SSTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& table_name);
    ~SSTable() = default;

    // Write entries to SSTable file
    common::Result<void> Write(const std::vector<KvEntry>& entries);

    // Read entry by key
    common::Result<std::unique_ptr<KvEntry>> Get(const std::string& key) const;

    // Range query support
    common::Result<std::vector<KvEntry>> GetRange(const std::string& start_key,
                                                  const std::string& end_key,
                                                  size_t limit = 1000) const;

    // Get metadata
    common::Result<MetaBlock> GetMetadata() const;

    // Get the name of this SSTable
    const std::string& name() const { return table_name_; }

private:
    // Helper functions for reading/writing entries and index
    common::Result<KvEntry> ReadEntry(const BlockHandle& handle) const;
    common::Result<BlockHandle> WriteEntry(const KvEntry& entry);
    common::Result<BlockHandle> WriteIndex() const;
    common::Result<void> LoadIndex(const BlockHandle& handle) const;

    // File system and index tree
    using IndexTree = common::BPlusTree<std::string, BlockHandle>;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string table_name_;
    std::unique_ptr<IndexTree> index_tree_;

    // Helper functions
    common::Result<void> WriteMetaBlock(const MetaBlock& meta);
    common::Result<void> WriteFooter(const Footer& footer);
    common::Result<Footer> ReadFooter() const;
    common::Result<BlockHandle> FindLeaf(const std::string& key) const;
    common::Result<BlockHandle> BuildBPlusTree(const std::vector<KvEntry>& sorted_entries);
};

}  // namespace pond::kv
