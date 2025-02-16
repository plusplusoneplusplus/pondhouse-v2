#include "sstable_manager.h"

#include <filesystem>
#include <mutex>
#include <shared_mutex>

#include "common/error.h"
#include "common/log.h"
#include "kv/memtable.h"
#include "kv/table_metadata.h"

namespace pond::kv {

namespace {
// File name format: L<level>_<file_number>.sst
std::string MakeSSTableFileName(size_t level, size_t file_number) {
    return "L" + std::to_string(level) + "_" + std::to_string(file_number) + ".sst";
}
}  // namespace

class SSTableManager::Impl {
public:
    // Helper functions for merging SSTables
    struct MergeContext {
        std::vector<std::shared_ptr<SSTableReader>> readers;
        std::vector<std::unique_ptr<SSTableReader::Iterator>> iterators;
        std::vector<FileInfo> source_files;
        std::string smallest_key;
        std::string largest_key;
        size_t total_size = 0;
        size_t total_entries = 0;

        // Delete copy constructor and assignment
        MergeContext(const MergeContext&) = delete;
        MergeContext& operator=(const MergeContext&) = delete;

        // Default constructor
        MergeContext() = default;

        // Move constructor and assignment
        MergeContext(MergeContext&&) = default;
        MergeContext& operator=(MergeContext&&) = default;
    };

    Impl(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
         const std::string& base_dir,
         std::shared_ptr<TableMetadataStateMachine> metadata_state_machine,
         const Config& config)
        : fs_(std::move(fs)),
          base_dir_(base_dir),
          config_(config),
          cache_(config.block_cache_size),
          metadata_state_machine_(std::move(metadata_state_machine)) {
        // Create base directory if it doesn't exist
        if (!fs_->Exists(base_dir_)) {
            auto result = fs_->CreateDirectory(base_dir_);
            if (!result.ok()) {
                LOG_ERROR(
                    "Failed to create base directory %s: %s", base_dir_.c_str(), result.error().message().c_str());
            }
        }

        LoadExistingSSTables();
    }

    common::Result<common::DataChunk> Get(const std::string& key) {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (sstables_.empty()) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "No SSTables found");
        }

        // Search L0 tables first (newest to oldest)
        for (auto it = sstables_[0].rbegin(); it != sstables_[0].rend(); ++it) {
            // Check metadata cache first
            if (!metadata_cache_.MayContainKey(0, *it, key)) {
                stats_.metadata_filter_cache_hits++;
                continue;  // Skip this file if metadata indicates key cannot exist
            }
            stats_.metadata_filter_cache_misses++;

            // Physical read required
            stats_.physical_reads++;
            auto reader = std::make_shared<SSTableReader>(fs_, GetTablePath(0, *it));
            auto open_result = reader->Open();
            if (!open_result.ok()) {
                continue;
            }

            auto result = reader->Get(key);
            if (result.ok()) {
                return result;
            }
            if (result.error().code() != common::ErrorCode::NotFound) {
                return result;
            }
        }

        // Search other levels (each level has non-overlapping ranges)
        for (size_t level = 1; level < sstables_.size(); level++) {
            for (const auto& file_number : sstables_[level]) {
                // Check metadata cache first
                if (!metadata_cache_.MayContainKey(level, file_number, key)) {
                    stats_.metadata_filter_cache_hits++;
                    continue;  // Skip this file if metadata indicates key cannot exist
                }
                stats_.metadata_filter_cache_misses++;

                // Physical read required
                stats_.physical_reads++;
                auto reader = std::make_shared<SSTableReader>(fs_, GetTablePath(level, file_number));
                auto open_result = reader->Open();
                if (!open_result.ok()) {
                    continue;
                }

                auto result = reader->Get(key);
                if (result.ok()) {
                    return result;
                }
                if (result.error().code() != common::ErrorCode::NotFound) {
                    return result;
                }
            }
        }

        return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
    }

    common::Result<FileInfo> CreateSSTableFromMemTable(const MemTable& memtable) {
        using ReturnType = common::Result<FileInfo>;
        std::unique_lock<std::shared_mutex> lock(mutex_);

        // Get next file number for L0
        size_t file_number = next_file_number_++;
        std::string file_path = GetTablePath(0, file_number);

        // Create SSTable writer
        SSTableWriter writer(fs_, file_path);

        // Enable bloom filter with estimated number of entries
        writer.EnableFilter(memtable.GetEntryCount());

        // Track smallest and largest keys
        std::string smallest_key;
        std::string largest_key;
        size_t entry_count = 0;

        // Write entries from MemTable
        std::unique_ptr<MemTable::Iterator> iter = memtable.NewIterator();
        while (iter->Valid()) {
            auto key = iter->key().value();
            if (smallest_key.empty()) {
                smallest_key = key;
            }
            largest_key = key;

            auto value_result = iter->value();
            RETURN_IF_ERROR_T(ReturnType, value_result);
            const auto& versioned_value = value_result.value().get();
            auto result = writer.Add(key, versioned_value->value(), versioned_value->version());
            RETURN_IF_ERROR_T(ReturnType, result);
            entry_count++;
            iter->Next();
        }

        // Finalize SSTable
        auto finish_result = writer.Finish();
        RETURN_IF_ERROR_T(ReturnType, finish_result);

        // Get file size
        auto file_result = fs_->OpenFile(file_path);
        RETURN_IF_ERROR_T(ReturnType, file_result);
        auto size_result = fs_->Size(file_result.value());
        RETURN_IF_ERROR_T(ReturnType, size_result);

        // Create metadata for the new SSTable
        auto reader = std::make_shared<SSTableReader>(fs_, file_path);
        auto open_result = reader->Open();
        if (open_result.ok()) {
            auto filter_result = reader->GetBloomFilter();
            if (filter_result.ok()) {
                metadata_cache_.AddTable(0,
                                         file_number,
                                         SSTableMetadata(file_path,
                                                         smallest_key,
                                                         largest_key,
                                                         size_result.value(),
                                                         std::move(filter_result).value(),
                                                         entry_count));
            }
        }

        // Add to L0 tables
        CreateLevelIfNotExists(0);
        sstables_[0].push_back(file_number);

        // Update statistics
        UpdateStats();

        // Track the operation in metadata state machine
        FileInfo file_info{MakeSSTableFileName(0, file_number),
                           size_result.value(),
                           0 /* mem table must be lvl 0 */,
                           smallest_key,
                           largest_key};
        std::vector<FileInfo> files{file_info};
        TableMetadataEntry entry(MetadataOpType::CreateSSTable, files);
        auto track_result = metadata_state_machine_->Apply(entry.Serialize());
        if (!track_result.ok()) {
            LOG_ERROR("Failed to track SSTable creation in metadata: %s", track_result.error().message().c_str());
            // Continue despite tracking failure - the SSTable is still valid
        }

        return common::Result<FileInfo>::success(file_info);
    }

    Stats GetStats() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return stats_;
    }

    common::Result<FileInfo> MergeL0ToL1() {
        using ReturnType = common::Result<FileInfo>;
        std::unique_lock<std::shared_mutex> lock(mutex_);

        // Check if there are L0 files to merge
        if (sstables_.empty() || sstables_[0].empty()) {
            return ReturnType::failure(common::ErrorCode::InvalidOperation, "No L0 files to merge");
        }

        // Initialize merge context
        auto ctx_result = InitializeMergeContext(0, sstables_[0]);
        if (!ctx_result.ok()) {
            return ReturnType::failure(ctx_result.error());
        }
        auto ctx = std::move(ctx_result).value();

        // Create L1 SSTable writer
        auto writer_result = CreateSSTableWriter(1, next_file_number_, *ctx);
        if (!writer_result.ok()) {
            return ReturnType::failure(writer_result.error());
        }
        auto [writer, file_number] = std::move(writer_result).value();

        // Merge files
        auto merge_result = MergeIterators(*writer, ctx->iterators);
        if (!merge_result.ok()) {
            return ReturnType::failure(merge_result.error());
        }

        // Finalize SSTable
        auto finish_result = writer->Finish();
        if (!finish_result.ok()) {
            return ReturnType::failure(finish_result.error());
        }

        // Update state and return result
        return UpdateStateAfterMerge(0, 1, file_number, GetTablePath(1, file_number), *ctx);
    }

private:
    // Initialize merge context with readers and iterators
    [[nodiscard]] common::Result<std::unique_ptr<MergeContext>> InitializeMergeContext(
        size_t level, const std::vector<size_t>& file_numbers) {
        auto ctx = std::make_unique<MergeContext>();

        for (const auto& file_number : file_numbers) {
            std::string file_path = GetTablePath(level, file_number);
            auto reader = std::make_shared<SSTableReader>(fs_, file_path);
            auto open_result = reader->Open();
            if (!open_result.ok()) {
                return common::Result<std::unique_ptr<MergeContext>>::failure(open_result.error());
            }

            ctx->readers.push_back(reader);

            // Track key range
            if (ctx->smallest_key.empty() || reader->GetSmallestKey() < ctx->smallest_key) {
                ctx->smallest_key = reader->GetSmallestKey();
            }
            if (ctx->largest_key.empty() || reader->GetLargestKey() > ctx->largest_key) {
                ctx->largest_key = reader->GetLargestKey();
            }

            // Track file info for metadata
            ctx->source_files.push_back(FileInfo(MakeSSTableFileName(level, file_number),
                                                 reader->GetFileSize(),
                                                 level,
                                                 reader->GetSmallestKey(),
                                                 reader->GetLargestKey()));

            ctx->total_size += reader->GetFileSize();
            ctx->total_entries += reader->GetEntryCount();
        }

        // Create iterators for all files
        for (const auto& reader : ctx->readers) {
            ctx->iterators.push_back(reader->NewIterator());
            ctx->iterators.back()->SeekToFirst();
        }

        return common::Result<std::unique_ptr<MergeContext>>::success(std::move(ctx));
    }

    // Create a new SSTable writer with the given parameters
    [[nodiscard]] common::Result<std::pair<std::unique_ptr<SSTableWriter>, size_t>> CreateSSTableWriter(
        size_t target_level, size_t& next_file_number, const MergeContext& ctx) {
        size_t file_number = next_file_number++;
        std::string file_path = GetTablePath(target_level, file_number);
        auto writer = std::make_unique<SSTableWriter>(fs_, file_path);

        // Enable bloom filter with combined entry count
        writer->EnableFilter(ctx.total_entries);

        return common::Result<std::pair<std::unique_ptr<SSTableWriter>, size_t>>::success(
            std::make_pair(std::move(writer), file_number));
    }

    // Merge multiple SSTable iterators into a single SSTable
    [[nodiscard]] common::Result<bool> MergeIterators(
        SSTableWriter& writer, std::vector<std::unique_ptr<SSTableReader::Iterator>>& iterators) {
        while (true) {
            // Find smallest key among all iterators
            std::string current_key;
            for (const auto& iter : iterators) {
                if (iter->Valid()) {
                    if (current_key.empty() || iter->key() < current_key) {
                        current_key = iter->key();
                    }
                }
            }

            if (current_key.empty()) {
                break;  // All iterators exhausted
            }

            // Find latest version for current key
            common::HybridTime latest_version = common::MinHybridTime();
            common::DataChunk latest_value;

            for (const auto& iter : iterators) {
                if (iter->Valid() && iter->key() == current_key) {
                    if (iter->version() > latest_version) {
                        latest_version = iter->version();
                        latest_value = iter->value();
                    }
                }
            }

            // Write to target file
            auto add_result = writer.Add(current_key, latest_value, latest_version);
            if (!add_result.ok()) {
                return common::Result<bool>::failure(add_result.error());
            }

            // Advance iterators past current key
            for (auto& iter : iterators) {
                if (iter->Valid() && iter->key() == current_key) {
                    iter->Next();
                }
            }
        }

        return common::Result<bool>::success(true);
    }

    // Update metadata and in-memory state after merge
    [[nodiscard]] common::Result<FileInfo> UpdateStateAfterMerge(size_t source_level,
                                                                 size_t target_level,
                                                                 size_t file_number,
                                                                 const std::string& file_path,
                                                                 const MergeContext& ctx) {
        // Get file size
        auto file_result = fs_->OpenFile(file_path);
        if (!file_result.ok()) {
            return common::Result<FileInfo>::failure(file_result.error());
        }
        auto size_result = fs_->Size(file_result.value());
        if (!size_result.ok()) {
            return common::Result<FileInfo>::failure(size_result.error());
        }

        // Create metadata for the new file
        FileInfo new_file(MakeSSTableFileName(target_level, file_number),
                          size_result.value(),
                          target_level,
                          ctx.smallest_key,
                          ctx.largest_key);

        // Update metadata state machine
        TableMetadataEntry compact_entry(MetadataOpType::CompactFiles, {new_file}, ctx.source_files);
        auto track_result = metadata_state_machine_->Apply(compact_entry.Serialize());
        if (!track_result.ok()) {
            return common::Result<FileInfo>::failure(track_result.error());
        }

        // Update in-memory state
        CreateLevelIfNotExists(target_level);
        sstables_[target_level].push_back(file_number);
        sstables_[source_level].clear();  // Remove source files

        // Update metadata cache
        auto reader = std::make_shared<SSTableReader>(fs_, file_path);
        auto open_result = reader->Open();
        if (open_result.ok()) {
            auto filter_result = reader->GetBloomFilter();
            if (filter_result.ok()) {
                metadata_cache_.AddTable(target_level,
                                         file_number,
                                         SSTableMetadata(file_path,
                                                         ctx.smallest_key,
                                                         ctx.largest_key,
                                                         size_result.value(),
                                                         std::move(filter_result).value(),
                                                         ctx.total_entries));
            }
        }

        // Update statistics
        UpdateStats();

        return common::Result<FileInfo>::success(new_file);
    }

    std::string GetTablePath(size_t level, size_t file_number) const {
        return base_dir_ + "/" + MakeSSTableFileName(level, file_number);
    }

    // must be called while holding mutex_
    void CreateLevelIfNotExists(size_t level) {
        if (sstables_.size() <= level) {
            sstables_.push_back({});
        }
    }

    void LoadExistingSSTables() {
        // Get SSTable files from metadata state machine
        for (size_t level = 0; level < metadata_state_machine_->GetLevelCount(); level++) {
            const auto& files = metadata_state_machine_->GetSSTableFiles(level);
            CreateLevelIfNotExists(level);

            for (const auto& file : files) {
                // Parse file number from name
                size_t num_pos = file.name.find('_');
                if (num_pos == std::string::npos) {
                    LOG_ERROR("Invalid SSTable file name: %s", file.name.c_str());
                    continue;
                }

                try {
                    size_t file_number = std::stoul(file.name.substr(num_pos + 1, file.name.length() - num_pos - 5));
                    sstables_[level].push_back(file_number);
                    next_file_number_ = std::max(next_file_number_, file_number + 1);

                    // Add to metadata cache
                    metadata_cache_.AddTable(level,
                                             file_number,
                                             SSTableMetadata(GetTablePath(level, file_number),
                                                             file.smallest_key,
                                                             file.largest_key,
                                                             file.size,
                                                             nullptr,  // Bloom filter will be loaded on demand
                                                             0));  // Entry count will be updated when filter is loaded
                } catch (const std::exception& e) {
                    LOG_ERROR("Failed to parse SSTable file name %s: %s", file.name.c_str(), e.what());
                    continue;
                }
            }
        }

        // Update statistics
        UpdateStats();
    }

    // must be called while holding mutex_
    void UpdateStats() {
        stats_.files_per_level.resize(sstables_.size());
        stats_.bytes_per_level.resize(sstables_.size());
        stats_.total_files = 0;
        stats_.total_bytes = 0;

        for (size_t level = 0; level < sstables_.size(); level++) {
            stats_.files_per_level[level] = sstables_[level].size();
            stats_.bytes_per_level[level] = 0;

            for (const auto& file_number : sstables_[level]) {
                auto reader = std::make_shared<SSTableReader>(fs_, GetTablePath(level, file_number));
                auto open_result = reader->Open();
                if (!open_result.ok()) {
                    continue;
                }

                stats_.bytes_per_level[level] += reader->GetFileSize();
            }

            stats_.total_files += stats_.files_per_level[level];
            stats_.total_bytes += stats_.bytes_per_level[level];
        }

        stats_.cache_stats = cache_.GetStats();
    }

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string base_dir_;
    Config config_;
    SSTableCache cache_;
    MetadataCache metadata_cache_;
    std::shared_ptr<TableMetadataStateMachine> metadata_state_machine_;

    // Protected by mutex_
    mutable std::shared_mutex mutex_;
    std::vector<std::vector<size_t>> sstables_;  // sstables_[level][index] = file_number
    size_t next_file_number_{1};
    Stats stats_;
};

SSTableManager::SSTableManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                               const std::string& base_dir,
                               std::shared_ptr<TableMetadataStateMachine> metadata_state_machine,
                               const Config& config)
    : impl_(std::make_unique<Impl>(std::move(fs), base_dir, std::move(metadata_state_machine), config)) {}

SSTableManager::~SSTableManager() = default;

common::Result<common::DataChunk> SSTableManager::Get(const std::string& key) {
    return impl_->Get(key);
}

common::Result<FileInfo> SSTableManager::CreateSSTableFromMemTable(const MemTable& memtable) {
    return impl_->CreateSSTableFromMemTable(memtable);
}

SSTableManager::Stats SSTableManager::GetStats() const {
    return impl_->GetStats();
}

// Compaction-related methods will be implemented later
common::Result<bool> SSTableManager::CompactLevel(int level) {
    return common::Result<bool>::failure(common::ErrorCode::NotImplemented, "Compaction not implemented");
}

common::Result<bool> SSTableManager::StartCompaction() {
    return common::Result<bool>::failure(common::ErrorCode::NotImplemented, "Compaction not implemented");
}

common::Result<bool> SSTableManager::StopCompaction() {
    return common::Result<bool>::failure(common::ErrorCode::NotImplemented, "Compaction not implemented");
}

common::Result<FileInfo> SSTableManager::MergeL0ToL1() {
    return impl_->MergeL0ToL1();
}

}  // namespace pond::kv
