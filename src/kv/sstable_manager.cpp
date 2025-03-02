#include "sstable_manager.h"

#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>

#include "common/error.h"
#include "common/log.h"
#include "kv/memtable.h"
#include "kv/table_metadata.h"

using namespace pond::common;
using namespace pond::format;

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
        std::vector<std::shared_ptr<SSTableReader::Iterator>> iterators;
        std::vector<FileInfo> source_files;
        std::string smallest_key;
        std::string largest_key;
        size_t total_size = 0;
        size_t total_entries = 0;

        // New fields for overlapping L1 files
        std::vector<std::shared_ptr<SSTableReader>> l1_readers;
        std::vector<std::shared_ptr<SSTableReader::Iterator>> l1_iterators;
        std::vector<FileInfo> l1_source_files;

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

    common::Result<std::shared_ptr<SSTableReader>> OpenSSTableReader(const std::string& file_name) {
        return std::make_shared<SSTableReader>(fs_, file_name);
    }

    common::Result<common::DataChunk> Get(const std::string& key, common::HybridTime version) {
        using ReturnType = common::Result<common::DataChunk>;
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (sstables_.empty()) {
            return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "No SSTables exist");
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
            auto reader_result = OpenSSTableReader(GetTablePath(0, *it));
            RETURN_IF_ERROR_T(ReturnType, reader_result);

            auto reader = std::move(reader_result).value();
            auto open_result = reader->Open();
            RETURN_IF_ERROR_T(ReturnType, open_result);

            auto result = reader->Get(key, version);
            if (result.ok()) {
                LOG_VERBOSE("Found in L0, key=%s, valueSize=%zu", key.c_str(), result.value().Size());
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
                auto reader_result = OpenSSTableReader(GetTablePath(level, file_number));
                RETURN_IF_ERROR_T(ReturnType, reader_result);

                auto reader = std::move(reader_result).value();
                auto open_result = reader->Open();
                RETURN_IF_ERROR_T(ReturnType, open_result);

                auto result = reader->Get(key, version);
                if (result.ok()) {
                    LOG_VERBOSE("Found in L%zu, key=%s, valueSize=%zu", level, key.c_str(), result.value().Size());
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
        // The mode should be IncludeTombstones because we want to include tombstones in the SSTable
        auto iter = memtable.NewIterator(common::IteratorMode::IncludeTombstones);
        while (iter->Valid()) {
            auto key = iter->key();
            if (smallest_key.empty()) {
                smallest_key = key;
            }
            largest_key = key;

            const auto& versioned_value = iter->value().get();
            auto result =
                writer.Add(key, versioned_value->value(), versioned_value->version(), versioned_value->IsDeleted());
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
        TableMetadataEntry entry(MetadataOpType::CreateSSTable, files, {}, memtable.GetMetadata().GetFlushSequence());

        // Replicate the operation to the metadata state machine
        {
            std::mutex mtx;
            std::condition_variable cv;
            bool replication_complete = false;
            
            auto track_result = metadata_state_machine_->Replicate(entry.Serialize(), [&]() {
                std::unique_lock<std::mutex> lock2(mtx);
                replication_complete = true;
                cv.notify_one();
            });

            RETURN_IF_ERROR_T(ReturnType, track_result);

            // Wait for replication to complete
            std::unique_lock<std::mutex> lock2(mtx);
            cv.wait(lock2, [&] { return replication_complete; });
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

        // Start tracking metrics for this compaction
        size_t job_id = next_compaction_id_++;
        auto& job_metrics = metrics_.StartCompactionJob(job_id, 0, 1);
        size_t total_memory_usage = 0;

        // Check if there are L0 files to merge
        if (sstables_.empty() || sstables_[0].empty()) {
            metrics_.CompleteCompactionJob(job_id, false);
            return ReturnType::failure(common::ErrorCode::InvalidOperation, "No L0 files to merge");
        }

        // Initialize merge context
        job_metrics.phase.UpdatePhase(CompactionPhase::Phase::FileSelection);
        auto ctx_result = InitializeMergeContext(0, sstables_[0]);
        if (!ctx_result.ok()) {
            metrics_.CompleteCompactionJob(job_id, false);
            return ReturnType::failure(ctx_result.error());
        }
        auto ctx = std::move(ctx_result).value();

        // Update job metrics with input files info
        job_metrics.input_files = ctx->source_files.size() + ctx->l1_source_files.size();
        job_metrics.input_bytes = ctx->total_size;

        // Create L1 SSTable writer
        job_metrics.phase.UpdatePhase(CompactionPhase::Phase::Reading);
        auto writer_result = CreateSSTableWriter(1, next_file_number_, *ctx);
        if (!writer_result.ok()) {
            metrics_.CompleteCompactionJob(job_id, false);
            return ReturnType::failure(writer_result.error());
        }
        auto [writer, file_number] = std::move(writer_result).value();

        // Track writer memory usage
        total_memory_usage += writer->GetMemoryUsage();
        metrics_.UpdateResourceUsage(total_memory_usage, 0, 0);

        // Merge files
        job_metrics.phase.UpdatePhase(CompactionPhase::Phase::Merging);
        auto merge_result = MergeIterators(*writer, ctx->iterators, ctx->l1_iterators);
        if (!merge_result.ok()) {
            metrics_.CompleteCompactionJob(job_id, false);
            return ReturnType::failure(merge_result.error());
        }

        // Finalize SSTable
        job_metrics.phase.UpdatePhase(CompactionPhase::Phase::Writing);
        auto finish_result = writer->Finish();
        if (!finish_result.ok()) {
            metrics_.CompleteCompactionJob(job_id, false);
            return ReturnType::failure(finish_result.error());
        }

        // Combine source files from both L0 and L1
        std::vector<FileInfo> all_source_files;
        all_source_files.insert(all_source_files.end(), ctx->source_files.begin(), ctx->source_files.end());
        all_source_files.insert(all_source_files.end(), ctx->l1_source_files.begin(), ctx->l1_source_files.end());

        // Track write bytes
        metrics_.disk_write_bytes_ += writer->GetFileSize();

        // Update state and metrics
        auto result = UpdateStateAfterMerge(0, 1, file_number, GetTablePath(1, file_number), *ctx);
        if (result.ok()) {
            // Update job metrics with final stats
            job_metrics.output_files = 1;
            job_metrics.output_bytes = result.value().size;
            job_metrics.phase.UpdatePhase(CompactionPhase::Phase::Complete);
            metrics_.CompleteCompactionJob(job_id, true);
        } else {
            job_metrics.phase.UpdatePhase(CompactionPhase::Phase::Failed);
            job_metrics.error_message = result.error().message();
            metrics_.CompleteCompactionJob(job_id, false);
        }

        return result;
    }

    // Add methods to expose metrics
    const CompactionMetrics& GetCompactionMetrics() const { return metrics_; }
    void ResetCompactionMetrics() { metrics_.Reset(); }

    common::Result<std::shared_ptr<Iterator>> NewSnapshotIterator(common::HybridTime read_time,
                                                                  common::IteratorMode mode) {
        using ReturnType = common::Result<std::shared_ptr<Iterator>>;

        // Get all SSTable readers from L0, in reverse order
        std::vector<std::shared_ptr<SSTableReader>> l0_readers;
        if (sstables_.empty()) {
            return ReturnType::success(
                std::make_shared<SSTableSnapshotIterator>(std::vector<std::shared_ptr<SSTableReader>>(),
                                                          std::vector<std::vector<std::shared_ptr<SSTableReader>>>(),
                                                          read_time,
                                                          mode));
        }

        if (!sstables_[0].empty()) {
            for (auto it = sstables_[0].rbegin(); it != sstables_[0].rend(); ++it) {
                auto reader_result = OpenSSTableReader(GetTablePath(0, *it));
                RETURN_IF_ERROR_T(ReturnType, reader_result);

                auto reader = std::move(reader_result).value();
                auto open_result = reader->Open();
                RETURN_IF_ERROR_T(ReturnType, open_result);

                l0_readers.push_back(std::move(reader));
            }
        }

        // Get all SSTable readers from other levels
        std::vector<std::vector<std::shared_ptr<SSTableReader>>> level_readers;
        for (size_t level = 1; level < sstables_.size(); level++) {
            std::vector<std::shared_ptr<SSTableReader>> readers;
            for (const auto& file_number : sstables_[level]) {
                auto reader_result = OpenSSTableReader(GetTablePath(level, file_number));
                RETURN_IF_ERROR_T(ReturnType, reader_result);

                auto reader = std::move(reader_result).value();
                auto open_result = reader->Open();
                RETURN_IF_ERROR_T(ReturnType, open_result);

                readers.push_back(std::move(reader));
            }
            if (!readers.empty()) {
                level_readers.push_back(std::move(readers));
            }
        }

        return ReturnType::success(std::make_shared<SSTableSnapshotIterator>(
            std::move(l0_readers), std::move(level_readers), read_time, mode));
    }

private:
    // Initialize merge context with readers and iterators
    [[nodiscard]] common::Result<std::unique_ptr<MergeContext>> InitializeMergeContext(
        size_t level, const std::vector<size_t>& file_numbers) {
        auto ctx = std::make_unique<MergeContext>();
        size_t total_memory_usage = 0;

        // Initialize source level files (L0)
        for (const auto& file_number : file_numbers) {
            std::string file_path = GetTablePath(level, file_number);
            auto reader = std::make_shared<SSTableReader>(fs_, file_path);
            auto open_result = reader->Open();
            if (!open_result.ok()) {
                return common::Result<std::unique_ptr<MergeContext>>::failure(open_result.error());
            }

            ctx->readers.push_back(reader);
            size_t file_size = reader->GetFileSize();
            metrics_.disk_read_bytes_ += file_size;
            total_memory_usage += reader->GetMemoryUsage();  // Track reader's memory usage

            // Track key range
            if (ctx->smallest_key.empty() || reader->GetSmallestKey() < ctx->smallest_key) {
                ctx->smallest_key = reader->GetSmallestKey();
            }
            if (ctx->largest_key.empty() || reader->GetLargestKey() > ctx->largest_key) {
                ctx->largest_key = reader->GetLargestKey();
            }

            // Track file info for metadata
            ctx->source_files.push_back(FileInfo(MakeSSTableFileName(level, file_number),
                                                 file_size,
                                                 level,
                                                 reader->GetSmallestKey(),
                                                 reader->GetLargestKey()));

            ctx->total_size += file_size;
            ctx->total_entries += reader->GetEntryCount();
        }

        // Update memory usage metrics
        metrics_.UpdateResourceUsage(total_memory_usage, 0, 0);

        // Create iterators for source files
        for (const auto& reader : ctx->readers) {
            ctx->iterators.push_back(
                reader->NewIterator(MaxHybridTime(),                     /*max time to include all versions*/
                                    IteratorMode::IncludeAllVersions));  // Get all versions for merging
            ctx->iterators.back()->SeekToFirst();
            total_memory_usage += sizeof(SSTableReader::Iterator);  // Approximate iterator memory usage
        }

        // If merging L0 to L1, find overlapping L1 files
        if (level == 0 && sstables_.size() > 1) {
            for (const auto& l1_file_number : sstables_[1]) {
                auto metadata = metadata_cache_.GetMetadata(1, l1_file_number);
                if (!metadata) {
                    continue;
                }
                const auto& meta = *metadata.value();
                // Check if L1 file overlaps with our key range
                if (meta.largest_key < ctx->smallest_key || meta.smallest_key > ctx->largest_key) {
                    continue;  // No overlap
                }
                // Add overlapping L1 file
                std::string l1_file_path = GetTablePath(1, l1_file_number);
                auto l1_reader = std::make_shared<SSTableReader>(fs_, l1_file_path);
                auto l1_open_result = l1_reader->Open();
                if (!l1_open_result.ok()) {
                    return common::Result<std::unique_ptr<MergeContext>>::failure(l1_open_result.error());
                }
                ctx->l1_readers.push_back(l1_reader);
                ctx->l1_source_files.push_back(FileInfo(MakeSSTableFileName(1, l1_file_number),
                                                        l1_reader->GetFileSize(),
                                                        1,
                                                        l1_reader->GetSmallestKey(),
                                                        l1_reader->GetLargestKey()));
                ctx->total_size += l1_reader->GetFileSize();
                ctx->total_entries += l1_reader->GetEntryCount();
            }
            // Create iterators for L1 files
            for (const auto& reader : ctx->l1_readers) {
                ctx->l1_iterators.push_back(
                    reader->NewIterator(MaxHybridTime(),                     /*max time to include all versions*/
                                        IteratorMode::IncludeAllVersions));  // Get all versions for merging
                ctx->l1_iterators.back()->SeekToFirst();
            }
        }

        // Update key range to include L1 files
        for (const auto& l1_reader : ctx->l1_readers) {
            if (ctx->smallest_key.empty() || l1_reader->GetSmallestKey() < ctx->smallest_key) {
                ctx->smallest_key = l1_reader->GetSmallestKey();
            }
            if (ctx->largest_key.empty() || l1_reader->GetLargestKey() > ctx->largest_key) {
                ctx->largest_key = l1_reader->GetLargestKey();
            }
        }

        // Update memory metrics again after creating iterators
        metrics_.UpdateResourceUsage(total_memory_usage, 0, 0);

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
        SSTableWriter& writer,
        std::vector<std::shared_ptr<SSTableReader::Iterator>>& iterators,
        std::vector<std::shared_ptr<SSTableReader::Iterator>>& l1_iterators) {
        while (true) {
            // Find smallest key among all iterators
            std::string current_key;

            // Check L0 iterators
            for (const auto& iter : iterators) {
                if (iter->Valid()) {
                    if (current_key.empty() || iter->key() < current_key) {
                        current_key = iter->key();
                    }
                }
            }

            // Check L1 iterators
            for (const auto& iter : l1_iterators) {
                if (iter->Valid()) {
                    if (current_key.empty() || iter->key() < current_key) {
                        current_key = iter->key();
                    }
                }
            }

            if (current_key.empty()) {
                break;  // All iterators exhausted
            }

            struct ValueTuple {
                common::DataChunk value;
                bool is_tombstone;
            };

            // Collect all versions for current key
            std::vector<std::pair<common::HybridTime, ValueTuple>> versions;

            // Check L0 iterators first (they have newer versions)
            for (const auto& iter : iterators) {
                if (iter->Valid() && iter->key() == current_key) {
                    versions.emplace_back(iter->version(), ValueTuple{iter->value(), iter->IsTombstone()});
                }
            }

            // Check L1 iterators
            for (const auto& iter : l1_iterators) {
                if (iter->Valid() && iter->key() == current_key) {
                    versions.emplace_back(iter->version(), ValueTuple{iter->value(), iter->IsTombstone()});
                }
            }

            // Sort versions by timestamp (newest first)
            std::sort(versions.begin(), versions.end(), [](const auto& a, const auto& b) { return a.first > b.first; });

            // Write all versions to target file
            for (const auto& [version, valueTuple] : versions) {
                auto add_result = writer.Add(current_key, valueTuple.value, version, valueTuple.is_tombstone);
                if (!add_result.ok()) {
                    return common::Result<bool>::failure(add_result.error());
                }
            }

            // Advance iterators past current key
            for (auto& iter : iterators) {
                if (iter->Valid() && iter->key() == current_key) {
                    iter->Next();
                }
            }
            for (auto& iter : l1_iterators) {
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

        // Combine all source files (both L0 and L1) that will be replaced
        std::vector<FileInfo> all_source_files;
        all_source_files.insert(all_source_files.end(), ctx.source_files.begin(), ctx.source_files.end());
        all_source_files.insert(all_source_files.end(), ctx.l1_source_files.begin(), ctx.l1_source_files.end());

        // Update metadata state machine
        TableMetadataEntry compact_entry(MetadataOpType::CompactFiles, {new_file}, all_source_files);
        auto track_result = metadata_state_machine_->Replicate(compact_entry.Serialize());
        if (!track_result.ok()) {
            return common::Result<FileInfo>::failure(track_result.error());
        }

        // Update in-memory state
        CreateLevelIfNotExists(target_level);

        // Remove source L0 files
        sstables_[source_level].clear();

        // Remove merged L1 files and add the new one
        if (!ctx.l1_source_files.empty()) {
            // Create a set of L1 file numbers to remove
            std::unordered_set<size_t> l1_files_to_remove;
            for (const auto& file_info : ctx.l1_source_files) {
                // Extract file number from name (format: L1_<number>.sst)
                size_t pos = file_info.name.find('_');
                if (pos != std::string::npos) {
                    std::string number_str = file_info.name.substr(pos + 1);
                    number_str = number_str.substr(0, number_str.length() - 4);  // Remove .sst
                    l1_files_to_remove.insert(std::stoull(number_str));
                }
            }

            // Remove old L1 files
            auto& l1_files = sstables_[target_level];
            l1_files.erase(
                std::remove_if(
                    l1_files.begin(), l1_files.end(), [&](size_t fn) { return l1_files_to_remove.count(fn) > 0; }),
                l1_files.end());
        }

        // Add the new file
        sstables_[target_level].push_back(file_number);

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

        LOG_STATUS("Summary of merge: Old=(L0 files: %u, L1 files: %u). New=(file: %s, min_key: %s, max_key: %s)",
                   ctx.source_files.size(),
                   ctx.l1_source_files.size(),
                   new_file.name.c_str(),
                   new_file.smallest_key.c_str(),
                   new_file.largest_key.c_str());

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

    // Add compaction metrics
    CompactionMetrics metrics_;
    std::atomic<size_t> next_compaction_id_{1};
};

SSTableManager::SSTableManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                               const std::string& base_dir,
                               std::shared_ptr<TableMetadataStateMachine> metadata_state_machine,
                               const Config& config)
    : impl_(std::make_unique<Impl>(std::move(fs), base_dir, std::move(metadata_state_machine), config)) {}

SSTableManager::~SSTableManager() = default;

common::Result<common::DataChunk> SSTableManager::Get(const std::string& key, common::HybridTime version) {
    return impl_->Get(key, version);
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

// Add implementation of the public metrics methods
const CompactionMetrics& SSTableManager::GetCompactionMetrics() const {
    return impl_->GetCompactionMetrics();
}

void SSTableManager::ResetCompactionMetrics() {
    impl_->ResetCompactionMetrics();
}

SSTableSnapshotIterator::SSTableSnapshotIterator(std::vector<std::shared_ptr<SSTableReader>> l0_readers,
                                                 std::vector<std::vector<std::shared_ptr<SSTableReader>>> level_readers,
                                                 common::HybridTime read_time,
                                                 common::IteratorMode mode)
    : SnapshotIterator(read_time, mode), l0_readers_(std::move(l0_readers)), level_readers_(std::move(level_readers)) {
    InitializeIterators();
}

void SSTableSnapshotIterator::InitializeIterators() {
    // Initialize L0 iterators
    for (const auto& reader : l0_readers_) {
        auto iter = reader->NewIterator(read_time_, mode_ | common::IteratorMode::IncludeTombstones);
        l0_iters_.push_back(iter);
    }

    // Initialize level iterators
    level_iters_.resize(level_readers_.size());
    for (size_t level = 0; level < level_readers_.size(); level++) {
        for (const auto& reader : level_readers_[level]) {
            auto iter = reader->NewIterator(read_time_, mode_ | common::IteratorMode::IncludeTombstones);
            level_iters_[level].push_back(iter);
        }
    }

    // Create UnionIterator
    union_iter_ = std::make_unique<common::UnionIterator<std::string, common::DataChunk>>(
        l0_iters_, level_iters_, read_time_, mode_);
}

void SSTableSnapshotIterator::Seek(const std::string& target) {
    union_iter_->Seek(target);
}

void SSTableSnapshotIterator::Next() {
    union_iter_->Next();
}

bool SSTableSnapshotIterator::Valid() const {
    return union_iter_->Valid();
}

const std::string& SSTableSnapshotIterator::key() const {
    return union_iter_->key();
}

const common::DataChunk& SSTableSnapshotIterator::value() const {
    return union_iter_->value();
}

bool SSTableSnapshotIterator::IsTombstone() const {
    return union_iter_->IsTombstone();
}

common::HybridTime SSTableSnapshotIterator::version() const {
    return union_iter_->version();
}

common::Result<std::shared_ptr<SSTableManager::Iterator>> SSTableManager::NewSnapshotIterator(
    common::HybridTime read_time, common::IteratorMode mode) {
    return impl_->NewSnapshotIterator(read_time, mode);
}

}  // namespace pond::kv
