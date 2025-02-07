#include "sstable_manager.h"

#include <filesystem>
#include <mutex>
#include <shared_mutex>

#include "common/error.h"
#include "common/log.h"
#include "kv/memtable.h"

namespace pond::kv {

namespace {
// File name format: L<level>_<file_number>.sst
std::string MakeSSTableFileName(size_t level, size_t file_number) {
    return "L" + std::to_string(level) + "_" + std::to_string(file_number) + ".sst";
}
}  // namespace

class SSTableManager::Impl {
public:
    Impl(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& base_dir, const Config& config)
        : fs_(std::move(fs)), base_dir_(base_dir), config_(config), cache_(config.block_cache_size) {
        // Create base directory if it doesn't exist
        if (!fs_->exists(base_dir_)) {
            auto result = fs_->createDirectory(base_dir_);
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
                auto reader = std::make_shared<SSTableReader>(fs_, GetTablePath(level, file_number));
                auto open_result = reader->Open();
                if (!open_result.ok()) {
                    continue;
                }

                // Check key range first
                if (key < reader->GetSmallestKey() || key > reader->GetLargestKey()) {
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

    common::Result<bool> CreateSSTableFromMemTable(const MemTable& memtable) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        // Get next file number for L0
        size_t file_number = next_file_number_++;
        std::string file_path = GetTablePath(0, file_number);

        // Create SSTable writer
        SSTableWriter writer(fs_, file_path);

        // Enable bloom filter with estimated number of entries
        writer.EnableFilter(memtable.GetEntryCount());

        // Write entries from MemTable
        auto iter = memtable.NewIterator();
        while (iter->Valid()) {
            auto key = iter->key().value();
            auto record = iter->record().value();
            auto value = record.get().Serialize();
            auto result = writer.Add(key, value);
            if (!result.ok()) {
                return common::Result<bool>::failure(result.error());
            }
            iter->Next();
        }

        // Finish writing
        auto result = writer.Finish();
        if (!result.ok()) {
            return result;
        }

        // Add to L0 tables
        CreateLevelIfNotExists(0);
        sstables_[0].push_back(file_number);

        // Update statistics
        UpdateStats();

        return common::Result<bool>::success(true);
    }

    Stats GetStats() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return stats_;
    }

private:
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
        // List files in base directory
        // TODO: handle the dirty files or use metadata file to record the file numbers
        auto list_result = fs_->list(base_dir_);
        if (!list_result.ok()) {
            LOG_ERROR("Failed to list directory %s: %s", base_dir_.c_str(), list_result.error().message().c_str());
            return;
        }

        // Parse file names and organize by level
        for (const auto& file : list_result.value()) {
            if (file.length() < 4 || file.substr(file.length() - 4) != ".sst") {
                continue;
            }

            // Parse level and file number from name
            size_t level_pos = file.find('L');
            size_t num_pos = file.find('_');
            if (level_pos == std::string::npos || num_pos == std::string::npos) {
                continue;
            }

            try {
                size_t level = std::stoul(file.substr(level_pos + 1, num_pos - level_pos - 1));
                size_t file_number = std::stoul(file.substr(num_pos + 1, file.length() - num_pos - 5));

                // Ensure we have enough levels
                CreateLevelIfNotExists(level);

                sstables_[level].push_back(file_number);
                next_file_number_ = std::max(next_file_number_, file_number + 1);
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to parse SSTable file name %s: %s", file.c_str(), e.what());
                continue;
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

    // Protected by mutex_
    mutable std::shared_mutex mutex_;
    std::vector<std::vector<size_t>> sstables_;  // sstables_[level][index] = file_number
    size_t next_file_number_{1};
    Stats stats_;
};

SSTableManager::SSTableManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                               const std::string& base_dir,
                               const Config& config)
    : impl_(std::make_unique<Impl>(std::move(fs), base_dir, config)) {}

SSTableManager::~SSTableManager() = default;

common::Result<common::DataChunk> SSTableManager::Get(const std::string& key) {
    return impl_->Get(key);
}

common::Result<bool> SSTableManager::CreateSSTableFromMemTable(const MemTable& memtable) {
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

}  // namespace pond::kv
