#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "common/append_only_fs.h"
#include "common/wal.h"
#include "rsm/replication.h"

namespace pond::rsm {

// WAL implementation of the replication interface
class WalReplication final : public IReplication {
public:
    explicit WalReplication(std::shared_ptr<common::IAppendOnlyFileSystem> fs) : fs_(std::move(fs)), wal_(fs_) {}

    ~WalReplication() override = default;

    // IReplication interface implementation
    Result<bool> Initialize(const ReplicationConfig& config) override {
        if (config.directory.empty()) {
            return Result<bool>::failure(common::ErrorCode::InvalidArgument, "Path cannot be empty");
        }
        config_ = config;
        return wal_.Open(GetFilePath(), false /* recover */);
    }

    Result<bool> Bootstrap() override {
        // No bootstrap required for WAL replication
        return Result<bool>::success(true);
    }

    Result<bool> Close() override { return wal_.Close(); }

    Result<uint64_t> Append(const DataChunk& data) override {
        std::lock_guard<std::mutex> lock(mutex_);

        ReplicationEntry entry;
        entry.SetIndex(common::INVALID_LSN);  // Let WAL assign the index
        entry.SetData(data);                  // Store the data directly
        auto result = wal_.Append(entry);
        if (!result.ok()) {
            return Result<uint64_t>::failure(result.error());
        }
        return Result<uint64_t>::success(result.value());
    }

    Result<std::vector<DataChunk>> Read(uint64_t start_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        auto result = wal_.Read(start_index);
        if (!result.ok()) {
            return Result<std::vector<DataChunk>>::failure(result.error());
        }

        std::vector<DataChunk> entries;
        entries.reserve(result.value().size());
        for (const auto& entry : result.value()) {
            entries.push_back(entry.data());
        }
        return Result<std::vector<DataChunk>>::success(std::move(entries));
    }

    uint64_t GetCurrentIndex() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return wal_.current_lsn();
    }

    void ResetIndex() override {
        std::lock_guard<std::mutex> lock(mutex_);
        wal_.reset_lsn();
    }

    std::string GetFilePath() const { return config_.directory + "/wal.log"; }

private:
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    ReplicationConfig config_;
    common::WAL<ReplicationEntry> wal_;
    mutable std::mutex mutex_;
};

}  // namespace pond::rsm