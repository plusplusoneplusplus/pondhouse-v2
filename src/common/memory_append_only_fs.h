#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>
#include <atomic>

#include "append_only_fs.h"

namespace pond::common {

// In-memory implementation of IAppendOnlyFileSystem
class MemoryAppendOnlyFileSystem final : public IAppendOnlyFileSystem {
public:
    // Constructor with probability of duplicate blocks (0.0 to 1.0)
    explicit MemoryAppendOnlyFileSystem(double duplicate_block_probability = 0.0);

    // Constructor with explicit control for test cases
    static std::shared_ptr<MemoryAppendOnlyFileSystem> createWithExplicitControl(bool should_duplicate_next = false);

    ~MemoryAppendOnlyFileSystem() override;

    // Set whether the next append should create a duplicate block (for testing)
    void setNextAppendDuplicate(bool should_duplicate);

    [[nodiscard]] Result<FileHandle> OpenFile(const std::string& path, bool createIfNotExists = false) override;
    Result<bool> CloseFile(FileHandle handle) override;
    [[nodiscard]] Result<PositionRecord> Append(FileHandle handle, const DataChunk& data) override;
    [[nodiscard]] Result<DataChunk> Read(FileHandle handle, size_t offset, size_t length) override;
    [[nodiscard]] Result<size_t> Size(FileHandle handle) const override;
    [[nodiscard]] bool Exists(const std::string& path) const override;
    [[nodiscard]] Result<bool> RenameFiles(const std::vector<RenameOperation>& renames) override;
    [[nodiscard]] Result<std::vector<std::string>> List(const std::string& path, bool recursive = false) override;
    [[nodiscard]] Result<std::vector<FileSystemEntry>> ListDetailed(
        const std::string& path, 
        bool recursive = false) override;
    [[nodiscard]] Result<bool> DeleteFiles(const std::vector<std::string>& paths) override;

    // Directory operations implementation
    [[nodiscard]] Result<bool> CreateDirectory(const std::string& path) override;
    [[nodiscard]] Result<bool> DeleteDirectory(const std::string& path, bool recursive = false) override;
    [[nodiscard]] Result<bool> MoveDirectory(const std::string& source_path, const std::string& target_path) override;
    [[nodiscard]] Result<DirectoryInfo> GetDirectoryInfo(const std::string& path) const override;
    [[nodiscard]] bool IsDirectory(const std::string& path) const override;

private:
    struct FileData {
        std::vector<uint8_t> data;
        std::atomic<uint64_t> open_count{0};
        bool has_duplicate{false};  // Track if this file has a duplicate block

        bool is_open() const { return open_count.load() > 0; }
    };

    struct DirectoryData {
        bool is_directory{true};
        std::map<std::string, std::unique_ptr<DirectoryData>> subdirs;
        std::map<std::string, std::unique_ptr<FileData>> files;
    };

    // Helper functions
    [[nodiscard]] std::pair<DirectoryData*, std::string> getDirectoryAndName(const std::string& path,
                                                                             bool create = false);
    [[nodiscard]] FileData* getFile(const std::string& path);
    void splitPath(const std::string& path, std::vector<std::string>& parts) const;
    [[nodiscard]] std::string joinPath(const std::vector<std::string>& parts) const;
    void listRecursive(const DirectoryData* dir,
                       const std::string& base_path,
                       std::vector<std::string>& result,
                       bool recursive) const;

    void listRecursive(const DirectoryData* dir,
                       const std::string& base_path,
                       std::vector<FileSystemEntry>& result,
                       bool recursive) const;

protected:
    bool exists(bool lock_held, const std::string& path) const;

private:
    bool shouldCreateDuplicate() const;

    DirectoryData root_;
    mutable std::mutex mutex_;
    std::map<FileHandle, std::string> open_files_;

    // Probability-based duplicate block control
    double duplicate_block_probability_;
    mutable std::mt19937 rng_{std::random_device{}()};
    mutable std::uniform_real_distribution<double> dist_{0.0, 1.0};

    // Explicit control for testing
    bool explicit_control_mode_{false};
    bool should_duplicate_next_{false};
};

static_assert(FileSystem<MemoryAppendOnlyFileSystem>, "MemoryAppendOnlyFileSystem must satisfy FileSystem concept");

}  // namespace pond::common