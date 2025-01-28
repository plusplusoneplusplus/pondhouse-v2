#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

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

    [[nodiscard]] Result<FileHandle> openFile(const std::string& path) override;
    Result<bool> closeFile(FileHandle handle) override;
    [[nodiscard]] Result<PositionRecord> append(FileHandle handle, const DataChunk& data) override;
    [[nodiscard]] Result<DataChunk> read(FileHandle handle, size_t offset, size_t length) override;
    [[nodiscard]] Result<size_t> size(FileHandle handle) const override;
    [[nodiscard]] bool exists(const std::string& path) const override;
    [[nodiscard]] Result<bool> renameFiles(const std::vector<RenameOperation>& renames) override;
    [[nodiscard]] Result<std::vector<std::string>> list(const std::string& path, bool recursive = false) override;
    [[nodiscard]] Result<bool> deleteFiles(const std::vector<std::string>& paths) override;

    // Directory operations implementation
    [[nodiscard]] Result<bool> createDirectory(const std::string& path) override;
    [[nodiscard]] Result<bool> deleteDirectory(const std::string& path, bool recursive = false) override;
    [[nodiscard]] Result<bool> moveDirectory(const std::string& source_path, const std::string& target_path) override;
    [[nodiscard]] Result<DirectoryInfo> getDirectoryInfo(const std::string& path) const override;
    [[nodiscard]] bool isDirectory(const std::string& path) const override;

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
    bool exists(bool lock_held, const std::string& path) const;
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