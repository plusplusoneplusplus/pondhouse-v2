#pragma once

#include <array>
#include <concepts>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/uuid.h"
#include "data_variant.h"

namespace pond::common {
class GenericFileHandle {
public:
    std::string path_;

    GenericFileHandle(const std::string& path) : path_(path) {}
    ~GenericFileHandle() = default;

    bool operator==(const GenericFileHandle& other) const { return path_ == other.path_; }
    bool operator!=(const GenericFileHandle& other) const { return !(*this == other); }

    // Add hash support for use in unordered_map
    struct Hash {
        size_t operator()(const GenericFileHandle& handle) const { return std::hash<std::string>{}(handle.path_); }
    };
};

// Handle type for file operations
using FileHandle = GenericFileHandle*;
constexpr FileHandle INVALID_HANDLE = nullptr;

// Structure to represent a rename operation
struct RenameOperation {
    std::string source_path;
    std::string target_path;

    bool operator==(const RenameOperation& other) const {
        return source_path == other.source_path && target_path == other.target_path;
    }
};

// Structure to represent a directory operation result
struct DirectoryInfo {
    bool exists;
    bool is_directory;
    size_t num_files;
    size_t total_size;
};
// Concept for file system implementations
template <typename T>
concept FileSystem = requires(T fs,
                              const std::string& path,
                              const DataChunk& data,
                              FileHandle handle,
                              size_t offset,
                              size_t length,
                              const std::vector<RenameOperation>& renames,
                              const std::vector<std::string>& paths,
                              bool recursive,
                              const std::string& source_path,
                              const std::string& target_path) {
    { fs.openFile(path) } -> std::same_as<Result<FileHandle>>;
    { fs.closeFile(handle) } -> std::same_as<Result<bool>>;
    { fs.append(handle, data) } -> std::same_as<Result<PositionRecord>>;
    { fs.read(handle, offset, length) } -> std::same_as<Result<DataChunk>>;
    { fs.size(handle) } -> std::same_as<Result<size_t>>;
    { fs.exists(path) } -> std::same_as<bool>;
    { fs.renameFiles(renames) } -> std::same_as<Result<bool>>;
    { fs.list(path, recursive) } -> std::same_as<Result<std::vector<std::string>>>;
    { fs.deleteFiles(paths) } -> std::same_as<Result<bool>>;
    { fs.createDirectory(path) } -> std::same_as<Result<bool>>;
    { fs.deleteDirectory(path, recursive) } -> std::same_as<Result<bool>>;
    { fs.moveDirectory(source_path, target_path) } -> std::same_as<Result<bool>>;
    { fs.getDirectoryInfo(path) } -> std::same_as<Result<DirectoryInfo>>;
    { fs.isDirectory(path) } -> std::same_as<bool>;
};

class IAppendOnlyFileSystem {
public:
    virtual ~IAppendOnlyFileSystem() = default;

    // Open or create a file for append-only operations
    [[nodiscard]] virtual Result<FileHandle> openFile(const std::string& path) = 0;

    // Close the file associated with the handle
    virtual Result<bool> closeFile(FileHandle handle) = 0;

    // Append data to the file associated with the handle
    // Return the offset of the data in the file
    [[nodiscard]] virtual Result<PositionRecord> append(FileHandle handle, const DataChunk& data) = 0;

    // Read a chunk of data from the file associated with the handle at given offset
    [[nodiscard]] virtual Result<DataChunk> read(FileHandle handle, size_t offset, size_t length) = 0;

    // Get the current size of the file associated with the handle
    [[nodiscard]] virtual Result<size_t> size(FileHandle handle) const = 0;

    // Check if file exists
    [[nodiscard]] virtual bool exists(const std::string& path) const = 0;

    // Atomically rename multiple files
    // If any rename fails, all renames are rolled back
    [[nodiscard]] virtual Result<bool> renameFiles(const std::vector<RenameOperation>& renames) = 0;

    // List files in a directory
    // If recursive is true, also list files in subdirectories
    // Returns a list of relative paths
    [[nodiscard]] virtual Result<std::vector<std::string>> list(const std::string& path, bool recursive = false) = 0;

    // Delete multiple files
    // If any deletion fails, continue with remaining files but return error
    [[nodiscard]] virtual Result<bool> deleteFiles(const std::vector<std::string>& paths) = 0;

    // Directory operations
    [[nodiscard]] virtual Result<bool> createDirectory(const std::string& path) = 0;
    [[nodiscard]] virtual Result<bool> deleteDirectory(const std::string& path, bool recursive = false) = 0;
    [[nodiscard]] virtual Result<bool> moveDirectory(const std::string& source_path,
                                                     const std::string& target_path) = 0;
    [[nodiscard]] virtual Result<DirectoryInfo> getDirectoryInfo(const std::string& path) const = 0;
    [[nodiscard]] virtual bool isDirectory(const std::string& path) const = 0;

    // Factory method to create concrete implementations
    [[nodiscard]] static std::unique_ptr<IAppendOnlyFileSystem> create(std::string_view type);

    // Factory method to create a shared pointer to the concrete implementation
    [[nodiscard]] static std::shared_ptr<IAppendOnlyFileSystem> createShared(std::string_view type);
};

// Concrete implementation for local file system
class LocalAppendOnlyFileSystem final : public IAppendOnlyFileSystem {
public:
    LocalAppendOnlyFileSystem();
    ~LocalAppendOnlyFileSystem() override;

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
    class Impl;
    std::unique_ptr<Impl> impl_;
};

static_assert(FileSystem<LocalAppendOnlyFileSystem>, "LocalAppendOnlyFileSystem must satisfy FileSystem concept");

}  // namespace pond::common