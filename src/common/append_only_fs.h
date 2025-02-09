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
                              bool createIfNotExists,
                              const std::string& source_path,
                              const std::string& target_path) {
    { fs.OpenFile(path, createIfNotExists) } -> std::same_as<Result<FileHandle>>;
    { fs.CloseFile(handle) } -> std::same_as<Result<bool>>;
    { fs.Append(handle, data) } -> std::same_as<Result<PositionRecord>>;
    { fs.Read(handle, offset, length) } -> std::same_as<Result<DataChunk>>;
    { fs.Size(handle) } -> std::same_as<Result<size_t>>;
    { fs.Exists(path) } -> std::same_as<bool>;
    { fs.RenameFiles(renames) } -> std::same_as<Result<bool>>;
    { fs.List(path, recursive) } -> std::same_as<Result<std::vector<std::string>>>;
    { fs.DeleteFiles(paths) } -> std::same_as<Result<bool>>;
    { fs.CreateDirectory(path) } -> std::same_as<Result<bool>>;
    { fs.DeleteDirectory(path, recursive) } -> std::same_as<Result<bool>>;
    { fs.MoveDirectory(source_path, target_path) } -> std::same_as<Result<bool>>;
    { fs.GetDirectoryInfo(path) } -> std::same_as<Result<DirectoryInfo>>;
    { fs.IsDirectory(path) } -> std::same_as<bool>;
};

class IAppendOnlyFileSystem {
public:
    virtual ~IAppendOnlyFileSystem() = default;

    /**
     * Opens a file for reading.
     * @param path The path to the file
     * @param createIfNotExists If true, creates the file if it doesn't exist
     * @return A Result containing either a FileHandle or an Error
     */
    virtual Result<FileHandle> OpenFile(const std::string& path, bool createIfNotExists = false) = 0;

    // Close the file associated with the handle
    virtual Result<bool> CloseFile(FileHandle handle) = 0;

    // Append data to the file associated with the handle
    // Return the offset of the data in the file
    [[nodiscard]] virtual Result<PositionRecord> Append(FileHandle handle, const DataChunk& data) = 0;

    // Read a chunk of data from the file associated with the handle at given offset
    [[nodiscard]] virtual Result<DataChunk> Read(FileHandle handle, size_t offset, size_t length) = 0;

    // Get the current size of the file associated with the handle
    [[nodiscard]] virtual Result<size_t> Size(FileHandle handle) const = 0;

    // Check if file exists
    [[nodiscard]] virtual bool Exists(const std::string& path) const = 0;

    // Atomically rename multiple files
    // If any rename fails, all renames are rolled back
    [[nodiscard]] virtual Result<bool> RenameFiles(const std::vector<RenameOperation>& renames) = 0;

    // List files in a directory
    // If recursive is true, also list files in subdirectories
    // Returns a list of relative paths
    [[nodiscard]] virtual Result<std::vector<std::string>> List(const std::string& path, bool recursive = false) = 0;

    // Delete multiple files
    // If any deletion fails, continue with remaining files but return error
    [[nodiscard]] virtual Result<bool> DeleteFiles(const std::vector<std::string>& paths) = 0;

    // Directory operations
    [[nodiscard]] virtual Result<bool> CreateDirectory(const std::string& path) = 0;
    [[nodiscard]] virtual Result<bool> DeleteDirectory(const std::string& path, bool recursive = false) = 0;
    [[nodiscard]] virtual Result<bool> MoveDirectory(const std::string& source_path,
                                                     const std::string& target_path) = 0;
    [[nodiscard]] virtual Result<DirectoryInfo> GetDirectoryInfo(const std::string& path) const = 0;
    [[nodiscard]] virtual bool IsDirectory(const std::string& path) const = 0;

    // Factory method to create concrete implementations
    [[nodiscard]] static std::unique_ptr<IAppendOnlyFileSystem> Create(std::string_view type);

    // Factory method to create a shared pointer to the concrete implementation
    [[nodiscard]] static std::shared_ptr<IAppendOnlyFileSystem> CreateShared(std::string_view type);
};

// Concrete implementation for local file system
class LocalAppendOnlyFileSystem final : public IAppendOnlyFileSystem {
public:
    LocalAppendOnlyFileSystem();
    ~LocalAppendOnlyFileSystem() override;

    [[nodiscard]] Result<FileHandle> OpenFile(const std::string& path, bool createIfNotExists = false) override;
    Result<bool> CloseFile(FileHandle handle) override;
    [[nodiscard]] Result<PositionRecord> Append(FileHandle handle, const DataChunk& data) override;
    [[nodiscard]] Result<DataChunk> Read(FileHandle handle, size_t offset, size_t length) override;
    [[nodiscard]] Result<size_t> Size(FileHandle handle) const override;
    [[nodiscard]] bool Exists(const std::string& path) const override;
    [[nodiscard]] Result<bool> RenameFiles(const std::vector<RenameOperation>& renames) override;
    [[nodiscard]] Result<std::vector<std::string>> List(const std::string& path, bool recursive = false) override;
    [[nodiscard]] Result<bool> DeleteFiles(const std::vector<std::string>& paths) override;

    // Directory operations implementation
    [[nodiscard]] Result<bool> CreateDirectory(const std::string& path) override;
    [[nodiscard]] Result<bool> DeleteDirectory(const std::string& path, bool recursive = false) override;
    [[nodiscard]] Result<bool> MoveDirectory(const std::string& source_path, const std::string& target_path) override;
    [[nodiscard]] Result<DirectoryInfo> GetDirectoryInfo(const std::string& path) const override;
    [[nodiscard]] bool IsDirectory(const std::string& path) const override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

static_assert(FileSystem<LocalAppendOnlyFileSystem>, "LocalAppendOnlyFileSystem must satisfy FileSystem concept");

}  // namespace pond::common