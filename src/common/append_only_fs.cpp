#include "append_only_fs.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <unordered_map>

#include "common/error.h"
#include "log.h"
#include "memory_append_only_fs.h"

namespace pond::common {

class LocalAppendOnlyFileSystem::Impl {
public:
    Impl() {}

    ~Impl() {
        // Clean up any remaining handles
        for (const auto& [handle, file] : files_) {
            if (file.stream) {
                file.stream->close();
            }

            delete handle;
        }
    }

    Result<FileHandle> openFile(const std::string& path, bool createIfNotExists) {
        std::lock_guard<std::mutex> lock(mutex);
        
        // Check if file exists
        if (!std::filesystem::exists(path)) {
            if (!createIfNotExists) {
                return Result<FileHandle>::failure(common::ErrorCode::FileNotFound, "File not found: " + path);
            }
            
            // Create parent directories if they don't exist
            std::filesystem::path fs_path(path);
            auto parent_path = fs_path.parent_path();
            if (!parent_path.empty()) {
                std::filesystem::create_directories(parent_path);
            }
        }

        auto stream = std::make_unique<std::fstream>();
        stream->open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

        if (!stream->is_open()) {
            // Try creating the file if it doesn't exist
            stream->open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
            if (!stream->is_open()) {
                LOG_ERROR("Failed to open file: %s %s", path.c_str(), strerror(errno));
                return Result<FileHandle>::failure(common::ErrorCode::FileOpenFailed, "Failed to open file: " + path);
            }
        }

        auto handle = new GenericFileHandle{path};
        files_[handle] = {std::move(stream), path};
        return Result<FileHandle>::success(handle);
    }

    Result<bool> closeFile(FileHandle handle) {
        if (!handle) {
            return Result<bool>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto it = files_.find(handle);
        if (it == files_.end()) {
            return Result<bool>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        files_.erase(it);

        if (it->second.stream) {
            it->second.stream->close();
        }

        delete handle;
        return Result<bool>::success(true);
    }

    Result<PositionRecord> append(FileHandle handle, const DataChunk& data) {
        if (!handle) {
            return Result<PositionRecord>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto it = files_.find(handle);
        if (it == files_.end()) {
            return Result<PositionRecord>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto& file = it->second;
        file.stream->seekp(0, std::ios::end);
        if (file.stream->fail()) {
            return Result<PositionRecord>::failure(common::ErrorCode::FileSeekFailed, "Failed to seek to end of file");
        }

        auto current_pos = file.stream->tellp();
        file.stream->write(reinterpret_cast<const char*>(data.data()), data.size());
        if (file.stream->fail()) {
            return Result<PositionRecord>::failure(common::ErrorCode::FileWriteFailed, "Failed to write to file");
        }

        // flush the file
        file.stream->flush();

        return Result<PositionRecord>::success(PositionRecord{
            .id_ = common::UUID::newUUID(), .offset_ = static_cast<size_t>(current_pos), .length_ = data.size()});
    }

    Result<DataChunk> read(FileHandle handle, size_t offset, size_t length) {
        if (!handle) {
            return Result<DataChunk>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto it = files_.find(handle);
        if (it == files_.end()) {
            return Result<DataChunk>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto& file = it->second;
        DataChunk chunk(length);

        file.stream->seekg(offset, std::ios::beg);
        if (file.stream->fail()) {
            LOG_ERROR("Failed to seek to offset: %zu", offset);
            return Result<DataChunk>::failure(common::ErrorCode::FileSeekFailed, "Failed to seek to offset");
        }

        file.stream->read(reinterpret_cast<char*>(chunk.data()), length);
        size_t bytes_read = file.stream->gcount();
        chunk.resize(bytes_read);

        if (file.stream->fail() && !file.stream->eof()) {
            return Result<DataChunk>::failure(common::ErrorCode::FileReadFailed, "Failed to read from file");
        }

        LOG_VERBOSE("Read %zu bytes from file: %zu", bytes_read, offset);
        return Result<DataChunk>::success(std::move(chunk));
    }

    Result<size_t> size(FileHandle handle) const {
        if (!handle) {
            return Result<size_t>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto it = files_.find(handle);
        if (it == files_.end()) {
            return Result<size_t>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
        }

        auto& file = it->second;
        auto current_pos = file.stream->tellg();

        file.stream->seekg(0, std::ios::end);
        auto size = file.stream->tellg();

        // Restore original position
        file.stream->seekg(current_pos);

        if (file.stream->fail()) {
            return Result<size_t>::failure(common::ErrorCode::FileSeekFailed, "Failed to get file size");
        }

        return Result<size_t>::success(static_cast<size_t>(size));
    }

    bool exists(const std::string& path) const { return std::filesystem::exists(path); }

    bool isFileOpen(const std::string& path) const {
        for (const auto& [handle, file] : files_) {
            if (file.path == path) {
                return true;
            }
        }
        return false;
    }

    Result<bool> deleteFiles(const std::vector<std::string>& paths) {
        bool had_error = false;
        std::string error_message;

        for (const auto& path : paths) {
            // Check if file is currently open
            if (isFileOpen(path)) {
                had_error = true;
                error_message += "File is currently open: " + path + "\n";
                continue;
            }

            try {
                if (std::filesystem::exists(path)) {
                    std::filesystem::remove(path);
                } else {
                    had_error = true;
                    error_message += "File does not exist: " + path + "\n";
                }
            } catch (const std::filesystem::filesystem_error& e) {
                had_error = true;
                error_message += "Failed to delete file " + path + ": " + e.what() + "\n";
            }
        }

        if (had_error) {
            return Result<bool>::failure(common::ErrorCode::FileDeleteFailed, error_message);
        }
        return Result<bool>::success(true);
    }

private:
    struct FileInfo {
        std::unique_ptr<std::fstream> stream;
        std::string path;
    };

    std::unordered_map<FileHandle, FileInfo> files_;
    std::mutex mutex;
};

// LocalAppendOnlyFileSystem implementation
LocalAppendOnlyFileSystem::LocalAppendOnlyFileSystem() : impl_(std::make_unique<Impl>()) {}
LocalAppendOnlyFileSystem::~LocalAppendOnlyFileSystem() = default;

Result<FileHandle> LocalAppendOnlyFileSystem::openFile(const std::string& path, bool createIfNotExists) {
    return impl_->openFile(path, createIfNotExists);
}

Result<bool> LocalAppendOnlyFileSystem::closeFile(FileHandle handle) {
    return impl_->closeFile(handle);
}

Result<PositionRecord> LocalAppendOnlyFileSystem::append(FileHandle handle, const DataChunk& data) {
    return impl_->append(handle, data);
}

Result<DataChunk> LocalAppendOnlyFileSystem::read(FileHandle handle, size_t offset, size_t length) {
    return impl_->read(handle, offset, length);
}

Result<size_t> LocalAppendOnlyFileSystem::size(FileHandle handle) const {
    return impl_->size(handle);
}

bool LocalAppendOnlyFileSystem::exists(const std::string& path) const {
    return impl_->exists(path);
}

Result<bool> LocalAppendOnlyFileSystem::renameFiles(const std::vector<RenameOperation>& renames) {
    // Validate operations first
    for (const auto& op : renames) {
        // Check if source exists
        if (!exists(op.source_path)) {
            return Result<bool>::failure(common::ErrorCode::FileNotFound,
                                         "Source file does not exist: " + op.source_path);
        }

        // Check if any target already exists
        if (exists(op.target_path)) {
            return Result<bool>::failure(common::ErrorCode::FileAlreadyExists,
                                         "Target file already exists: " + op.target_path);
        }

        // Check if source file is currently open
        if (impl_->isFileOpen(op.source_path)) {
            return Result<bool>::failure(common::ErrorCode::FileOpenFailed,
                                         "Source file is currently open: " + op.source_path);
        }
    }

    // Create temporary renames for atomic operation
    std::vector<RenameOperation> temp_renames;
    temp_renames.reserve(renames.size());

    try {
        // First phase: rename to temporary files
        for (size_t i = 0; i < renames.size(); i++) {
            std::string temp_path = renames[i].source_path + ".tmp" + std::to_string(i);
            std::filesystem::rename(renames[i].source_path, temp_path);
            temp_renames.push_back({temp_path, renames[i].target_path});
        }

        // Second phase: rename from temporary to final destinations
        for (const auto& op : temp_renames) {
            std::filesystem::rename(op.source_path, op.target_path);
        }

        return Result<bool>::success(true);
    } catch (const std::filesystem::filesystem_error& e) {
        // Rollback on failure
        try {
            // Rollback temp renames
            for (auto it = temp_renames.rbegin(); it != temp_renames.rend(); ++it) {
                if (std::filesystem::exists(it->source_path)) {
                    std::filesystem::rename(it->source_path,
                                            renames[std::distance(temp_renames.rbegin(), it)].source_path);
                }
            }
        } catch (...) {
            // If rollback fails, we're in an inconsistent state
            return Result<bool>::failure(common::ErrorCode::FileOpenFailed,
                                         "Fatal error: rollback failed during rename operation");
        }
        return Result<bool>::failure(common::ErrorCode::FileOpenFailed, std::string("Rename failed: ") + e.what());
    }
}

Result<std::vector<std::string>> LocalAppendOnlyFileSystem::list(const std::string& path, bool recursive) {
    try {
        std::vector<std::string> files;
        std::filesystem::path base_path = path;

        if (!std::filesystem::exists(base_path)) {
            return Result<std::vector<std::string>>::failure(common::ErrorCode::DirectoryNotFound,
                                                             "Directory does not exist: " + path);
        }

        if (!std::filesystem::is_directory(base_path)) {
            return Result<std::vector<std::string>>::failure(common::ErrorCode::NotADirectory,
                                                             "Path is not a directory: " + path);
        }

        // Convert base_path to canonical form for proper relative path calculation
        base_path = std::filesystem::canonical(base_path);

        if (recursive) {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(base_path)) {
                if (entry.is_regular_file()) {
                    // Calculate relative path from base_path
                    std::filesystem::path relative_path = std::filesystem::relative(entry.path(), base_path);
                    files.push_back(relative_path.string());
                }
            }
        } else {
            for (const auto& entry : std::filesystem::directory_iterator(base_path)) {
                if (entry.is_regular_file()) {
                    // For non-recursive listing, just use the filename
                    files.push_back(entry.path().filename().string());
                }
            }
        }

        return Result<std::vector<std::string>>::success(std::move(files));
    } catch (const std::filesystem::filesystem_error& e) {
        return Result<std::vector<std::string>>::failure(common::ErrorCode::FileOpenFailed,
                                                         std::string("Failed to list directory: ") + e.what());
    }
}

Result<bool> LocalAppendOnlyFileSystem::deleteFiles(const std::vector<std::string>& paths) {
    return impl_->deleteFiles(paths);
}

Result<bool> LocalAppendOnlyFileSystem::createDirectory(const std::string& path) {
    try {
        if (std::filesystem::exists(path)) {
            if (std::filesystem::is_directory(path)) {
                return Result<bool>::success(true);  // Directory already exists
            }
            return Result<bool>::failure(common::ErrorCode::NotADirectory,
                                         "Path exists but is not a directory: " + path);
        }

        bool success = std::filesystem::create_directories(path);
        if (!success) {
            return Result<bool>::failure(common::ErrorCode::DirectoryCreateFailed,
                                         "Failed to create directory: " + path);
        }
        return Result<bool>::success(true);
    } catch (const std::filesystem::filesystem_error& e) {
        return Result<bool>::failure(common::ErrorCode::DirectoryCreateFailed,
                                     "Failed to create directory " + path + ": " + e.what());
    }
}

Result<bool> LocalAppendOnlyFileSystem::deleteDirectory(const std::string& path, bool recursive) {
    try {
        if (!std::filesystem::exists(path)) {
            return Result<bool>::failure(common::ErrorCode::DirectoryNotFound, "Directory does not exist: " + path);
        }

        if (!std::filesystem::is_directory(path)) {
            return Result<bool>::failure(common::ErrorCode::NotADirectory, "Path is not a directory: " + path);
        }

        // Check if directory is empty when not recursive
        if (!recursive && !std::filesystem::is_empty(path)) {
            return Result<bool>::failure(common::ErrorCode::DirectoryNotEmpty,
                                         "Directory is not empty and recursive deletion was not specified: " + path);
        }

        std::error_code ec;
        if (recursive) {
            std::filesystem::remove_all(path, ec);
        } else {
            std::filesystem::remove(path, ec);
        }

        if (ec) {
            return Result<bool>::failure(common::ErrorCode::DirectoryDeleteFailed,
                                         "Failed to delete directory " + path + ": " + ec.message());
        }

        return Result<bool>::success(true);
    } catch (const std::filesystem::filesystem_error& e) {
        return Result<bool>::failure(common::ErrorCode::DirectoryDeleteFailed,
                                     "Failed to delete directory " + path + ": " + e.what());
    }
}

Result<bool> LocalAppendOnlyFileSystem::moveDirectory(const std::string& source_path, const std::string& target_path) {
    try {
        if (!std::filesystem::exists(source_path)) {
            return Result<bool>::failure(common::ErrorCode::DirectoryNotFound,
                                         "Source directory does not exist: " + source_path);
        }

        if (!std::filesystem::is_directory(source_path)) {
            return Result<bool>::failure(common::ErrorCode::NotADirectory,
                                         "Source path is not a directory: " + source_path);
        }

        if (std::filesystem::exists(target_path)) {
            return Result<bool>::failure(common::ErrorCode::DirectoryAlreadyExists,
                                         "Target path already exists: " + target_path);
        }

        // Create parent directory if it doesn't exist
        auto parent_path = std::filesystem::path(target_path).parent_path();
        if (!parent_path.empty() && !std::filesystem::exists(parent_path)) {
            std::filesystem::create_directories(parent_path);
        }

        std::filesystem::rename(source_path, target_path);
        return Result<bool>::success(true);
    } catch (const std::filesystem::filesystem_error& e) {
        return Result<bool>::failure(
            common::ErrorCode::DirectoryMoveFailed,
            "Failed to move directory " + source_path + " to " + target_path + ": " + e.what());
    }
}

Result<DirectoryInfo> LocalAppendOnlyFileSystem::getDirectoryInfo(const std::string& path) const {
    try {
        DirectoryInfo info{.exists = false, .is_directory = false, .num_files = 0, .total_size = 0};

        if (!std::filesystem::exists(path)) {
            return Result<DirectoryInfo>::success(info);
        }

        info.exists = true;
        info.is_directory = std::filesystem::is_directory(path);

        if (!info.is_directory) {
            return Result<DirectoryInfo>::success(info);
        }

        // Count files and total size
        for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
            if (entry.is_regular_file()) {
                info.num_files++;
                info.total_size += entry.file_size();
            }
        }

        return Result<DirectoryInfo>::success(info);
    } catch (const std::filesystem::filesystem_error& e) {
        return Result<DirectoryInfo>::failure(common::ErrorCode::DirectoryAccessFailed,
                                              "Failed to get directory info for " + path + ": " + e.what());
    }
}

bool LocalAppendOnlyFileSystem::isDirectory(const std::string& path) const {
    try {
        return std::filesystem::is_directory(path);
    } catch (const std::filesystem::filesystem_error&) {
        return false;
    }
}

// Factory method implementation
std::unique_ptr<IAppendOnlyFileSystem> IAppendOnlyFileSystem::create(std::string_view type) {
    if (type == "local") {
        return std::make_unique<LocalAppendOnlyFileSystem>();
    } else if (type == "memory") {
        return std::make_unique<MemoryAppendOnlyFileSystem>();
    } else if (type == "memory_random_duplicate") {
        return std::make_unique<MemoryAppendOnlyFileSystem>(true);
    }
    return nullptr;
}

// Factory method to create a shared pointer to the concrete implementation
std::shared_ptr<IAppendOnlyFileSystem> IAppendOnlyFileSystem::createShared(std::string_view type) {
    if (type == "local") {
        return std::make_shared<LocalAppendOnlyFileSystem>();
    } else if (type == "memory") {
        return std::make_shared<MemoryAppendOnlyFileSystem>();
    } else if (type == "memory_random_duplicate") {
        return std::make_shared<MemoryAppendOnlyFileSystem>(true);
    }
    return nullptr;
}

}  // namespace pond::common