#include "memory_append_only_fs.h"

#include <filesystem>
#include <sstream>

#include "common/error.h"
#include "log.h"

namespace pond::common {

MemoryAppendOnlyFileSystem::MemoryAppendOnlyFileSystem(double duplicate_block_probability)
    : duplicate_block_probability_(std::clamp(duplicate_block_probability, 0.0, 1.0)),
      explicit_control_mode_(false),
      should_duplicate_next_(false) {}

std::shared_ptr<MemoryAppendOnlyFileSystem> MemoryAppendOnlyFileSystem::createWithExplicitControl(
    bool should_duplicate_next) {
    auto fs = std::make_shared<MemoryAppendOnlyFileSystem>(0.0);  // probability doesn't matter in explicit mode
    fs->explicit_control_mode_ = true;
    fs->should_duplicate_next_ = should_duplicate_next;
    return fs;
}

void MemoryAppendOnlyFileSystem::setNextAppendDuplicate(bool should_duplicate) {
    std::lock_guard lock(mutex_);
    should_duplicate_next_ = should_duplicate;
}

bool MemoryAppendOnlyFileSystem::shouldCreateDuplicate() const {
    if (explicit_control_mode_) {
        return should_duplicate_next_;
    }
    return dist_(rng_) < duplicate_block_probability_;
}

MemoryAppendOnlyFileSystem::~MemoryAppendOnlyFileSystem() = default;

void MemoryAppendOnlyFileSystem::splitPath(const std::string& path, std::vector<std::string>& parts) const {
    std::filesystem::path fs_path(path);
    for (const auto& part : fs_path) {
        auto part_str = part.string();
        if (!part_str.empty() && part_str != "/" && part_str != ".") {
            parts.push_back(part_str);
        }
    }
}

std::string MemoryAppendOnlyFileSystem::joinPath(const std::vector<std::string>& parts) const {
    std::filesystem::path result;
    for (const auto& part : parts) {
        result /= part;
    }
    return result.string();
}

std::pair<MemoryAppendOnlyFileSystem::DirectoryData*, std::string> MemoryAppendOnlyFileSystem::getDirectoryAndName(
    const std::string& path, bool create) {
    std::vector<std::string> parts;
    splitPath(path, parts);

    if (parts.empty()) {
        return {&root_, ""};
    }

    DirectoryData* current = &root_;
    for (size_t i = 0; i < parts.size() - 1; ++i) {
        const auto& part = parts[i];
        auto it = current->subdirs.find(part);
        if (it == current->subdirs.end()) {
            if (!create) {
                return {nullptr, ""};
            }
            auto new_dir = std::make_unique<DirectoryData>();
            auto next = new_dir.get();
            current->subdirs[part] = std::move(new_dir);
            current = next;
        } else {
            current = it->second.get();
        }
    }

    return {current, parts.back()};
}

MemoryAppendOnlyFileSystem::FileData* MemoryAppendOnlyFileSystem::getFile(const std::string& path) {
    auto [dir, name] = getDirectoryAndName(path, false);
    if (!dir || name.empty()) {
        return nullptr;
    }

    auto it = dir->files.find(name);
    return it != dir->files.end() ? it->second.get() : nullptr;
}

Result<FileHandle> MemoryAppendOnlyFileSystem::OpenFile(const std::string& path, bool createIfNotExists) {
    std::lock_guard lock(mutex_);

    auto [dir, name] = getDirectoryAndName(path, createIfNotExists);
    if (!dir || name.empty()) {
        return Result<FileHandle>::failure(common::ErrorCode::FileOpenFailed, "Invalid path: " + path);
    }

    auto it = dir->files.find(name);
    if (it == dir->files.end()) {
        if (!createIfNotExists) {
            return Result<FileHandle>::failure(common::ErrorCode::FileNotFound, "File not found: " + path);
        }
        // Create new file
        auto file = std::make_unique<FileData>();
        file->open_count.fetch_add(1);
        it = dir->files.emplace(name, std::move(file)).first;
    } else {
        it->second->open_count.fetch_add(1);
    }

    auto handle = new GenericFileHandle(path);
    open_files_[handle] = path;
    return Result<FileHandle>::success(handle);
}

Result<bool> MemoryAppendOnlyFileSystem::CloseFile(FileHandle handle) {
    if (!handle) {
        return Result<bool>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    std::lock_guard lock(mutex_);

    auto it = open_files_.find(handle);
    if (it == open_files_.end()) {
        return Result<bool>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    auto file = getFile(it->second);
    if (file) {
        file->open_count.fetch_sub(1);
    }

    open_files_.erase(it);
    delete handle;
    return Result<bool>::success(true);
}

Result<PositionRecord> MemoryAppendOnlyFileSystem::Append(FileHandle handle, const DataChunk& data) {
    if (!handle) {
        return Result<PositionRecord>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    std::lock_guard lock(mutex_);

    auto it = open_files_.find(handle);
    if (it == open_files_.end()) {
        return Result<PositionRecord>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    auto file = getFile(it->second);
    if (!file || !file->is_open()) {
        return Result<PositionRecord>::failure(common::ErrorCode::FileWriteFailed, "File not open");
    }

    size_t offset = file->data.size();
    bool should_duplicate = shouldCreateDuplicate();

    // In explicit mode, reset the flag after checking
    if (explicit_control_mode_) {
        should_duplicate_next_ = false;
    }

    if (should_duplicate && !file->has_duplicate) {
        // First attempt with duplicate - simulate failure but write the data
        file->data.insert(file->data.end(), data.Data(), data.Data() + data.Size());
        file->has_duplicate = true;
    }

    // Normal append or retry after duplicate
    file->data.insert(file->data.end(), data.Data(), data.Data() + data.Size());

    return Result<PositionRecord>::success(
        PositionRecord{.id_ = common::UUID::newUUID(), .offset_ = offset, .length_ = data.Size()});
}

Result<DataChunk> MemoryAppendOnlyFileSystem::Read(FileHandle handle, size_t offset, size_t length) {
    if (!handle) {
        return Result<DataChunk>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    std::lock_guard lock(mutex_);

    auto it = open_files_.find(handle);
    if (it == open_files_.end()) {
        return Result<DataChunk>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    auto file = getFile(it->second);
    if (!file || !file->is_open()) {
        return Result<DataChunk>::failure(common::ErrorCode::FileReadFailed, "File not open");
    }

    if (offset >= file->data.size()) {
        return Result<DataChunk>::success(DataChunk());
    }

    length = std::min(length, file->data.size() - offset);
    return Result<DataChunk>::success(DataChunk(file->data.data() + offset, length));
}

Result<size_t> MemoryAppendOnlyFileSystem::Size(FileHandle handle) const {
    if (!handle) {
        return Result<size_t>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    std::lock_guard lock(mutex_);

    auto it = open_files_.find(handle);
    if (it == open_files_.end()) {
        return Result<size_t>::failure(common::ErrorCode::InvalidHandle, "Invalid handle");
    }

    auto file = const_cast<MemoryAppendOnlyFileSystem*>(this)->getFile(it->second);
    if (!file || !file->is_open()) {
        return Result<size_t>::failure(common::ErrorCode::FileReadFailed, "File not open");
    }

    return Result<size_t>::success(file->data.size());
}

bool MemoryAppendOnlyFileSystem::Exists(const std::string& path) const {
    std::lock_guard lock(mutex_);

    auto [dir, name] = const_cast<MemoryAppendOnlyFileSystem*>(this)->getDirectoryAndName(path, false);
    if (!dir) {
        return false;
    }

    if (name.empty()) {
        return true;  // Root directory
    }

    return dir->files.find(name) != dir->files.end() || dir->subdirs.find(name) != dir->subdirs.end();
}

Result<bool> MemoryAppendOnlyFileSystem::RenameFiles(const std::vector<RenameOperation>& renames) {
    std::lock_guard lock(mutex_);

    // Validate operations first
    for (const auto& op : renames) {
        if (!exists(true, op.source_path)) {
            return Result<bool>::failure(common::ErrorCode::FileNotFound,
                                         "Source file does not exist: " + op.source_path);
        }
        if (exists(true, op.target_path)) {
            return Result<bool>::failure(common::ErrorCode::FileAlreadyExists,
                                         "Target file already exists: " + op.target_path);
        }
    }

    // Perform renames
    for (const auto& op : renames) {
        auto [src_dir, src_name] = getDirectoryAndName(op.source_path, false);
        auto [dst_dir, dst_name] = getDirectoryAndName(op.target_path, true);

        if (!src_dir || !dst_dir || src_name.empty() || dst_name.empty()) {
            return Result<bool>::failure(common::ErrorCode::FileNotFound, "Invalid path");
        }

        auto it = src_dir->files.find(src_name);
        if (it == src_dir->files.end()) {
            return Result<bool>::failure(common::ErrorCode::FileNotFound, "Source file not found");
        }

        if (it->second->open_count.load() > 0) {
            return Result<bool>::failure(common::ErrorCode::FileOpenFailed, "Source file is open");
        }

        dst_dir->files[dst_name] = std::move(it->second);
        src_dir->files.erase(it);
    }

    return Result<bool>::success(true);
}

void MemoryAppendOnlyFileSystem::listRecursive(const DirectoryData* dir,
                                               const std::string& base_path,
                                               std::vector<std::string>& result,
                                               bool recursive) const {
    for (const auto& [name, file] : dir->files) {
        result.push_back(base_path.empty() ? name : base_path + "/" + name);
    }

    if (recursive) {
        for (const auto& [name, subdir] : dir->subdirs) {
            std::string new_base = base_path.empty() ? name : base_path + "/" + name;
            listRecursive(subdir.get(), new_base, result, true);
        }
    }
}

Result<std::vector<std::string>> MemoryAppendOnlyFileSystem::List(const std::string& path, bool recursive) {
    std::lock_guard lock(mutex_);

    auto [dir, name] = getDirectoryAndName(path, false);
    if (!dir) {
        return Result<std::vector<std::string>>::failure(common::ErrorCode::DirectoryNotFound,
                                                         "Directory not found: " + path);
    }

    if (!name.empty()) {
        auto it = dir->subdirs.find(name);
        if (it == dir->subdirs.end()) {
            return Result<std::vector<std::string>>::failure(common::ErrorCode::DirectoryNotFound,
                                                             "Directory not found: " + path);
        }
        dir = it->second.get();
    }

    std::vector<std::string> result;
    listRecursive(dir, "", result, recursive);
    return Result<std::vector<std::string>>::success(std::move(result));
}

Result<bool> MemoryAppendOnlyFileSystem::DeleteFiles(const std::vector<std::string>& paths) {
    std::lock_guard lock(mutex_);

    bool had_error = false;
    std::string error_message;

    for (const auto& path : paths) {
        auto [dir, name] = getDirectoryAndName(path, false);
        if (!dir || name.empty()) {
            had_error = true;
            error_message += "Invalid path: " + path + "\n";
            continue;
        }

        auto it = dir->files.find(name);
        if (it == dir->files.end()) {
            had_error = true;
            error_message += "File not found: " + path + "\n";
            continue;
        }

        if (it->second->is_open()) {
            had_error = true;
            error_message += "File is open: " + path + "\n";
            continue;
        }

        dir->files.erase(it);
    }

    if (had_error) {
        return Result<bool>::failure(common::ErrorCode::FileDeleteFailed, error_message);
    }
    return Result<bool>::success(true);
}

Result<bool> MemoryAppendOnlyFileSystem::CreateDirectory(const std::string& path) {
    std::lock_guard lock(mutex_);

    auto [dir, name] = getDirectoryAndName(path, true);
    if (!dir) {
        return Result<bool>::failure(common::ErrorCode::DirectoryCreateFailed, "Failed to create directory: " + path);
    }

    if (name.empty()) {
        return Result<bool>::success(true);  // Root directory
    }

    if (dir->files.find(name) != dir->files.end()) {
        return Result<bool>::failure(common::ErrorCode::NotADirectory, "Path exists but is not a directory: " + path);
    }

    if (dir->subdirs.find(name) == dir->subdirs.end()) {
        dir->subdirs[name] = std::make_unique<DirectoryData>();
    }

    return Result<bool>::success(true);
}

Result<bool> MemoryAppendOnlyFileSystem::DeleteDirectory(const std::string& path, bool recursive) {
    std::lock_guard lock(mutex_);

    auto [parent_dir, name] = getDirectoryAndName(path, false);
    if (!parent_dir) {
        return Result<bool>::failure(common::ErrorCode::DirectoryNotFound, "Directory not found: " + path);
    }

    if (name.empty()) {
        return Result<bool>::failure(common::ErrorCode::InvalidArgument, "Cannot delete root directory");
    }

    auto it = parent_dir->subdirs.find(name);
    if (it == parent_dir->subdirs.end()) {
        return Result<bool>::failure(common::ErrorCode::DirectoryNotFound, "Directory not found: " + path);
    }

    if (!recursive && (!it->second->files.empty() || !it->second->subdirs.empty())) {
        return Result<bool>::failure(common::ErrorCode::DirectoryNotEmpty,
                                     "Directory not empty and recursive deletion not specified: " + path);
    }

    parent_dir->subdirs.erase(it);
    return Result<bool>::success(true);
}

Result<bool> MemoryAppendOnlyFileSystem::MoveDirectory(const std::string& source_path, const std::string& target_path) {
    std::lock_guard lock(mutex_);

    if (!exists(true, source_path)) {
        return Result<bool>::failure(common::ErrorCode::DirectoryNotFound,
                                     "Source directory does not exist: " + source_path);
    }

    if (exists(true, target_path)) {
        return Result<bool>::failure(common::ErrorCode::DirectoryAlreadyExists,
                                     "Target path already exists: " + target_path);
    }

    auto [src_parent, src_name] = getDirectoryAndName(source_path, false);
    auto [dst_parent, dst_name] = getDirectoryAndName(target_path, true);

    if (!src_parent || !dst_parent || src_name.empty() || dst_name.empty()) {
        return Result<bool>::failure(common::ErrorCode::InvalidArgument, "Invalid path");
    }

    auto it = src_parent->subdirs.find(src_name);
    if (it == src_parent->subdirs.end()) {
        return Result<bool>::failure(common::ErrorCode::NotADirectory, "Source is not a directory: " + source_path);
    }

    dst_parent->subdirs[dst_name] = std::move(it->second);
    src_parent->subdirs.erase(it);

    return Result<bool>::success(true);
}

Result<DirectoryInfo> MemoryAppendOnlyFileSystem::GetDirectoryInfo(const std::string& path) const {
    std::lock_guard lock(mutex_);

    DirectoryInfo info{.exists = false, .is_directory = false, .num_files = 0, .total_size = 0};

    auto [dir, name] = const_cast<MemoryAppendOnlyFileSystem*>(this)->getDirectoryAndName(path, false);
    if (!dir) {
        return Result<DirectoryInfo>::success(info);
    }

    if (!name.empty()) {
        auto it = dir->subdirs.find(name);
        if (it == dir->subdirs.end()) {
            return Result<DirectoryInfo>::success(info);
        }
        dir = it->second.get();
    }

    info.exists = true;
    info.is_directory = true;

    // Count files and total size recursively
    std::function<void(const DirectoryData*)> count_recursive = [&](const DirectoryData* d) {
        for (const auto& [_, file] : d->files) {
            info.num_files++;
            info.total_size += file->data.size();
        }
        for (const auto& [_, subdir] : d->subdirs) {
            count_recursive(subdir.get());
        }
    };

    count_recursive(dir);
    return Result<DirectoryInfo>::success(info);
}

bool MemoryAppendOnlyFileSystem::IsDirectory(const std::string& path) const {
    std::lock_guard lock(mutex_);

    auto [dir, name] = const_cast<MemoryAppendOnlyFileSystem*>(this)->getDirectoryAndName(path, false);
    if (!dir) {
        return false;
    }

    if (name.empty()) {
        return true;  // Root directory
    }

    auto it = dir->subdirs.find(name);
    return it != dir->subdirs.end();
}

bool MemoryAppendOnlyFileSystem::exists(bool lock_held, const std::string& path) const {
    if (!lock_held) {
        std::lock_guard lock(mutex_);
        return exists(true, path);
    }

    auto [dir, name] = const_cast<MemoryAppendOnlyFileSystem*>(this)->getDirectoryAndName(path, false);
    if (!dir) {
        return false;
    }
    if (name.empty()) {
        return true;  // Root or existing directory path
    }
    return dir->subdirs.find(name) != dir->subdirs.end() || dir->files.find(name) != dir->files.end();
}

}  // namespace pond::common