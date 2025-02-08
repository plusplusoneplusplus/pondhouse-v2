#pragma once

#include <cstring>

#include "append_only_fs.h"
#include "log.h"
#include "result.h"
#include "serializable.h"
#include "wal_entry.h"

namespace pond::common {

template <typename T>
concept WalEntryType = std::derived_from<T, WalEntry>;

namespace detail {
// WAL entry format:
// | Magic (4B) | LSN (8B) | EntrySize (4B) | Entry (serialized) |
constexpr size_t MAGIC_SIZE = sizeof(uint32_t);
constexpr size_t LSN_SIZE = sizeof(LSN);
constexpr size_t ENTRY_SIZE_SIZE = sizeof(uint32_t);
constexpr size_t HEADER_SIZE = MAGIC_SIZE + LSN_SIZE + ENTRY_SIZE_SIZE;
}  // namespace detail

template <WalEntryType T>
class WAL {
public:
    static constexpr uint32_t MAGIC_NUMBER = 0x57414C45;  // "WALE" in hex
    static constexpr uint32_t VERSION = 1;

    explicit WAL(std::shared_ptr<IAppendOnlyFileSystem> fs) : fs_(std::move(fs)) {}

    ~WAL() {
        if (handle_ != INVALID_HANDLE) {
            close();
        }
    }

    // Open or create WAL file
    common::Result<bool> open(const std::string& path, bool recover = false) {
        LOG_STATUS("Opening WAL file: %s", path.c_str());

        path_ = path;
        auto result = fs_->openFile(path, true);
        if (!result.ok()) {
            return common::Result<bool>::failure(result.error());
        }
        handle_ = result.value();

        // Read the last LSN if file is not empty
        auto size_result = fs_->size(handle_);
        if (!size_result.ok()) {
            LOG_ERROR("Failed to get file size: %s", size_result.error().c_str());
            return common::Result<bool>::failure(size_result.error());
        }

        if (recover && size_result.value() > 0) {
            LOG_VERBOSE("Recovering from WAL file: %s", path.c_str());

            auto entries = read(0);
            if (!entries.ok()) {
                LOG_ERROR("Failed to read entries: %s", entries.error().c_str());
                return common::Result<bool>::failure(entries.error());
            }
            if (!entries.value().empty()) {
                current_lsn_ = entries.value().back().lsn() + 1;
            }
        }

        return common::Result<bool>::success(true);
    }

    // Append an entry to WAL
    common::Result<LSN> append(T& entry) {
        if (entry.lsn() == INVALID_LSN) {
            entry.set_lsn(inc_lsn());
        } else if (entry.lsn() != current_lsn()) {
            LOG_ERROR("Invalid LSN: %zu < %zu", entry.lsn(), current_lsn());
            return common::Result<LSN>::failure(ErrorCode::InvalidArgument, "Invalid LSN");
        } else {
            current_lsn_.fetch_add(1);
        }

        // Prepare entry data
        auto entry_data = entry.Serialize();
        size_t total_size = detail::HEADER_SIZE + entry_data.size();

        common::DataChunk data(total_size);
        uint8_t* ptr = data.data();

        // Write header
        std::memcpy(ptr, &MAGIC_NUMBER, detail::MAGIC_SIZE);
        ptr += detail::MAGIC_SIZE;

        LSN current = entry.lsn();
        std::memcpy(ptr, &current, detail::LSN_SIZE);
        ptr += detail::LSN_SIZE;

        uint32_t entry_size = entry_data.size();
        std::memcpy(ptr, &entry_size, detail::ENTRY_SIZE_SIZE);
        ptr += detail::ENTRY_SIZE_SIZE;

        std::memcpy(ptr, entry_data.data(), entry_data.size());

        // Append to file
        auto result = fs_->append(handle_, data);
        if (!result.ok()) {
            return common::Result<LSN>::failure(result.error().code(), result.error().message());
        }

        LOG_VERBOSE("Appended entry to WAL: LSN=%zu", current);

        return common::Result<LSN>::success(entry.lsn());
    }

    // Read entries from WAL starting from given LSN
    common::Result<std::vector<T>> read(LSN start_lsn) {
        std::vector<T> entries;
        auto size_result = fs_->size(handle_);
        if (!size_result.ok()) {
            LOG_ERROR("Failed to get file size: %s", size_result.error().c_str());
            return common::Result<std::vector<T>>::failure(size_result.error().code(), size_result.error().message());
        }

        size_t file_size = size_result.value();
        size_t offset = 0;

        while (offset < file_size) {
            // Read header first
            auto header_result = fs_->read(handle_, offset, detail::HEADER_SIZE);
            if (!header_result.ok()) {
                LOG_ERROR("Failed to read header: %s", header_result.error().c_str());
                return common::Result<std::vector<T>>::failure(header_result.error().code(),
                                                               header_result.error().message());
            }

            if (header_result.value().size() < detail::HEADER_SIZE) {
                LOG_ERROR("Incomplete header read: expected %zu bytes, got %zu",
                          detail::HEADER_SIZE,
                          header_result.value().size());
                return common::Result<std::vector<T>>::failure(ErrorCode::FileReadFailed, "Incomplete header read");
            }

            const uint8_t* ptr = header_result.value().data();

            // Verify magic number
            uint32_t magic;
            std::memcpy(&magic, ptr, detail::MAGIC_SIZE);
            ptr += detail::MAGIC_SIZE;

            if (magic != MAGIC_NUMBER) {
                LOG_ERROR(
                    "Invalid WAL entry magic number at offset %zu: expected %#x, got %#x", offset, MAGIC_NUMBER, magic);
                return common::Result<std::vector<T>>::failure(ErrorCode::FileCorrupted,
                                                               "Invalid WAL entry magic number");
            }

            // Read LSN
            LSN lsn;
            std::memcpy(&lsn, ptr, detail::LSN_SIZE);
            ptr += detail::LSN_SIZE;

            uint32_t entry_size;
            std::memcpy(&entry_size, ptr, detail::ENTRY_SIZE_SIZE);
            ptr += detail::ENTRY_SIZE_SIZE;

            if (lsn < start_lsn) {
                // Skip this entry
                LOG_WARNING("Skipping entry: LSN=%zu, start_lsn=%zu", lsn, start_lsn);
                offset += detail::HEADER_SIZE + entry_size;
                continue;
            }

            // Read the entry data
            auto entry_data_result = fs_->read(handle_, offset + detail::HEADER_SIZE, entry_size);
            if (!entry_data_result.ok()) {
                LOG_ERROR("Failed to read entry data: %s", entry_data_result.error().c_str());
                return common::Result<std::vector<T>>::failure(entry_data_result.error().code(),
                                                               entry_data_result.error().message());
            }

            T entry;
            if (!entry.Deserialize(entry_data_result.value())) {
                LOG_ERROR("Failed to deserialize entry: LSN=%zu", lsn);
                return common::Result<std::vector<T>>::failure(ErrorCode::FileCorrupted, "Failed to deserialize entry");
            }

            entries.push_back(std::move(entry));

            // Update offset to next entry
            offset += detail::HEADER_SIZE + entry_size;

            start_lsn = lsn + 1;
            LOG_VERBOSE("Completed reading entry from WAL: LSN=%zu", lsn);
        }

        if (!entries.empty()) {
            current_lsn_ = entries.back().lsn() + 1;
        }

        return common::Result<std::vector<T>>::success(std::move(entries));
    }

    // Close WAL file
    common::Result<bool> close() {
        if (handle_ == INVALID_HANDLE) {
            return common::Result<bool>::success(true);
        }

        auto result = fs_->closeFile(handle_);
        handle_ = INVALID_HANDLE;
        return result;
    }

    LSN current_lsn() const { return current_lsn_.load(); }

    void reset_lsn() { current_lsn_ = 0; }

    LSN inc_lsn() { return current_lsn_++; }

    FileHandle handle() const { return handle_; }

protected:
    std::shared_ptr<IAppendOnlyFileSystem> fs_;
    FileHandle handle_{INVALID_HANDLE};
    std::atomic<LSN> current_lsn_{0};
    std::string path_;
};

}  // namespace pond::common