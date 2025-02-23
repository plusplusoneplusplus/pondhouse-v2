#pragma once

#include <memory>
#include <optional>

#include "common/data_chunk.h"
#include "common/time.h"

namespace pond::kv {

/**
 * VersionedValue represents a single version of a value in the MVCC system.
 * It maintains a chain of versions through the prev_version pointer.
 *
 * @tparam T The type of value to store. Must be copyable.
 */
template <typename T>
class VersionedValue : public std::enable_shared_from_this<VersionedValue<T>> {
public:
    VersionedValue(T value,
                   common::HybridTime version,
                   uint64_t txn_id,
                   bool is_deleted = false,
                   std::shared_ptr<const VersionedValue<T>> prev = nullptr)
        : value_(std::move(value)),
          version_(version),
          txn_id_(txn_id),
          is_deleted_(is_deleted),
          prev_version_(std::move(prev)) {}

    // Accessors
    const T& value() const { return value_; }
    common::HybridTime version() const { return version_; }
    uint64_t txn_id() const { return txn_id_; }
    bool IsDeleted() const { return is_deleted_; }
    const std::shared_ptr<const VersionedValue<T>>& prev_version() const { return prev_version_; }

    // Get the value visible at a specific timestamp
    std::optional<std::reference_wrapper<const T>> GetValueAt(common::HybridTime timestamp) const {
        // If this version is newer than the requested timestamp, check previous versions
        if (version_ > timestamp) {
            return prev_version_ ? prev_version_->GetValueAt(timestamp) : std::nullopt;
        }

        // If this version is a deletion marker, the value doesn't exist at this timestamp
        if (is_deleted_) {
            return std::nullopt;
        }

        return std::cref(value_);
    }

    // Get the value visible for a specific transaction
    std::optional<std::reference_wrapper<const T>> GetValueForTxn(uint64_t txn_id) const {
        // If this version belongs to the transaction, return it
        if (txn_id_ == txn_id) {
            return is_deleted_ ? std::nullopt : std::optional<std::reference_wrapper<const T>>(std::cref(value_));
        }

        // If this version is from a different transaction, check previous versions
        return prev_version_ ? prev_version_->GetValueForTxn(txn_id) : std::nullopt;
    }

    // Create a new version of this value
    std::shared_ptr<VersionedValue<T>> CreateNewVersion(T value,
                                                        common::HybridTime version,
                                                        uint64_t txn_id,
                                                        bool is_deleted = false) const {
        return std::make_shared<VersionedValue<T>>(
            std::move(value), version, txn_id, is_deleted, this->shared_from_this());
    }

    // Create a deletion marker version
    std::shared_ptr<VersionedValue<T>> CreateDeletionMarker(common::HybridTime version, uint64_t txn_id) const {
        return std::make_shared<VersionedValue<T>>(T(), version, txn_id, true, this->shared_from_this());
    }

private:
    T value_;                                                // The actual value data
    common::HybridTime version_;                             // Version timestamp
    uint64_t txn_id_;                                        // Creating transaction ID
    bool is_deleted_;                                        // Deletion marker
    std::shared_ptr<const VersionedValue<T>> prev_version_;  // Previous version in the chain
};

// Type alias for the common case of DataChunk values
using DataChunkVersionedValue = VersionedValue<common::DataChunk>;

}  // namespace pond::kv