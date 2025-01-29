#pragma once

#include <memory>

#include "data_chunk.h"
#include "result.h"
#include "serializable.h"
#include "types.h"

namespace pond::common {

// Abstract base class for all WAL entries
class WalEntry : public ISerializable {
public:
    WalEntry() = default;
    explicit WalEntry(LSN seq) : lsn_(seq) {}
    virtual ~WalEntry() = default;

    // Getters
    LSN lsn() const { return lsn_; }

    // Setters
    void set_lsn(LSN lsn) { lsn_ = lsn; }

protected:
    LSN lsn_{0};
};

}  // namespace pond::common