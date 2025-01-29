#pragma once

#include <memory>

#include "common/data_chunk.h"
#include "common/result.h"

namespace pond::common {

class ISerializable {
public:
    virtual ~ISerializable() = default;

    // Serialize the object into a DataChunk
    virtual DataChunk serialize() const = 0;

    // Serialize the object into an existing DataChunk
    virtual void serialize(DataChunk& data) const = 0;

    // Deserialize the object from a DataChunk
    // Returns true if deserialization is successful, false otherwise
    virtual bool deserialize(const DataChunk& data) = 0;

    // Deserialize the object from a DataChunk and return a unique pointer to the object
    virtual common::Result<std::unique_ptr<ISerializable>> deserializeAsUniquePtr(const DataChunk& data) const = 0;
};

}  // namespace pond::common