#pragma once

#include <memory>

#include "common/data_chunk.h"
#include "common/result.h"

namespace pond::common {

class ISerializable {
public:
    virtual ~ISerializable() = default;

    // Serialize the object into a DataChunk
    virtual DataChunk Serialize() const = 0;

    // Serialize the object into an existing DataChunk
    virtual void Serialize(DataChunk& data) const = 0;

    // Deserialize the object from a DataChunk
    // Returns true if deserialization is successful, false otherwise
    virtual bool Deserialize(const DataChunk& data) = 0;

    // Deserialize the object from a DataChunk and return a unique pointer to the object
    virtual common::Result<std::unique_ptr<ISerializable>> DeserializeAsUniquePtr(const DataChunk& data) const = 0;
};

}  // namespace pond::common