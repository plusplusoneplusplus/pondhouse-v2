#pragma once

#include <google/protobuf/io/zero_copy_stream.h>

#include "common/data_chunk.h"

namespace pond::common {

/**
 * DataChunkOutputStream implements the Protocol Buffers ZeroCopyOutputStream interface
 * to allow direct serialization into a DataChunk without intermediate copies.
 */
class DataChunkOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
public:
    /**
     * Constructs a new DataChunkOutputStream that writes to the given DataChunk.
     * @param chunk The DataChunk to write to
     */
    explicit DataChunkOutputStream(DataChunk& chunk) : chunk_(chunk), position_(0) {}

    /**
     * Returns a buffer of size *size for writing.
     * @param[out] data Pointer to the buffer for writing
     * @param[out] size Size of the buffer
     * @return true if successful, false if there was an error
     */
    bool Next(void** data, int* size) override {
        // Pre-allocate some space in the chunk
        size_t old_size = chunk_.Size();
        chunk_.Resize(old_size + kBlockSize);
        *data = chunk_.Data() + old_size;
        *size = kBlockSize;
        position_ = old_size + kBlockSize;
        return true;
    }

    /**
     * Backs up a number of bytes, undoing the last Next() call.
     * @param count Number of bytes to back up
     */
    void BackUp(int count) override {
        // Adjust the size to account for unused bytes
        chunk_.Resize(position_ - count);
        position_ -= count;
    }

    /**
     * Returns the total number of bytes written.
     * @return Number of bytes written
     */
    int64_t ByteCount() const override { return position_; }

private:
    static constexpr size_t kBlockSize = 4096;  // Size of blocks to allocate
    DataChunk& chunk_;
    size_t position_;
};

}  // namespace pond::common