#pragma once

#include <memory>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/stream.h"

namespace pond::common {

/**
 * MemoryInputStream provides an implementation of InputStream that reads from a DataChunk.
 * All operations are performed in memory without any I/O.
 */
class MemoryInputStream : public InputStream {
public:
    /**
     * Creates a new MemoryInputStream from a DataChunk.
     * @param data The data to read from
     * @return A new MemoryInputStream instance
     */
    static std::unique_ptr<MemoryInputStream> Create(const DataChunkPtr& data);

    /**
     * Creates a new MemoryInputStream from raw data.
     * @param data Pointer to the data to read from
     * @param size Size of the data in bytes
     * @return A new MemoryInputStream instance
     */
    static std::unique_ptr<MemoryInputStream> Create(const void* data, size_t size);

    ~MemoryInputStream() override = default;

    /**
     * Reads data from the current position.
     * @param length The number of bytes to read
     * @return Result containing the read data or an error
     */
    Result<DataChunkPtr> Read(size_t length) override;

    /**
     * Reads raw data from the current position.
     * @param data Pointer to the buffer to store the read data
     * @param size The number of bytes to read
     * @return Result containing the number of bytes read or an error
     */
    Result<size_t> Read(void* data, size_t size) override;

    /**
     * Gets the size of the stream.
     * @return Result containing the stream size or an error
     */
    Result<size_t> Size() const override;

    /**
     * Updates the size of the stream.
     * @param size The new size
     * @return Result indicating success or failure
     */
    Result<bool> UpdateSize(size_t size) override;

    /**
     * Gets the current position in the stream.
     * @return The current position
     */
    size_t Position() const override { return position_; }

    /**
     * Sets the current position in the stream.
     * @param position The new position
     * @return Result indicating success or failure
     */
    Result<bool> Seek(size_t position) override;

private:
    explicit MemoryInputStream(const DataChunkPtr& data);

    DataChunkPtr data_;
    size_t position_;
    size_t size_;
};

/**
 * MemoryOutputStream provides an implementation of OutputStream that writes to a DataChunk.
 * All operations are performed in memory without any I/O.
 */
class MemoryOutputStream : public OutputStream {
public:
    /**
     * Creates a new MemoryOutputStream with an initial capacity.
     * @param initial_capacity Initial capacity in bytes (default: 4KB)
     * @return A new MemoryOutputStream instance
     */
    static std::unique_ptr<MemoryOutputStream> Create(size_t initial_capacity = 4096);

    ~MemoryOutputStream() override = default;

    /**
     * Writes data to the stream.
     * @param data The data to write
     * @return Result containing the number of bytes written or an error
     */
    Result<size_t> Write(const DataChunkPtr& data) override;

    /**
     * Writes raw data to the stream.
     * @param data Pointer to the data to write
     * @param size Number of bytes to write
     * @return Result containing the number of bytes written or an error
     */
    Result<size_t> Write(const void* data, size_t size) override;

    /**
     * Gets the current position in the stream.
     * @return The current position
     */
    size_t Position() const override { return data_->Size(); }

    /**
     * Gets the current contents of the stream.
     * @return The accumulated data
     */
    const DataChunkPtr& Data() const { return data_; }

private:
    explicit MemoryOutputStream(size_t initial_capacity);

    DataChunkPtr data_;
};

}  // namespace pond::common