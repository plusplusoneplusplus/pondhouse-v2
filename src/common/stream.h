#pragma once

#include <memory>

#include "common/data_chunk.h"
#include "common/result.h"

namespace pond::common {

/**
 * Interface for input streams that provide sequential read access to data.
 */
class InputStream {
public:
    virtual ~InputStream() = default;

    /**
     * Reads data from the current position.
     * @param length The number of bytes to read
     * @return Result containing the read data or an error
     */
    virtual Result<DataChunkPtr> Read(size_t length) = 0;

    /**
     * Reads raw data from the current position.
     * @param data Pointer to the buffer to store the read data
     * @param size The number of bytes to read
     * @return Result containing the number of bytes read or an error
     */
    virtual Result<size_t> Read(void* data, size_t size) = 0;

    /**
     * Gets the size of the stream.
     * @return Result containing the stream size or an error
     */
    virtual Result<size_t> Size() const = 0;

    /**
     * Updates the size of the stream.
     * @param size The new size
     * @return Result indicating success or failure
     */
    virtual Result<bool> UpdateSize(size_t size) = 0;

    /**
     * Gets the current position in the stream.
     * @return The current position
     */
    virtual size_t Position() const = 0;

    /**
     * Sets the current position in the stream.
     * @param position The new position
     * @return Result indicating success or failure
     */
    virtual Result<bool> Seek(size_t position) = 0;
};

/**
 * Interface for output streams that provide sequential write access to data.
 */
class OutputStream {
public:
    virtual ~OutputStream() = default;

    /**
     * Writes data to the stream.
     * @param data The data to write
     * @return Result containing the number of bytes written or an error
     */
    virtual Result<size_t> Write(const DataChunkPtr& data) = 0;

    /**
     * Writes raw data to the stream.
     * @param data Pointer to the data to write
     * @param size Number of bytes to write
     * @return Result containing the number of bytes written or an error
     */
    virtual Result<size_t> Write(const void* data, size_t size) = 0;

    /**
     * Gets the current position in the stream.
     * @return The current position
     */
    virtual size_t Position() const = 0;
};

}  // namespace pond::common