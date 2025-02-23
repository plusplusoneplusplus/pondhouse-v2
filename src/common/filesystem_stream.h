#pragma once

#include <memory>
#include <string>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "common/stream.h"

namespace pond::common {

/**
 * FileSystemInputStream provides a filesystem-based implementation of InputStream.
 * It manages the file handle and provides convenient methods for reading data with position tracking.
 */
class FileSystemInputStream : public InputStream {
public:
    /**
     * Creates a new FileSystemInputStream for the given file.
     * @param fs The filesystem to use
     * @param path The path to the file to read
     * @return Result containing the FileSystemInputStream or an error
     */
    static Result<std::unique_ptr<FileSystemInputStream>> Create(std::shared_ptr<IAppendOnlyFileSystem> fs,
                                                                 const std::string& path);

    ~FileSystemInputStream() override;

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
     * Reads data from the specified position.
     * @param offset The offset to read from
     * @param length The number of bytes to read
     * @return Result containing the read data or an error
     */
    Result<DataChunkPtr> ReadAt(size_t offset, size_t length);

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
    FileSystemInputStream(std::shared_ptr<IAppendOnlyFileSystem> fs, FileHandle handle);

    std::shared_ptr<IAppendOnlyFileSystem> fs_;
    FileHandle handle_;
    size_t position_;
    size_t size_;
};

/**
 * FileSystemOutputStream provides a filesystem-based implementation of OutputStream.
 * It manages the file handle and provides convenient methods for writing data with position tracking.
 * The stream is append-only, meaning all writes happen at the end of the file.
 */
class FileSystemOutputStream : public OutputStream {
public:
    /**
     * Creates a new FileSystemOutputStream for the given file.
     * @param fs The filesystem to use
     * @param path The path to the file to write
     * @param create_if_not_exists Whether to create the file if it doesn't exist
     * @return Result containing the FileSystemOutputStream or an error
     */
    static Result<std::unique_ptr<FileSystemOutputStream>> Create(std::shared_ptr<IAppendOnlyFileSystem> fs,
                                                                  const std::string& path,
                                                                  bool create_if_not_exists = true);

    ~FileSystemOutputStream() override;

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
    size_t Position() const override { return position_; }

private:
    FileSystemOutputStream(std::shared_ptr<IAppendOnlyFileSystem> fs, FileHandle handle);

    std::shared_ptr<IAppendOnlyFileSystem> fs_;
    FileHandle handle_;
    size_t position_;
};

}  // namespace pond::common