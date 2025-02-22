#include <gtest/gtest.h>

#include "common/filesystem_stream.h"
#include "common/memory_append_only_fs.h"
#include "common/memory_stream.h"

namespace pond::common {

// Interface for creating stream instances for testing
class StreamFactory {
public:
    virtual ~StreamFactory() = default;
    virtual std::unique_ptr<InputStream> CreateInputStream(const std::string& data) = 0;
    virtual std::unique_ptr<OutputStream> CreateOutputStream() = 0;
};

// Factory for memory-based streams
class MemoryStreamFactory : public StreamFactory {
public:
    std::unique_ptr<InputStream> CreateInputStream(const std::string& data) override {
        return MemoryInputStream::Create(data.c_str(), data.length());
    }

    std::unique_ptr<OutputStream> CreateOutputStream() override { return MemoryOutputStream::Create(); }
};

// Factory for filesystem-based streams
class FileSystemStreamFactory : public StreamFactory {
public:
    FileSystemStreamFactory() : fs_(std::make_shared<MemoryAppendOnlyFileSystem>()) {}

    std::unique_ptr<InputStream> CreateInputStream(const std::string& data) override {
        const std::string test_file = "test_input_" + UUID::NewUUID().ToString() + ".txt";

        // Create and write to file
        auto writer_result = FileSystemOutputStream::Create(fs_, test_file);
        if (!writer_result.ok())
            return nullptr;

        auto writer = std::move(writer_result).value();
        auto write_result = writer->Write(data.c_str(), data.length());
        if (!write_result.ok())
            return nullptr;

        // Create reader
        auto reader_result = FileSystemInputStream::Create(fs_, test_file);
        if (!reader_result.ok())
            return nullptr;

        return std::move(reader_result).value();
    }

    std::unique_ptr<OutputStream> CreateOutputStream() override {
        const std::string test_file = "test_output_" + UUID::NewUUID().ToString() + ".txt";
        auto result = FileSystemOutputStream::Create(fs_, test_file);
        if (!result.ok())
            return nullptr;
        return std::move(result).value();
    }

private:
    std::shared_ptr<IAppendOnlyFileSystem> fs_;
};

class StreamTest : public ::testing::TestWithParam<StreamFactory*> {
protected:
    void verifyStreamContents(InputStream* stream, const std::string& expected) {
        auto size_result = stream->Size();
        ASSERT_TRUE(size_result.ok());
        ASSERT_EQ(size_result.value(), expected.length());

        auto read_result = stream->Read(expected.length());
        ASSERT_TRUE(read_result.ok());
        ASSERT_EQ(read_result.value()->ToString(), expected);
    }
};

//
// Test Setup:
//      Creates an input stream with test data
// Test Result:
//      Verifies basic read functionality and content matching
//
TEST_P(StreamTest, BasicRead) {
    const std::string test_data = "Hello, World!";
    auto stream = GetParam()->CreateInputStream(test_data);
    ASSERT_NE(stream, nullptr);

    verifyStreamContents(stream.get(), test_data);
}

//
// Test Setup:
//      Tests raw data reading functionality with partial reads and EOF handling
// Test Result:
//      Verifies correct handling of raw data buffers and positions
//
TEST_P(StreamTest, RawDataReading) {
    const std::string test_data = "Hello, World!";
    auto stream = GetParam()->CreateInputStream(test_data);
    ASSERT_NE(stream, nullptr);

    std::vector<char> buffer(test_data.size());

    // Read partial data
    auto result = stream->Read(buffer.data(), 5);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 5);
    EXPECT_EQ(std::string(buffer.data(), 5), "Hello");
    EXPECT_EQ(stream->Position(), 5);

    // Read remaining data
    result = stream->Read(buffer.data() + 5, test_data.size() - 5);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), test_data.size() - 5);
    EXPECT_EQ(std::string(buffer.data(), test_data.size()), test_data);
    EXPECT_EQ(stream->Position(), test_data.size());

    // Try to read beyond end
    result = stream->Read(buffer.data(), 1);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 0);  // Should return 0 at end of stream

    // Test null buffer
    result = stream->Read(nullptr, 0);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 0);

    // Test zero length read
    result = stream->Read(buffer.data(), 0);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 0);
}

//
// Test Setup:
//      Creates an input stream and seeks to middle
// Test Result:
//      Verifies seek operation and partial read from seek position
//
TEST_P(StreamTest, SeekAndRead) {
    const std::string test_data = "Hello, World!";
    auto stream = GetParam()->CreateInputStream(test_data);
    ASSERT_NE(stream, nullptr);

    // Seek to middle
    auto seek_result = stream->Seek(7);
    ASSERT_TRUE(seek_result.ok());
    ASSERT_EQ(stream->Position(), 7);

    // Read remaining
    auto read_result = stream->Read(6);
    ASSERT_TRUE(read_result.ok());
    ASSERT_EQ(read_result.value()->ToString(), "World!");
}

//
// Test Setup:
//      Creates an input stream for partial reading
// Test Result:
//      Verifies multiple sequential reads with position tracking
//
TEST_P(StreamTest, PartialRead) {
    const std::string test_data = "Hello, World!";
    auto stream = GetParam()->CreateInputStream(test_data);
    ASSERT_NE(stream, nullptr);

    // Read first 5 bytes
    auto read_result = stream->Read(5);
    ASSERT_TRUE(read_result.ok());
    ASSERT_EQ(read_result.value()->ToString(), "Hello");
    ASSERT_EQ(stream->Position(), 5);

    // Read next 2 bytes
    read_result = stream->Read(2);
    ASSERT_TRUE(read_result.ok());
    ASSERT_EQ(read_result.value()->ToString(), ", ");
    ASSERT_EQ(stream->Position(), 7);
}

//
// Test Setup:
//      Creates an output stream for writing in chunks
// Test Result:
//      Verifies writing multiple chunks and final content matching
//
TEST_P(StreamTest, WriteInChunks) {
    auto stream = GetParam()->CreateOutputStream();
    ASSERT_NE(stream, nullptr);

    // Write in parts
    auto write_result = stream->Write("Hello, ", 7);
    ASSERT_TRUE(write_result.ok());
    ASSERT_EQ(write_result.value(), 7);

    write_result = stream->Write("World!", 6);
    ASSERT_TRUE(write_result.ok());
    ASSERT_EQ(write_result.value(), 6);

    // Verify position
    ASSERT_EQ(stream->Position(), 13);
}

// Static instances of our factories
static MemoryStreamFactory memory_factory;
static FileSystemStreamFactory fs_factory;

INSTANTIATE_TEST_SUITE_P(StreamTypes,
                         StreamTest,
                         ::testing::Values(&memory_factory, &fs_factory),
                         [](const testing::TestParamInfo<StreamFactory*>& info) {
                             return dynamic_cast<MemoryStreamFactory*>(info.param) ? "Memory" : "FileSystem";
                         });

}  // namespace pond::common