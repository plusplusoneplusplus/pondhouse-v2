#include "common/append_only_fs.h"

#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/data_chunk.h"
#include "common/memory_append_only_fs.h"
#include "test_helper.h"

using namespace pond::common;

class AppendOnlyFSTest : public ::testing::Test {
protected:
    void SetUp() override { cleanupTestFiles(); }

    void TearDown() override { cleanupTestFiles(); }

    void cleanupTestFiles() {
        const std::vector<std::string> testFiles = {"test.dat",
                                                    "test_append.dat",
                                                    "test_rename1.dat",
                                                    "test_rename2.dat",
                                                    "test_rename1_new.dat",
                                                    "test_rename2_new.dat",
                                                    "test_dir/file1.txt",
                                                    "test_dir/file2.txt",
                                                    "test_dir/subdir/file3.txt",
                                                    "nonexistent.txt",
                                                    "test.txt"};

        for (const auto& file : testFiles) {
            if (std::filesystem::exists(file)) {
                std::filesystem::remove(file);
            }
        }

        const std::vector<std::string> testDirs = {"test_dir/subdir", "test_dir", "empty_dir"};

        for (const auto& dir : testDirs) {
            if (std::filesystem::exists(dir)) {
                std::filesystem::remove_all(dir);
            }
        }
    }

    void createTestDirectory(const std::string& path) {
        if (!std::filesystem::exists(path)) {
            std::filesystem::create_directories(path);
        }
    }

    void verifyFileContents(IAppendOnlyFileSystem* fs, FileHandle handle, const std::string& expected) {
        auto result = fs->read(handle, 0, expected.length());
        VERIFY_RESULT_MSG(result, "Failed to read file contents");
        ASSERT_EQ(result.value().toString(), expected) << "File contents don't match";
    }
};

class AppendOnlyFSTypeTest : public AppendOnlyFSTest, public ::testing::WithParamInterface<std::string> {
protected:
    std::unique_ptr<IAppendOnlyFileSystem> createFS() {
        const std::string& type = GetParam();
        if (type == "local") {
            return std::make_unique<LocalAppendOnlyFileSystem>();
        } else if (type == "memory") {
            return std::make_unique<MemoryAppendOnlyFileSystem>();
        }
        return nullptr;
    }
};

// Basic File Operations Tests
TEST_P(AppendOnlyFSTypeTest, BasicFileOperations) {
    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Test file creation
    auto result = fs->openFile("test.dat", true);
    VERIFY_RESULT_MSG(result, "Failed to create file");
    auto handle = result.value();
    ASSERT_NE(handle, INVALID_HANDLE);

    // Test file existence
    ASSERT_TRUE(fs->exists("test.dat")) << "File should exist after creation";

    // Test file size
    auto sizeResult = fs->size(handle);
    VERIFY_RESULT_MSG(sizeResult, "Failed to get file size");
    ASSERT_EQ(sizeResult.value(), 0) << "New file should be empty";

    // Test file close
    VERIFY_RESULT_MSG(fs->closeFile(handle), "Failed to close file");
}

// Data Operations Tests
TEST_P(AppendOnlyFSTypeTest, DataOperations) {
    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    auto result = fs->openFile("test_append.dat", true);
    VERIFY_RESULT_MSG(result, "Failed to open file");
    auto handle = result.value();

    // Test append operation
    std::string testData = "Hello, World!";
    auto appendResult = fs->append(handle, DataChunk::fromString(testData));
    VERIFY_RESULT_MSG(appendResult, "Failed to append data");
    ASSERT_EQ(appendResult.value().offset_, 0) << "First append should start at offset 0";
    ASSERT_EQ(appendResult.value().length_, testData.length()) << "Append length should match data length";

    // Test read operation
    auto readResult = fs->read(handle, 0, testData.length());
    VERIFY_RESULT_MSG(readResult, "Failed to read data");
    ASSERT_EQ(readResult.value().toString(), testData) << "Read data doesn't match written data";

    // Test partial read
    auto partialRead = fs->read(handle, 0, 5);
    VERIFY_RESULT_MSG(partialRead, "Failed to perform partial read");
    ASSERT_EQ(partialRead.value().toString(), "Hello") << "Partial read data doesn't match";

    // Test read beyond EOF
    auto beyondEOF = fs->read(handle, testData.length(), 10);
    VERIFY_RESULT_MSG(beyondEOF, "Read beyond EOF failed");
    ASSERT_TRUE(beyondEOF.value().empty()) << "Read beyond EOF should return empty data";

    VERIFY_RESULT_MSG(fs->closeFile(handle), "Failed to close file");
}

// Directory Operations Tests
TEST_P(AppendOnlyFSTypeTest, DirectoryOperations) {
    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Test directory creation
    VERIFY_RESULT_MSG(fs->createDirectory("test_dir"), "Failed to create directory");
    ASSERT_TRUE(fs->isDirectory("test_dir")) << "Created path should be a directory";

    // Test nested directory creation
    VERIFY_RESULT_MSG(fs->createDirectory("test_dir/subdir"), "Failed to create nested directory");
    ASSERT_TRUE(fs->isDirectory("test_dir/subdir")) << "Created nested path should be a directory";

    // Test file creation in directory
    auto result = fs->openFile("test_dir/file1.txt", true);
    VERIFY_RESULT_MSG(result, "Failed to create file in directory");
    auto handle = result.value();
    VERIFY_RESULT_MSG(fs->append(handle, DataChunk::fromString("test content")),
                      "Failed to write to file in directory");
    VERIFY_RESULT_MSG(fs->closeFile(handle), "Failed to close file in directory");

    // Test directory listing
    auto listResult = fs->list("test_dir", false);
    VERIFY_RESULT_MSG(listResult, "Failed to list directory");
    ASSERT_EQ(listResult.value().size(), 1) << "Directory should contain 1 file";

    // create a file in the subdir
    result = fs->openFile("test_dir/subdir/file3.txt", true);
    VERIFY_RESULT_MSG(result, "Failed to create file in subdirectory");
    handle = result.value();
    VERIFY_RESULT_MSG(fs->append(handle, DataChunk::fromString("test content")),
                      "Failed to write to file in subdirectory");
    VERIFY_RESULT_MSG(fs->closeFile(handle), "Failed to close file in subdirectory");

    // Test recursive directory listing
    auto recursiveList = fs->list("test_dir", true);
    VERIFY_RESULT_MSG(recursiveList, "Failed to list directory recursively");
    ASSERT_EQ(recursiveList.value().size(), 2) << "Recursive listing should show all items";

    // Test directory info
    auto infoResult = fs->getDirectoryInfo("test_dir");
    VERIFY_RESULT_MSG(infoResult, "Failed to get directory info");
    ASSERT_TRUE(infoResult.value().exists) << "Directory should exist";
    ASSERT_TRUE(infoResult.value().is_directory) << "Should be a directory";
    ASSERT_GT(infoResult.value().total_size, 0) << "Directory should have non-zero size";

    // Test directory move
    VERIFY_RESULT_MSG(fs->moveDirectory("test_dir", "test_dir_moved"), "Failed to move directory");
    ASSERT_FALSE(fs->exists("test_dir")) << "Original directory should not exist after move";
    ASSERT_TRUE(fs->exists("test_dir_moved")) << "Target directory should exist after move";

    // Test directory deletion
    VERIFY_RESULT_MSG(fs->deleteDirectory("test_dir_moved", true), "Failed to delete directory");
    ASSERT_FALSE(fs->exists("test_dir_moved")) << "Directory should not exist after deletion";
}

TEST_P(AppendOnlyFSTypeTest, NestedDirectoryOperations) {
    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Create nested directories
    VERIFY_RESULT_MSG(fs->createDirectory("test_dir/subdir/subsubdir"), "Failed to create nested directory");
    ASSERT_TRUE(fs->isDirectory("test_dir/subdir/subsubdir")) << "Created nested path should be a directory";
}

// File Rename Operations Tests
TEST_P(AppendOnlyFSTypeTest, RenameOperations) {
    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Create test files
    auto handle1 = fs->openFile("test_rename1.dat", true).value();
    auto handle2 = fs->openFile("test_rename2.dat", true).value();

    VERIFY_RESULT_MSG(fs->append(handle1, DataChunk::fromString("file1")), "Failed to write to first file");
    VERIFY_RESULT_MSG(fs->append(handle2, DataChunk::fromString("file2")), "Failed to write to second file");

    VERIFY_RESULT_MSG(fs->closeFile(handle1), "Failed to close first file");
    VERIFY_RESULT_MSG(fs->closeFile(handle2), "Failed to close second file");

    // Test atomic rename
    std::vector<RenameOperation> renameOps = {{"test_rename1.dat", "test_rename1_new.dat"},
                                              {"test_rename2.dat", "test_rename2_new.dat"}};

    VERIFY_RESULT_MSG(fs->renameFiles(renameOps), "Atomic rename operation failed");
    ASSERT_FALSE(fs->exists("test_rename1.dat")) << "Original file 1 should not exist";
    ASSERT_FALSE(fs->exists("test_rename2.dat")) << "Original file 2 should not exist";
    ASSERT_TRUE(fs->exists("test_rename1_new.dat")) << "New file 1 should exist";
    ASSERT_TRUE(fs->exists("test_rename2_new.dat")) << "New file 2 should exist";

    // Test rename failure (source doesn't exist)
    renameOps = {{"nonexistent.dat", "target.dat"}};
    auto result = fs->renameFiles(renameOps);
    ASSERT_FALSE(result.ok()) << "Rename of non-existent file should fail";
}

// Error Handling Tests
TEST_P(AppendOnlyFSTypeTest, ErrorHandling) {
    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Test invalid handle operations
    VERIFY_ERROR_CODE(fs->closeFile(INVALID_HANDLE), ErrorCode::InvalidHandle);
    VERIFY_ERROR_CODE(fs->append(INVALID_HANDLE, DataChunk::fromString("test")), ErrorCode::InvalidHandle);
    VERIFY_ERROR_CODE(fs->read(INVALID_HANDLE, 0, 10), ErrorCode::InvalidHandle);
    VERIFY_ERROR_CODE(fs->size(INVALID_HANDLE), ErrorCode::InvalidHandle);

    // Test operations on non-existent files
    ASSERT_FALSE(fs->exists("nonexistent.dat")) << "Non-existent file should not exist";
    VERIFY_ERROR_CODE(fs->list("nonexistent_dir", false), ErrorCode::DirectoryNotFound);

    // Test invalid directory operations
    VERIFY_ERROR_CODE(fs->deleteDirectory("nonexistent_dir", true), ErrorCode::DirectoryNotFound);
    VERIFY_ERROR_CODE(fs->moveDirectory("nonexistent_dir", "target_dir"), ErrorCode::DirectoryNotFound);
}

// Memory-specific Tests
TEST_F(AppendOnlyFSTest, MemorySpecificBehavior_DuplicateBlock) {
    auto fs = std::make_shared<MemoryAppendOnlyFileSystem>();
    ASSERT_NE(fs, nullptr);

    // Test with explicit duplicate control
    auto fs_explicit = MemoryAppendOnlyFileSystem::createWithExplicitControl(true);
    ASSERT_NE(fs_explicit, nullptr);

    auto result = fs_explicit->openFile("test.dat", true);
    VERIFY_RESULT_MSG(result, "Failed to open file");
    auto handle = result.value();
    auto data = DataChunk::fromString("test data");

    // First append with duplicate
    auto appendResult1 = fs_explicit->append(handle, data);
    VERIFY_RESULT_MSG(appendResult1, "Failed first append");

    // Second append without duplicate
    auto appendResult2 = fs_explicit->append(handle, data);
    VERIFY_RESULT_MSG(appendResult2, "Failed second append");

    // Verify total size
    auto sizeResult = fs_explicit->size(handle);
    VERIFY_RESULT_MSG(sizeResult, "Failed to get file size");
    ASSERT_EQ(sizeResult.value(), data.size() * (2 + 1))
        << "Size should reflect duplicated data, additional block for duplicate flag";

    VERIFY_RESULT_MSG(fs_explicit->closeFile(handle), "Failed to close file");
}

TEST_P(AppendOnlyFSTypeTest, OpenNonExistentFile) {
    const std::string path = "nonexistent.txt";

    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Try to open non-existent file without create flag
    auto result = fs->openFile(path, false);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), ErrorCode::FileNotFound);

    // Try to open non-existent file with create flag
    result = fs->openFile(path, true);
    ASSERT_TRUE(result.ok());

    // Close the file
    auto close_result = fs->closeFile(result.value());
    ASSERT_TRUE(close_result.ok());
}

TEST_P(AppendOnlyFSTypeTest, OpenExistingFile) {
    const std::string path = "test.txt";

    auto fs = createFS();
    ASSERT_NE(fs, nullptr);

    // Create file first
    auto create_result = fs->openFile(path, true);
    ASSERT_TRUE(create_result.ok());
    fs->closeFile(create_result.value());

    // Open existing file without create flag
    auto result = fs->openFile(path, false);
    ASSERT_TRUE(result.ok());

    // Close the file
    auto close_result = fs->closeFile(result.value());
    ASSERT_TRUE(close_result.ok());
}

INSTANTIATE_TEST_SUITE_P(FSTypes, AppendOnlyFSTypeTest, ::testing::Values("local", "memory"));