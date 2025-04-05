#include <cstdlib>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include "common/error.h"
#include "test_helper.h"

namespace pond::test {

class RocksDBOptionsTest : public ::testing::Test {
protected:
    void TearDown() override {
        // Clean up the test database
        if (db_) {
            delete db_;
            db_ = nullptr;
        }

        if (!test_db_path_.empty()) {
            std::filesystem::remove_all(test_db_path_);
        }
    }

    // Helper to create a unique temporary path
    std::filesystem::path create_temp_path() {
        auto path = std::filesystem::temp_directory_path() / ("rocksdb_options_test_" + random_string(8));
        std::filesystem::create_directories(path);
        return path;
    }

    // Helper to generate a random string
    std::string random_string(size_t length) {
        const std::string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        std::string result;
        result.reserve(length);
        for (size_t i = 0; i < length; i++) {
            result += chars[std::rand() % chars.size()];
        }
        return result;
    }

    rocksdb::DB* db_ = nullptr;
    std::filesystem::path test_db_path_;
};

TEST_F(RocksDBOptionsTest, CompressionOptions) {
    test_db_path_ = create_temp_path();

    // Configure options with Snappy compression
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kSnappyCompression;

    // Open database with compression options
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database with compression: " << status.ToString();

    // Write some data to be compressed
    const int kNumEntries = 1000;
    const std::string kValue = std::string(1000, 'a');  // Highly compressible data

    for (int i = 0; i < kNumEntries; i++) {
        status = db_->Put(rocksdb::WriteOptions(), "key" + std::to_string(i), kValue);
        ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();
    }

    // Force a flush to ensure data is written to disk
    status = db_->Flush(rocksdb::FlushOptions());
    ASSERT_TRUE(status.ok()) << "Failed to flush database: " << status.ToString();

    // Verify data can still be read
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), "key0", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value after compression: " << status.ToString();
    ASSERT_EQ(kValue, value);
}

TEST_F(RocksDBOptionsTest, BloomFilterOptions) {
    test_db_path_ = create_temp_path();

    // Configure options with Bloom filter
    rocksdb::Options options;
    options.create_if_missing = true;

    // Create a bloom filter with 10 bits per key
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // Open database with bloom filter options
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database with bloom filter: " << status.ToString();

    // Insert some data
    status = db_->Put(rocksdb::WriteOptions(), "bloom_key1", "bloom_value1");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();

    // Force a flush to ensure data is written to SST files
    status = db_->Flush(rocksdb::FlushOptions());
    ASSERT_TRUE(status.ok()) << "Failed to flush database: " << status.ToString();

    // Check if the key exists (this should use the bloom filter)
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), "bloom_key1", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get existing key: " << status.ToString();
    ASSERT_EQ("bloom_value1", value);

    // Check if a non-existent key doesn't exist (this should use the bloom filter)
    status = db_->Get(rocksdb::ReadOptions(), "non_existent_key", &value);
    ASSERT_TRUE(status.IsNotFound()) << "Expected key not found status, got: " << status.ToString();
}

TEST_F(RocksDBOptionsTest, WriteBufferSize) {
    test_db_path_ = create_temp_path();

    // Configure options with small write buffer to trigger more frequent flushes
    rocksdb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 4096;  // Small write buffer (4KB)
    options.max_write_buffer_number = 2;

    // Open database with write buffer options
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database with custom write buffer size: " << status.ToString();

    // Write enough data to trigger multiple flushes
    const int kNumEntries = 100;
    const std::string kValue = std::string(100, 'x');  // 100-byte value

    for (int i = 0; i < kNumEntries; i++) {
        status = db_->Put(rocksdb::WriteOptions(), "buffer_key" + std::to_string(i), kValue);
        ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();
    }

    // Verify data can be read
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), "buffer_key0", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value with custom write buffer: " << status.ToString();
    ASSERT_EQ(kValue, value);
}

TEST_F(RocksDBOptionsTest, ReadCacheOptions) {
    test_db_path_ = create_temp_path();

    // Configure options with block cache
    rocksdb::Options options;
    options.create_if_missing = true;

    // Create a 64MB block cache
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // Open database with block cache options
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database with block cache: " << status.ToString();

    // Insert some data
    const int kNumEntries = 1000;
    const std::string kValue = std::string(1000, 'y');  // 1KB value

    for (int i = 0; i < kNumEntries; i++) {
        status = db_->Put(rocksdb::WriteOptions(), "cache_key" + std::to_string(i), kValue);
        ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();
    }

    // Force a flush to ensure data is written to disk
    status = db_->Flush(rocksdb::FlushOptions());
    ASSERT_TRUE(status.ok()) << "Failed to flush database: " << status.ToString();

    // Read the data multiple times to exercise the cache
    for (int j = 0; j < 3; j++) {
        for (int i = 0; i < kNumEntries; i += 100) {  // Read every 100th entry
            std::string value;
            status = db_->Get(rocksdb::ReadOptions(), "cache_key" + std::to_string(i), &value);
            ASSERT_TRUE(status.ok()) << "Failed to get value with block cache: " << status.ToString();
            ASSERT_EQ(kValue, value);
        }
    }
}

}  // namespace pond::test
