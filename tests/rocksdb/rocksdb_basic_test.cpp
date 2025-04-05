#include <cstdlib>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "common/error.h"
#include "test_helper.h"

namespace pond::test {

class RocksDBBasicTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for the RocksDB database
        test_db_path_ = std::filesystem::temp_directory_path() / ("rocksdb_test_" + random_string(8));
        std::filesystem::create_directories(test_db_path_);

        // Open the database
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
        ASSERT_TRUE(status.ok()) << "Failed to open database: " << status.ToString();
    }

    void TearDown() override {
        // Close the database
        if (db_) {
            delete db_;
            db_ = nullptr;
        }

        // Clean up the test directory
        std::filesystem::remove_all(test_db_path_);
    }

    // Helper to generate a random string for test path uniqueness
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

TEST_F(RocksDBBasicTest, PutAndGetTest) {
    // Put a key-value pair
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), "key1", "value1");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();

    // Get the value back
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), "key1", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value: " << status.ToString();
    ASSERT_EQ("value1", value);
}

TEST_F(RocksDBBasicTest, DeleteTest) {
    // Put a key-value pair
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), "key2", "value2");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();

    // Delete the key
    status = db_->Delete(rocksdb::WriteOptions(), "key2");
    ASSERT_TRUE(status.ok()) << "Failed to delete key: " << status.ToString();

    // Try to get the deleted key
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), "key2", &value);
    ASSERT_TRUE(status.IsNotFound()) << "Key should be deleted, but was found";
}

TEST_F(RocksDBBasicTest, MultiPutAndGetTest) {
    // Put multiple key-value pairs
    rocksdb::WriteBatch batch;
    batch.Put("batch_key1", "batch_value1");
    batch.Put("batch_key2", "batch_value2");
    batch.Put("batch_key3", "batch_value3");

    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    ASSERT_TRUE(status.ok()) << "Failed to write batch: " << status.ToString();

    // Get the values back
    std::string value1, value2, value3;
    status = db_->Get(rocksdb::ReadOptions(), "batch_key1", &value1);
    ASSERT_TRUE(status.ok()) << "Failed to get value1: " << status.ToString();
    ASSERT_EQ("batch_value1", value1);

    status = db_->Get(rocksdb::ReadOptions(), "batch_key2", &value2);
    ASSERT_TRUE(status.ok()) << "Failed to get value2: " << status.ToString();
    ASSERT_EQ("batch_value2", value2);

    status = db_->Get(rocksdb::ReadOptions(), "batch_key3", &value3);
    ASSERT_TRUE(status.ok()) << "Failed to get value3: " << status.ToString();
    ASSERT_EQ("batch_value3", value3);
}

TEST_F(RocksDBBasicTest, IteratorTest) {
    // Put some key-value pairs
    rocksdb::WriteBatch batch;
    batch.Put("iter_key1", "iter_value1");
    batch.Put("iter_key2", "iter_value2");
    batch.Put("iter_key3", "iter_value3");

    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    ASSERT_TRUE(status.ok()) << "Failed to write batch: " << status.ToString();

    // Use an iterator to scan all key-value pairs
    rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
    std::map<std::string, std::string> expected_kvs = {
        {"iter_key1", "iter_value1"}, {"iter_key2", "iter_value2"}, {"iter_key3", "iter_value3"}};
    std::map<std::string, std::string> actual_kvs;

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        // Filter only the keys related to this test
        if (it->key().ToString().find("iter_key") == 0) {
            actual_kvs[it->key().ToString()] = it->value().ToString();
        }
    }

    ASSERT_TRUE(it->status().ok()) << "Iterator error: " << it->status().ToString();
    ASSERT_EQ(expected_kvs, actual_kvs);

    // Clean up
    delete it;
}

}  // namespace pond::test