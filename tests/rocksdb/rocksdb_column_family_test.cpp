#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

#include "common/error.h"
#include "test_helper.h"

namespace pond::test {

class RocksDBColumnFamilyTest : public ::testing::Test {
protected:
    void TearDown() override {
        // Close column family handles
        for (auto handle : column_handles_) {
            if (handle) {
                delete handle;
            }
        }

        // Close the database
        if (db_) {
            delete db_;
            db_ = nullptr;
        }

        // Clean up the test directory
        if (!test_db_path_.empty()) {
            std::filesystem::remove_all(test_db_path_);
        }
    }

    // Helper to create a unique temporary path
    std::filesystem::path create_temp_path() {
        auto path = std::filesystem::temp_directory_path() / ("rocksdb_cf_test_" + random_string(8));
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
    std::vector<rocksdb::ColumnFamilyHandle*> column_handles_;
    std::filesystem::path test_db_path_;
};

TEST_F(RocksDBColumnFamilyTest, CreateColumnFamilies) {
    test_db_path_ = create_temp_path();

    // First create a DB with default column family
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    // Open database with default column family
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database: " << status.ToString();

    // Create two additional column families
    rocksdb::ColumnFamilyHandle* cf1;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf1", &cf1);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf1: " << status.ToString();
    column_handles_.push_back(cf1);

    rocksdb::ColumnFamilyHandle* cf2;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf2", &cf2);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf2: " << status.ToString();
    column_handles_.push_back(cf2);

    // Close the database to test reopening with column families
    for (auto handle : column_handles_) {
        delete handle;
    }
    column_handles_.clear();
    delete db_;
    db_ = nullptr;

    // Reopen the database with all column families
    std::vector<std::string> column_families;
    status = rocksdb::DB::ListColumnFamilies(options, test_db_path_.string(), &column_families);
    ASSERT_TRUE(status.ok()) << "Failed to list column families: " << status.ToString();
    ASSERT_EQ(3, column_families.size());

    // Prepare column family descriptors
    std::vector<rocksdb::ColumnFamilyDescriptor> column_family_descriptors;
    for (const auto& cf_name : column_families) {
        column_family_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, rocksdb::ColumnFamilyOptions()));
    }

    // Set error_if_exists to false when reopening an existing DB
    options.error_if_exists = false;

    // Reopen with all column families
    status = rocksdb::DB::Open(options, test_db_path_.string(), column_family_descriptors, &column_handles_, &db_);
    ASSERT_TRUE(status.ok()) << "Failed to reopen database with column families: " << status.ToString();
}

TEST_F(RocksDBColumnFamilyTest, OperationsAcrossColumnFamilies) {
    test_db_path_ = create_temp_path();

    // First create a DB with default column family
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    // Open database with default column family
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database: " << status.ToString();

    // Create two additional column families
    rocksdb::ColumnFamilyHandle* cf1;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf1", &cf1);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf1: " << status.ToString();
    column_handles_.push_back(cf1);

    rocksdb::ColumnFamilyHandle* cf2;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf2", &cf2);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf2: " << status.ToString();
    column_handles_.push_back(cf2);

    // Default column family handle
    rocksdb::ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();

    // Put data in each column family
    status = db_->Put(rocksdb::WriteOptions(), default_cf, "key_default", "value_default");
    ASSERT_TRUE(status.ok()) << "Failed to put in default column family: " << status.ToString();

    status = db_->Put(rocksdb::WriteOptions(), cf1, "key_cf1", "value_cf1");
    ASSERT_TRUE(status.ok()) << "Failed to put in cf1: " << status.ToString();

    status = db_->Put(rocksdb::WriteOptions(), cf2, "key_cf2", "value_cf2");
    ASSERT_TRUE(status.ok()) << "Failed to put in cf2: " << status.ToString();

    // Read data from each column family
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), default_cf, "key_default", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get from default column family: " << status.ToString();
    ASSERT_EQ("value_default", value);

    status = db_->Get(rocksdb::ReadOptions(), cf1, "key_cf1", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get from cf1: " << status.ToString();
    ASSERT_EQ("value_cf1", value);

    status = db_->Get(rocksdb::ReadOptions(), cf2, "key_cf2", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get from cf2: " << status.ToString();
    ASSERT_EQ("value_cf2", value);

    // Verify keys don't exist in other column families
    status = db_->Get(rocksdb::ReadOptions(), default_cf, "key_cf1", &value);
    ASSERT_TRUE(status.IsNotFound()) << "Key from cf1 should not exist in default cf";

    status = db_->Get(rocksdb::ReadOptions(), cf1, "key_cf2", &value);
    ASSERT_TRUE(status.IsNotFound()) << "Key from cf2 should not exist in cf1";
}

TEST_F(RocksDBColumnFamilyTest, BatchWriteAcrossColumnFamilies) {
    test_db_path_ = create_temp_path();

    // First create a DB with default column family
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    // Open database with default column family
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database: " << status.ToString();

    // Create two additional column families
    rocksdb::ColumnFamilyHandle* cf1;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf1", &cf1);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf1: " << status.ToString();
    column_handles_.push_back(cf1);

    rocksdb::ColumnFamilyHandle* cf2;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf2", &cf2);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf2: " << status.ToString();
    column_handles_.push_back(cf2);

    // Default column family handle
    rocksdb::ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();

    // Create a batch write across all column families
    rocksdb::WriteBatch batch;
    batch.Put(default_cf, "batch_key_default", "batch_value_default");
    batch.Put(cf1, "batch_key_cf1", "batch_value_cf1");
    batch.Put(cf2, "batch_key_cf2", "batch_value_cf2");

    // Execute the batch
    status = db_->Write(rocksdb::WriteOptions(), &batch);
    ASSERT_TRUE(status.ok()) << "Failed to execute batch write: " << status.ToString();

    // Read the batch data
    std::string value;
    status = db_->Get(rocksdb::ReadOptions(), default_cf, "batch_key_default", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get batch data from default cf: " << status.ToString();
    ASSERT_EQ("batch_value_default", value);

    status = db_->Get(rocksdb::ReadOptions(), cf1, "batch_key_cf1", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get batch data from cf1: " << status.ToString();
    ASSERT_EQ("batch_value_cf1", value);

    status = db_->Get(rocksdb::ReadOptions(), cf2, "batch_key_cf2", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get batch data from cf2: " << status.ToString();
    ASSERT_EQ("batch_value_cf2", value);
}

TEST_F(RocksDBColumnFamilyTest, IteratorWithColumnFamilies) {
    test_db_path_ = create_temp_path();

    // First create a DB with default column family
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    // Open database with default column family
    rocksdb::Status status = rocksdb::DB::Open(options, test_db_path_.string(), &db_);
    ASSERT_TRUE(status.ok()) << "Failed to open database: " << status.ToString();

    // Create an additional column family
    rocksdb::ColumnFamilyHandle* cf1;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "cf1", &cf1);
    ASSERT_TRUE(status.ok()) << "Failed to create column family cf1: " << status.ToString();
    column_handles_.push_back(cf1);

    // Default column family handle
    rocksdb::ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();

    // Insert data into both column families
    const int kNumEntries = 10;
    for (int i = 0; i < kNumEntries; i++) {
        status = db_->Put(rocksdb::WriteOptions(),
                          default_cf,
                          "default_key" + std::to_string(i),
                          "default_value" + std::to_string(i));
        ASSERT_TRUE(status.ok()) << "Failed to put in default cf: " << status.ToString();

        status = db_->Put(rocksdb::WriteOptions(), cf1, "cf1_key" + std::to_string(i), "cf1_value" + std::to_string(i));
        ASSERT_TRUE(status.ok()) << "Failed to put in cf1: " << status.ToString();
    }

    // Create iterator for default column family
    rocksdb::Iterator* default_iter = db_->NewIterator(rocksdb::ReadOptions(), default_cf);

    // Verify data in default column family
    int count = 0;
    for (default_iter->SeekToFirst(); default_iter->Valid(); default_iter->Next()) {
        std::string key = default_iter->key().ToString();
        std::string value = default_iter->value().ToString();

        ASSERT_TRUE(key.find("default_key") == 0) << "Unexpected key in default cf: " << key;
        ASSERT_TRUE(value.find("default_value") == 0) << "Unexpected value in default cf: " << value;
        count++;
    }
    ASSERT_EQ(kNumEntries, count) << "Wrong number of entries in default column family";
    ASSERT_TRUE(default_iter->status().ok()) << "Iterator error: " << default_iter->status().ToString();
    delete default_iter;

    // Create iterator for cf1
    rocksdb::Iterator* cf1_iter = db_->NewIterator(rocksdb::ReadOptions(), cf1);

    // Verify data in cf1
    count = 0;
    for (cf1_iter->SeekToFirst(); cf1_iter->Valid(); cf1_iter->Next()) {
        std::string key = cf1_iter->key().ToString();
        std::string value = cf1_iter->value().ToString();

        ASSERT_TRUE(key.find("cf1_key") == 0) << "Unexpected key in cf1: " << key;
        ASSERT_TRUE(value.find("cf1_value") == 0) << "Unexpected value in cf1: " << value;
        count++;
    }
    ASSERT_EQ(kNumEntries, count) << "Wrong number of entries in cf1";
    ASSERT_TRUE(cf1_iter->status().ok()) << "Iterator error: " << cf1_iter->status().ToString();
    delete cf1_iter;
}

}  // namespace pond::test
