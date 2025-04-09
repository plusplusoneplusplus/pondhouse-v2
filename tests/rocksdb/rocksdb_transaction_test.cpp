#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

#include "common/error.h"
#include "test_helper.h"

namespace pond::test {

class RocksDBTransactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for the RocksDB database
        test_db_path_ = std::filesystem::temp_directory_path() / ("rocksdb_txn_test_" + random_string(8));
        std::filesystem::create_directories(test_db_path_);
    }

    void TearDown() override {
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

    std::filesystem::path test_db_path_;
};

//
// Test Setup:
//     Open an OptimisticTransactionDB and perform a basic put/get operation
// Test Result:
//     The transaction should commit successfully and the value should be retrievable
//
TEST_F(RocksDBTransactionTest, OptimisticTransactionBasicTest) {
    // Open an OptimisticTransactionDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::OptimisticTransactionDB* txn_db;
    rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(options, test_db_path_.string(), &txn_db);
    ASSERT_TRUE(status.ok()) << "Failed to open OptimisticTransactionDB: " << status.ToString();

    // Create a transaction
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    rocksdb::OptimisticTransactionOptions txn_options;

    rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn, nullptr);

    // Put a key-value pair in the transaction
    status = txn->Put("key1", "value1");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();

    // Commit the transaction
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "Failed to commit transaction: " << status.ToString();

    // Clean up the transaction
    delete txn;

    // Get the value from the database
    std::string value;
    status = txn_db->Get(read_options, "key1", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value: " << status.ToString();
    ASSERT_EQ("value1", value);

    // Clean up the database
    delete txn_db;
}

//
// Test Setup:
//     Open an OptimisticTransactionDB and test for write conflicts
// Test Result:
//     The second transaction should fail with a conflict error
//
TEST_F(RocksDBTransactionTest, OptimisticTransactionConflictTest) {
    // Open an OptimisticTransactionDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::OptimisticTransactionDB* txn_db;
    rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(options, test_db_path_.string(), &txn_db);
    ASSERT_TRUE(status.ok()) << "Failed to open OptimisticTransactionDB: " << status.ToString();

    // Create the first transaction
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    rocksdb::OptimisticTransactionOptions txn_options;

    rocksdb::Transaction* txn1 = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn1, nullptr);

    // Put a key-value pair in the first transaction
    status = txn1->Put("conflict_key", "value_from_txn1");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair in txn1: " << status.ToString();

    // Create a second transaction
    rocksdb::Transaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn2, nullptr);

    // Put a key-value pair with the same key in the second transaction
    status = txn2->Put("conflict_key", "value_from_txn2");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair in txn2: " << status.ToString();

    // Commit the first transaction
    status = txn1->Commit();
    ASSERT_TRUE(status.ok()) << "Failed to commit txn1: " << status.ToString();

    // Try to commit the second transaction, which should fail
    status = txn2->Commit();
    ASSERT_TRUE(status.IsTimedOut() || status.IsBusy()) << "Expected conflict error, got: " << status.ToString();

    // Clean up the transactions
    delete txn1;
    delete txn2;

    // Verify the value from the first transaction
    std::string value;
    status = txn_db->Get(read_options, "conflict_key", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value: " << status.ToString();
    ASSERT_EQ("value_from_txn1", value);

    // Clean up the database
    delete txn_db;
}

//
// Test Setup:
//     Open a PessimisticTransactionDB and perform a basic put/get operation
// Test Result:
//     The transaction should commit successfully and the value should be retrievable
//
TEST_F(RocksDBTransactionTest, PessimisticTransactionBasicTest) {
    // Options for the TransactionDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::TransactionDBOptions txn_db_options;

    // Open a TransactionDB (pessimistic)
    rocksdb::TransactionDB* txn_db;
    rocksdb::Status status = rocksdb::TransactionDB::Open(options, txn_db_options, test_db_path_.string(), &txn_db);
    ASSERT_TRUE(status.ok()) << "Failed to open TransactionDB: " << status.ToString();

    // Create a transaction
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    rocksdb::TransactionOptions txn_options;

    rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn, nullptr);

    // Put a key-value pair in the transaction
    status = txn->Put("key1", "value1");
    ASSERT_TRUE(status.ok()) << "Failed to put key-value pair: " << status.ToString();

    // Commit the transaction
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "Failed to commit transaction: " << status.ToString();

    // Clean up the transaction
    delete txn;

    // Get the value from the database
    std::string value;
    status = txn_db->Get(read_options, "key1", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value: " << status.ToString();
    ASSERT_EQ("value1", value);

    // Clean up the database
    delete txn_db;
}

//
// Test Setup:
//     Open a PessimisticTransactionDB and test for locking behavior
// Test Result:
//     The second transaction should be blocked until the first one completes
//
TEST_F(RocksDBTransactionTest, PessimisticTransactionLockingTest) {
    // Options for the TransactionDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::TransactionDBOptions txn_db_options;

    // Open a TransactionDB (pessimistic)
    rocksdb::TransactionDB* txn_db;
    rocksdb::Status status = rocksdb::TransactionDB::Open(options, txn_db_options, test_db_path_.string(), &txn_db);
    ASSERT_TRUE(status.ok()) << "Failed to open TransactionDB: " << status.ToString();

    // Set up initial data
    rocksdb::WriteOptions write_options;
    status = txn_db->Put(write_options, "locked_key", "initial_value");
    ASSERT_TRUE(status.ok()) << "Failed to put initial value: " << status.ToString();

    // Create the first transaction
    rocksdb::ReadOptions read_options;
    rocksdb::TransactionOptions txn_options;
    txn_options.deadlock_detect = true;

    // Create flag for synchronization between threads
    std::atomic<bool> txn1_got_lock(false);
    std::atomic<bool> txn2_completed(false);

    // Start the first transaction in a separate thread
    std::thread thread1([&]() {
        rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);

        // Get exclusive lock on the key
        std::string value_str;
        rocksdb::Status get_for_update_status = txn->GetForUpdate(read_options, "locked_key", &value_str);
        ASSERT_TRUE(get_for_update_status.ok()) << "Failed to lock key in txn1: " << get_for_update_status.ToString();

        // Signal that we have the lock
        txn1_got_lock.store(true);

        // Sleep to simulate work and ensure txn2 tries to access the locked key
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Update the value
        rocksdb::Status put_status = txn->Put("locked_key", "value_from_txn1");
        ASSERT_TRUE(put_status.ok()) << "Failed to put value in txn1: " << put_status.ToString();

        // Commit the transaction
        rocksdb::Status commit_status = txn->Commit();
        ASSERT_TRUE(commit_status.ok()) << "Failed to commit txn1: " << commit_status.ToString();

        // Clean up
        delete txn;
    });

    // Wait for txn1 to acquire the lock
    while (!txn1_got_lock.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Start the second transaction in a separate thread
    std::thread thread2([&]() {
        rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);

        // Try to lock the same key (this should block until txn1 commits)
        std::string value_str;
        rocksdb::Status get_for_update_status = txn->GetForUpdate(read_options, "locked_key", &value_str);
        ASSERT_TRUE(get_for_update_status.ok()) << "Failed to lock key in txn2: " << get_for_update_status.ToString();

        // Update the value
        rocksdb::Status put_status = txn->Put("locked_key", "value_from_txn2");
        ASSERT_TRUE(put_status.ok()) << "Failed to put value in txn2: " << put_status.ToString();

        // Commit the transaction
        rocksdb::Status commit_status = txn->Commit();
        ASSERT_TRUE(commit_status.ok()) << "Failed to commit txn2: " << commit_status.ToString();

        // Signal completion
        txn2_completed.store(true);

        // Clean up
        delete txn;
    });

    // Wait for threads to complete
    thread1.join();
    thread2.join();

    // Verify the final value (should be from txn2)
    std::string value;
    status = txn_db->Get(read_options, "locked_key", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value: " << status.ToString();
    ASSERT_EQ("value_from_txn2", value);
    ASSERT_TRUE(txn2_completed.load()) << "Transaction 2 did not complete";

    // Clean up the database
    delete txn_db;
}

//
// Test Setup:
//     Open a PessimisticTransactionDB and test transactions with multiple operations
// Test Result:
//     The transaction should commit all operations atomically
//
TEST_F(RocksDBTransactionTest, PessimisticTransactionAtomicUpdateTest) {
    // Options for the TransactionDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::TransactionDBOptions txn_db_options;

    // Open a TransactionDB (pessimistic)
    rocksdb::TransactionDB* txn_db;
    rocksdb::Status status = rocksdb::TransactionDB::Open(options, txn_db_options, test_db_path_.string(), &txn_db);
    ASSERT_TRUE(status.ok()) << "Failed to open TransactionDB: " << status.ToString();

    // Create a transaction
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    rocksdb::TransactionOptions txn_options;

    rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn, nullptr);

    // Put multiple key-value pairs in the transaction
    std::vector<std::string> keys = {"multi_key1", "multi_key2", "multi_key3"};
    std::vector<std::string> values = {"multi_value1", "multi_value2", "multi_value3"};

    for (size_t i = 0; i < keys.size(); i++) {
        status = txn->Put(keys[i], values[i]);
        ASSERT_TRUE(status.ok()) << "Failed to put key-value pair " << i << ": " << status.ToString();
    }

    // Commit the transaction
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "Failed to commit transaction: " << status.ToString();

    // Clean up the transaction
    delete txn;

    // Get the values from the database
    for (size_t i = 0; i < keys.size(); i++) {
        std::string value;
        status = txn_db->Get(read_options, keys[i], &value);
        ASSERT_TRUE(status.ok()) << "Failed to get value for key " << keys[i] << ": " << status.ToString();
        ASSERT_EQ(values[i], value);
    }

    // Create another transaction
    txn = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn, nullptr);

    // Delete one key, update another, and add a new one
    status = txn->Delete(keys[0]);
    ASSERT_TRUE(status.ok()) << "Failed to delete key: " << status.ToString();

    status = txn->Put(keys[1], "updated_" + values[1]);
    ASSERT_TRUE(status.ok()) << "Failed to update key: " << status.ToString();

    status = txn->Put("multi_key4", "multi_value4");
    ASSERT_TRUE(status.ok()) << "Failed to add new key: " << status.ToString();

    // Commit the transaction
    status = txn->Commit();
    ASSERT_TRUE(status.ok()) << "Failed to commit transaction: " << status.ToString();

    // Clean up the transaction
    delete txn;

    // Verify the changes
    std::string value;

    // First key should be deleted
    status = txn_db->Get(read_options, keys[0], &value);
    ASSERT_TRUE(status.IsNotFound()) << "Key should be deleted: " << keys[0];

    // Second key should be updated
    status = txn_db->Get(read_options, keys[1], &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value for key " << keys[1] << ": " << status.ToString();
    ASSERT_EQ("updated_" + values[1], value);

    // Third key should be unchanged
    status = txn_db->Get(read_options, keys[2], &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value for key " << keys[2] << ": " << status.ToString();
    ASSERT_EQ(values[2], value);

    // Fourth key should be added
    status = txn_db->Get(read_options, "multi_key4", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value for key multi_key4: " << status.ToString();
    ASSERT_EQ("multi_value4", value);

    // Clean up the database
    delete txn_db;
}

//
// Test Setup:
//     Test rollback functionality in a pessimistic transaction
// Test Result:
//     Changes should be discarded when a transaction is rolled back
//
TEST_F(RocksDBTransactionTest, PessimisticTransactionRollbackTest) {
    // Options for the TransactionDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::TransactionDBOptions txn_db_options;

    // Open a TransactionDB (pessimistic)
    rocksdb::TransactionDB* txn_db;
    rocksdb::Status status = rocksdb::TransactionDB::Open(options, txn_db_options, test_db_path_.string(), &txn_db);
    ASSERT_TRUE(status.ok()) << "Failed to open TransactionDB: " << status.ToString();

    // Set up initial data
    rocksdb::WriteOptions write_options;
    status = txn_db->Put(write_options, "rollback_key", "initial_value");
    ASSERT_TRUE(status.ok()) << "Failed to put initial value: " << status.ToString();

    // Create a transaction
    rocksdb::ReadOptions read_options;
    rocksdb::TransactionOptions txn_options;

    rocksdb::Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
    ASSERT_NE(txn, nullptr);

    // Modify the key
    status = txn->Put("rollback_key", "modified_value");
    ASSERT_TRUE(status.ok()) << "Failed to put modified value: " << status.ToString();

    // Add a new key
    status = txn->Put("new_rollback_key", "new_value");
    ASSERT_TRUE(status.ok()) << "Failed to put new key-value pair: " << status.ToString();

    // Verify the changes are visible within the transaction
    std::string value;
    status = txn->Get(read_options, "rollback_key", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value within transaction: " << status.ToString();
    ASSERT_EQ("modified_value", value);

    status = txn->Get(read_options, "new_rollback_key", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get new value within transaction: " << status.ToString();
    ASSERT_EQ("new_value", value);

    // Rollback the transaction instead of committing
    txn->Rollback();

    // Clean up the transaction
    delete txn;

    // Verify the original value is preserved
    status = txn_db->Get(read_options, "rollback_key", &value);
    ASSERT_TRUE(status.ok()) << "Failed to get value after rollback: " << status.ToString();
    ASSERT_EQ("initial_value", value);

    // Verify the new key was not added
    status = txn_db->Get(read_options, "new_rollback_key", &value);
    ASSERT_TRUE(status.IsNotFound()) << "New key should not exist after rollback";

    // Clean up the database
    delete txn_db;
}

}  // namespace pond::test