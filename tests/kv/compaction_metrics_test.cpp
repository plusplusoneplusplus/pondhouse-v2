#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "kv/memtable.h"
#include "kv/record.h"
#include "kv/sstable_manager.h"
#include "test_helper.h"

using namespace pond::common;

namespace pond::kv {

class CompactionMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();
        schema_ = std::make_shared<Schema>(std::vector<ColumnSchema>{
            {"value", ColumnType::STRING},
        });
        metadata_state_machine_ = std::make_shared<TableMetadataStateMachine>(fs_, "test_db_metadata");
        VERIFY_RESULT(metadata_state_machine_->Open());
        manager_ = std::make_unique<SSTableManager>(fs_, "test_db", metadata_state_machine_);
    }

    // Helper to create a MemTable with test data
    std::unique_ptr<MemTable> CreateTestMemTable(size_t start_key, size_t num_entries) {
        auto memtable = std::make_unique<MemTable>();
        for (size_t i = 0; i < num_entries; i++) {
            std::string key = pond::test::GenerateKey(start_key + i);
            std::string value = "value" + std::to_string(start_key + i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, value);
            EXPECT_TRUE(memtable->Put(key, record->Serialize(), 0 /* txn_id */).ok());
        }
        return memtable;
    }

    DataChunk FromRecord(const Record& record) { return record.Serialize(); }

    std::unique_ptr<Record> ToRecord(const DataChunk& data) {
        auto result = Record::Deserialize(data, schema_);
        EXPECT_TRUE(result.ok());
        return std::move(result).value();
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
    std::unique_ptr<SSTableManager> manager_;
    std::shared_ptr<Schema> schema_;
    std::shared_ptr<TableMetadataStateMachine> metadata_state_machine_;
};

TEST_F(CompactionMetricsTest, BasicMetricsTracking) {
    // Create some L0 files
    auto memtable1 = CreateTestMemTable(0, 100);
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable1));

    auto memtable2 = CreateTestMemTable(100, 100);
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable2));

    // Get initial metrics
    const auto& initial_metrics = manager_->GetCompactionMetrics();
    EXPECT_EQ(initial_metrics.total_compactions_, 0);
    EXPECT_EQ(initial_metrics.compaction_queue_length_, 0);

    // Perform L0 to L1 merge
    VERIFY_RESULT(manager_->MergeL0ToL1());

    // Verify metrics after compaction
    const auto& metrics = manager_->GetCompactionMetrics();
    EXPECT_EQ(metrics.total_compactions_, 1);
    EXPECT_EQ(metrics.failed_compactions_, 0);
    EXPECT_GT(metrics.total_bytes_compacted_, 0);
    EXPECT_EQ(metrics.compaction_queue_length_, 0);
    EXPECT_GT(metrics.total_bytes_written_, 0);
    EXPECT_GT(metrics.total_bytes_read_, 0);
}

TEST_F(CompactionMetricsTest, CompactionPhases) {
    // Create L0 files
    auto memtable = CreateTestMemTable(0, 100);
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));

    // Reset metrics before test
    manager_->ResetCompactionMetrics();

    // Start compaction and track phases
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Get metrics
    const auto& metrics = manager_->GetCompactionMetrics();
    const auto& job = metrics.active_compactions_.begin()->second;

    // Verify all phases were recorded
    EXPECT_GT(job.phase.GetPhaseDuration(CompactionPhase::Phase::FileSelection), TimeInterval(0));
    EXPECT_GT(job.phase.GetPhaseDuration(CompactionPhase::Phase::Reading), TimeInterval(0));
    EXPECT_GT(job.phase.GetPhaseDuration(CompactionPhase::Phase::Merging), TimeInterval(0));
    EXPECT_GT(job.phase.GetPhaseDuration(CompactionPhase::Phase::Writing), TimeInterval(0));
    EXPECT_EQ(job.phase.GetCurrentPhase(), CompactionPhase::Phase::Complete);
}

TEST_F(CompactionMetricsTest, WriteAmplification) {
    // Create multiple L0 files with overlapping ranges
    auto memtable1 = CreateTestMemTable(0, 100);
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable1));

    auto memtable2 = CreateTestMemTable(50, 100);  // Overlaps with first file
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable2));

    // Reset metrics
    manager_->ResetCompactionMetrics();

    // Perform merge
    VERIFY_RESULT(manager_->MergeL0ToL1());

    // Verify write amplification
    const auto& metrics = manager_->GetCompactionMetrics();
    double write_amp = metrics.GetWriteAmplification();
    EXPECT_GT(write_amp, 0.0);
    EXPECT_LT(write_amp, 3.0);  // Should be reasonable for this test case
}

TEST_F(CompactionMetricsTest, FailedCompaction) {
    // Create an invalid state that will cause compaction to fail
    // (empty L0, so merge will fail)

    // Reset metrics
    manager_->ResetCompactionMetrics();

    // Attempt merge (should fail)
    auto result = manager_->MergeL0ToL1();
    EXPECT_FALSE(result.ok());

    // Verify failure metrics
    const auto& metrics = manager_->GetCompactionMetrics();
    EXPECT_EQ(metrics.total_compactions_, 0);
    EXPECT_EQ(metrics.failed_compactions_, 1);
    EXPECT_EQ(metrics.error_count_, 1);
    EXPECT_EQ(metrics.compaction_queue_length_, 0);
}

TEST_F(CompactionMetricsTest, ResourceUsage) {
    // Create some L0 files
    auto memtable = CreateTestMemTable(0, 1000);  // Larger dataset
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));

    // Reset metrics
    manager_->ResetCompactionMetrics();

    // Perform merge
    VERIFY_RESULT(manager_->MergeL0ToL1());

    // Verify resource metrics
    const auto& metrics = manager_->GetCompactionMetrics();
    EXPECT_GT(metrics.disk_read_bytes_, 0);
    EXPECT_GT(metrics.disk_write_bytes_, 0);
    EXPECT_GT(metrics.current_memory_usage_, 0);
    EXPECT_GT(metrics.peak_memory_usage_, 0);
}

}  // namespace pond::kv