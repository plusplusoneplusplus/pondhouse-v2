#pragma once

#include <memory>

#include "catalog/catalog.h"
#include "common/append_only_fs.h"
#include "query/data/data_accessor.h"
#include "query/executor/executor.h"

namespace pond::query {

/**
 * @brief A batch iterator implementation for MaterializedExecutor
 *
 * This iterator is simple - it returns all data in a single batch
 * and then returns nullptr for subsequent calls.
 */
class MaterializedBatchIterator : public BatchIterator {
public:
    explicit MaterializedBatchIterator(ArrowDataBatchSharedPtr batch) : batch_(std::move(batch)), consumed_(false) {}

    ~MaterializedBatchIterator() override = default;

    /**
     * @brief Pull the next batch of data
     *
     * @return common::Result<ArrowDataBatchSharedPtr> Next batch or end of data (nullptr) if no more data
     */
    common::Result<ArrowDataBatchSharedPtr> Next() override {
        if (consumed_) {
            return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
        }
        consumed_ = true;
        return common::Result<ArrowDataBatchSharedPtr>::success(batch_);
    }

    /**
     * @brief Check if more data is available without consuming it
     *
     * @return bool True if more data is available, false otherwise
     */
    bool HasNext() const override { return !consumed_; }

private:
    ArrowDataBatchSharedPtr batch_;
    bool consumed_;
};

/**
 * @brief A simple implementation of the Executor interface
 *
 * This implementation supports basic SELECT * FROM table queries
 * by executing sequential scan operations on tables.
 */
class MaterializedExecutor : public Executor, public PhysicalPlanVisitor {
public:
    /**
     * @brief Create a new MaterializedExecutor
     * @param catalog The catalog to use for metadata
     * @param data_accessor The data accessor to use for reading data
     */
    MaterializedExecutor(std::shared_ptr<catalog::Catalog> catalog, std::shared_ptr<DataAccessor> data_accessor);

    ~MaterializedExecutor() override = default;

    // Execute a physical plan
    common::Result<std::unique_ptr<BatchIterator>> Execute(std::shared_ptr<PhysicalPlanNode> plan) override;

    // Visitor methods for different physical operators
    void Visit(PhysicalSequentialScanNode& node) override;
    void Visit(PhysicalIndexScanNode& node) override;
    void Visit(PhysicalFilterNode& node) override;
    void Visit(PhysicalProjectionNode& node) override;
    void Visit(PhysicalHashJoinNode& node) override;
    void Visit(PhysicalNestedLoopJoinNode& node) override;
    void Visit(PhysicalHashAggregateNode& node) override;
    void Visit(PhysicalSortNode& node) override;
    void Visit(PhysicalLimitNode& node) override;
    void Visit(PhysicalShuffleExchangeNode& node) override;

    // Get the current batch
    common::Result<ArrowDataBatchSharedPtr> CurrentBatch() const;

protected:
    // Helper methods for execution
    common::Result<ArrowDataBatchSharedPtr> ExecuteChildren(PhysicalPlanNode& node);
    common::Result<bool> ProduceResults(const common::Schema& schema);

private:
    std::shared_ptr<catalog::Catalog> catalog_;
    std::shared_ptr<DataAccessor> data_accessor_;
    ArrowDataBatchSharedPtr current_batch_;                   // Current result batch
    common::Result<ArrowDataBatchSharedPtr> current_result_;  // Current result status
};

}  // namespace pond::query