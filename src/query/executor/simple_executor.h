#pragma once

#include <memory>

#include "catalog/catalog.h"
#include "common/append_only_fs.h"
#include "query/data/data_accessor.h"
#include "query/executor/executor.h"

namespace pond::query {

/**
 * @brief A simple implementation of the Executor interface
 *
 * This implementation supports basic SELECT * FROM table queries
 * by executing sequential scan operations on tables.
 */
class SimpleExecutor : public Executor {
public:
    /**
     * @brief Create a new SimpleExecutor
     * @param catalog The catalog to use for metadata
     * @param data_accessor The data accessor to use for reading data
     */
    SimpleExecutor(std::shared_ptr<catalog::Catalog> catalog, std::shared_ptr<DataAccessor> data_accessor);

    ~SimpleExecutor() override = default;

    // Execute a physical plan
    common::Result<DataBatchSharedPtr> execute(std::shared_ptr<PhysicalPlanNode> plan) override;

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
    common::Result<DataBatchSharedPtr> CurrentBatch() const override;

protected:
    // Helper methods for execution
    common::Result<DataBatchSharedPtr> ExecuteChildren(PhysicalPlanNode& node) override;
    common::Result<bool> ProduceResults(const common::Schema& schema) override;

private:
    std::shared_ptr<catalog::Catalog> catalog_;
    std::shared_ptr<DataAccessor> data_accessor_;
    DataBatchSharedPtr current_batch_;                   // Current result batch
    common::Result<DataBatchSharedPtr> current_result_;  // Current result status
};

}  // namespace pond::query