#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "common/expression.h"
#include "common/schema.h"
#include "common/uuid.h"
#include "logical_plan_node.h"

namespace pond::query {

// Forward declarations
class PhysicalPlanVisitor;
class Executor;

enum class PhysicalNodeType {
    Invalid,
    SequentialScan,
    IndexScan,
    Filter,
    Projection,
    HashJoin,
    NestedLoopJoin,
    HashAggregate,
    SortAggregate,
    Sort,
    Limit,
    ShuffleExchange
};

using ArrowDataBatchSharedPtr = std::shared_ptr<arrow::RecordBatch>;

/**
 * @brief Base class for all physical plan nodes
 * Physical plan nodes represent the actual algorithms and access methods
 * that will be used to execute the query.
 */
class PhysicalPlanNode {
public:
    PhysicalPlanNode(PhysicalNodeType type) : type_(type) {
        // Generate a UUID for the node
        node_id_ = common::UUID::NewUUID();
    }
    virtual ~PhysicalPlanNode() = default;

    PhysicalNodeType Type() const { return type_; }
    const common::UUID& NodeId() const { return node_id_; }
    std::string NodeIdString() const { return node_id_.ToString(); }
    virtual void Accept(PhysicalPlanVisitor& visitor) = 0;
    virtual const common::Schema& OutputSchema() const = 0;

    // Child nodes
    const std::vector<std::shared_ptr<PhysicalPlanNode>>& Children() const { return children_; }
    void AddChild(std::shared_ptr<PhysicalPlanNode> child) { children_.push_back(std::move(child)); }

    // Cast to a specific node type
    template <typename T>
    T* as() {
        return static_cast<T*>(this);
    }

    // Execute this node using the provided executor
    virtual common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) = 0;

protected:
    PhysicalNodeType type_;
    common::UUID node_id_;  // Unique identifier for this node
    std::vector<std::shared_ptr<PhysicalPlanNode>> children_;
};

/**
 * @brief Node for sequential scan operations
 */
class PhysicalSequentialScanNode : public PhysicalPlanNode {
public:
    PhysicalSequentialScanNode(const std::string& table_name,
                               const common::Schema& schema,
                               std::shared_ptr<common::Expression> predicate = nullptr,
                               std::optional<common::Schema> projection_schema = std::nullopt,
                               std::optional<size_t> limit = std::nullopt,
                               std::optional<size_t> offset = std::nullopt)
        : PhysicalPlanNode(PhysicalNodeType::SequentialScan),
          table_name_(table_name),
          schema_(schema),
          predicate_(std::move(predicate)),
          projection_schema_(projection_schema),
          limit_(limit),
          offset_(offset) {}

    const std::string& TableName() const { return table_name_; }
    const common::Schema& OutputSchema() const override {
        if (projection_schema_) {
            return *projection_schema_;
        }

        return schema_;
    }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

    const std::shared_ptr<common::Expression>& Predicate() const { return predicate_; }
    const std::optional<common::Schema>& ProjectionSchema() const { return projection_schema_; }

    std::optional<size_t> Limit() const { return limit_; }
    std::optional<size_t> Offset() const { return offset_; }

private:
    std::string table_name_;
    common::Schema schema_;
    std::shared_ptr<common::Expression> predicate_;
    std::optional<common::Schema> projection_schema_;
    std::optional<size_t> limit_;
    std::optional<size_t> offset_;
};

/**
 * @brief Node for index scan operations
 */
class PhysicalIndexScanNode : public PhysicalPlanNode {
public:
    PhysicalIndexScanNode(std::string table_name, std::string index_name, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::IndexScan),
          table_name_(std::move(table_name)),
          index_name_(std::move(index_name)),
          schema_(std::move(schema)) {}

    const std::string& TableName() const { return table_name_; }
    const std::string& IndexName() const { return index_name_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::string table_name_;
    std::string index_name_;
    common::Schema schema_;
};

/**
 * @brief Node for filter operations
 */
class PhysicalFilterNode : public PhysicalPlanNode {
public:
    PhysicalFilterNode(std::shared_ptr<common::Expression> predicate, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::Filter), predicate_(std::move(predicate)), schema_(std::move(schema)) {}

    const std::shared_ptr<common::Expression>& Predicate() const { return predicate_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::shared_ptr<common::Expression> predicate_;
    common::Schema schema_;
};

/**
 * @brief Node for projection operations
 */
class PhysicalProjectionNode : public PhysicalPlanNode {
public:
    PhysicalProjectionNode(std::vector<std::shared_ptr<common::Expression>> projections, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::Projection),
          projections_(std::move(projections)),
          schema_(std::move(schema)) {}

    const std::vector<std::shared_ptr<common::Expression>>& Projections() const { return projections_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::vector<std::shared_ptr<common::Expression>> projections_;
    common::Schema schema_;
};

/**
 * @brief Node for hash join operations
 */
class PhysicalHashJoinNode : public PhysicalPlanNode {
public:
    PhysicalHashJoinNode(std::shared_ptr<common::Expression> condition, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::HashJoin), condition_(std::move(condition)), schema_(std::move(schema)) {}

    const std::shared_ptr<common::Expression>& Condition() const { return condition_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::shared_ptr<common::Expression> condition_;
    common::Schema schema_;
};

/**
 * @brief Node for nested loop join operations
 */
class PhysicalNestedLoopJoinNode : public PhysicalPlanNode {
public:
    PhysicalNestedLoopJoinNode(std::shared_ptr<common::Expression> condition, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::NestedLoopJoin),
          condition_(std::move(condition)),
          schema_(std::move(schema)) {}

    const std::shared_ptr<common::Expression>& Condition() const { return condition_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::shared_ptr<common::Expression> condition_;
    common::Schema schema_;
};

/**
 * @brief Node for hash-based aggregate operations
 */
class PhysicalHashAggregateNode : public PhysicalPlanNode {
public:
    PhysicalHashAggregateNode(std::vector<std::shared_ptr<common::Expression>> group_by,
                              std::vector<std::shared_ptr<common::Expression>> aggregates,
                              common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::HashAggregate),
          group_by_(std::move(group_by)),
          aggregates_(std::move(aggregates)),
          schema_(std::move(schema)) {}

    const std::vector<std::shared_ptr<common::Expression>>& GroupBy() const { return group_by_; }
    const std::vector<std::shared_ptr<common::Expression>>& Aggregates() const { return aggregates_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::vector<std::shared_ptr<common::Expression>> group_by_;
    std::vector<std::shared_ptr<common::Expression>> aggregates_;
    common::Schema schema_;
};

/**
 * @brief Node for sort operations
 */
class PhysicalSortNode : public PhysicalPlanNode {
public:
    PhysicalSortNode(std::vector<SortSpec> sort_specs, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::Sort), sort_specs_(std::move(sort_specs)), schema_(std::move(schema)) {}

    const std::vector<SortSpec>& sortSpecs() const { return sort_specs_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::vector<SortSpec> sort_specs_;
    common::Schema schema_;
};

/**
 * @brief Node for limit operations
 */
class PhysicalLimitNode : public PhysicalPlanNode {
public:
    PhysicalLimitNode(size_t limit, size_t offset, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::Limit), limit_(limit), offset_(offset), schema_(std::move(schema)) {}

    size_t Limit() const { return limit_; }
    size_t Offset() const { return offset_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    size_t limit_;
    size_t offset_;
    common::Schema schema_;
};

/**
 * @brief Node for shuffle exchange operations
 */
class PhysicalShuffleExchangeNode : public PhysicalPlanNode {
public:
    PhysicalShuffleExchangeNode(std::vector<std::shared_ptr<common::Expression>> partition_by, common::Schema schema)
        : PhysicalPlanNode(PhysicalNodeType::ShuffleExchange),
          partition_by_(std::move(partition_by)),
          schema_(std::move(schema)) {}

    const std::vector<std::shared_ptr<common::Expression>>& PartitionBy() const { return partition_by_; }
    const common::Schema& OutputSchema() const override { return schema_; }
    void Accept(PhysicalPlanVisitor& visitor) override;
    common::Result<ArrowDataBatchSharedPtr> Execute(Executor& executor) override;

private:
    std::vector<std::shared_ptr<common::Expression>> partition_by_;
    common::Schema schema_;
};

/**
 * @brief Visitor interface for physical plan nodes
 */
class PhysicalPlanVisitor {
public:
    virtual ~PhysicalPlanVisitor() = default;
    virtual void Visit(PhysicalSequentialScanNode& node) = 0;
    virtual void Visit(PhysicalIndexScanNode& node) = 0;
    virtual void Visit(PhysicalFilterNode& node) = 0;
    virtual void Visit(PhysicalProjectionNode& node) = 0;
    virtual void Visit(PhysicalHashJoinNode& node) = 0;
    virtual void Visit(PhysicalNestedLoopJoinNode& node) = 0;
    virtual void Visit(PhysicalHashAggregateNode& node) = 0;
    virtual void Visit(PhysicalSortNode& node) = 0;
    virtual void Visit(PhysicalLimitNode& node) = 0;
    virtual void Visit(PhysicalShuffleExchangeNode& node) = 0;
};

std::string GetPhysicalPlanUserFriendlyString(PhysicalPlanNode& root);

}  // namespace pond::query