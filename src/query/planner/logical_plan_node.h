#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/expression.h"
#include "common/schema.h"

using namespace pond::common;

namespace pond::query {

// Forward declarations
class LogicalPlanVisitor;

enum class LogicalNodeType { Invalid, Scan, Filter, Projection, Join, Aggregate, Sort, Limit };

// Base class for all logical plan nodes
class LogicalPlanNode {
public:
    LogicalPlanNode(LogicalNodeType type) : type_(type) {}
    virtual ~LogicalPlanNode() = default;
    LogicalNodeType Type() const { return type_; }
    virtual void Accept(LogicalPlanVisitor& visitor) = 0;
    virtual const common::Schema& OutputSchema() const = 0;

    // Child nodes
    const std::vector<std::shared_ptr<LogicalPlanNode>>& Children() const { return children_; }
    void AddChild(std::shared_ptr<LogicalPlanNode> child) { children_.push_back(std::move(child)); }

    // Cast to a specific node type
    template <typename T>
    T* as() {
        return static_cast<T*>(this);
    }

    virtual bool IsValid() const {
        for (const auto& child : children_) {
            if (!child->IsValid()) {
                return false;
            }
        }
        return true;
    }

    void ReplaceChild(size_t index, std::shared_ptr<LogicalPlanNode> new_child) {
        if (index >= children_.size()) {
            throw std::runtime_error("Index out of bounds");
        }
        children_[index] = std::move(new_child);
    }

protected:
    LogicalNodeType type_;
    std::vector<std::shared_ptr<LogicalPlanNode>> children_;
};

// Sort direction
enum class SortDirection { Ascending, Descending };

// Sort specification for a column
struct SortSpec {
    std::shared_ptr<Expression> expr;
    SortDirection direction;

    std::string ToString() const {
        return expr->ToString() + (direction == SortDirection::Ascending ? " ASC" : " DESC");
    }
};

// Scan node represents table scan operation
class LogicalScanNode : public LogicalPlanNode {
public:
    LogicalScanNode(std::string table_name, common::Schema schema)
        : LogicalPlanNode(LogicalNodeType::Scan), table_name_(std::move(table_name)), schema_(std::move(schema)) {}

    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return schema_; }
    const std::string& TableName() const { return table_name_; }
    bool IsValid() const override { return children_.size() < 2 && LogicalPlanNode::IsValid(); }

private:
    std::string table_name_;
    common::Schema schema_;
};

// Join node represents JOIN operations
class LogicalJoinNode : public LogicalPlanNode {
public:
    LogicalJoinNode(std::shared_ptr<LogicalPlanNode> left,
                    std::shared_ptr<LogicalPlanNode> right,
                    JoinType join_type,
                    std::shared_ptr<Expression> condition,
                    common::Schema output_schema)
        : LogicalPlanNode(LogicalNodeType::Join),
          join_type_(join_type),
          condition_(std::move(condition)),
          output_schema_(std::move(output_schema)) {
        AddChild(std::move(left));
        AddChild(std::move(right));
    }

    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return output_schema_; }
    JoinType GetJoinType() const { return join_type_; }
    const std::shared_ptr<Expression>& GetCondition() const { return condition_; }
    bool IsValid() const override { return children_.size() == 2 && condition_ && LogicalPlanNode::IsValid(); }

private:
    JoinType join_type_;
    std::shared_ptr<Expression> condition_;
    common::Schema output_schema_;
};

// Filter node represents WHERE clause
class LogicalFilterNode : public LogicalPlanNode {
public:
    LogicalFilterNode(std::shared_ptr<LogicalPlanNode> input, std::shared_ptr<Expression> condition)
        : LogicalPlanNode(LogicalNodeType::Filter), condition_(std::move(condition)) {
        AddChild(std::move(input));
    }

    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return children_[0]->OutputSchema(); }
    const std::shared_ptr<Expression>& GetCondition() const { return condition_; }
    bool IsValid() const override { return children_.size() == 1 && LogicalPlanNode::IsValid(); }

private:
    std::shared_ptr<Expression> condition_;
};

// Projection node represents SELECT clause
class LogicalProjectionNode : public LogicalPlanNode {
public:
    LogicalProjectionNode(std::shared_ptr<LogicalPlanNode> input,
                          common::Schema output_schema,
                          std::vector<std::shared_ptr<Expression>> expressions,
                          std::string table_name)
        : LogicalPlanNode(LogicalNodeType::Projection),
          output_schema_(std::move(output_schema)),
          expressions_(std::move(expressions)),
          table_name_(std::move(table_name)) {
        AddChild(std::move(input));
    }

    const std::string& TableName() const { return table_name_; }
    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return output_schema_; }
    const std::vector<std::shared_ptr<Expression>>& GetExpressions() const { return expressions_; }
    bool IsValid() const override { return children_.size() == 1 && LogicalPlanNode::IsValid(); }

private:
    common::Schema output_schema_;
    std::vector<std::shared_ptr<Expression>> expressions_;
    std::string table_name_;
};

// Aggregate node represents GROUP BY and aggregate functions
class LogicalAggregateNode : public LogicalPlanNode {
public:
    LogicalAggregateNode(std::shared_ptr<LogicalPlanNode> input,
                         std::vector<std::shared_ptr<Expression>> group_by,
                         std::vector<std::shared_ptr<Expression>> aggregates,
                         common::Schema output_schema)
        : LogicalPlanNode(LogicalNodeType::Aggregate),
          group_by_(std::move(group_by)),
          aggregates_(std::move(aggregates)),
          output_schema_(std::move(output_schema)) {
        AddChild(std::move(input));
    }

    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return output_schema_; }
    const std::vector<std::shared_ptr<Expression>>& GetGroupBy() const { return group_by_; }
    const std::vector<std::shared_ptr<Expression>>& GetAggregates() const { return aggregates_; }
    bool IsValid() const override { return children_.size() == 1 && LogicalPlanNode::IsValid(); }

private:
    std::vector<std::shared_ptr<Expression>> group_by_;
    std::vector<std::shared_ptr<Expression>> aggregates_;
    common::Schema output_schema_;
};

// Sort node represents ORDER BY clause
class LogicalSortNode : public LogicalPlanNode {
public:
    LogicalSortNode(std::shared_ptr<LogicalPlanNode> input, std::vector<SortSpec> sort_specs)
        : LogicalPlanNode(LogicalNodeType::Sort), sort_specs_(std::move(sort_specs)) {
        AddChild(std::move(input));
    }

    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return children_[0]->OutputSchema(); }
    const std::vector<SortSpec>& GetSortSpecs() const { return sort_specs_; }
    bool IsValid() const override { return children_.size() == 1 && LogicalPlanNode::IsValid(); }

private:
    std::vector<SortSpec> sort_specs_;
};

// Limit node represents LIMIT clause
class LogicalLimitNode : public LogicalPlanNode {
public:
    LogicalLimitNode(std::shared_ptr<LogicalPlanNode> input, int64_t limit, int64_t offset = 0)
        : LogicalPlanNode(LogicalNodeType::Limit), limit_(limit), offset_(offset) {
        AddChild(std::move(input));
    }

    void Accept(LogicalPlanVisitor& visitor) override;
    const common::Schema& OutputSchema() const override { return children_[0]->OutputSchema(); }
    int64_t Limit() const { return limit_; }
    int64_t Offset() const { return offset_; }
    bool IsValid() const override {
        return limit_ >= 0 && offset_ >= 0 && children_.size() == 1 && LogicalPlanNode::IsValid();
    }

private:
    int64_t limit_;
    int64_t offset_;
};

// Visitor interface for logical plan nodes
class LogicalPlanVisitor {
public:
    virtual ~LogicalPlanVisitor() = default;
    virtual void Visit(LogicalScanNode& node) = 0;
    virtual void Visit(LogicalFilterNode& node) = 0;
    virtual void Visit(LogicalProjectionNode& node) = 0;
    virtual void Visit(LogicalJoinNode& node) = 0;
    virtual void Visit(LogicalAggregateNode& node) = 0;
    virtual void Visit(LogicalSortNode& node) = 0;
    virtual void Visit(LogicalLimitNode& node) = 0;
};

std::string GetLogicalPlanUserFriendlyString(LogicalPlanNode& root);

}  // namespace pond::query