#pragma once

#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "query/planner/logical_optimizer.h"
#include "query/planner/logical_planner.h"
#include "query/planner/physical_planner.h"

namespace pond::query {

class QueryTestContext {
public:
    QueryTestContext(const std::string& catalog_name) : catalog_name_(catalog_name) {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, catalog_name_);
        db_ = std::move(db_result.value());
        catalog_ = std::make_shared<catalog::KVCatalog>(db_);
    }

    void SetupOrdersTable() {
        auto schema = std::make_shared<common::Schema>();
        schema->AddField("order_id", common::ColumnType::INT32);
        schema->AddField("user_id", common::ColumnType::INT32);
        schema->AddField("amount", common::ColumnType::DOUBLE);
        catalog_->CreateTable("orders", schema, partition_spec_, "test_catalog/orders");
    }

    void SetupUsersTable() {
        auto schema = std::make_shared<common::Schema>();
        schema->AddField("id", common::ColumnType::INT32);
        schema->AddField("name", common::ColumnType::STRING);
        schema->AddField("age", common::ColumnType::INT32);
        schema->AddField("salary", common::ColumnType::DOUBLE);
        catalog_->CreateTable("users", schema, partition_spec_, "test_catalog/users");
    }

    Result<std::shared_ptr<LogicalPlanNode>> PlanLogical(const std::string& query, bool optimize = false) {
        LogicalPlanner planner(*catalog_);
        hsql::SQLParserResult parse_result;
        hsql::SQLParser::parse(query, &parse_result);
        EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";
        EXPECT_EQ(1, parse_result.size()) << "Should have one statement";
        auto logical_plan = planner.Plan(parse_result.getStatement(0));
        if (optimize) {
            return Optimize(logical_plan.value());
        }
        return logical_plan;
    }

    Result<std::shared_ptr<LogicalPlanNode>> Optimize(std::shared_ptr<LogicalPlanNode> plan) {
        LogicalOptimizer optimizer;
        auto optimized_plan_result = optimizer.Optimize(plan);
        EXPECT_TRUE(optimized_plan_result.ok()) << "Logical optimization should succeed";
        return optimized_plan_result;
    }

    Result<std::shared_ptr<PhysicalPlanNode>> PlanPhysical(const std::string& query, bool optimize = false) {
        PhysicalPlanner planner(*catalog_);
        auto logical_plan = PlanLogical(query, optimize);
        EXPECT_TRUE(logical_plan.ok()) << "Logical planning should succeed";
        return planner.Plan(logical_plan.value());
    }

public:
    std::string catalog_name_;
    std::string table_name_;

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<catalog::KVCatalog> catalog_;
    catalog::PartitionSpec partition_spec_;
};

}  // namespace pond::query