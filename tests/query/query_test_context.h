#pragma once

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "format/parquet/schema_converter.h"
#include "query/planner/logical_optimizer.h"
#include "query/planner/logical_planner.h"
#include "query/planner/physical_planner.h"
#include "test_helper.h"

namespace pond::query {

class TestTableInfo {
public:
    TestTableInfo(const std::string& name,
                  const std::shared_ptr<common::Schema>& schema,
                  const catalog::PartitionSpec& partition_spec)
        : name_(name), schema_(schema), partition_spec_(partition_spec) {
        arrow_schema_ = format::SchemaConverter::ToArrowSchema(*schema).value();
    }

    const std::string& Name() const { return name_; }
    const std::shared_ptr<common::Schema>& Schema() const { return schema_; }
    const std::shared_ptr<arrow::Schema>& ArrowSchema() const { return arrow_schema_; }

private:
    std::string name_;
    std::shared_ptr<common::Schema> schema_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    catalog::PartitionSpec partition_spec_;
};

class QueryTestContext : public ::testing::Test {
public:
    QueryTestContext(const std::string& catalog_name) : catalog_name_(catalog_name) {}

    void SetUp() override {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, catalog_name_);
        db_ = std::move(db_result.value());
        catalog_ = std::make_shared<catalog::KVCatalog>(db_);

        auto schema = std::make_shared<common::Schema>();
        schema->AddField("order_id", common::ColumnType::INT32);
        schema->AddField("user_id", common::ColumnType::INT32);
        schema->AddField("amount", common::ColumnType::DOUBLE);
        tables_["orders"] = std::make_unique<TestTableInfo>("orders", schema, partition_spec_);

        schema = std::make_shared<common::Schema>();
        schema->AddField("id", common::ColumnType::INT32);
        schema->AddField("name", common::ColumnType::STRING);
        schema->AddField("age", common::ColumnType::INT32);
        schema->AddField("salary", common::ColumnType::DOUBLE);
        tables_["users"] = std::make_unique<TestTableInfo>("users", schema, partition_spec_);

        // same schema as users, but different table name
        tables_["multi_users"] = std::make_unique<TestTableInfo>("multi_users", schema, partition_spec_);

        for (const auto& [name, table_info] : tables_) {
            catalog_->CreateTable(name, table_info->Schema(), partition_spec_, "test_catalog/" + name);
        }
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

    void IngestData(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& record_batch) {
        auto ingestor_result = catalog::DataIngestor::Create(catalog_, fs_, table_name);
        VERIFY_RESULT(ingestor_result);
        auto ingestor = std::move(ingestor_result).value();
        auto ingest_result = ingestor->IngestBatch(record_batch);
        VERIFY_RESULT(ingest_result);
    }

    std::shared_ptr<common::Schema> GetSchema(std::string name) { return tables_[name]->Schema(); }

    std::shared_ptr<arrow::Schema> GetArrowSchema(std::string name) { return tables_[name]->ArrowSchema(); }

public:
    std::string catalog_name_;
    std::unordered_map<std::string, std::unique_ptr<TestTableInfo>> tables_;

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<catalog::KVCatalog> catalog_;
    catalog::PartitionSpec partition_spec_;
};

}  // namespace pond::query