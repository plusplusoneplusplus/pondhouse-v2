#pragma once

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "format/parquet/schema_converter.h"
#include "query/planner/planner.h"
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

        planner_ = std::make_shared<query::Planner>(catalog_);
    }

    Result<std::shared_ptr<LogicalPlanNode>> PlanLogical(const std::string& query, bool optimize = false) {
        auto logical_plan = planner_->PlanLogical(query, optimize);
        if (!logical_plan.ok()) {
            LOG_ERROR("Logical planning failed: %s", logical_plan.error().message().c_str());
        }
        if (logical_plan.ok()) {
            std::cout << "Logical plan: " << GetLogicalPlanUserFriendlyString(*logical_plan.value()) << std::endl;
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
        auto physical_plan_result = planner_->PlanPhysical(query, optimize);
        if (physical_plan_result.ok()) {
            std::cout << "Physical plan: " << GetPhysicalPlanUserFriendlyString(*physical_plan_result.value())
                      << std::endl;
        }
        return physical_plan_result;
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

    std::vector<std::shared_ptr<arrow::RecordBatch>> GetSampleBatches(std::string table_name,
                                                                      int row_count = 5,
                                                                      int batch_count = 3) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        if (table_name == "users") {
            for (int batch_idx = 0; batch_idx < batch_count; batch_idx++) {
                // Create Arrow arrays
                arrow::Int32Builder id_builder;
                arrow::StringBuilder name_builder;
                arrow::Int32Builder age_builder;
                arrow::DoubleBuilder salary_builder;

                // Each batch has 5 rows
                // e.g.
                // batch 0:
                // id: [1, 2, 3, 4, 5]
                // name: ["User1", "User2", "User3", "User4", "User5"]
                // age: [20, 21, 22, 23, 24]
                // salary: [10000.0, 20000.0, 30000.0, 40000.0, 50000.0]
                for (int row_idx = 0; row_idx < row_count; row_idx++) {
                    int id = batch_idx * row_count + row_idx + 1;
                    std::string name = "User" + std::to_string(id);
                    // Ages: batch 0: 20-24, batch 1: 30-34, batch 2: 40-44
                    int age = (batch_idx + 2) * 10 + row_idx;
                    double salary = (batch_idx + 2) * 10000.0 + row_idx * 10000.0;

                    EXPECT_TRUE(id_builder.Append(id).ok());
                    EXPECT_TRUE(name_builder.Append(name).ok());
                    EXPECT_TRUE(age_builder.Append(age).ok());
                    EXPECT_TRUE(salary_builder.Append(salary).ok());
                }

                // Create arrays
                std::shared_ptr<arrow::Array> id_array, name_array, age_array, salary_array;
                EXPECT_TRUE(id_builder.Finish(&id_array).ok());
                EXPECT_TRUE(name_builder.Finish(&name_array).ok());
                EXPECT_TRUE(age_builder.Finish(&age_array).ok());
                EXPECT_TRUE(salary_builder.Finish(&salary_array).ok());

                auto batch = arrow::RecordBatch::Make(
                    GetArrowSchema("users"), row_count, {id_array, name_array, age_array, salary_array});

                batches.push_back(batch);
            }
        } else {
            throw std::invalid_argument("Unsupported table name: " + table_name);
        }

        return batches;
    }

public:
    std::string catalog_name_;
    std::unordered_map<std::string, std::unique_ptr<TestTableInfo>> tables_;

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<catalog::KVCatalog> catalog_;
    catalog::PartitionSpec partition_spec_;
    std::shared_ptr<query::Planner> planner_;
};

}  // namespace pond::query