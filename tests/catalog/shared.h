#pragma once

#include <arrow/table.h>
#include <gtest/gtest.h>

#include "catalog/data_ingestor.h"
#include "catalog/data_ingestor_util.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "format/parquet/parquet_reader.h"
#include "format/parquet/schema_converter.h"
#include "test_helper.h"

namespace pond::catalog {

class DataIngestorTestBase : public ::testing::Test {
public:
    void SetUp() override {
        // Create test schema
        schema_ = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, "test_catalog");
        VERIFY_RESULT(db_result);
        db_ = std::move(db_result.value());

        catalog_ = std::make_shared<KVCatalog>(db_);

        // Create test table
        auto result = catalog_->CreateTable("test_table",
                                            schema_,
                                            PartitionSpec{},  // No partitioning
                                            "/tmp/test_table",
                                            {});
        ASSERT_TRUE(result.ok());
        table_metadata_ = result.value();
    }

    std::shared_ptr<common::Schema> schema_;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<Catalog> catalog_;
    TableMetadata table_metadata_;

    std::shared_ptr<arrow::RecordBatch> CreateTestBatch() {
        // Create test data
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        // Create ID column
        arrow::Int32Builder id_builder;
        EXPECT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
        arrays.push_back(id_builder.Finish().ValueOrDie());

        // Create name column
        arrow::StringBuilder name_builder;
        EXPECT_TRUE(name_builder.AppendValues({"one", "two", "three"}).ok());
        arrays.push_back(name_builder.Finish().ValueOrDie());

        // Create value column
        arrow::DoubleBuilder value_builder;
        EXPECT_TRUE(value_builder.AppendValues({1.1, 2.2, 3.3}).ok());
        arrays.push_back(value_builder.Finish().ValueOrDie());

        auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema_).value();
        return arrow::RecordBatch::Make(arrow_schema, 3, arrays);
    }

    // Add new helper method to verify data in a Parquet file
    void VerifyParquetFileContents(const std::string& file_path,
                                   const std::vector<int32_t>& expected_ids,
                                   const std::vector<std::string>& expected_names_or_dates,
                                   const std::vector<double>& expected_values) {
        auto reader_result = format::ParquetReader::create(fs_, file_path);
        VERIFY_RESULT(reader_result);
        auto reader = std::move(reader_result).value();

        // Read the data
        auto table_result = reader->read();
        VERIFY_RESULT(table_result);
        auto table = table_result.value();

        // Verify number of rows
        ASSERT_EQ(table->num_rows(), expected_ids.size());

        // Get columns
        auto id_array = std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(0));

        // Determine if second field is 'name' or 'date'
        std::shared_ptr<arrow::StringArray> name_or_date_array;
        if (table->schema()->field(1)->name() == "name") {
            name_or_date_array = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
        } else if (table->schema()->field(1)->name() == "date") {
            name_or_date_array = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
        } else {
            FAIL() << "Unexpected second field name: " << table->schema()->field(1)->name();
        }

        auto value_array = std::static_pointer_cast<arrow::DoubleArray>(table->column(2)->chunk(0));

        // Verify contents
        for (int64_t i = 0; i < table->num_rows(); i++) {
            EXPECT_EQ(id_array->Value(i), expected_ids[i]);
            EXPECT_EQ(name_or_date_array->GetString(i), expected_names_or_dates[i]);
            EXPECT_DOUBLE_EQ(value_array->Value(i), expected_values[i]);
        }
    }
};

}  // namespace pond::catalog