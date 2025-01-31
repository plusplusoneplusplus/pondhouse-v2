#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include "common/memory_append_only_fs.h"
#include "format/parquet/parquet_reader.h"
#include "format/parquet/parquet_writer.h"
#include "test_helper.h"

namespace pond::test {

using namespace pond::common;
using namespace pond::format;

class ParquetTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();

        // Create a sample schema
        auto id_field = arrow::field("id", arrow::int64());
        auto name_field = arrow::field("name", arrow::utf8());
        auto value_field = arrow::field("value", arrow::float64());
        schema_ = arrow::schema({id_field, name_field, value_field});

        // Create sample data
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::DoubleBuilder value_builder;

        ASSERT_OK(id_builder.AppendValues({1, 2, 3, 4, 5}));
        ASSERT_OK(name_builder.AppendValues({"one", "two", "three", "four", "five"}));
        ASSERT_OK(value_builder.AppendValues({1.1, 2.2, 3.3, 4.4, 5.5}));

        std::vector<std::shared_ptr<arrow::Array>> arrays = {
            id_builder.Finish().ValueOrDie(), name_builder.Finish().ValueOrDie(), value_builder.Finish().ValueOrDie()};

        table_ = arrow::Table::Make(schema_, arrays);
    }

    void TearDown() override {
        fs_.reset();
        schema_.reset();
        table_.reset();
    }

    std::shared_ptr<IAppendOnlyFileSystem> fs_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::Table> table_;
};

TEST_F(ParquetTest, BasicWriteRead) {
    const std::string path = "test.parquet";
    auto _ = fs_->deleteFiles({path});

    // Write the table
    auto writer_result = ParquetWriter::create(fs_, path, schema_);
    VERIFY_RESULT_MSG(writer_result, "Failed to create writer");
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(table_);
    VERIFY_RESULT_MSG(write_result, "Failed to write table");

    auto close_result = writer->close();
    VERIFY_RESULT_MSG(close_result, "Failed to close writer");

    // Read the table back
    auto reader_result = ParquetReader::create(fs_, path);
    VERIFY_RESULT_MSG(reader_result, "Failed to create reader");
    auto reader = std::move(reader_result).value();

    auto read_result = reader->read();
    VERIFY_RESULT_MSG(read_result, "Failed to read table");
    auto read_table = std::move(read_result).value();

    // Verify the data
    ASSERT_EQ(table_->num_rows(), read_table->num_rows());
    ASSERT_EQ(table_->num_columns(), read_table->num_columns());

    // Compare schemas
    ASSERT_TRUE(table_->schema()->Equals(read_table->schema()));

    // Compare data
    for (int i = 0; i < table_->num_columns(); ++i) {
        ASSERT_TRUE(table_->column(i)->Equals(read_table->column(i)));
    }
}

TEST_F(ParquetTest, ColumnSelection) {
    const std::string path = "column_selection.parquet";
    auto _ = fs_->deleteFiles({path});

    // Write the table
    auto writer_result = ParquetWriter::create(fs_, path, schema_);
    VERIFY_RESULT_MSG(writer_result, "Failed to create writer");
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(table_);
    VERIFY_RESULT_MSG(write_result, "Failed to write table");
    writer->close();

    // Read specific columns
    auto reader_result = ParquetReader::create(fs_, path);
    VERIFY_RESULT_MSG(reader_result, "Failed to create reader");
    auto reader = std::move(reader_result).value();

    std::vector<int> column_indices = {0, 2};  // Read 'id' and 'value' columns
    auto read_result = reader->read(column_indices);
    VERIFY_RESULT_MSG(read_result, "Failed to read selected columns");
    auto read_table = std::move(read_result).value();

    // Verify the data
    ASSERT_EQ(table_->num_rows(), read_table->num_rows());
    ASSERT_EQ(column_indices.size(), read_table->num_columns());

    // Compare selected columns
    ASSERT_TRUE(table_->column(0)->Equals(read_table->column(0)));  // id
    ASSERT_TRUE(table_->column(2)->Equals(read_table->column(1)));  // value
}

TEST_F(ParquetTest, RecordBatchWrite) {
    const std::string path = "record_batch.parquet";
    auto _ = fs_->deleteFiles({path});

    // Convert table to record batch
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < table_->num_columns(); ++i) {
        arrays.push_back(table_->column(i)->chunk(0));
    }
    auto batch = arrow::RecordBatch::Make(schema_, table_->num_rows(), arrays);

    // Write the record batch
    auto writer_result = ParquetWriter::create(fs_, path, schema_);
    VERIFY_RESULT_MSG(writer_result, "Failed to create writer");
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(batch);
    VERIFY_RESULT_MSG(write_result, "Failed to write record batch");
    writer->close();

    // Read and verify
    auto reader_result = ParquetReader::create(fs_, path);
    VERIFY_RESULT_MSG(reader_result, "Failed to create reader");
    auto reader = std::move(reader_result).value();

    auto read_result = reader->read();
    VERIFY_RESULT_MSG(read_result, "Failed to read table");
    auto read_table = std::move(read_result).value();

    ASSERT_EQ(table_->num_rows(), read_table->num_rows());
    ASSERT_EQ(table_->num_columns(), read_table->num_columns());
    ASSERT_TRUE(table_->Equals(*read_table));
}

TEST_F(ParquetTest, RowGroupOperations) {
    const std::string path = "row_groups.parquet";
    auto _ = fs_->deleteFiles({path});

    // Create a larger table with more rows
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::DoubleBuilder value_builder;

    std::vector<int64_t> ids;
    std::vector<std::string> names;
    std::vector<double> values;
    for (int i = 0; i < 10; i++) {
        ids.push_back(i);
        names.push_back("name_" + std::to_string(i));
        values.push_back(i * 1.1);
    }

    ASSERT_OK(id_builder.AppendValues(ids));
    ASSERT_OK(name_builder.AppendValues(names));
    ASSERT_OK(value_builder.AppendValues(values));

    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        id_builder.Finish().ValueOrDie(), name_builder.Finish().ValueOrDie(), value_builder.Finish().ValueOrDie()};

    auto large_table = arrow::Table::Make(schema_, arrays);

    // Write with custom row group size
    parquet::WriterProperties::Builder builder;
    builder.write_batch_size(2);      // Create multiple row groups
    builder.max_row_group_length(2);  // Ensure each row group has at most 2 rows

    auto writer_result = ParquetWriter::create(fs_, path, schema_, builder);
    VERIFY_RESULT_MSG(writer_result, "Failed to create writer");
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(large_table);
    VERIFY_RESULT_MSG(write_result, "Failed to write table");
    writer->close();

    // Read row groups
    auto reader_result = ParquetReader::create(fs_, path);
    VERIFY_RESULT_MSG(reader_result, "Failed to create reader");
    auto reader = std::move(reader_result).value();

    auto num_groups_result = reader->num_row_groups();
    VERIFY_RESULT_MSG(num_groups_result, "Failed to get number of row groups");
    ASSERT_GT(num_groups_result.value(), 1);

    // Read each row group and verify
    int total_rows = 0;
    for (int i = 0; i < num_groups_result.value(); ++i) {
        auto group_result = reader->read_row_group(i);
        VERIFY_RESULT_MSG(group_result, "Failed to read row group");
        auto group_table = group_result.value();

        ASSERT_EQ(large_table->num_columns(), group_table->num_columns());
        total_rows += group_table->num_rows();
    }

    ASSERT_EQ(large_table->num_rows(), total_rows);
}

TEST_F(ParquetTest, ErrorHandling) {
    const std::string path = "nonexistent.parquet";
    auto _ = fs_->deleteFiles({path});

    // Try to read non-existent file
    auto reader_result = ParquetReader::create(fs_, path);
    ASSERT_FALSE(reader_result.ok());
    ASSERT_EQ(reader_result.error().code(), ErrorCode::FileNotFound);

    // Try to write with invalid schema
    auto invalid_schema = arrow::schema({});
    auto writer_result = ParquetWriter::create(fs_, path, invalid_schema);
    ASSERT_FALSE(writer_result.ok());
}

}  // namespace pond::test
