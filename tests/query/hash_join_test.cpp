#include "query/executor/hash_join.h"

#include <gtest/gtest.h>

#include "query/data/arrow_record_batch_builder.h"
#include "query/data/arrow_util.h"
#include "test_helper.h"

namespace pond::query {

class HashJoinTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create sample batches for testing
        auto left_batch_result = ArrowRecordBatchBuilder()
                                     .AddInt32Column("id", {1, 2, 3})
                                     .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
                                     .AddInt32Column("age", {25, 30, 35})
                                     .Build();
        VERIFY_RESULT(left_batch_result);
        left_batch_ = left_batch_result.value();

        auto right_batch_result = ArrowRecordBatchBuilder()
                                      .AddInt32Column("id", {1, 2, 4})
                                      .AddStringColumn("dept", {"HR", "Engineering", "Sales"})
                                      .AddDoubleColumn("salary", {50000.0, 80000.0, 60000.0})
                                      .Build();
        VERIFY_RESULT(right_batch_result);
        right_batch_ = right_batch_result.value();
    }

    // Helper method to create join condition
    std::shared_ptr<common::Expression> CreateJoinCondition(const std::string& left_col, const std::string& right_col) {
        auto left_expr = common::MakeColumnExpression("", left_col);
        auto right_expr = common::MakeColumnExpression("", right_col);
        return common::MakeComparisonExpression(common::BinaryOpType::Equal, left_expr, right_expr);
    }

    std::shared_ptr<arrow::RecordBatch> left_batch_;
    std::shared_ptr<arrow::RecordBatch> right_batch_;
};

//
// Test Setup:
//      Join on same column name (id = id)
// Test Result:
//      Output schema should contain id column only once
//
TEST_F(HashJoinTest, SameColumnNameJoin) {
    // Create join condition: left.id = right.id
    auto condition = CreateJoinCondition("id", "id");

    // Create hash join context
    HashJoinContext join_context(left_batch_, right_batch_, *condition, common::JoinType::Inner);

    // Build the join
    auto build_result = join_context.Build();
    VERIFY_RESULT(build_result);

    // Get output schema
    auto schema_result = join_context.GetOutputSchema();
    VERIFY_RESULT(schema_result);

    auto output_schema = schema_result.value();

    // Verify schema structure
    ASSERT_EQ(output_schema->num_fields(), 5);  // id, name, age, dept, salary

    // Check column names and order
    EXPECT_EQ(output_schema->field(0)->name(), "id");
    EXPECT_EQ(output_schema->field(1)->name(), "name");
    EXPECT_EQ(output_schema->field(2)->name(), "age");
    EXPECT_EQ(output_schema->field(3)->name(), "dept");
    EXPECT_EQ(output_schema->field(4)->name(), "salary");

    // Verify id appears only once
    int id_count = 0;
    for (int i = 0; i < output_schema->num_fields(); i++) {
        if (output_schema->field(i)->name() == "id") {
            id_count++;
        }
    }
    EXPECT_EQ(id_count, 1);
}

//
// Test Setup:
//      Join on different column names (id = dept_id)
// Test Result:
//      Output schema should contain both columns with original names
//
TEST_F(HashJoinTest, DifferentColumnNameJoin) {
    // Create right batch with different join column name
    auto right_batch_result = ArrowRecordBatchBuilder()
                                  .AddInt32Column("dept_id", {1, 2, 4})
                                  .AddStringColumn("dept", {"HR", "Engineering", "Sales"})
                                  .AddDoubleColumn("salary", {50000.0, 80000.0, 60000.0})
                                  .Build();
    VERIFY_RESULT(right_batch_result);
    auto right_batch_diff_col = right_batch_result.value();

    // Create join condition: left.id = right.dept_id
    auto condition = CreateJoinCondition("id", "dept_id");

    // Create hash join context
    HashJoinContext join_context(left_batch_, right_batch_diff_col, *condition, common::JoinType::Inner);

    // Build the join
    auto build_result = join_context.Build();
    VERIFY_RESULT(build_result);

    // Get output schema
    auto schema_result = join_context.GetOutputSchema();
    VERIFY_RESULT(schema_result);

    auto output_schema = schema_result.value();

    // Verify schema structure
    ASSERT_EQ(output_schema->num_fields(), 6);  // id, name, age, dept_id, dept, salary

    // Check column names and order
    EXPECT_EQ(output_schema->field(0)->name(), "id");
    EXPECT_EQ(output_schema->field(1)->name(), "name");
    EXPECT_EQ(output_schema->field(2)->name(), "age");
    EXPECT_EQ(output_schema->field(3)->name(), "dept_id");
    EXPECT_EQ(output_schema->field(4)->name(), "dept");
    EXPECT_EQ(output_schema->field(5)->name(), "salary");

    // Verify both id and dept_id appear
    bool has_id = false;
    bool has_dept_id = false;
    for (int i = 0; i < output_schema->num_fields(); i++) {
        if (output_schema->field(i)->name() == "id") {
            has_id = true;
        }
        if (output_schema->field(i)->name() == "dept_id") {
            has_dept_id = true;
        }
    }
    EXPECT_TRUE(has_id);
    EXPECT_TRUE(has_dept_id);
}

//
// Test Setup:
//      Join with column name conflicts (non-join columns)
// Test Result:
//      Output schema should prefix conflicting columns with "right_"
//
TEST_F(HashJoinTest, ColumnNameConflicts) {
    // Create batches with conflicting column names
    auto left_batch_result = ArrowRecordBatchBuilder()
                                 .AddInt32Column("id", {1, 2, 3})
                                 .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
                                 .AddStringColumn("dept", {"Finance", "IT", "Marketing"})
                                 .Build();
    VERIFY_RESULT(left_batch_result);
    auto left_batch_conflict = left_batch_result.value();

    auto right_batch_result = ArrowRecordBatchBuilder()
                                  .AddInt32Column("id", {1, 2, 4})
                                  .AddStringColumn("dept", {"HR", "Engineering", "Sales"})
                                  .AddDoubleColumn("salary", {50000.0, 80000.0, 60000.0})
                                  .Build();
    VERIFY_RESULT(right_batch_result);
    auto right_batch_conflict = right_batch_result.value();

    // Create join condition: left.id = right.id
    auto condition = CreateJoinCondition("id", "id");

    // Create hash join context
    HashJoinContext join_context(left_batch_conflict, right_batch_conflict, *condition, common::JoinType::Inner);

    // Build the join
    auto build_result = join_context.Build();
    VERIFY_RESULT(build_result);

    // Get output schema
    auto schema_result = join_context.GetOutputSchema();
    VERIFY_RESULT(schema_result);

    auto output_schema = schema_result.value();

    // Verify schema structure
    ASSERT_EQ(output_schema->num_fields(), 5);  // id, name, dept, right_dept, salary

    // Check column names and order
    EXPECT_EQ(output_schema->field(0)->name(), "id");
    EXPECT_EQ(output_schema->field(1)->name(), "name");
    EXPECT_EQ(output_schema->field(2)->name(), "dept");
    EXPECT_EQ(output_schema->field(3)->name(), "right_dept");
    EXPECT_EQ(output_schema->field(4)->name(), "salary");

    // Verify dept appears once with original name and once with prefix
    bool has_dept = false;
    bool has_right_dept = false;
    for (int i = 0; i < output_schema->num_fields(); i++) {
        if (output_schema->field(i)->name() == "dept") {
            has_dept = true;
        }
        if (output_schema->field(i)->name() == "right_dept") {
            has_right_dept = true;
        }
    }
    EXPECT_TRUE(has_dept);
    EXPECT_TRUE(has_right_dept);
}

//
// Test Setup:
//      Complex case with same join column and other conflicting columns
// Test Result:
//      Output schema should handle both cases correctly
//
TEST_F(HashJoinTest, ComplexColumnHandling) {
    // Create batches with multiple conflicts
    auto left_batch_result = ArrowRecordBatchBuilder()
                                 .AddInt32Column("id", {1, 2, 3})
                                 .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
                                 .AddStringColumn("dept", {"Finance", "IT", "Marketing"})
                                 .AddDoubleColumn("salary", {45000.0, 55000.0, 65000.0})
                                 .Build();
    VERIFY_RESULT(left_batch_result);
    auto left_batch_complex = left_batch_result.value();

    auto right_batch_result = ArrowRecordBatchBuilder()
                                  .AddInt32Column("id", {1, 2, 4})
                                  .AddStringColumn("dept", {"HR", "Engineering", "Sales"})
                                  .AddDoubleColumn("salary", {50000.0, 80000.0, 60000.0})
                                  .AddStringColumn("location", {"NY", "SF", "LA"})
                                  .Build();
    VERIFY_RESULT(right_batch_result);
    auto right_batch_complex = right_batch_result.value();

    // Create join condition: left.id = right.id
    auto condition = CreateJoinCondition("id", "id");

    // Create hash join context
    HashJoinContext join_context(left_batch_complex, right_batch_complex, *condition, common::JoinType::Inner);

    // Build the join
    auto build_result = join_context.Build();
    VERIFY_RESULT(build_result);

    // Get output schema
    auto schema_result = join_context.GetOutputSchema();
    VERIFY_RESULT(schema_result);

    auto output_schema = schema_result.value();

    // Verify schema structure
    ASSERT_EQ(output_schema->num_fields(), 7);  // id, name, dept, salary, right_dept, right_salary, location

    // Check column names and order
    EXPECT_EQ(output_schema->field(0)->name(), "id");
    EXPECT_EQ(output_schema->field(1)->name(), "name");
    EXPECT_EQ(output_schema->field(2)->name(), "dept");
    EXPECT_EQ(output_schema->field(3)->name(), "salary");
    EXPECT_EQ(output_schema->field(4)->name(), "right_dept");
    EXPECT_EQ(output_schema->field(5)->name(), "right_salary");
    EXPECT_EQ(output_schema->field(6)->name(), "location");

    // Verify id appears only once
    int id_count = 0;
    for (int i = 0; i < output_schema->num_fields(); i++) {
        if (output_schema->field(i)->name() == "id") {
            id_count++;
        }
    }
    EXPECT_EQ(id_count, 1);

    // Verify dept and salary appear with both original and prefixed names
    bool has_dept = false;
    bool has_right_dept = false;
    bool has_salary = false;
    bool has_right_salary = false;
    bool has_location = false;
    for (int i = 0; i < output_schema->num_fields(); i++) {
        if (output_schema->field(i)->name() == "dept") {
            has_dept = true;
        }
        if (output_schema->field(i)->name() == "right_dept") {
            has_right_dept = true;
        }
        if (output_schema->field(i)->name() == "salary") {
            has_salary = true;
        }
        if (output_schema->field(i)->name() == "right_salary") {
            has_right_salary = true;
        }
        if (output_schema->field(i)->name() == "location") {
            has_location = true;
        }
    }
    EXPECT_TRUE(has_dept);
    EXPECT_TRUE(has_right_dept);
    EXPECT_TRUE(has_salary);
    EXPECT_TRUE(has_right_salary);
    EXPECT_TRUE(has_location);
}

}  // namespace pond::query