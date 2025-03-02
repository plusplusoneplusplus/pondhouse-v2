#include "common/expression.h"

#include <gtest/gtest.h>

#include "test_helper.h"

namespace pond::common {

class ExpressionTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

//
// Test Setup:
//      Create a constant expression with integer value
// Test Result:
//      Verify type, value, and string representation
//
TEST_F(ExpressionTest, TestConstantExpression) {
    auto const_expr = std::make_shared<ConstantExpression>("42", ColumnType::INT64);
    EXPECT_EQ(const_expr->Type(), ExprType::Constant);
    EXPECT_EQ(const_expr->Value(), "42");
    EXPECT_EQ(const_expr->ToString(), "42");
    EXPECT_EQ(const_expr->GetColumnType(), ColumnType::INT64);
}

//
// Test Setup:
//      Create a column expression with name and type
// Test Result:
//      Verify name, type and string representation
//
TEST_F(ExpressionTest, TestColumnExpression) {
    auto col_expr = std::make_shared<ColumnExpression>("test_table", "test_col");
    EXPECT_EQ(col_expr->Type(), ExprType::Column);
    EXPECT_EQ(col_expr->TableName(), "test_table");
    EXPECT_EQ(col_expr->ColumnName(), "test_col");
    EXPECT_EQ(col_expr->ToString(), "test_table.test_col");

    col_expr = std::make_shared<ColumnExpression>("", "test_col");
    EXPECT_EQ(col_expr->Type(), ExprType::Column);
    EXPECT_EQ(col_expr->TableName(), "");
    EXPECT_EQ(col_expr->ColumnName(), "test_col");
    EXPECT_EQ(col_expr->ToString(), "test_col");
}

//
// Test Setup:
//      Create a star expression
// Test Result:
//      Verify type and string representation
//
TEST_F(ExpressionTest, TestStarExpression) {
    auto star_expr = std::make_shared<StarExpression>();
    EXPECT_EQ(star_expr->Type(), ExprType::Star);
    EXPECT_EQ(star_expr->ToString(), "*");
}

//
// Test Setup:
//      Create binary expressions with different operators
// Test Result:
//      Verify operator types, result types and string representations
//
TEST_F(ExpressionTest, TestBinaryExpression) {
    auto left = std::make_shared<ConstantExpression>("10", ColumnType::INT64);
    auto right = std::make_shared<ConstantExpression>("5", ColumnType::INT64);

    auto add_expr = std::make_shared<BinaryExpression>(BinaryOpType::Add, left, right);
    EXPECT_EQ(add_expr->Type(), ExprType::BinaryOp);
    EXPECT_EQ(add_expr->OpType(), BinaryOpType::Add);
    EXPECT_EQ(add_expr->ToString(), "(10 + 5)");

    auto sub_expr = std::make_shared<BinaryExpression>(BinaryOpType::Subtract, left, right);
    EXPECT_EQ(sub_expr->ToString(), "(10 - 5)");

    auto mul_expr = std::make_shared<BinaryExpression>(BinaryOpType::Multiply, left, right);
    EXPECT_EQ(mul_expr->ToString(), "(10 * 5)");

    auto div_expr = std::make_shared<BinaryExpression>(BinaryOpType::Divide, left, right);
    EXPECT_EQ(div_expr->ToString(), "(10 / 5)");
}

//
// Test Setup:
//      Create aggregate expressions with different types
// Test Result:
//      Verify aggregate types, result types and string representations
//
TEST_F(ExpressionTest, TestAggregateExpression) {
    auto col_expr = std::make_shared<ColumnExpression>("", "value");

    auto count_expr = std::make_shared<AggregateExpression>(AggregateType::Count, col_expr);
    EXPECT_EQ(count_expr->Type(), ExprType::Aggregate);
    EXPECT_EQ(count_expr->AggType(), AggregateType::Count);
    EXPECT_EQ(count_expr->ToString(), "COUNT(value)");
    EXPECT_EQ(count_expr->ResultType(), ColumnType::UINT64);
    EXPECT_EQ(count_expr->ResultName(), "count_value");

    auto sum_expr = std::make_shared<AggregateExpression>(AggregateType::Sum, col_expr);
    EXPECT_EQ(sum_expr->Type(), ExprType::Aggregate);
    EXPECT_EQ(sum_expr->AggType(), AggregateType::Sum);
    EXPECT_EQ(sum_expr->ToString(), "SUM(value)");
    EXPECT_EQ(sum_expr->ResultType(), ColumnType::DOUBLE);
    EXPECT_EQ(sum_expr->ResultName(), "sum_value");

    auto avg_expr = std::make_shared<AggregateExpression>(AggregateType::Avg, col_expr);
    EXPECT_EQ(avg_expr->Type(), ExprType::Aggregate);
    EXPECT_EQ(avg_expr->AggType(), AggregateType::Avg);
    EXPECT_EQ(avg_expr->ToString(), "AVG(value)");
    EXPECT_EQ(avg_expr->ResultType(), ColumnType::DOUBLE);
    EXPECT_EQ(avg_expr->ResultName(), "avg_value");

    auto min_expr = std::make_shared<AggregateExpression>(AggregateType::Min, col_expr);
    EXPECT_EQ(min_expr->Type(), ExprType::Aggregate);
    EXPECT_EQ(min_expr->AggType(), AggregateType::Min);
    EXPECT_EQ(min_expr->ToString(), "MIN(value)");
    EXPECT_EQ(min_expr->ResultType(), ColumnType::UINT64);
    EXPECT_EQ(min_expr->ResultName(), "min_value");

    auto max_expr = std::make_shared<AggregateExpression>(AggregateType::Max, col_expr);
    EXPECT_EQ(max_expr->Type(), ExprType::Aggregate);
    EXPECT_EQ(max_expr->AggType(), AggregateType::Max);
    EXPECT_EQ(max_expr->ToString(), "MAX(value)");
    EXPECT_EQ(max_expr->ResultType(), ColumnType::UINT64);
    EXPECT_EQ(max_expr->ResultName(), "max_value");
}

//
// Test Setup:
//      Create nested expressions combining different types
// Test Result:
//      Verify complex expression behavior and string representations
//
TEST_F(ExpressionTest, TestNestedExpressions) {
    auto col1 = std::make_shared<ColumnExpression>("", "price");
    auto col2 = std::make_shared<ColumnExpression>("", "quantity");
    auto const_expr = std::make_shared<ConstantExpression>("100", ColumnType::INT64);

    // Create a binary expression: (price * quantity)
    auto mul_expr = std::make_shared<BinaryExpression>(BinaryOpType::Multiply, col1, col2);
    EXPECT_EQ(mul_expr->ToString(), "(price * quantity)");

    // Create an aggregate of the binary expression: SUM(price * quantity)
    auto sum_expr = std::make_shared<AggregateExpression>(AggregateType::Sum, mul_expr);
    EXPECT_EQ(sum_expr->ToString(), "SUM((price * quantity))");
    EXPECT_EQ(sum_expr->ResultType(), ColumnType::DOUBLE);
    EXPECT_EQ(sum_expr->ResultName(), "sum_(price * quantity)");

    // Create a more complex expression: AVG((price * quantity) / 100)
    auto div_expr = std::make_shared<BinaryExpression>(BinaryOpType::Divide, mul_expr, const_expr);
    auto avg_expr = std::make_shared<AggregateExpression>(AggregateType::Avg, div_expr);
    EXPECT_EQ(avg_expr->ToString(), "AVG(((price * quantity) / 100))");
    EXPECT_EQ(avg_expr->ResultType(), ColumnType::DOUBLE);
}

//
// Test Setup:
//      Create constant expressions with different types and edge cases
// Test Result:
//      Verify handling of different data types and edge cases
//
TEST_F(ExpressionTest, TestConstantExpressionEdgeCases) {
    // Test different types
    auto int_expr = std::make_shared<ConstantExpression>("42", ColumnType::INT64);
    EXPECT_EQ(int_expr->GetColumnType(), ColumnType::INT64);

    auto double_expr = std::make_shared<ConstantExpression>("3.14", ColumnType::DOUBLE);
    EXPECT_EQ(double_expr->GetColumnType(), ColumnType::DOUBLE);

    auto string_expr = std::make_shared<ConstantExpression>("test", ColumnType::STRING);
    EXPECT_EQ(string_expr->GetColumnType(), ColumnType::STRING);

    // Test empty string
    auto empty_expr = std::make_shared<ConstantExpression>("", ColumnType::STRING);
    EXPECT_EQ(empty_expr->Value(), "");
    EXPECT_EQ(empty_expr->ToString(), "''");

    // Test special characters
    auto special_expr = std::make_shared<ConstantExpression>("test'with\"quotes", ColumnType::STRING);
    EXPECT_EQ(special_expr->Value(), "test'with\"quotes");
}

//
// Test Setup:
//      Create column expressions with edge cases and invalid inputs
// Test Result:
//      Verify handling of edge cases and invalid inputs
//
TEST_F(ExpressionTest, TestColumnExpressionEdgeCases) {
    // Test empty table and column names
    auto empty_names = std::make_shared<ColumnExpression>("", "");
    EXPECT_EQ(empty_names->TableName(), "");
    EXPECT_EQ(empty_names->ColumnName(), "");
    EXPECT_EQ(empty_names->ToString(), "");

    // Test special characters in names
    auto special_chars = std::make_shared<ColumnExpression>("table.with.dots", "column$with#special@chars");
    EXPECT_EQ(special_chars->TableName(), "table.with.dots");
    EXPECT_EQ(special_chars->ColumnName(), "column$with#special@chars");
    EXPECT_EQ(special_chars->ToString(), "table.with.dots.column$with#special@chars");

    // Test very long names
    std::string long_name(1000, 'a');
    auto long_names = std::make_shared<ColumnExpression>(long_name, long_name);
    EXPECT_EQ(long_names->TableName(), long_name);
    EXPECT_EQ(long_names->ColumnName(), long_name);
}

//
// Test Setup:
//      Create binary expressions with edge cases and type mismatches
// Test Result:
//      Verify handling of edge cases and type compatibility
//
TEST_F(ExpressionTest, TestBinaryExpressionEdgeCases) {
    auto int_expr = std::make_shared<ConstantExpression>("42", ColumnType::INT64);
    auto double_expr = std::make_shared<ConstantExpression>("3.14", ColumnType::DOUBLE);
    auto string_expr = std::make_shared<ConstantExpression>("test", ColumnType::STRING);

    // Test mixing numeric types
    auto mixed_types = std::make_shared<BinaryExpression>(BinaryOpType::Add, int_expr, double_expr);
    EXPECT_EQ(mixed_types->ToString(), "(42 + 3.14)");

    // Test division by zero (if supported)
    auto zero_expr = std::make_shared<ConstantExpression>("0", ColumnType::INT64);
    auto div_by_zero = std::make_shared<BinaryExpression>(BinaryOpType::Divide, int_expr, zero_expr);
    EXPECT_EQ(div_by_zero->ToString(), "(42 / 0)");

    // Test nested binary expressions with multiple levels
    auto nested_expr = std::make_shared<BinaryExpression>(
        BinaryOpType::Add,
        std::make_shared<BinaryExpression>(BinaryOpType::Multiply, int_expr, double_expr),
        std::make_shared<BinaryExpression>(BinaryOpType::Divide, double_expr, int_expr));
    EXPECT_EQ(nested_expr->ToString(), "((42 * 3.14) + (3.14 / 42))");
}

//
// Test Setup:
//      Create aggregate expressions with edge cases and invalid inputs
// Test Result:
//      Verify handling of edge cases and invalid aggregations
//
TEST_F(ExpressionTest, TestAggregateExpressionEdgeCases) {
    // Test aggregation on constant
    auto const_expr = std::make_shared<ConstantExpression>("42", ColumnType::INT64);
    auto agg_const = std::make_shared<AggregateExpression>(AggregateType::Sum, const_expr);
    EXPECT_EQ(agg_const->ToString(), "SUM(42)");
    EXPECT_EQ(agg_const->ResultName(), "sum");

    // Test nested aggregates with binary expressions
    auto col1 = std::make_shared<ColumnExpression>("", "val1");
    auto col2 = std::make_shared<ColumnExpression>("", "val2");
    auto binary_expr = std::make_shared<BinaryExpression>(BinaryOpType::Add, col1, col2);
    auto nested_agg = std::make_shared<AggregateExpression>(AggregateType::Avg, binary_expr);
    EXPECT_EQ(nested_agg->ToString(), "AVG((val1 + val2))");
    EXPECT_EQ(nested_agg->ResultName(), "avg_(val1 + val2)");

    // Test aggregation on string column
    auto string_col = std::make_shared<ColumnExpression>("", "str_col");
    auto count_strings = std::make_shared<AggregateExpression>(AggregateType::Count, string_col);
    EXPECT_EQ(count_strings->ToString(), "COUNT(str_col)");
    EXPECT_EQ(count_strings->ResultType(), ColumnType::UINT64);
}

//
// Test Setup:
//      Create deeply nested expressions with mixed types
// Test Result:
//      Verify correct handling of complex nested expressions
//
TEST_F(ExpressionTest, TestComplexNestedExpressions) {
    auto col1 = std::make_shared<ColumnExpression>("t1", "price");
    auto col2 = std::make_shared<ColumnExpression>("t2", "quantity");
    auto const1 = std::make_shared<ConstantExpression>("100", ColumnType::INT64);
    auto const2 = std::make_shared<ConstantExpression>("0.5", ColumnType::DOUBLE);

    // Create a complex nested expression: AVG(((t1.price * t2.quantity) + (100 * 0.5)) / (t1.price + 100))
    auto mul1 = std::make_shared<BinaryExpression>(BinaryOpType::Multiply, col1, col2);
    auto mul2 = std::make_shared<BinaryExpression>(BinaryOpType::Multiply, const1, const2);
    auto add1 = std::make_shared<BinaryExpression>(BinaryOpType::Add, mul1, mul2);
    auto add2 = std::make_shared<BinaryExpression>(BinaryOpType::Add, col1, const1);
    auto div1 = std::make_shared<BinaryExpression>(BinaryOpType::Divide, add1, add2);
    auto avg1 = std::make_shared<AggregateExpression>(AggregateType::Avg, div1);

    EXPECT_EQ(avg1->ToString(), "AVG((((t1.price * t2.quantity) + (100 * 0.5)) / (t1.price + 100)))");
    EXPECT_EQ(avg1->Type(), ExprType::Aggregate);
    EXPECT_EQ(avg1->ResultType(), ColumnType::DOUBLE);
}

//
// Test Setup:
//      Test expression type checking and validation
// Test Result:
//      Verify type checking and validation behavior
//
TEST_F(ExpressionTest, TestExpressionTypeValidation) {
    auto int_col = std::make_shared<ColumnExpression>("", "int_col");
    auto str_col = std::make_shared<ColumnExpression>("", "str_col");
    auto dbl_col = std::make_shared<ColumnExpression>("", "dbl_col");

    // Test numeric operations
    auto add_nums = std::make_shared<BinaryExpression>(BinaryOpType::Add, int_col, dbl_col);
    EXPECT_EQ(add_nums->ToString(), "(int_col + dbl_col)");

    // Test string operations
    auto str_const = std::make_shared<ConstantExpression>("test", ColumnType::STRING);
    auto str_expr = std::make_shared<BinaryExpression>(BinaryOpType::Add, str_col, str_const);
    EXPECT_EQ(str_expr->ToString(), "(str_col + 'test')");

    // Test aggregates with different types
    auto sum_dbl = std::make_shared<AggregateExpression>(AggregateType::Sum, dbl_col);
    EXPECT_EQ(sum_dbl->ResultType(), ColumnType::DOUBLE);

    auto count_str = std::make_shared<AggregateExpression>(AggregateType::Count, str_col);
    EXPECT_EQ(count_str->ResultType(), ColumnType::UINT64);
}

}  // namespace pond::common