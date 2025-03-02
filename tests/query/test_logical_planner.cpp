#include <memory>

#include <gtest/gtest.h>

#include "catalog/catalog.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "query/planner/logical_optimizer.h"
#include "query/planner/logical_planner.h"
#include "test_helper.h"

using namespace pond::catalog;
using namespace pond::query;
using namespace pond::common;

namespace pond::query {

class QueryTestContext {
public:
    QueryTestContext(const std::string& catalog_name) : catalog_name_(catalog_name) {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, catalog_name_);
        db_ = std::move(db_result.value());
        catalog_ = std::make_shared<KVCatalog>(db_);
    }

    void SetupOrdersTable() {
        auto schema = std::make_shared<Schema>();
        schema->AddField("order_id", ColumnType::INT32);
        schema->AddField("user_id", ColumnType::INT32);
        schema->AddField("amount", ColumnType::DOUBLE);
        catalog_->CreateTable("orders", schema, partition_spec_, "test_catalog/orders");
    }

    void SetupUsersTable() {
        auto schema = std::make_shared<Schema>();
        schema->AddField("id", ColumnType::INT32);
        schema->AddField("name", ColumnType::STRING);
        schema->AddField("age", ColumnType::INT32);
        schema->AddField("salary", ColumnType::DOUBLE);
        catalog_->CreateTable("users", schema, partition_spec_, "test_catalog/users");
    }

public:
    std::string catalog_name_;
    std::string table_name_;

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<KVCatalog> catalog_;
    PartitionSpec partition_spec_;
};

class LogicalPlannerTest : public ::testing::Test {
protected:
    void SetUp() override {
        context_ = std::make_unique<QueryTestContext>("test_catalog");
        context_->SetupUsersTable();
        context_->SetupOrdersTable();
    }

    Result<std::shared_ptr<LogicalPlanNode>> PlanLogical(const std::string& query, bool optimize = false) {
        LogicalPlanner planner(*context_->catalog_);
        hsql::SQLParserResult parse_result;
        hsql::SQLParser::parse(query, &parse_result);
        EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";
        EXPECT_EQ(1, parse_result.size()) << "Should have one statement";
        auto logical_plan = planner.Plan(parse_result.getStatement(0));
        if (optimize) {
            LogicalOptimizer optimizer;
            auto optimized_plan_result = optimizer.Optimize(logical_plan.value());
            EXPECT_TRUE(optimized_plan_result.ok()) << "Logical optimization should succeed";
            return optimized_plan_result;
        }
        return logical_plan;
    }

    Catalog* catalog() { return context_->catalog_.get(); }

    std::unique_ptr<QueryTestContext> context_;
};

//
// Test Setup:
//      Create a basic SELECT query with WHERE clause
// Test Result:
//      Verify the logical plan structure for basic select
//
TEST_F(LogicalPlannerTest, BasicSelect) {
    // Parse SQL
    const std::string query = "SELECT name, age FROM users WHERE age > 30;";
    auto plan_result = PlanLogical(query);
    ASSERT_TRUE(plan_result.ok()) << "Planning should succeed";

    // expected
    // Projection(users: name, age)
    //   Filter(age > 30)
    //     Scan(users)

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ("users", projection->TableName()) << "Table name should be users";
    EXPECT_EQ(2, projection->GetExpressions().size()) << "Should have two projection expressions";

    {
        auto column_expr = projection->GetExpressions()[0]->as<ColumnExpression>();
        EXPECT_EQ("name", column_expr->ColumnName()) << "First projection should be name";

        auto column_expr2 = projection->GetExpressions()[1]->as<ColumnExpression>();
        EXPECT_EQ("age", column_expr2->ColumnName()) << "Second projection should be age";
    }

    auto filter = projection->Children()[0]->as<LogicalFilterNode>();
    EXPECT_EQ(LogicalNodeType::Filter, filter->Type()) << "Second node should be filter";
    EXPECT_EQ("(age > 30)", filter->GetCondition()->ToString()) << "Filter condition should match";

    auto scan = filter->Children()[0]->as<LogicalScanNode>();
    EXPECT_EQ(LogicalNodeType::Scan, scan->Type()) << "Leaf should be scan";
    EXPECT_EQ("users", scan->TableName()) << "Should scan users table";
}

//
// Test Setup:
//      Create a complex SELECT query with multiple conditions
// Test Result:
//      Verify the logical plan structure for complex predicates
//
TEST_F(LogicalPlannerTest, ComplexPredicate) {
    // Parse SQL with complex WHERE clause using AND, OR, parentheses, and various operators
    const std::string query = "SELECT name, age, salary FROM users "
                              "WHERE (age > 30 AND salary >= 50000) "
                              "   OR (age <= 25 AND salary > 60000) "
                              "   AND (name LIKE '%Smith%' OR name LIKE '%Jones%') "
                              "   AND ((age + 5) * 2 > 50 OR salary / 1000 <= 40);";

    auto plan_result = PlanLogical(query, false);
    ASSERT_TRUE(plan_result.ok()) << "Planning should succeed";

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ(3, projection->GetExpressions().size()) << "Should have three projection expressions";

    // Verify projection columns
    {
        auto name_expr = projection->GetExpressions()[0]->as<ColumnExpression>();
        EXPECT_EQ("name", name_expr->ColumnName()) << "First projection should be name";

        auto age_expr = projection->GetExpressions()[1]->as<ColumnExpression>();
        EXPECT_EQ("age", age_expr->ColumnName()) << "Second projection should be age";

        auto salary_expr = projection->GetExpressions()[2]->as<ColumnExpression>();
        EXPECT_EQ("salary", salary_expr->ColumnName()) << "Third projection should be salary";
    }

    auto filter = projection->Children()[0]->as<LogicalFilterNode>();
    EXPECT_EQ(LogicalNodeType::Filter, filter->Type()) << "Second node should be filter";

    // Verify complex filter condition exists
    ASSERT_TRUE(filter->GetCondition() != nullptr) << "Filter should have condition";
    EXPECT_EQ(ExprType::BinaryOp, filter->GetCondition()->Type()) << "Filter condition should be binary op";

    auto scan = filter->Children()[0]->as<LogicalScanNode>();
    EXPECT_EQ(LogicalNodeType::Scan, scan->Type()) << "Leaf should be scan";
    EXPECT_EQ("users", scan->TableName()) << "Should scan users table";
}

//
// Test Setup:
//      Create a SELECT query with non-existent table
// Test Result:
//      Verify error handling for invalid table
//
TEST_F(LogicalPlannerTest, InvalidTable) {
    const std::string query = "SELECT * FROM nonexistent_table;";
    auto plan_result = PlanLogical(query, false);
    EXPECT_FALSE(plan_result.ok()) << "Planning should fail for nonexistent table";
    EXPECT_EQ(ErrorCode::TableNotFoundInCatalog, plan_result.error().code()) << "Should have table not found error";
}

//
// Test Setup:
//      Create a SELECT query with non-existent column
// Test Result:
//      Verify error handling for invalid column
//
TEST_F(LogicalPlannerTest, InvalidColumn) {
    const std::string query = "SELECT nonexistent_column FROM users;";
    auto plan_result = PlanLogical(query, false);
    EXPECT_FALSE(plan_result.ok()) << "Planning should fail for nonexistent column";
    EXPECT_EQ(ErrorCode::SchemaFieldNotFound, plan_result.error().code()) << "Should have column not found error";
}

//
// Test Setup:
//      Create a SELECT query with INNER JOIN
// Test Result:
//      Verify the logical plan structure for inner join
//
TEST_F(LogicalPlannerTest, InnerJoin) {
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id "
                              "WHERE orders.amount > 100;";

    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ(2, projection->GetExpressions().size()) << "Should have two projection expressions";

    {
        auto expr = projection->GetExpressions()[0];
        EXPECT_EQ(ExprType::Column, expr->Type()) << "First expression should be column";
        auto column = expr->as<ColumnExpression>();
        EXPECT_EQ("users", column->TableName()) << "First column should be users";
        EXPECT_EQ("name", column->ColumnName()) << "First column should be name";

        expr = projection->GetExpressions()[1];
        EXPECT_EQ(ExprType::Column, expr->Type()) << "Second expression should be column";
        column = expr->as<ColumnExpression>();
        EXPECT_EQ("orders", column->TableName()) << "Second column should be orders";
        EXPECT_EQ("amount", column->ColumnName()) << "Second column should be amount";
    }

    // validate schema
    auto schema = projection->OutputSchema();
    EXPECT_EQ(2, schema.FieldCount()) << "Schema should have two columns";
    EXPECT_EQ("name", schema.Fields()[0].name) << "First column should be name";
    EXPECT_EQ("amount", schema.Fields()[1].name) << "Second column should be amount";

    auto filter = projection->Children()[0]->as<LogicalFilterNode>();
    EXPECT_EQ(LogicalNodeType::Filter, filter->Type()) << "Second node should be filter";
    EXPECT_EQ("(orders.amount > 100)", filter->GetCondition()->ToString()) << "Filter condition should match";

    auto condition = filter->GetCondition();
    EXPECT_EQ(ExprType::BinaryOp, condition->Type()) << "Condition should be binary op";
    auto binary_op = condition->as<BinaryExpression>();
    EXPECT_EQ(BinaryOpType::Greater, binary_op->OpType()) << "Binary op should be greater";

    auto left_expr = binary_op->Left();
    EXPECT_EQ(ExprType::Column, left_expr->Type()) << "Left expression should be column";
    auto left_column = left_expr->as<ColumnExpression>();
    EXPECT_EQ("orders", left_column->TableName()) << "Left column should be orders";
    EXPECT_EQ("amount", left_column->ColumnName()) << "Left column should be amount";

    auto join = filter->Children()[0]->as<LogicalJoinNode>();
    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Third node should be join";
    EXPECT_EQ(JoinType::Inner, join->GetJoinType()) << "Should be inner join";
    EXPECT_EQ("(users.id = orders.user_id)", join->GetCondition()->ToString()) << "Join condition should match";

    EXPECT_EQ(2, join->Children().size()) << "Join should have two children";
    auto left_scan = join->Children()[0]->as<LogicalScanNode>();
    auto right_scan = join->Children()[1]->as<LogicalScanNode>();
    EXPECT_EQ("users", left_scan->TableName()) << "Left table should be users";
    EXPECT_EQ("orders", right_scan->TableName()) << "Right table should be orders";
}

//
// Test Setup:
//      Create a SELECT query with LEFT JOIN
// Test Result:
//      Verify the logical plan structure for left join
//
TEST_F(LogicalPlannerTest, LeftJoin) {
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "LEFT JOIN orders ON users.id = orders.user_id;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    auto projection = root->as<LogicalProjectionNode>();
    auto join = projection->Children()[0]->as<LogicalJoinNode>();

    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Should be join";
    EXPECT_EQ(JoinType::Left, join->GetJoinType()) << "Should be left join";
    EXPECT_EQ("(users.id = orders.user_id)", join->GetCondition()->ToString()) << "Join condition should match";

    auto condition = join->GetCondition();
    EXPECT_EQ(ExprType::BinaryOp, condition->Type()) << "Condition should be binary op";
    auto binary_op = condition->as<BinaryExpression>();
    EXPECT_EQ(BinaryOpType::Equal, binary_op->OpType()) << "Binary op should be equal";

    auto left_expr = binary_op->Left();
    EXPECT_EQ(ExprType::Column, left_expr->Type()) << "Left expression should be column";
    auto left_column = left_expr->as<ColumnExpression>();
    EXPECT_EQ("users", left_column->TableName()) << "Left column should be users";
    EXPECT_EQ("id", left_column->ColumnName()) << "Left column should be id";

    auto right_expr = binary_op->Right();
    EXPECT_EQ(ExprType::Column, right_expr->Type()) << "Right expression should be column";
    auto right_column = right_expr->as<ColumnExpression>();
    EXPECT_EQ("orders", right_column->TableName()) << "Right column should be orders";
    EXPECT_EQ("user_id", right_column->ColumnName()) << "Right column should be user_id";
}

//
// Test Setup:
//      Create a SELECT query with RIGHT JOIN
// Test Result:
//      Verify the logical plan structure for right join
//
TEST_F(LogicalPlannerTest, RightJoin) {
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "RIGHT JOIN orders ON users.id = orders.user_id;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    auto projection = root->as<LogicalProjectionNode>();
    auto join = projection->Children()[0]->as<LogicalJoinNode>();

    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Should be join";
    EXPECT_EQ(JoinType::Right, join->GetJoinType()) << "Should be right join";
    EXPECT_EQ("(users.id = orders.user_id)", join->GetCondition()->ToString()) << "Join condition should match";

    auto condition = join->GetCondition();
    EXPECT_EQ(ExprType::BinaryOp, condition->Type()) << "Condition should be binary op";
    auto binary_op = condition->as<BinaryExpression>();
    EXPECT_EQ(BinaryOpType::Equal, binary_op->OpType()) << "Binary op should be equal";

    auto left_expr = binary_op->Left();
    EXPECT_EQ(ExprType::Column, left_expr->Type()) << "Left expression should be column";
    auto left_column = left_expr->as<ColumnExpression>();
    EXPECT_EQ("users", left_column->TableName()) << "Left column should be users";
    EXPECT_EQ("id", left_column->ColumnName()) << "Left column should be id";

    auto right_expr = binary_op->Right();
    EXPECT_EQ(ExprType::Column, right_expr->Type()) << "Right expression should be column";
    auto right_column = right_expr->as<ColumnExpression>();
    EXPECT_EQ("orders", right_column->TableName()) << "Right column should be orders";
    EXPECT_EQ("user_id", right_column->ColumnName()) << "Right column should be user_id";
}

//
// Test Setup:
//      Create a SELECT query with FULL/OUTER JOIN
// Test Result:
//      Verify the logical plan structure for full join
//
TEST_F(LogicalPlannerTest, FullJoin) {
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "OUTER JOIN orders ON users.id = orders.user_id;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    auto projection = root->as<LogicalProjectionNode>();
    auto join = projection->Children()[0]->as<LogicalJoinNode>();

    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Should be join";
    EXPECT_EQ(JoinType::Full, join->GetJoinType()) << "Should be full join";
    EXPECT_EQ("(users.id = orders.user_id)", join->GetCondition()->ToString()) << "Join condition should match";

    auto condition = join->GetCondition();
    EXPECT_EQ(ExprType::BinaryOp, condition->Type()) << "Condition should be binary op";
    auto binary_op = condition->as<BinaryExpression>();
    EXPECT_EQ(BinaryOpType::Equal, binary_op->OpType()) << "Binary op should be equal";

    auto left_expr = binary_op->Left();
    auto right_expr = binary_op->Right();
    EXPECT_EQ(ExprType::Column, left_expr->Type()) << "Left expression should be column";
    EXPECT_EQ(ExprType::Column, right_expr->Type()) << "Right expression should be column";
    auto left_column = left_expr->as<ColumnExpression>();
    auto right_column = right_expr->as<ColumnExpression>();
    EXPECT_EQ("users", left_column->TableName()) << "Left column should be users";
    EXPECT_EQ("id", left_column->ColumnName()) << "Left column should be id";
    EXPECT_EQ("orders", right_column->TableName()) << "Right column should be orders";
    EXPECT_EQ("user_id", right_column->ColumnName()) << "Right column should be user_id";

    auto left_scan = join->Children()[0]->as<LogicalScanNode>();
    auto right_scan = join->Children()[1]->as<LogicalScanNode>();
    EXPECT_EQ("users", left_scan->TableName()) << "Left table should be users";
    EXPECT_EQ("orders", right_scan->TableName()) << "Right table should be orders";
}

//
// Test Setup:
//      Create a SELECT query with CROSS JOIN
// Test Result:
//      Verify that CROSS JOIN is not supported
//
TEST_F(LogicalPlannerTest, CrossJoinNotSupported) {
    const std::string query = "SELECT users.name AS user_name, orders.amount AS order_amount "
                              "FROM users "
                              "CROSS JOIN orders;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);

    EXPECT_FALSE(parse_result.isValid()) << "SQL parsing should fail";
}

//
// Test Setup:
//      Create a JOIN query with non-existent column
// Test Result:
//      Verify error handling for invalid join column
//
TEST_F(LogicalPlannerTest, InvalidJoinColumn) {
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.nonexistent = orders.user_id;";

    auto plan_result = PlanLogical(query, false);
    EXPECT_FALSE(plan_result.ok()) << "Planning should fail for nonexistent join column";
    EXPECT_EQ(ErrorCode::SchemaFieldNotFound, plan_result.error().code()) << "Should have column not found error";
}

//
// Test Setup:
//      Create a SELECT * query
// Test Result:
//      Verify the logical plan structure for selecting all columns
//
TEST_F(LogicalPlannerTest, SelectAll) {
    // Parse SQL
    const std::string query = "SELECT * FROM users;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ("users", projection->TableName()) << "Table name should be users";

    // Verify that all columns from the schema are included
    auto schema = projection->OutputSchema();
    EXPECT_EQ(4, schema.FieldCount()) << "Should have all columns from users table";
    EXPECT_EQ("id", schema.Fields()[0].name) << "First column should be id";
    EXPECT_EQ("name", schema.Fields()[1].name) << "Second column should be name";
    EXPECT_EQ("age", schema.Fields()[2].name) << "Third column should be age";
    EXPECT_EQ("salary", schema.Fields()[3].name) << "Fourth column should be salary";

    auto scan = projection->Children()[0]->as<LogicalScanNode>();
    EXPECT_EQ(LogicalNodeType::Scan, scan->Type()) << "Child should be scan";
    EXPECT_EQ("users", scan->TableName()) << "Should scan users table";
}

//
// Test Setup:
//      Create a SELECT * query with JOIN
// Test Result:
//      Verify the logical plan structure for selecting all columns with join
//
TEST_F(LogicalPlannerTest, SelectAllWithJoin) {
    const std::string query = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ("joined_users_orders", projection->TableName()) << "Table name should include both tables";

    // Verify that all columns from both tables are included
    auto schema = projection->OutputSchema();
    EXPECT_EQ(7, schema.FieldCount()) << "Should have all columns from both tables";
    EXPECT_EQ("id", schema.Fields()[0].name) << "First column should be id";
    EXPECT_EQ("name", schema.Fields()[1].name) << "Second column should be name";
    EXPECT_EQ("age", schema.Fields()[2].name) << "Third column should be age";
    EXPECT_EQ("salary", schema.Fields()[3].name) << "Fourth column should be salary";
    EXPECT_EQ("order_id", schema.Fields()[4].name) << "Fifth column should be order_id";
    EXPECT_EQ("user_id", schema.Fields()[5].name) << "Sixth column should be user_id";
    EXPECT_EQ("amount", schema.Fields()[6].name) << "Seventh column should be amount";

    auto join = projection->Children()[0]->as<LogicalJoinNode>();
    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Child should be join";
    EXPECT_EQ(JoinType::Inner, join->GetJoinType()) << "Should be inner join";
}

//
// Test Setup:
//      Create a SELECT query with aggregate functions
// Test Result:
//      Verify the logical plan structure for aggregation
//
TEST_F(LogicalPlannerTest, Aggregate) {
    const std::string query = "SELECT name, COUNT(*), SUM(salary), AVG(age) FROM users GROUP BY name;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ("users", projection->TableName()) << "Table name should be users";
    EXPECT_EQ(3, projection->GetExpressions().size()) << "Should have four projection expressions";

    // verify projection schema
    auto schema = projection->OutputSchema();
    EXPECT_EQ(4, schema.FieldCount()) << "Should have four output columns";
    EXPECT_EQ("name", schema.Fields()[0].name) << "First column should be name";
    EXPECT_EQ("count", schema.Fields()[1].name) << "Second column should be count";
    EXPECT_EQ("sum_salary", schema.Fields()[2].name) << "Third column should be sum_salary";
    EXPECT_EQ("avg_age", schema.Fields()[3].name) << "Fourth column should be avg_age";

    auto agg_node = projection->Children()[0]->as<LogicalAggregateNode>();
    EXPECT_EQ(LogicalNodeType::Aggregate, agg_node->Type()) << "Child should be aggregate";
    EXPECT_EQ(1, agg_node->GetGroupBy().size()) << "Should have one group by expression";
    EXPECT_EQ(3, agg_node->GetAggregates().size()) << "Should have three aggregate expressions";

    // verify output schema
    schema = agg_node->OutputSchema();
    EXPECT_EQ(4, schema.FieldCount()) << "Should have four output columns";
    EXPECT_EQ("name", schema.Fields()[0].name) << "First column should be name";
    EXPECT_EQ("count", schema.Fields()[1].name) << "Second column should be count";
    EXPECT_EQ("sum_salary", schema.Fields()[2].name) << "Third column should be sum_salary";
    EXPECT_EQ("avg_age", schema.Fields()[3].name) << "Fourth column should be avg_age";

    // Verify group by expression
    auto group_by = agg_node->GetGroupBy()[0];
    EXPECT_EQ(ExprType::Column, group_by->Type()) << "Group by should be column";
    EXPECT_EQ("name", group_by->as<ColumnExpression>()->ColumnName()) << "Group by should be name column";

    // Verify aggregate expressions
    {
        auto count_expr = agg_node->GetAggregates()[0]->as<AggregateExpression>();
        EXPECT_EQ(AggregateType::Count, count_expr->AggType()) << "First aggregate should be COUNT";

        auto sum_expr = agg_node->GetAggregates()[1]->as<AggregateExpression>();
        EXPECT_EQ(AggregateType::Sum, sum_expr->AggType()) << "Second aggregate should be SUM";
        EXPECT_EQ("salary", sum_expr->Input()->as<ColumnExpression>()->ColumnName()) << "SUM should be on salary";

        auto avg_expr = agg_node->GetAggregates()[2]->as<AggregateExpression>();
        EXPECT_EQ(AggregateType::Avg, avg_expr->AggType()) << "Third aggregate should be AVG";
        EXPECT_EQ("age", avg_expr->Input()->as<ColumnExpression>()->ColumnName()) << "AVG should be on age";
    }

    auto scan = agg_node->Children()[0]->as<LogicalScanNode>();
    EXPECT_EQ(LogicalNodeType::Scan, scan->Type()) << "Leaf should be scan";
    EXPECT_EQ("users", scan->TableName()) << "Should scan users table";
}

//
// Test Setup:
//      Create a SELECT query with ORDER BY
// Test Result:
//      Verify the logical plan structure for sorting
//
TEST_F(LogicalPlannerTest, Sort) {
    const std::string query = "SELECT name, age FROM users ORDER BY age DESC, name ASC;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto projection = root->as<LogicalProjectionNode>();
    EXPECT_EQ("users", projection->TableName()) << "Table name should be users";
    EXPECT_EQ(2, projection->GetExpressions().size()) << "Should have two projection expressions";

    {
        auto name_expr = projection->GetExpressions()[0]->as<ColumnExpression>();
        EXPECT_EQ("name", name_expr->ColumnName()) << "First projection should be name";

        auto age_expr = projection->GetExpressions()[1]->as<ColumnExpression>();
        EXPECT_EQ("age", age_expr->ColumnName()) << "Second projection should be age";
    }

    auto sort_node = projection->Children()[0]->as<LogicalSortNode>();
    EXPECT_EQ(LogicalNodeType::Sort, sort_node->Type()) << "Child should be sort";
    EXPECT_EQ(2, sort_node->GetSortSpecs().size()) << "Should have two sort specifications";

    // Verify sort specifications
    {
        const auto& first_spec = sort_node->GetSortSpecs()[0];
        EXPECT_EQ(ExprType::Column, first_spec.expr->Type()) << "First sort expr should be column";
        EXPECT_EQ("age", first_spec.expr->as<ColumnExpression>()->ColumnName()) << "First sort should be age";
        EXPECT_EQ(SortDirection::Descending, first_spec.direction) << "First sort should be descending";

        const auto& second_spec = sort_node->GetSortSpecs()[1];
        EXPECT_EQ(ExprType::Column, second_spec.expr->Type()) << "Second sort expr should be column";
        EXPECT_EQ("name", second_spec.expr->as<ColumnExpression>()->ColumnName()) << "Second sort should be name";
        EXPECT_EQ(SortDirection::Ascending, second_spec.direction) << "Second sort should be ascending";
    }

    auto scan = sort_node->Children()[0]->as<LogicalScanNode>();
    EXPECT_EQ(LogicalNodeType::Scan, scan->Type()) << "Leaf should be scan";
    EXPECT_EQ("users", scan->TableName()) << "Should scan users table";
}

//
// Test Setup:
//      Create a SELECT query with LIMIT and OFFSET
// Test Result:
//      Verify the logical plan structure for limit and offset
//
TEST_F(LogicalPlannerTest, Limit) {
    const std::string query = "SELECT name FROM users LIMIT 10 OFFSET 5;";
    auto plan_result = PlanLogical(query, false);
    VERIFY_RESULT(plan_result);

    auto root = plan_result.value();
    EXPECT_EQ(LogicalNodeType::Limit, root->Type()) << "Root should be limit";

    auto limit_node = root->as<LogicalLimitNode>();
    EXPECT_EQ(10, limit_node->Limit()) << "Limit should be 10";
    EXPECT_EQ(5, limit_node->Offset()) << "Offset should be 5";
}

}  // namespace pond::query