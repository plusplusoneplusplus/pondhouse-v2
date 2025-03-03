#include <memory>

#include "query/planner/logical_plan_printer.h"
#include "query/planner/logical_planner.h"
#include "query_test_helper.h"
#include "SQLParser.h"
#include "test_helper.h"

using namespace pond::catalog;
using namespace pond::query;

namespace pond::query {

class LogicalOptimizerTest : public ::testing::Test {
public:
    void SetUp() override {
        context_ = std::make_unique<QueryTestContext>("test_catalog");
        context_->SetupUsersTable();
        context_->SetupOrdersTable();
    }

    Result<std::shared_ptr<LogicalPlanNode>> PlanLogical(const std::string& query, bool optimize = false) {
        return context_->PlanLogical(query, optimize);
    }

    Result<std::shared_ptr<LogicalPlanNode>> Optimize(std::shared_ptr<LogicalPlanNode> plan) {
        return context_->Optimize(plan);
    }

    Catalog* catalog() { return context_->catalog_.get(); }

    std::unique_ptr<QueryTestContext> context_;
};

TEST_F(LogicalOptimizerTest, PredicatePushdown) {
    LOG_STATUS("Running testLogicalPlanPredicatePushdown...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyPredicatePushdown());

    // Query with filter that can be pushed down
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id "
                              "WHERE users.age > 30;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    auto root = optimized_result.value();

    // expected plan:
    // Projection(users: name, orders.amount)
    //   Join, Inner(users, orders)
    //     Filter(users.age > 30)
    //       Scan(users)
    //     Scan(orders)

    // Verify root is projection
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";
    EXPECT_EQ(1, root->Children().size()) << "Projection should have one child";

    // Verify join under projection
    auto join = root->Children()[0];
    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Child of projection should be join";
    EXPECT_EQ(JoinType::Inner, join->as<LogicalJoinNode>()->GetJoinType()) << "Join should be inner";
    EXPECT_EQ(2, join->Children().size()) << "Join should have two children";

    // Verify filter under left side of join
    auto left = join->Children()[0];
    EXPECT_EQ(LogicalNodeType::Filter, left->Type()) << "Left child of join should be filter";
    EXPECT_EQ("(users.age > 30)", left->as<LogicalFilterNode>()->GetCondition()->ToString())
        << "Filter condition should match";
    EXPECT_EQ(1, left->Children().size()) << "Filter should have one child";

    // Verify scan under filter
    auto users_scan = left->Children()[0];
    EXPECT_EQ(LogicalNodeType::Scan, users_scan->Type()) << "Child of filter should be scan";
    EXPECT_EQ("users", users_scan->as<LogicalScanNode>()->TableName()) << "Left scan should be users table";
    EXPECT_EQ(0, users_scan->Children().size()) << "Scan should have no children";

    // Verify scan on right side of join
    auto orders_scan = join->Children()[1];
    EXPECT_EQ(LogicalNodeType::Scan, orders_scan->Type()) << "Right child of join should be scan";
    EXPECT_EQ("orders", orders_scan->as<LogicalScanNode>()->TableName()) << "Right scan should be orders table";
    EXPECT_EQ(0, orders_scan->Children().size()) << "Scan should have no children";
}

TEST_F(LogicalOptimizerTest, JoinPredicatePushdown) {
    LOG_STATUS("Running testLogicalPlanJoinPredicatePushdown...");

    // Test case 1: Simple join predicate pushdown
    {
        const std::string query = "SELECT * FROM users JOIN orders ON users.id = orders.user_id "
                                  "WHERE users.age > 30 AND orders.amount > 200.0";
        auto optimized_result = PlanLogical(query, true /* optimize */);
        VERIFY_RESULT(optimized_result);

        std::cout << "Plan result: " << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

        auto plan = optimized_result.value();
        EXPECT_EQ(LogicalNodeType::Projection, plan->Type()) << "Root should be a projection node";

        auto projection = plan->as<LogicalProjectionNode>();
        EXPECT_EQ(1, projection->Children().size()) << "Projection should have one child";

        auto join = projection->Children()[0];
        EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Child of projection should be a join node";

        // Verify that predicates are pushed down to respective tables
        auto join_node = join->as<LogicalJoinNode>();
        EXPECT_NE(nullptr, join_node) << "Should be a join node";

        EXPECT_EQ(2, join_node->Children().size()) << "Should have two children";

        // Check left child (users table)
        auto left_child = join_node->Children()[0];
        EXPECT_EQ(LogicalNodeType::Filter, left_child->Type()) << "Left child should be a filter";
        auto left_filter = left_child->as<LogicalFilterNode>();
        EXPECT_EQ("(users.age > 30)", left_filter->GetCondition()->ToString())
            << "Left predicate should be pushed down";

        // Check right child (orders table)
        auto right_child = join_node->Children()[1];
        EXPECT_EQ(LogicalNodeType::Filter, right_child->Type()) << "Right child should be a filter";
        auto right_filter = right_child->as<LogicalFilterNode>();
        EXPECT_EQ("(orders.amount > 200.000000)", right_filter->GetCondition()->ToString())
            << "Right predicate should be pushed down";
    }

    // Test case 2: Complex conjunctive predicates
    {
        const std::string query = "SELECT * FROM users JOIN orders ON users.id = orders.user_id "
                                  "WHERE users.age > 30 AND orders.amount > 200.0 AND users.salary < 75000.0 "
                                  "AND orders.order_id < 1000 AND (users.id = orders.user_id OR users.name = 'John')";
        auto result = PlanLogical(query, false /* optimize */);
        VERIFY_RESULT(result);

        std::cout << "Before optimization: " << GetLogicalPlanUserFriendlyString(*result.value()) << std::endl;

        LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyPredicatePushdown());
        auto optimized_result = optimizer.Optimize(result.value());
        VERIFY_RESULT(optimized_result);

        auto plan = optimized_result.value();

        std::cout << "After optimization: " << GetLogicalPlanUserFriendlyString(*plan) << std::endl;

        EXPECT_EQ(LogicalNodeType::Projection, plan->Type()) << "Root should be a projection node";

        auto projection = plan->as<LogicalProjectionNode>();
        EXPECT_EQ(1, projection->Children().size()) << "Projection should have one child";

        auto filter = projection->Children()[0];
        EXPECT_EQ(LogicalNodeType::Filter, filter->Type()) << "Child of projection should be a filter node";

        auto filter_node = filter->as<LogicalFilterNode>();
        EXPECT_EQ("((users.id = orders.user_id) OR (users.name = 'John'))", filter_node->GetCondition()->ToString())
            << "Filter condition should match";

        auto join = filter->Children()[0];
        EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Child of filter should be a join node";

        auto join_node = join->as<LogicalJoinNode>();
        EXPECT_NE(nullptr, join_node) << "Should be a join node";

        // Verify join condition includes the OR condition
        EXPECT_EQ("(users.id = orders.user_id)", join_node->GetCondition()->ToString())
            << "Join condition should include the OR predicate";

        EXPECT_EQ(2, join_node->Children().size()) << "Should have two children";

        // Check left child predicates (users table)
        auto left_child = join_node->Children()[0];
        EXPECT_EQ(LogicalNodeType::Filter, left_child->Type()) << "Left child should be a filter";
        auto left_filter = left_child->as<LogicalFilterNode>();
        EXPECT_EQ("((users.age > 30) AND (users.salary < 75000.000000))", left_filter->GetCondition()->ToString())
            << "Left predicates should be pushed down and combined";

        // Check right child predicates (orders table)
        auto right_child = join_node->Children()[1];
        EXPECT_EQ(LogicalNodeType::Filter, right_child->Type()) << "Right child should be a filter";
        auto right_filter = right_child->as<LogicalFilterNode>();
        EXPECT_EQ("((orders.amount > 200.000000) AND (orders.order_id < 1000))",
                  right_filter->GetCondition()->ToString())
            << "Right predicates should be pushed down and combined";
    }
}

TEST_F(LogicalOptimizerTest, ColumnPruning) {
    LOG_STATUS("Running testLogicalPlanColumnPruning...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyColumnPruning());

    // Query that only needs some columns
    const std::string query = "SELECT name FROM users WHERE age > 30;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    auto root = optimized_result.value();
    auto scan = root->Children()[0]->Children()[0]->as<LogicalScanNode>();

    // Verify only required columns are in scan schema
    auto schema = scan->OutputSchema();
    EXPECT_EQ(2, schema.FieldCount()) << "Should only have required columns";
    EXPECT_EQ("name", schema.Fields()[0].name) << "Should have name column";
    EXPECT_EQ("age", schema.Fields()[1].name) << "Should have age column for filter";
}

TEST_F(LogicalOptimizerTest, ConstantFolding) {
    LOG_STATUS("Running testLogicalPlanConstantFolding...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyConstantFolding());

    // Query with constant expression in filter
    const std::string query = "SELECT name FROM users WHERE age > (20 + 10);";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    auto root = optimized_result.value();
    auto filter = root->Children()[0]->as<LogicalFilterNode>();
    auto condition = filter->GetCondition();

    EXPECT_EQ("(age > 30)", condition->ToString()) << "Condition should be folded";

    EXPECT_EQ(ExprType::BinaryOp, condition->Type()) << "Condition should be binary op";
    auto binary_op = condition->as<BinaryExpression>();
    EXPECT_EQ(BinaryOpType::Greater, binary_op->OpType()) << "Binary op should be greater";
}

TEST_F(LogicalOptimizerTest, JoinReordering) {
    LOG_STATUS("Running testLogicalPlanJoinReordering...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions{
        .enable_predicate_pushdown = true,
        .enable_column_pruning = false,
        .enable_constant_folding = false,
        .enable_join_reordering = true,
        .enable_projection_pushdown = false,
    });

    // Query with multiple joins
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id "
                              "WHERE orders.amount < 100;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    auto root = optimized_result.value();
    auto join = root->Children()[0]->as<LogicalJoinNode>();

    // Verify orders table (with filter) is on left side of join
    auto left = join->Children()[0];
    EXPECT_EQ(LogicalNodeType::Filter, left->Type()) << "Left child should be filtered orders";
    auto filter = left->as<LogicalFilterNode>();

    auto condition = filter->GetCondition();
    EXPECT_EQ("(orders.amount < 100)", condition->ToString()) << "Filter condition should match";
    EXPECT_EQ(ExprType::BinaryOp, condition->Type()) << "Condition should be binary op";

    auto binary_op = condition->as<BinaryExpression>();
    EXPECT_EQ(BinaryOpType::Less, binary_op->OpType()) << "Binary op should be less";

    auto left_expr = binary_op->Left();
    EXPECT_EQ(ExprType::Column, left_expr->Type()) << "Left expression should be column";
    auto left_column = left_expr->as<ColumnExpression>();
    EXPECT_EQ("orders", left_column->TableName()) << "Orders table name should match";
    EXPECT_EQ("amount", left_column->ColumnName()) << "Orders amount column name should match";

    auto right_expr = binary_op->Right();
    EXPECT_EQ(ExprType::Constant, right_expr->Type()) << "Right expression should be constant";
    auto right_constant = right_expr->as<ConstantExpression>();
    EXPECT_EQ(common::ColumnType::INT64, right_constant->GetColumnType()) << "Right constant should be integer";
    EXPECT_EQ("100", right_constant->Value()) << "Right constant should be 100";
    EXPECT_EQ(100, right_constant->GetInteger()) << "Right constant should be 100";
}

TEST_F(LogicalOptimizerTest, ConjunctivePredicatePushdown) {
    LOG_STATUS("Running testLogicalPlanConjunctivePredicatePushdown...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions{
        .enable_projection_pushdown = false,
    });

    // Query with conjunctive predicates that can be pushed to different sides
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id "
                              "WHERE users.age > 30 AND orders.amount < 100;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    auto root = optimized_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto join = root->Children()[0]->as<LogicalJoinNode>();
    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Second node should be join";

    // Verify that predicates were pushed to both sides
    auto left_child = join->Children()[0];
    auto right_child = join->Children()[1];

    EXPECT_EQ(LogicalNodeType::Filter, left_child->Type()) << "Left child should be filter";
    EXPECT_EQ(LogicalNodeType::Filter, right_child->Type()) << "Right child should be filter";

    auto left_filter = left_child->as<LogicalFilterNode>();
    auto right_filter = right_child->as<LogicalFilterNode>();

    EXPECT_EQ("(users.age > 30)", left_filter->GetCondition()->ToString()) << "Left filter condition should match";
    EXPECT_EQ("(orders.amount < 100)", right_filter->GetCondition()->ToString())
        << "Right filter condition should match";

    // Verify the filter conditions
    {
        auto left_condition = left_filter->GetCondition();
        EXPECT_EQ(ExprType::BinaryOp, left_condition->Type()) << "Left condition should be binary op";
        auto left_binary = left_condition->as<BinaryExpression>();
        EXPECT_EQ(BinaryOpType::Greater, left_binary->OpType()) << "Left binary op should be greater";

        auto right_condition = right_filter->GetCondition();
        EXPECT_EQ(ExprType::BinaryOp, right_condition->Type()) << "Right condition should be binary op";
        auto right_binary = right_condition->as<BinaryExpression>();
        EXPECT_EQ(BinaryOpType::Less, right_binary->OpType()) << "Right binary op should be less";
    }

    // Verify scan nodes are present under filters
    auto left_scan = left_filter->Children()[0]->as<LogicalScanNode>();
    auto right_scan = right_filter->Children()[0]->as<LogicalScanNode>();

    EXPECT_EQ("users", left_scan->TableName()) << "Left scan should be users";
    EXPECT_EQ("orders", right_scan->TableName()) << "Right scan should be orders";
}

TEST_F(LogicalOptimizerTest, ColumnPruningWithJoin) {
    LOG_STATUS("Running testLogicalPlanColumnPruningWithJoin...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyColumnPruning());

    // Query that only needs specific columns from both tables
    // Note: users table has (id, name, age, salary)
    // orders table has (order_id, user_id, amount)
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    // Verify the optimized plan structure
    auto root = optimized_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto join = root->Children()[0]->as<LogicalJoinNode>();
    EXPECT_EQ(LogicalNodeType::Join, join->Type()) << "Second node should be join";

    // Verify left side (users table) schema
    auto left_scan = join->Children()[0]->as<LogicalScanNode>();
    EXPECT_EQ("users", left_scan->TableName()) << "Left scan should be users table";
    auto left_schema = left_scan->OutputSchema();
    EXPECT_EQ(2, left_schema.FieldCount()) << "Users scan should only have required columns";
    EXPECT_EQ("id", left_schema.Fields()[0].name) << "First column should be id (needed for join)";
    EXPECT_EQ("name", left_schema.Fields()[1].name) << "Second column should be name (needed for projection)";

    // Verify right side (orders table) schema
    auto right_scan = join->Children()[1]->as<LogicalScanNode>();
    EXPECT_EQ("orders", right_scan->TableName()) << "Right scan should be orders table";
    auto right_schema = right_scan->OutputSchema();
    EXPECT_EQ(2, right_schema.FieldCount()) << "Orders scan should only have required columns";
    EXPECT_EQ("user_id", right_schema.Fields()[0].name) << "First column should be user_id (needed for join)";
    EXPECT_EQ("amount", right_schema.Fields()[1].name) << "Second column should be amount (needed for projection)";

    // Verify join output schema
    auto join_schema = join->OutputSchema();
    EXPECT_EQ(4, join_schema.FieldCount()) << "Join should output all required columns from both sides";
}

TEST_F(LogicalOptimizerTest, LikeConstantFolding) {
    LOG_STATUS("Running testLogicalPlanLikeConstantFolding...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyConstantFolding());

    // Test cases for LIKE constant folding
    const std::vector<std::pair<std::string, bool>> test_cases = {
        // Basic pattern matching
        {"SELECT * FROM users WHERE 'John Smith' LIKE '%Smith%';", true},
        {"SELECT * FROM users WHERE 'John Doe' LIKE '%Smith%';", false},
        // Start/end anchors
        {"SELECT * FROM users WHERE 'test' LIKE 'test';", true},
        {"SELECT * FROM users WHERE 'test123' LIKE 'test%';", true},
        {"SELECT * FROM users WHERE '123test' LIKE '%test';", true},
        // Single character wildcard
        {"SELECT * FROM users WHERE 'test' LIKE 't_st';", true},
        {"SELECT * FROM users WHERE 'test' LIKE '_est';", true},
        {"SELECT * FROM users WHERE 'test' LIKE 't__t';", true},
        {"SELECT * FROM users WHERE 'test' LIKE 't___t';", false},
        // Complex patterns
        {"SELECT * FROM users WHERE 'abc123xyz' LIKE '%123%xyz';", true},
        {"SELECT * FROM users WHERE 'abc123xyz' LIKE '_bc%xyz';", true}};

    for (const auto& [query, expected_result] : test_cases) {
        hsql::SQLParserResult parse_result;
        hsql::SQLParser::parse(query, &parse_result);
        EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

        auto plan_result = planner.Plan(parse_result.getStatement(0));
        EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

        auto optimized_result = optimizer.Optimize(plan_result.value());
        EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

        // The optimized plan should have folded the LIKE condition into a constant true/false
        auto root = optimized_result.value();
        auto filter = root->Children()[0]->as<LogicalFilterNode>();
        auto condition = filter->GetCondition();
        EXPECT_EQ(ExprType::Constant, condition->Type())
            << "LIKE condition should be folded to constant for query: " + query;

        auto constant = condition->as<ConstantExpression>();
        EXPECT_EQ(common::ColumnType::BOOLEAN, constant->GetColumnType())
            << "Folded constant should be boolean for query: " + query;
        EXPECT_EQ(expected_result, constant->GetBoolean())
            << "LIKE pattern match result should be correct for query: " + query;
    }
}

TEST_F(LogicalOptimizerTest, ProjectionPushdown) {
    LOG_STATUS("Running testLogicalPlanProjectionPushdown...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyProjectionPushdown());

    // Test cases for projection pushdown
    const std::vector<std::string> test_cases = {
        // Push through filter
        "SELECT name FROM users WHERE age > 30;",
        // Push through join
        "SELECT users.name FROM users INNER JOIN orders ON users.id = orders.user_id;",
        // Push to both sides of join
        "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id;"};

    for (const auto& query : test_cases) {
        hsql::SQLParserResult parse_result;
        hsql::SQLParser::parse(query, &parse_result);
        EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed for query: " + query;

        auto plan_result = planner.Plan(parse_result.getStatement(0));
        EXPECT_TRUE(plan_result.ok()) << "Planning should succeed for query: " + query;

        std::cout << "Original plan for query: " << query << std::endl;
        std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

        auto optimized_result = optimizer.Optimize(plan_result.value());
        EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed for query: " + query;

        std::cout << "Optimized plan:" << std::endl;
        std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

        // Verify that projections are pushed down appropriately
        auto root = optimized_result.value();
        switch (root->Type()) {
            case LogicalNodeType::Join: {
                // For join queries, verify projections are pushed to appropriate sides
                auto join = root->as<LogicalJoinNode>();
                auto left = join->Children()[0];
                auto right = join->Children()[1];

                // Check if projections are pushed down
                if (query.find("users.name") != std::string::npos && query.find("orders.amount") == std::string::npos) {
                    // Only users.name is selected, should be pushed to left side
                    EXPECT_EQ(LogicalNodeType::Projection, left->Type())
                        << "Left child should be projection for query: " + query;
                    EXPECT_EQ(LogicalNodeType::Scan, right->Type()) << "Right child should be scan for query: " + query;
                } else if (query.find("users.name") != std::string::npos
                           && query.find("orders.amount") != std::string::npos) {
                    // Both sides have columns, should have projections on both sides
                    EXPECT_EQ(LogicalNodeType::Projection, left->Type())
                        << "Left child should be projection for query: " + query;
                    EXPECT_EQ(LogicalNodeType::Projection, right->Type())
                        << "Right child should be projection for query: " + query;
                }
                break;
            }
            case LogicalNodeType::Filter: {
                // For filter queries, verify projection is pushed below filter
                auto filter = root->as<LogicalFilterNode>();
                auto child = filter->Children()[0];
                EXPECT_EQ(LogicalNodeType::Projection, child->Type())
                    << "Filter's child should be projection for query: " + query;
                break;
            }
            case LogicalNodeType::Projection: {
                // For nested projections, verify they are combined
                auto child = root->Children()[0];
                EXPECT_NE(LogicalNodeType::Projection, child->Type())
                    << "Consecutive projections should be combined for query: " + query;
                break;
            }
            default:
                break;
        }
    }
}

TEST_F(LogicalOptimizerTest, MultipleOptimizations) {
    LOG_STATUS("Running testLogicalPlanMultipleOptimizations...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions{});

    // Query that can benefit from multiple optimizations
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id "
                              "WHERE users.age > (20 + 10) AND orders.amount < 100;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    // Create and optimize plan
    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "original plan: " << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "optimized plan: " << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    auto root = optimized_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    // Verify column pruning
    auto schema = root->OutputSchema();
    EXPECT_EQ(2, schema.FieldCount()) << "Should only have required columns";
    EXPECT_EQ("name", schema.Fields()[0].name) << "Should have name column";
    EXPECT_EQ("amount", schema.Fields()[1].name) << "Should have amount column";

    auto children = root->Children();
    EXPECT_EQ(1, children.size()) << "Root should have one child";
    EXPECT_EQ(LogicalNodeType::Join, children[0]->Type()) << "Child should be join";

    // Verify join reordering and filter pushdown
    auto join = children[0]->as<LogicalJoinNode>();
    auto left = join->Children()[0];
    auto right = join->Children()[1];

    // Orders table with filter should be on left
    EXPECT_EQ(LogicalNodeType::Filter, left->Type()) << "Left child should be filtered users";
    auto users_filter = left->as<LogicalFilterNode>();
    EXPECT_EQ("(users.age > 30)", users_filter->GetCondition()->ToString()) << "Users filter condition should match";

    // Orders table with filter should be on right
    EXPECT_EQ(LogicalNodeType::Filter, right->Type()) << "Right child should be filtered orders";
    auto orders_filter = right->as<LogicalFilterNode>();
    EXPECT_EQ("(orders.amount < 100)", orders_filter->GetCondition()->ToString())
        << "Orders filter condition should match";
}

TEST_F(LogicalOptimizerTest, Printer) {
    LOG_STATUS("Running testLogicalPlanPrinter...");

    LogicalPlanner planner(*catalog());

    // Test a complex query that exercises all node types
    const std::string query = "SELECT users.name, orders.amount "
                              "FROM users "
                              "INNER JOIN orders ON users.id = orders.user_id "
                              "WHERE users.age > 30;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    // Print the plan using the visitor
    LogicalPlanPrinter printer;
    plan_result.value()->Accept(printer);
    std::string printed_plan = printer.ToString();

    std::cout << "Printed plan using visitor:" << std::endl;
    std::cout << printed_plan << std::endl;

    // Verify the printed plan contains all expected components
    EXPECT_TRUE(printed_plan.find("Projection(") != std::string::npos) << "Should contain projection";
    EXPECT_TRUE(printed_plan.find("Join(type=INNER") != std::string::npos) << "Should contain join";
    EXPECT_TRUE(printed_plan.find("Filter(") != std::string::npos) << "Should contain filter";
    EXPECT_TRUE(printed_plan.find("Scan(users") != std::string::npos) << "Should contain users scan";
    EXPECT_TRUE(printed_plan.find("Scan(orders") != std::string::npos) << "Should contain orders scan";
}

TEST_F(LogicalOptimizerTest, LimitPushdown) {
    LOG_STATUS("Running testLogicalPlanLimitPushdown...");

    LogicalPlanner planner(*catalog());
    LogicalOptimizer optimizer(LogicalOptimizerOptions::OnlyLimitPushdown());

    // Query with limit that can be pushed down through projection
    const std::string query = "SELECT name FROM users LIMIT 10;";

    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    EXPECT_TRUE(parse_result.isValid()) << "SQL parsing should succeed";

    auto plan_result = planner.Plan(parse_result.getStatement(0));
    EXPECT_TRUE(plan_result.ok()) << "Planning should succeed";

    std::cout << "Original plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*plan_result.value()) << std::endl;

    auto optimized_result = optimizer.Optimize(plan_result.value());
    EXPECT_TRUE(optimized_result.ok()) << "Optimization should succeed";

    std::cout << "Optimized plan:" << std::endl;
    std::cout << GetLogicalPlanUserFriendlyString(*optimized_result.value()) << std::endl;

    // Verify that limit was pushed below projection
    auto root = optimized_result.value();
    EXPECT_EQ(LogicalNodeType::Projection, root->Type()) << "Root should be projection";

    auto proj = root->as<LogicalProjectionNode>();
    auto child = proj->Children()[0];
    EXPECT_EQ(LogicalNodeType::Limit, child->Type()) << "Child should be limit";

    auto limit_node = child->as<LogicalLimitNode>();
    EXPECT_EQ(10, limit_node->Limit()) << "Limit should be 10";
    EXPECT_EQ(0, limit_node->Offset()) << "Offset should be 0";
}

}  // namespace pond::query
