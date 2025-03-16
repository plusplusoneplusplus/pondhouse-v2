#include "query/planner/physical_planner.h"

#include <memory>

#include "query/query_test_context.h"
#include "test_helper.h"

using namespace pond::catalog;

namespace pond::query {

class PhysicalPlannerTest : public QueryTestContext {
public:
    PhysicalPlannerTest() : QueryTestContext("test_catalog") {}
};

TEST_F(PhysicalPlannerTest, PhysicalPlanBasicSelect) {
    LOG_STATUS("Running testPhysicalPlanBasicSelect...");

    // Parse SQL
    const std::string query = "SELECT name, age FROM users WHERE age > 30";

    auto physical_plan = PlanPhysical(query, true /* optimize */);
    VERIFY_RESULT(physical_plan);

    // Verify physical plan structure
    auto root = physical_plan.value();
    ASSERT_EQ(root->Type(), PhysicalNodeType::SequentialScan);

    auto scan = root->as<PhysicalSequentialScanNode>();
    ASSERT_EQ(scan->TableName(), "users");

    auto children = scan->Children();
    ASSERT_EQ(children.size(), 0);

    auto filter = scan->Predicate();
    ASSERT_EQ(filter->ToString(), "(age > 30)");

    auto projection_schema_opt = scan->ProjectionSchema();
    ASSERT_TRUE(projection_schema_opt.has_value());
    auto projection_schema = projection_schema_opt.value();
    ASSERT_EQ(projection_schema.FieldCount(), 2);
    ASSERT_EQ(projection_schema.Fields()[0].name, "name");
    ASSERT_EQ(projection_schema.Fields()[1].name, "age");
}

TEST_F(PhysicalPlannerTest, PhysicalPlanPrinter) {
    LOG_STATUS("Running testPhysicalPlanPrinter...");

    // Parse SQL
    const std::string query = "SELECT users.name, SUM(orders.amount) as total FROM users "
                              "JOIN orders ON users.id = orders.user_id "
                              "WHERE orders.amount > 100 "
                              "GROUP BY users.name "
                              "HAVING SUM(orders.amount) > 1000 "
                              "ORDER BY total DESC "
                              "LIMIT 10";

    auto physical_plan = PlanPhysical(query, true /* optimize */);
    VERIFY_RESULT(physical_plan);

    // Print the physical plan
    std::string plan_str = GetPhysicalPlanUserFriendlyString(*physical_plan.value());
    std::cout << plan_str << std::endl;

    // Verify the printed plan contains expected components
    ASSERT_TRUE(plan_str.find("Limit") != std::string::npos);
    ASSERT_TRUE(plan_str.find("Sort") != std::string::npos);
    ASSERT_TRUE(plan_str.find("HashAggregate") != std::string::npos);
    ASSERT_TRUE(plan_str.find("HashJoin") != std::string::npos);
    ASSERT_TRUE(plan_str.find("Filter") != std::string::npos);
    ASSERT_TRUE(plan_str.find("SequentialScan") != std::string::npos);
}

TEST_F(PhysicalPlannerTest, PredicatePushdown) {
    LOG_STATUS("Running testPredicatePushdown...");

    // Parse SQL with multiple predicates
    const std::string query = "SELECT * FROM users WHERE age > 30 AND salary < 100000";

    auto logical_plan = PlanLogical(query, true /* optimize */);
    VERIFY_RESULT(logical_plan);

    std::cout << "Logical plan: " << GetLogicalPlanUserFriendlyString(*logical_plan.value()) << std::endl;

    auto physical_plan = PlanPhysical(query, true /* optimize */);
    VERIFY_RESULT(physical_plan);

    std::cout << "Physical plan: " << GetPhysicalPlanUserFriendlyString(*physical_plan.value()) << std::endl;

    // Verify physical plan structure
    auto root = physical_plan.value();

    // The root should be a sequential scan since predicates should be pushed down
    ASSERT_EQ(root->Type(), PhysicalNodeType::SequentialScan);

    auto scan = root->as<PhysicalSequentialScanNode>();

    // Verify predicate was pushed down
    ASSERT_TRUE(scan->Predicate() != nullptr);

    // Check that the predicate contains both conditions
    std::string predicate_str = scan->Predicate()->ToString();
    ASSERT_TRUE(predicate_str.find("age > 30") != std::string::npos);
    ASSERT_TRUE(predicate_str.find("salary < 100000") != std::string::npos);
}

TEST_F(PhysicalPlannerTest, ProjectionPushdown) {
    LOG_STATUS("Running testProjectionPushdown...");

    // Parse SQL with specific column projections
    const std::string query = "SELECT name, age FROM users";

    auto physical_plan = PlanPhysical(query, true /* optimize */);
    VERIFY_RESULT(physical_plan);

    // Print the physical plan
    std::string plan_str = GetPhysicalPlanUserFriendlyString(*physical_plan.value());
    std::cout << plan_str << std::endl;

    // Verify physical plan structure
    auto root = physical_plan.value();

    // The root should be a sequential scan since projections should be pushed down
    ASSERT_EQ(root->Type(), PhysicalNodeType::SequentialScan);

    auto scan = root->as<PhysicalSequentialScanNode>();

    // Verify projection was pushed down
    auto projection_schema_opt = scan->ProjectionSchema();
    ASSERT_TRUE(projection_schema_opt.has_value());
    auto projection_schema = projection_schema_opt.value();
    ASSERT_EQ(2, projection_schema.FieldCount());
    ASSERT_EQ("name", projection_schema.Fields()[0].name);
    ASSERT_EQ("age", projection_schema.Fields()[1].name);
}

TEST_F(PhysicalPlannerTest, PhysicalPlanHashJoin) {
    LOG_STATUS("Running testPhysicalPlanHashJoin...");

    // Parse SQL with inner join
    const std::string query = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";

    auto logical_plan = PlanLogical(query, true /* optimize */);
    VERIFY_RESULT(logical_plan);

    std::cout << "Logical plan: " << GetLogicalPlanUserFriendlyString(*logical_plan.value()) << std::endl;

    auto physical_plan = PlanPhysical(query, true /* optimize */);
    VERIFY_RESULT(physical_plan);

    // Verify physical plan structure
    auto root = physical_plan.value();

    std::cout << "Physical plan: " << GetPhysicalPlanUserFriendlyString(*root) << std::endl;

    // The root should be a hash join node
    ASSERT_EQ(root->Type(), PhysicalNodeType::Projection);

    auto projection = root->as<PhysicalProjectionNode>();

    // verify projection schema
    ASSERT_EQ(7, projection->OutputSchema().FieldCount());

    auto children = projection->Children();
    ASSERT_EQ(1, children.size());

    auto child = children[0];
    ASSERT_EQ(child->Type(), PhysicalNodeType::HashJoin);

    auto hash_join = child->as<PhysicalHashJoinNode>();

    // Verify join condition
    auto join_condition = hash_join->Condition();
    ASSERT_TRUE(join_condition != nullptr);
    ASSERT_TRUE(join_condition->ToString().find("users.id = orders.user_id") != std::string::npos);

    // Verify output schema includes all columns from both tables
    auto output_schema = hash_join->OutputSchema();
    ASSERT_EQ(7, output_schema.FieldCount());

    // Verify child nodes are sequential scans
    ASSERT_EQ(2, hash_join->Children().size());

    auto left_scan = hash_join->LeftChild()->as<PhysicalSequentialScanNode>();
    auto right_scan = hash_join->RightChild()->as<PhysicalSequentialScanNode>();

    ASSERT_EQ("users", left_scan->TableName());
    ASSERT_EQ("orders", right_scan->TableName());

    // verify left scan schema
    ASSERT_EQ(4, left_scan->OutputSchema().FieldCount());

    // verify right scan schema
    ASSERT_EQ(3, right_scan->OutputSchema().FieldCount());
}

TEST_F(PhysicalPlannerTest, PredicatePushdownWithJoin) {
    LOG_STATUS("Running testPredicatePushdownWithJoin...");

    // Parse SQL with predicates on both tables and join condition
    const std::string query = "SELECT users.name, orders.amount FROM users "
                              "JOIN orders ON users.id = orders.user_id "
                              "WHERE users.age > 30 AND orders.amount > 100";

    auto physical_plan = PlanPhysical(query, true /* optimize */);
    VERIFY_RESULT(physical_plan);

    // Verify physical plan structure
    auto root = physical_plan.value();

    // The root should be a projection node
    ASSERT_EQ(root->Type(), PhysicalNodeType::Projection);

    auto projection = root->as<PhysicalProjectionNode>();

    auto children = projection->Children();
    ASSERT_EQ(1, children.size());

    auto child = children[0];
    ASSERT_EQ(child->Type(), PhysicalNodeType::HashJoin);

    auto hash_join = child->as<PhysicalHashJoinNode>();

    // Verify join condition
    auto join_condition = hash_join->Condition();
    ASSERT_TRUE(join_condition != nullptr);
    ASSERT_TRUE(join_condition->ToString().find("users.id = orders.user_id") != std::string::npos);

    // Verify child nodes are sequential scans with pushed down predicates
    ASSERT_EQ(2, hash_join->Children().size());

    auto left_scan = hash_join->LeftChild()->as<PhysicalSequentialScanNode>();
    auto right_scan = hash_join->RightChild()->as<PhysicalSequentialScanNode>();

    ASSERT_EQ("users", left_scan->TableName());
    ASSERT_EQ("orders", right_scan->TableName());

    // Verify predicate pushdown
    ASSERT_TRUE(left_scan->Predicate() != nullptr);
    ASSERT_TRUE(left_scan->Predicate()->ToString().find("age > 30") != std::string::npos);

    ASSERT_TRUE(right_scan->Predicate() != nullptr);
    ASSERT_TRUE(right_scan->Predicate()->ToString().find("amount > 100") != std::string::npos);
}

TEST_F(PhysicalPlannerTest, PhysicalPlanAggregation) {
    LOG_STATUS("Running testPhysicalPlanAggregation...");

    // Test simple aggregation
    {
        const std::string query = "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id";

        auto physical_plan = PlanPhysical(query, true /* optimize */);
        VERIFY_RESULT(physical_plan);

        // Print the physical plan
        std::string plan_str = GetPhysicalPlanUserFriendlyString(*physical_plan.value());
        std::cout << "Simple aggregation plan: " << std::endl << plan_str << std::endl;

        // Verify physical plan structure
        auto root = physical_plan.value();
        ASSERT_EQ(root->Type(), PhysicalNodeType::Projection);

        auto projection = root->as<PhysicalProjectionNode>();
        auto children = projection->Children();
        ASSERT_EQ(1, children.size());

        auto agg_node = children[0];
        ASSERT_EQ(agg_node->Type(), PhysicalNodeType::HashAggregate);

        auto hash_agg = agg_node->as<PhysicalHashAggregateNode>();

        // Verify group by columns
        auto group_by = hash_agg->GroupBy();
        ASSERT_EQ(1, group_by.size());
        ASSERT_TRUE(group_by[0]->ToString().find("user_id") != std::string::npos);

        // Verify aggregate expressions
        auto aggregates = hash_agg->Aggregates();
        ASSERT_EQ(1, aggregates.size());
        ASSERT_TRUE(aggregates[0]->ToString().find("SUM") != std::string::npos);
    }

    // Test aggregation with having clause
    // TODO: support having clause
    // {
    //     const std::string query = "SELECT user_id, SUM(amount) as total_amount "
    //                               "FROM orders "
    //                               "GROUP BY user_id "
    //                               "HAVING SUM(amount) > 1000";

    //     auto physical_plan = context.plan_physical(query);
    //     test::assert_ok(physical_plan, "Physical planning should succeed");

    //     // Print the physical plan
    //     std::string plan_str = GetPhysicalPlanUserFriendlyString(*physical_plan.value());
    //     std::cout << "Aggregation with HAVING plan: " << std::endl << plan_str << std::endl;

    //     // Verify physical plan structure
    //     auto root = physical_plan.value();
    //     test::assert_are_equal(PhysicalNodeType::Projection, root->type(), "Root should be projection node");

    //     auto projection = root->as<PhysicalProjectionNode>();
    //     auto children = projection->children();
    //     test::assert_are_equal(1, children.size(), "Should have one child");

    //     auto filter_node = children[0];
    //     test::assert_are_equal(PhysicalNodeType::Filter, filter_node->type(), "Child should be filter node");

    //     auto filter = filter_node->as<PhysicalFilterNode>();
    //     test::assert_true(filter->predicate()->toString().find("SUM") != std::string::npos,
    //                       "Filter should contain SUM");

    //     auto agg_node = filter->children()[0];
    //     test::assert_are_equal(
    //         PhysicalNodeType::HashAggregate, agg_node->type(), "Should have hash aggregate node");

    //     auto hash_agg = agg_node->as<PhysicalHashAggregateNode>();

    //     // Verify group by columns
    //     auto group_by = hash_agg->groupBy();
    //     test::assert_are_equal(1, group_by.size(), "Should have one group by column");
    //     test::assert_true(group_by[0]->toString().find("user_id") != std::string::npos,
    //                       "Group by should be user_id");

    //     // Verify aggregate expressions
    //     auto aggregates = hash_agg->aggregates();
    //     test::assert_are_equal(1, aggregates.size(), "Should have one aggregate expression");
    //     test::assert_true(aggregates[0]->toString().find("SUM") != std::string::npos, "Should have SUM
    //     aggregate");
    // }

    // Test multiple aggregates with group by and having
    // {
    //     const std::string query = "SELECT user_id, COUNT(*) as order_count, AVG(amount) as avg_amount "
    //                               "FROM orders "
    //                               "GROUP BY user_id "
    //                               "HAVING COUNT(*) > 2 AND AVG(amount) > 500";

    //     auto physical_plan = context.plan_physical(query);
    //     test::assert_ok(physical_plan, "Physical planning should succeed");

    //     // Print the physical plan
    //     std::string plan_str = GetPhysicalPlanUserFriendlyString(*physical_plan.value());
    //     std::cout << "Multiple aggregates plan: " << std::endl << plan_str << std::endl;

    //     // Verify physical plan structure
    //     auto root = physical_plan.value();
    //     test::assert_are_equal(PhysicalNodeType::Projection, root->type(), "Root should be projection node");

    //     auto projection = root->as<PhysicalProjectionNode>();
    //     auto children = projection->children();
    //     test::assert_are_equal(1, children.size(), "Should have one child");

    //     auto filter_node = children[0];
    //     test::assert_are_equal(PhysicalNodeType::Filter, filter_node->type(), "Child should be filter node");

    //     auto filter = filter_node->as<PhysicalFilterNode>();
    //     auto predicate_str = filter->predicate()->toString();
    //     test::assert_true(predicate_str.find("COUNT") != std::string::npos, "Filter should contain COUNT");
    //     test::assert_true(predicate_str.find("AVG") != std::string::npos, "Filter should contain AVG");

    //     auto agg_node = filter->children()[0];
    //     test::assert_are_equal(
    //         PhysicalNodeType::HashAggregate, agg_node->type(), "Should have hash aggregate node");

    //     auto hash_agg = agg_node->as<PhysicalHashAggregateNode>();

    //     // Verify group by columns
    //     auto group_by = hash_agg->groupBy();
    //     test::assert_are_equal(1, group_by.size(), "Should have one group by column");
    //     test::assert_true(group_by[0]->toString().find("user_id") != std::string::npos,
    //                       "Group by should be user_id");

    //     // Verify aggregate expressions
    //     auto aggregates = hash_agg->aggregates();
    //     test::assert_are_equal(2, aggregates.size(), "Should have two aggregate expressions");
    //     bool has_count = false;
    //     bool has_avg = false;
    //     for (const auto& agg : aggregates) {
    //         if (agg->toString().find("COUNT") != std::string::npos)
    //             has_count = true;
    //         if (agg->toString().find("AVG") != std::string::npos)
    //             has_avg = true;
    //     }
    //     test::assert_true(has_count, "Should have COUNT aggregate");
    //     test::assert_true(has_avg, "Should have AVG aggregate");
    // }
}

}  // namespace pond::query