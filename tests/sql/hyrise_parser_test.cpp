#include <SQLParser.h>

#include <gtest/gtest.h>
#include <util/sqlhelper.h>

namespace pond::test {

class HyriseSQLParserTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

//
// Test Setup:
//      Parse a basic SELECT query with a simple WHERE clause
// Test Result:
//      Verify the parsed structure matches the expected SELECT statement components
//
TEST_F(HyriseSQLParserTest, BasicSelect) {
    // Sample SQL query
    const std::string query = "SELECT * FROM users WHERE age > 18;";

    // Parse query
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);

    // Check if parsing was successful
    ASSERT_TRUE(result.isValid()) << "Parsing should be successful";
    ASSERT_EQ(1, result.size()) << "Should have exactly one statement";

    // Get the first statement
    const hsql::SQLStatement* stmt = result.getStatement(0);
    ASSERT_EQ(hsql::kStmtSelect, stmt->type()) << "Statement should be SELECT";

    // Cast to SELECT statement
    const hsql::SelectStatement* select = static_cast<const hsql::SelectStatement*>(stmt);

    // Verify FROM clause
    ASSERT_NE(nullptr, select->fromTable) << "FROM clause should exist";
    EXPECT_EQ(std::string("users"), std::string(select->fromTable->getName())) << "Table name should be 'users'";

    // Verify WHERE clause
    ASSERT_NE(nullptr, select->whereClause) << "WHERE clause should exist";
    EXPECT_EQ(hsql::kExprOperator, select->whereClause->type) << "WHERE clause should be an operator";
    EXPECT_EQ(hsql::kOpGreater, select->whereClause->opType) << "Operator should be '>'";
}

//
// Test Setup:
//      Parse a complex SELECT query with JOIN, WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT clauses
// Test Result:
//      Verify all components of the complex query are correctly parsed and structured
//
TEST_F(HyriseSQLParserTest, ComplexQuery) {
    const std::string query = "SELECT users.name, COUNT(*) as user_count "
                              "FROM users "
                              "JOIN orders ON users.id = orders.user_id "
                              "WHERE users.age >= 21 "
                              "GROUP BY users.name "
                              "HAVING COUNT(*) > 5 "
                              "ORDER BY user_count DESC "
                              "LIMIT 10;";

    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);

    ASSERT_TRUE(result.isValid()) << "Parsing should be successful";
    ASSERT_EQ(1, result.size()) << "Should have exactly one statement";

    const hsql::SQLStatement* stmt = result.getStatement(0);
    ASSERT_EQ(hsql::kStmtSelect, stmt->type()) << "Statement should be SELECT";

    const hsql::SelectStatement* select = static_cast<const hsql::SelectStatement*>(stmt);

    // Verify SELECT list
    ASSERT_EQ(2, select->selectList->size()) << "Should have two items in SELECT list";
    EXPECT_EQ(hsql::kExprColumnRef, select->selectList->at(0)->type) << "First item should be a column reference";
    EXPECT_EQ(hsql::kExprFunctionRef, select->selectList->at(1)->type) << "Second item should be a function reference";

    // Verify JOIN
    ASSERT_NE(nullptr, select->fromTable) << "FROM clause should exist";
    ASSERT_EQ(hsql::kTableJoin, select->fromTable->type) << "Should be a JOIN operation";

    auto* join = static_cast<hsql::JoinDefinition*>(select->fromTable->join);
    ASSERT_NE(nullptr, join) << "JOIN definition should exist";
    EXPECT_EQ(hsql::kJoinInner, join->type) << "JOIN should be INNER";
    EXPECT_EQ(std::string("users"), std::string(join->left->name)) << "LEFT table should be 'users'";
    EXPECT_EQ(std::string("orders"), std::string(join->right->name)) << "RIGHT table should be 'orders'";

    // Verify JOIN condition
    auto* condition = static_cast<hsql::Expr*>(join->condition);
    ASSERT_NE(nullptr, condition) << "JOIN condition should exist";
    EXPECT_EQ(hsql::kExprOperator, condition->type) << "JOIN condition should be an operator";
    EXPECT_EQ(hsql::kOpEquals, condition->opType) << "Operator should be '='";

    // Verify JOIN condition left side
    auto* left = static_cast<hsql::Expr*>(condition->expr);
    ASSERT_NE(nullptr, left) << "LEFT expression should exist";
    EXPECT_EQ(hsql::kExprColumnRef, left->type) << "LEFT expression should be a column reference";
    EXPECT_EQ(std::string("users"), std::string(left->table)) << "LEFT table should be 'users'";
    EXPECT_EQ(std::string("id"), std::string(left->name)) << "LEFT column should be 'id'";

    // Verify JOIN condition right side
    auto* right = static_cast<hsql::Expr*>(condition->expr2);
    ASSERT_NE(nullptr, right) << "RIGHT expression should exist";
    EXPECT_EQ(hsql::kExprColumnRef, right->type) << "RIGHT expression should be a column reference";
    EXPECT_EQ(std::string("orders"), std::string(right->table)) << "RIGHT table should be 'orders'";
    EXPECT_EQ(std::string("user_id"), std::string(right->name)) << "RIGHT column should be 'user_id'";

    // Verify ORDER BY and LIMIT
    ASSERT_NE(nullptr, select->order) << "ORDER BY clause should exist";
    ASSERT_NE(nullptr, select->limit) << "LIMIT clause should exist";
    EXPECT_EQ(10, select->limit->limit->ival) << "LIMIT should be 10";
}

}  // namespace pond::test