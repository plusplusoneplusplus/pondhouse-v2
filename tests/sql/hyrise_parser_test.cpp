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

//
// Test Setup:
//      Parse a CREATE TABLE statement with column definitions including primary key and NOT NULL constraints
// Test Result:
//      Verify the parsed structure matches the expected CREATE TABLE statement components
//
TEST_F(HyriseSQLParserTest, CreateTable) {
    // Sample CREATE TABLE SQL query
    const std::string query = "CREATE TABLE students ("
                              "id INTEGER PRIMARY KEY, "
                              "name VARCHAR(50) NOT NULL, "
                              "age INTEGER, "
                              "email VARCHAR(100) UNIQUE, "
                              "created_at TIMESTAMP, "
                              "balance DOUBLE"
                              ");";

    // Parse query
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);

    // Check if parsing was successful and print error message if not
    if (!result.isValid()) {
        std::cerr << "SQL Parsing Error: " << result.errorMsg() << std::endl;
    }
    ASSERT_TRUE(result.isValid()) << "Parsing should be successful";
    ASSERT_EQ(1, result.size()) << "Should have exactly one statement";

    // Get the first statement
    const hsql::SQLStatement* stmt = result.getStatement(0);
    ASSERT_EQ(hsql::kStmtCreate, stmt->type()) << "Statement should be CREATE";

    // Cast to CREATE statement
    const hsql::CreateStatement* create = static_cast<const hsql::CreateStatement*>(stmt);

    // Verify it's a CREATE TABLE statement
    ASSERT_EQ(hsql::kCreateTable, create->type) << "Create type should be CREATE TABLE";
    EXPECT_EQ(std::string("students"), std::string(create->tableName)) << "Table name should be 'students'";

    // Verify column definitions
    ASSERT_EQ(6, create->columns->size()) << "Should have 6 columns";

    // Verify id column (PRIMARY KEY)
    const hsql::ColumnDefinition* idColumn = create->columns->at(0);
    EXPECT_EQ(std::string("id"), std::string(idColumn->name)) << "First column should be 'id'";
    EXPECT_EQ(hsql::DataType::INT, idColumn->type.data_type) << "id should be INTEGER";

    // Check if id is a primary key by examining column constraints
    bool isPrimaryKey = false;
    if (idColumn->column_constraints != nullptr) {
        isPrimaryKey = idColumn->column_constraints->count(hsql::ConstraintType::PrimaryKey) > 0;
    }
    EXPECT_TRUE(isPrimaryKey) << "id should be PRIMARY KEY";

    // Verify name column (NOT NULL)
    const hsql::ColumnDefinition* nameColumn = create->columns->at(1);
    EXPECT_EQ(std::string("name"), std::string(nameColumn->name)) << "Second column should be 'name'";
    EXPECT_EQ(hsql::DataType::VARCHAR, nameColumn->type.data_type) << "name should be VARCHAR";
    EXPECT_EQ(50, nameColumn->type.length) << "name should have length 50";
    EXPECT_FALSE(nameColumn->nullable) << "name should be NOT NULL";

    // Verify age column
    const hsql::ColumnDefinition* ageColumn = create->columns->at(2);
    EXPECT_EQ(std::string("age"), std::string(ageColumn->name)) << "Third column should be 'age'";
    EXPECT_EQ(hsql::DataType::INT, ageColumn->type.data_type) << "age should be INTEGER";
    EXPECT_TRUE(ageColumn->nullable) << "age should be nullable by default";

    // Verify email column (UNIQUE)
    const hsql::ColumnDefinition* emailColumn = create->columns->at(3);
    EXPECT_EQ(std::string("email"), std::string(emailColumn->name)) << "Fourth column should be 'email'";
    EXPECT_EQ(hsql::DataType::VARCHAR, emailColumn->type.data_type) << "email should be VARCHAR";
    EXPECT_EQ(100, emailColumn->type.length) << "email should have length 100";
    EXPECT_TRUE(emailColumn->nullable) << "email should be nullable by default";

    // Check if email has UNIQUE constraint
    bool isUnique = false;
    if (emailColumn->column_constraints != nullptr) {
        isUnique = emailColumn->column_constraints->count(hsql::ConstraintType::Unique) > 0;
    }
    EXPECT_TRUE(isUnique) << "email should have UNIQUE constraint";

    // Verify created_at column
    const hsql::ColumnDefinition* createdAtColumn = create->columns->at(4);
    EXPECT_EQ(std::string("created_at"), std::string(createdAtColumn->name)) << "Fifth column should be 'created_at'";
    EXPECT_EQ(hsql::DataType::DATETIME, createdAtColumn->type.data_type) << "created_at should be TIMESTAMP";

    // Verify balance column
    const hsql::ColumnDefinition* balanceColumn = create->columns->at(5);
    EXPECT_EQ(std::string("balance"), std::string(balanceColumn->name)) << "Sixth column should be 'balance'";
    EXPECT_EQ(hsql::DataType::DOUBLE, balanceColumn->type.data_type) << "balance should be DOUBLE";
}

}  // namespace pond::test