#include "logical_planner.h"

#include <memory>
#include <string>
#include <vector>

#include "common/log.h"
#include "common/result.h"

using namespace pond::common;

namespace pond::query {

const char* hsqlTableTypeToString(hsql::TableRefType type) {
    switch (type) {
        case hsql::kTableJoin:
            return "JOIN";
        case hsql::kTableName:
            return "TABLE";
        default:
            return "UNKNOWN";
    }
}

const char* hsqlJoinTypeToString(hsql::JoinType type) {
    switch (type) {
        case hsql::kJoinInner:
            return "INNER";
        case hsql::kJoinLeft:
            return "LEFT";
        case hsql::kJoinRight:
            return "RIGHT";
        case hsql::kJoinFull:
            return "FULL";
        default:
            return "UNKNOWN";
    }
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::Plan(const hsql::SQLStatement* statement) {
    LOG_VERBOSE("Planning statement: 0x%p", statement);

    if (!statement) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null statement");
    }

    switch (statement->type()) {
        case hsql::kStmtSelect: {
            auto result = PlanSelect(static_cast<const hsql::SelectStatement*>(statement));
            if (!result.ok()) {
                return result;
            }

            if (!result.value()->IsValid()) {
                return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                         "Invalid logical plan");
            }

            return Result<std::shared_ptr<LogicalPlanNode>>::success(result.value());
        }

        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::NotImplemented,
                                                                     "Statement type not supported");
    }
}

std::string constructJoinedTableName(const hsql::TableRef* table) {
    if (table->type != hsql::kTableJoin) {
        return table->name;
    }

    std::string left_name;
    if (table->join->left->type == hsql::kTableJoin) {
        left_name = constructJoinedTableName(table->join->left);
    } else {
        left_name = table->join->left->name;
    }

    std::string right_name;
    if (table->join->right->type == hsql::kTableJoin) {
        right_name = constructJoinedTableName(table->join->right);
    } else {
        right_name = table->join->right->name;
    }
    return "joined_" + left_name + "_" + right_name;
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanSelect(const hsql::SelectStatement* select) {
    // Plan the FROM clause
    auto from_result = PlanTableRef(select->fromTable);
    if (!from_result.ok()) {
        return from_result;
    }
    auto current = from_result.value();

    // Plan the WHERE clause
    if (select->whereClause != nullptr) {
        auto where_result = PlanWhere(current, select->whereClause);
        if (!where_result.ok()) {
            return where_result;
        }
        current = where_result.value();
    }

    // Plan the GROUP BY clause
    auto group_result = PlanGroupBy(current, select->groupBy, select->selectList);
    if (!group_result.ok()) {
        return group_result;
    }
    auto group_node = group_result.value();
    current = group_node;

    // Plan the ORDER BY clause
    auto sort_result = PlanSort(current, select->order);
    if (!sort_result.ok()) {
        return sort_result;
    }
    current = sort_result.value();

    // Plan the projection
    std::string table_name = constructJoinedTableName(select->fromTable);
    auto proj_result = PlanProjection(table_name, current, select->selectList, group_node);
    if (!proj_result.ok()) {
        return proj_result;
    }
    current = proj_result.value();

    // Plan the LIMIT clause
    auto limit_result = PlanLimit(current, select->limit);
    if (!limit_result.ok()) {
        return limit_result;
    }
    current = limit_result.value();

    return Result<std::shared_ptr<LogicalPlanNode>>::success(current);
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanTableRef(const hsql::TableRef* table) {
    using ReturnType = Result<std::shared_ptr<LogicalPlanNode>>;

    LOG_VERBOSE(
        "Planning table reference: 0x%p, type: %s, name: %s", table, hsqlTableTypeToString(table->type), table->name);

    if (!table) {
        LOG_VERBOSE("Null table reference");
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null table reference");
    }

    switch (table->type) {
        case hsql::kTableName: {
            // convert the table name to real table name without LEFT, RIGHT, INNER, etc.
            std::string real_table_name = table->name;
            if (real_table_name.ends_with("LEFT") || real_table_name.ends_with("FULL")) {
                real_table_name = real_table_name.substr(0, real_table_name.size() - 4);
            } else if (real_table_name.ends_with("RIGHT") || real_table_name.ends_with("INNER")) {
                real_table_name = real_table_name.substr(0, real_table_name.size() - 5);
            }

            auto schema_result = VerifyTableExists(real_table_name);
            RETURN_IF_ERROR_T(ReturnType, schema_result);

            LOG_VERBOSE("planned table scan: 0x%p, name: %s", table, real_table_name.c_str());
            return Result<std::shared_ptr<LogicalPlanNode>>::success(
                std::make_shared<LogicalScanNode>(real_table_name, schema_result.value()));
        }
        case hsql::kTableJoin:
            return PlanJoin(table);
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::NotImplemented,
                                                                     "Table reference type not supported");
    }
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanJoin(const hsql::TableRef* table) {
    LOG_VERBOSE("Planning join: 0x%p, type: %s", table, hsqlTableTypeToString(table->type));

    if (!table->join) {
        LOG_VERBOSE("Null join definition");
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null join definition");
    }

    // Plan right side
    auto right_result = PlanTableRef(table->join->right);
    if (!right_result.ok()) {
        return right_result;
    }

    // Plan left side
    auto left_result = PlanTableRef(table->join->left);
    if (!left_result.ok()) {
        return left_result;
    }

    // Convert join type
    JoinType join_type;
    switch (table->join->type) {
        case hsql::kJoinInner:
            join_type = JoinType::Inner;
            break;
        case hsql::kJoinLeft:
            join_type = JoinType::Left;
            break;
        case hsql::kJoinRight:
            join_type = JoinType::Right;
            break;
        case hsql::kJoinFull:
            join_type = JoinType::Full;
            break;
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::failure(
                ErrorCode::NotImplemented,
                std::string("Join type not supported: ") + std::to_string(table->join->type));
    }

    LOG_VERBOSE("planned join condition: 0x%p, type: %s, join type: %d",
                table->join,
                hsqlJoinTypeToString(table->join->type),
                join_type);

    return PlanJoinCondition(left_result.value(), right_result.value(), join_type, table->join);
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanJoinCondition(std::shared_ptr<LogicalPlanNode> left,
                                                                           std::shared_ptr<LogicalPlanNode> right,
                                                                           JoinType join_type,
                                                                           const hsql::JoinDefinition* join) {
    using ReturnType = Result<std::shared_ptr<LogicalPlanNode>>;
    LOG_VERBOSE(
        "Planning join condition: 0x%p, type: %s, join type: %d", join, hsqlJoinTypeToString(join->type), join_type);

    // Create join schema
    auto schema_result = CreateJoinSchema(left->OutputSchema(), right->OutputSchema());
    RETURN_IF_ERROR_T(ReturnType, schema_result);

    // For cross joins, we don't need a condition
    if (join_type == JoinType::Cross) {
        return Result<std::shared_ptr<LogicalPlanNode>>::success(
            std::make_shared<LogicalJoinNode>(left, right, join_type, nullptr, schema_result.value()));
    }

    // Verify join condition
    if (!join->condition) {
        LOG_VERBOSE("Missing join condition");
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Missing join condition");
    }

    // Build expression for join condition
    auto expr_result = BuildExpression(join->condition);
    RETURN_IF_ERROR_T(ReturnType, expr_result);

    // Verify that columns in join condition exist
    auto VerifyColumn = [&](const std::shared_ptr<Expression>& expr) -> Result<bool> {
        if (expr->Type() == ExprType::Column) {
            auto col_expr = std::static_pointer_cast<ColumnExpression>(expr);
            if (!DoesColumnExist(left->OutputSchema(), col_expr->ColumnName())
                && !DoesColumnExist(right->OutputSchema(), col_expr->ColumnName())) {
                return Result<bool>::failure(ErrorCode::SchemaFieldNotFound,
                                             std::string("Column not found: ") + col_expr->ColumnName());
            }
        }
        return Result<bool>::success(true);
    };

    std::function<Result<bool>(const std::shared_ptr<Expression>&)> VerifyExpr =
        [&](const std::shared_ptr<Expression>& expr) -> Result<bool> {
        if (!expr) {
            return Result<bool>::success(true);
        }

        if (expr->Type() == ExprType::Column) {
            return VerifyColumn(expr);
        } else if (expr->Type() == ExprType::BinaryOp) {
            auto bin_expr = std::static_pointer_cast<BinaryExpression>(expr);
            auto left_result = VerifyExpr(bin_expr->Left());
            if (!left_result.ok()) {
                return left_result;
            }
            return VerifyExpr(bin_expr->Right());
        }
        return Result<bool>::success(true);
    };

    auto verify_result = VerifyExpr(expr_result.value());
    RETURN_IF_ERROR_T(ReturnType, verify_result);

    return Result<std::shared_ptr<LogicalPlanNode>>::success(
        std::make_shared<LogicalJoinNode>(left, right, join_type, expr_result.value(), schema_result.value()));
}

Result<common::Schema> LogicalPlanner::CreateJoinSchema(const common::Schema& left_schema,
                                                        const common::Schema& right_schema) {
    LOG_VERBOSE("Creating join schema: 0x%p, 0x%p", &left_schema, &right_schema);

    common::Schema join_schema{};

    // Add fields from left schema
    for (const auto& field : left_schema.Columns()) {
        join_schema.AddField(field.name, field.type, field.nullability);
    }

    // Add fields from right schema
    for (const auto& field : right_schema.Columns()) {
        join_schema.AddField(field.name, field.type, field.nullability);
    }

    return Result<common::Schema>::success(join_schema);
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanWhere(std::shared_ptr<LogicalPlanNode> input,
                                                                   const hsql::Expr* where) {
    using ReturnType = Result<std::shared_ptr<LogicalPlanNode>>;
    LOG_VERBOSE("Planning where clause: 0x%p", where);

    if (!where) {
        LOG_VERBOSE("Null where clause");
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null where clause");
    }

    auto expr_result = BuildExpression(where);
    RETURN_IF_ERROR_T(ReturnType, expr_result);

    LOG_VERBOSE("planned where clause: 0x%p, expr: %s", where, expr_result.value()->ToString().c_str());

    return Result<std::shared_ptr<LogicalPlanNode>>::success(
        std::make_shared<LogicalFilterNode>(input, expr_result.value()));
}

Result<std::shared_ptr<Expression>> LogicalPlanner::BuildExpression(const hsql::Expr* expr) {
    if (!expr) {
        LOG_VERBOSE("Null expression");
        return Result<std::shared_ptr<Expression>>::failure(ErrorCode::InvalidArgument, "Null expression");
    }

    switch (expr->type) {
        case hsql::kExprOperator: {
            auto left_result = BuildExpression(expr->expr);
            if (!left_result.ok()) {
                return left_result;
            }

            auto right_result = BuildExpression(expr->expr2);
            if (!right_result.ok()) {
                return right_result;
            }

            BinaryOpType op_type;
            switch (expr->opType) {
                case hsql::kOpPlus:
                    op_type = BinaryOpType::Add;
                    break;
                case hsql::kOpMinus:
                    op_type = BinaryOpType::Subtract;
                    break;
                case hsql::kOpAsterisk:
                    op_type = BinaryOpType::Multiply;
                    break;
                case hsql::kOpSlash:
                    op_type = BinaryOpType::Divide;
                    break;
                case hsql::kOpEquals:
                    op_type = BinaryOpType::Equal;
                    break;
                case hsql::kOpNotEquals:
                    op_type = BinaryOpType::NotEqual;
                    break;
                case hsql::kOpLess:
                    op_type = BinaryOpType::Less;
                    break;
                case hsql::kOpLessEq:
                    op_type = BinaryOpType::LessEqual;
                    break;
                case hsql::kOpGreater:
                    op_type = BinaryOpType::Greater;
                    break;
                case hsql::kOpGreaterEq:
                    op_type = BinaryOpType::GreaterEqual;
                    break;
                case hsql::kOpAnd:
                    op_type = BinaryOpType::And;
                    break;
                case hsql::kOpOr:
                    op_type = BinaryOpType::Or;
                    break;
                case hsql::kOpLike:
                    op_type = BinaryOpType::Like;
                    break;
                default:
                    return Result<std::shared_ptr<Expression>>::failure(
                        ErrorCode::NotImplemented,
                        std::string("Operator not supported: ") + std::to_string(expr->opType));
            }

            LOG_VERBOSE("planned binary operation: 0x%p, type: %d, op: %d", expr, expr->type, op_type);
            return Result<std::shared_ptr<Expression>>::success(
                std::make_shared<BinaryExpression>(op_type, left_result.value(), right_result.value()));
        }

        case hsql::kExprColumnRef: {
            std::string table_name = expr->table ? expr->table : "";
            LOG_VERBOSE("planned column expression: 0x%p, table: %s, column: %s", expr, table_name.c_str(), expr->name);
            return Result<std::shared_ptr<Expression>>::success(
                std::make_shared<ColumnExpression>(table_name, expr->name));
        }

        case hsql::kExprLiteralInt:
            return Result<std::shared_ptr<Expression>>::success(ConstantExpression::CreateInteger(expr->ival));

        case hsql::kExprLiteralFloat:
            return Result<std::shared_ptr<Expression>>::success(ConstantExpression::CreateFloat(expr->fval));

        case hsql::kExprLiteralString:
            return Result<std::shared_ptr<Expression>>::success(ConstantExpression::CreateString(expr->name));

        case hsql::kExprFunctionRef: {
            // Check if we have an argument
            if (expr->exprList == nullptr || expr->exprList->size() != 1) {
                return Result<std::shared_ptr<Expression>>::failure(ErrorCode::InvalidArgument,
                                                                    "Aggregate functions require exactly one argument");
            }

            // Get the input expression
            auto input_result = BuildExpression((*expr->exprList)[0]);
            if (!input_result.ok()) {
                return input_result;
            }

            // Map HSQL function to our aggregate type
            AggregateType agg_type;
            std::string func_name = expr->name;
            if (func_name == "COUNT") {
                agg_type = AggregateType::Count;
            } else if (func_name == "SUM") {
                agg_type = AggregateType::Sum;
            } else if (func_name == "AVG") {
                agg_type = AggregateType::Avg;
            } else if (func_name == "MIN") {
                agg_type = AggregateType::Min;
            } else if (func_name == "MAX") {
                agg_type = AggregateType::Max;
            } else {
                return Result<std::shared_ptr<Expression>>::failure(
                    ErrorCode::NotImplemented, std::string("Aggregate function not supported: ") + func_name);
            }

            return Result<std::shared_ptr<Expression>>::success(
                std::make_shared<AggregateExpression>(agg_type, input_result.value()));
        }

        case hsql::kExprStar:
            return Result<std::shared_ptr<Expression>>::success(std::make_shared<StarExpression>());

        default:
            return Result<std::shared_ptr<Expression>>::failure(
                ErrorCode::NotImplemented,
                std::string("HSQL expression type not supported: ") + std::to_string(expr->type));
    }
}

bool AreSameColumn(const std::shared_ptr<Expression>& expr1, const std::shared_ptr<Expression>& expr2) {
    if (expr1->Type() != ExprType::Column || expr2->Type() != ExprType::Column) {
        return false;
    }
    auto col1 = std::static_pointer_cast<ColumnExpression>(expr1);
    auto col2 = std::static_pointer_cast<ColumnExpression>(expr2);
    return col1->ColumnName() == col2->ColumnName();
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanProjection(const std::string& table_name,
                                                                        std::shared_ptr<LogicalPlanNode> input,
                                                                        const std::vector<hsql::Expr*>* exprs,
                                                                        const std::shared_ptr<LogicalPlanNode>& agg) {
    using ReturnType = Result<std::shared_ptr<LogicalPlanNode>>;

    LOG_VERBOSE("Planning projection: 0x%p, table: %s, exprs: %d", input.get(), table_name.c_str(), exprs->size());

    if (!exprs) {
        LOG_VERBOSE("Null projection list");
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null projection list");
    }

    // Handle SELECT *
    if (exprs->size() == 1 && (*exprs)[0]->type == hsql::kExprStar) {
        // For SELECT *, we create column expressions for each field in the input schema
        std::vector<std::shared_ptr<Expression>> expr_list;
        const auto& input_schema = input->OutputSchema();
        expr_list.reserve(input_schema.NumColumns());
        for (const auto& field : input_schema.Columns()) {
            expr_list.push_back(std::make_shared<ColumnExpression>("", field.name));
        }
        return Result<std::shared_ptr<LogicalPlanNode>>::success(
            std::make_shared<LogicalProjectionNode>(input, input_schema, std::move(expr_list), table_name));
    }

    common::Schema outputSchema;

    std::vector<std::shared_ptr<Expression>> child_exprs;

    // Handle GROUP BY
    if (agg && agg->Type() == LogicalNodeType::Aggregate) {
        auto groupNode = agg->as<LogicalAggregateNode>();
        auto& group_by = groupNode->GetGroupBy();
        // add schema
        for (const auto& expr : group_by) {
            child_exprs.push_back(expr);
        }
    }

    // Build expressions
    std::vector<std::shared_ptr<Expression>> expr_list;
    expr_list.reserve(exprs->size());
    for (const auto* expr : *exprs) {
        auto expr_result = BuildExpression(expr);
        RETURN_IF_ERROR_T(ReturnType, expr_result);

        bool skip = false;
        for (const auto& child_expr : child_exprs) {
            if (AreSameColumn(expr_result.value(), child_expr)) {
                LOG_VERBOSE("Skipping duplicate column: %s", child_expr->ToString().c_str());
                skip = true;
            }
        }

        if (!skip) {
            expr_list.push_back(expr_result.value());
        }
    }

    // Create projection schema
    auto schema_result = CreateProjectionSchema(input->OutputSchema(), expr_list, child_exprs);
    RETURN_IF_ERROR_T(ReturnType, schema_result);

    outputSchema = schema_result.value();

    LOG_VERBOSE("planned projection: 0x%p, schema: %s, exprs: %d",
                input.get(),
                outputSchema.ToString().c_str(),
                expr_list.size());

    return Result<std::shared_ptr<LogicalPlanNode>>::success(
        std::make_shared<LogicalProjectionNode>(input, outputSchema, std::move(expr_list), table_name));
}

Result<common::Schema> LogicalPlanner::VerifyTableExists(const std::string& table_name) {
    using ReturnType = Result<common::Schema>;

    auto table_metadata_result = catalog_.LoadTable(table_name);
    RETURN_IF_ERROR_T(ReturnType, table_metadata_result);

    return Result<common::Schema>::success(*table_metadata_result.value().schema);
}

bool LogicalPlanner::DoesColumnExist(const common::Schema& schema, const std::string& column_name) {
    return schema.HasField(column_name);
}

Result<common::Schema> LogicalPlanner::CreateProjectionSchema(
    const common::Schema& input_schema,
    const std::vector<std::shared_ptr<Expression>>& current_exprs,
    const std::vector<std::shared_ptr<Expression>>& child_exprs) {
    using ReturnType = Result<common::Schema>;

    common::Schema projection_schema{};

    for (const auto& exprGroup : {child_exprs, current_exprs}) {
        for (const auto& expr : exprGroup) {
            if (expr->Type() == ExprType::Column) {
                auto col_expr = std::static_pointer_cast<ColumnExpression>(expr);
                if (!DoesColumnExist(input_schema, col_expr->ColumnName())) {
                    return Result<common::Schema>::failure(ErrorCode::SchemaFieldNotFound,
                                                           std::string("Column not found: ") + col_expr->ColumnName());
                }
                auto column = input_schema.GetColumn(col_expr->ColumnName());
                projection_schema.AddField(column.name, column.type, column.nullability);
            } else if (expr->Type() == ExprType::Aggregate) {
                auto agg_expr = std::static_pointer_cast<AggregateExpression>(expr);
                projection_schema.AddField(
                    agg_expr->ResultName(), agg_expr->ResultType(), common::Nullability::NULLABLE);
            } else {
                // For now, treat all other expressions as strings
                projection_schema.AddField(expr->ToString(), common::ColumnType::STRING, common::Nullability::NULLABLE);
            }
        }
    }

    return Result<common::Schema>::success(projection_schema);
}

std::string ExprToString(const hsql::Expr* expr) {
    if (!expr) {
        return "";
    }

    std::ostringstream oss;
    switch (expr->type) {
        case hsql::kExprColumnRef:
            oss << (expr->table ? std::string(expr->table) + "." : "") << expr->name;
            break;
        case hsql::kExprLiteralString:
            oss << "'" << expr->name << "'";
            break;
        case hsql::kExprLiteralInt:
            oss << expr->ival;
            break;
        case hsql::kExprLiteralFloat:
            oss << expr->fval;
            break;
        case hsql::kExprOperator:
            oss << "(" << ExprToString(expr->expr) << " ";
            switch (expr->opType) {
                case hsql::kOpEquals:
                    oss << "=";
                    break;
                case hsql::kOpNotEquals:
                    oss << "!=";
                    break;
                case hsql::kOpLess:
                    oss << "<";
                    break;
                case hsql::kOpLessEq:
                    oss << "<=";
                    break;
                case hsql::kOpGreater:
                    oss << ">";
                    break;
                case hsql::kOpGreaterEq:
                    oss << ">=";
                    break;
                case hsql::kOpAnd:
                    oss << "AND";
                    break;
                case hsql::kOpOr:
                    oss << "OR";
                    break;
                case hsql::kOpPlus:
                    oss << " + ";
                    break;
                case hsql::kOpMinus:
                    oss << " - ";
                    break;
                case hsql::kOpLike:
                    oss << " LIKE ";
                    break;
                default:
                    oss << "?";
            }
            oss << " " << ExprToString(expr->expr2) << ")";
            break;
        default:
            LOG_VERBOSE("Unknown expression type: %d", expr->type);
            oss << "?";
            break;
    }
    return oss.str();
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanGroupBy(std::shared_ptr<LogicalPlanNode> input,
                                                                     const hsql::GroupByDescription* group_by,
                                                                     const std::vector<hsql::Expr*>* select_list) {
    if (!group_by || group_by->columns == nullptr || group_by->columns->empty()) {
        return Result<std::shared_ptr<LogicalPlanNode>>::success(input);
    }

    std::vector<std::shared_ptr<Expression>> group_by_exprs;
    std::vector<std::shared_ptr<Expression>> agg_exprs;

    // Convert GROUP BY expressions
    for (const auto* expr : *group_by->columns) {
        auto group_expr_result = BuildExpression(expr);
        if (!group_expr_result.ok()) {
            return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                     "Failed to build GROUP BY expression");
        }
        group_by_exprs.push_back(group_expr_result.value());
    }

    // Convert aggregate expressions from SELECT list
    for (const auto* expr : *select_list) {
        if (expr->type == hsql::kExprFunctionRef) {
            auto agg_expr_result = BuildExpression(expr);
            if (!agg_expr_result.ok()) {
                return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                         "Failed to build aggregate expression");
            }
            agg_exprs.push_back(agg_expr_result.value());
        } else {
            // LOG_VERBOSE("Skipping non-aggregate expression: %s", ExprToString(expr).c_str());
        }
    }

    // Create output schema for aggregate node
    common::Schema agg_schema{};

    // Add GROUP BY columns to schema
    for (const auto& expr : group_by_exprs) {
        if (expr->Type() == ExprType::Column) {
            auto col_expr = std::static_pointer_cast<ColumnExpression>(expr);
            auto field_result = input->OutputSchema().GetColumn(col_expr->ColumnName());
            agg_schema.AddField(field_result.name, field_result.type, field_result.nullability);
        }
    }

    // Add aggregate columns to schema
    for (const auto& expr : agg_exprs) {
        if (expr->Type() == ExprType::Aggregate) {
            auto agg_expr = std::static_pointer_cast<AggregateExpression>(expr);
            agg_schema.AddField(agg_expr->ResultName(), agg_expr->ResultType(), common::Nullability::NULLABLE);
        }
    }

    return Result<std::shared_ptr<LogicalPlanNode>>::success(
        std::make_shared<LogicalAggregateNode>(input, group_by_exprs, agg_exprs, agg_schema));
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanSort(std::shared_ptr<LogicalPlanNode> input,
                                                                  const std::vector<hsql::OrderDescription*>* order) {
    if (!order || order->empty()) {
        return Result<std::shared_ptr<LogicalPlanNode>>::success(input);
    }

    std::vector<SortSpec> sort_specs;

    for (const auto* order_desc : *order) {
        auto expr_result = BuildExpression(order_desc->expr);
        if (!expr_result.ok()) {
            return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                     "Failed to build ORDER BY expression");
        }

        SortSpec spec;
        spec.expr = expr_result.value();
        spec.direction = order_desc->type == hsql::kOrderAsc ? SortDirection::Ascending : SortDirection::Descending;
        sort_specs.push_back(spec);
    }

    return Result<std::shared_ptr<LogicalPlanNode>>::success(
        std::make_shared<LogicalSortNode>(input, std::move(sort_specs)));
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalPlanner::PlanLimit(std::shared_ptr<LogicalPlanNode> input,
                                                                   const hsql::LimitDescription* limit) {
    if (!limit) {
        return Result<std::shared_ptr<LogicalPlanNode>>::success(input);
    }

    if (!limit->limit) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                 "LIMIT clause requires a limit value");
    }

    if (limit->limit->type != hsql::kExprLiteralInt) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                 "LIMIT clause requires an integer value");
    }

    int64_t limit_value = limit->limit->ival;
    int64_t offset_value = limit->offset ? limit->offset->ival : 0;

    if (limit_value < 0 || offset_value < 0) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                 "LIMIT and OFFSET values must be non-negative");
    }

    return Result<std::shared_ptr<LogicalPlanNode>>::success(
        std::make_shared<LogicalLimitNode>(input, limit_value, offset_value));
}

}  // namespace pond::query