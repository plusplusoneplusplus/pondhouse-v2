#pragma once

#include <memory>
#include <sstream>
#include <string>

#include "common/column_type.h"
#include "common/expression_types.h"

namespace pond::common {

/**
 * @brief Base class for all expressions in the system
 * This is used across query processing, storage, and catalog layers
 */
class Expression {
public:
    Expression(ExprType type) : type_(type) {}
    virtual ~Expression() = default;

    ExprType Type() const { return type_; }
    virtual std::string ToString() const = 0;

    template <typename T>
    T* as() {
        return static_cast<T*>(this);
    }

protected:
    ExprType type_;
};

using ExpressionPtr = std::shared_ptr<Expression>;

/**
 * @brief Represents a constant value in an expression
 */
class ConstantExpression : public Expression {
public:
    // Type-specific constructors
    static std::shared_ptr<ConstantExpression> CreateInteger(int64_t value) {
        return std::make_shared<ConstantExpression>(std::to_string(value), ColumnType::INT64);
    }

    static std::shared_ptr<ConstantExpression> CreateFloat(double value) {
        return std::make_shared<ConstantExpression>(std::to_string(value), ColumnType::FLOAT);
    }

    static std::shared_ptr<ConstantExpression> CreateString(const std::string& value) {
        return std::make_shared<ConstantExpression>(value, ColumnType::STRING);
    }

    static std::shared_ptr<ConstantExpression> CreateBoolean(bool value) {
        return std::make_shared<ConstantExpression>(value ? "1" : "0", ColumnType::BOOLEAN);
    }

    static std::shared_ptr<ConstantExpression> CreateNull() {
        return std::make_shared<ConstantExpression>("", ColumnType::INVALID);
    }

    ConstantExpression(std::string value, ColumnType type)
        : Expression(ExprType::Constant), value_(std::move(value)), type_(type) {}

    const std::string& Value() const { return value_; }
    ColumnType GetColumnType() const { return type_; }

    // Type-specific value getters
    int64_t GetInteger() const { return std::stoll(value_); }
    double GetFloat() const { return std::stod(value_); }
    bool GetBoolean() const { return value_ == "1"; }

    std::string ToString() const override {
        switch (type_) {
            case ColumnType::STRING:
                return "'" + value_ + "'";
            default:
                return value_;
        }
    }

private:
    std::string value_;
    ColumnType type_;
};

using ConstantExpressionPtr = std::shared_ptr<ConstantExpression>;

/**
 * @brief Represents a column reference in an expression
 */
class ColumnExpression : public Expression {
public:
    ColumnExpression(std::string table_name, std::string column_name)
        : Expression(ExprType::Column), table_name_(std::move(table_name)), column_name_(std::move(column_name)) {}

    const std::string& TableName() const { return table_name_; }
    const std::string& ColumnName() const { return column_name_; }

    std::string ToString() const override {
        return table_name_.empty() ? column_name_ : table_name_ + "." + column_name_;
    }

private:
    std::string table_name_;
    std::string column_name_;
};

using ColumnExpressionPtr = std::shared_ptr<ColumnExpression>;

/**
 * @brief Represents a binary operation in an expression
 */
class BinaryExpression : public Expression {
public:
    BinaryExpression(BinaryOpType op_type, ExpressionPtr left, ExpressionPtr right)
        : Expression(ExprType::BinaryOp), op_type_(op_type), left_(std::move(left)), right_(std::move(right)) {}

    BinaryOpType OpType() const { return op_type_; }
    const ExpressionPtr& Left() const { return left_; }
    const ExpressionPtr& Right() const { return right_; }

    std::string ToString() const override {
        std::string op_str;
        switch (op_type_) {
            case BinaryOpType::Add:
                op_str = "+";
                break;
            case BinaryOpType::Subtract:
                op_str = "-";
                break;
            case BinaryOpType::Multiply:
                op_str = "*";
                break;
            case BinaryOpType::Divide:
                op_str = "/";
                break;
            case BinaryOpType::Equal:
                op_str = "=";
                break;
            case BinaryOpType::NotEqual:
                op_str = "!=";
                break;
            case BinaryOpType::Less:
                op_str = "<";
                break;
            case BinaryOpType::LessEqual:
                op_str = "<=";
                break;
            case BinaryOpType::Greater:
                op_str = ">";
                break;
            case BinaryOpType::GreaterEqual:
                op_str = ">=";
                break;
            case BinaryOpType::And:
                op_str = "AND";
                break;
            case BinaryOpType::Or:
                op_str = "OR";
                break;
            case BinaryOpType::Like:
                op_str = "LIKE";
                break;
        }

        return "(" + left_->ToString() + " " + op_str + " " + right_->ToString() + ")";
    }

private:
    BinaryOpType op_type_;
    ExpressionPtr left_;
    ExpressionPtr right_;
};

using BinaryExpressionPtr = std::shared_ptr<BinaryExpression>;

/**
 * @brief Represents an aggregate function in an expression
 */
class AggregateExpression : public Expression {
public:
    AggregateExpression(AggregateType agg_type, std::shared_ptr<Expression> input)
        : Expression(ExprType::Aggregate), agg_type_(agg_type), input_(std::move(input)) {
        // Generate default result name if not overridden later
        result_name_ = AggFunctionToString(agg_type_);
        if (input_) {
            if (input_->Type() == ExprType::Column) {
                auto col = std::static_pointer_cast<ColumnExpression>(input_);
                result_name_ += "_" + col->ColumnName();
            } else if (input_->Type() == ExprType::Constant || input_->Type() == ExprType::Star) {
                // pass
            } else {
                result_name_ += "_" + input_->ToString();
            }
        }
    }

    std::string ToString() const override {
        std::ostringstream oss;
        oss << AggFunctionToString(agg_type_, true /* uppercase */) << "(";
        if (input_) {
            oss << input_->ToString();
        } else {
            oss << "*";
        }
        oss << ")";
        return oss.str();
    }

    // Getters
    AggregateType AggType() const { return agg_type_; }
    std::shared_ptr<Expression> Input() const { return input_; }
    bool HasInput() const { return input_ != nullptr; }

    // Return the result name (either default generated or custom alias)
    std::string ResultName() const { return result_name_; }

    // Set a custom result name (alias)
    void SetResultName(const std::string& alias) { result_name_ = alias; }

    // Return the data type of the aggregate result
    ColumnType ResultType() const {
        switch (agg_type_) {
            case AggregateType::Count:
                return ColumnType::UINT64;
            case AggregateType::Sum:
                // Sum type depends on input type
                if (input_ && input_->Type() == ExprType::Column) {
                    auto col = std::static_pointer_cast<ColumnExpression>(input_);
                    // For now, assume sum of integers yields int64, floats yield double
                    // This is simplistic and should be improved
                    // TODO: properly infer result type based on input type
                    return ColumnType::DOUBLE;
                }
                return ColumnType::DOUBLE;
            case AggregateType::Avg:
                return ColumnType::DOUBLE;
            case AggregateType::Min:
            case AggregateType::Max:
                // Min/Max have same type as input
                if (input_ && input_->Type() == ExprType::Column) {
                    // TODO: properly infer type
                    return ColumnType::INT64;
                }
                return ColumnType::INT64;
            default:
                // Default to double for unknown aggregates
                return ColumnType::INVALID;
        }
    }

private:
    AggregateType agg_type_;
    std::shared_ptr<Expression> input_;
    std::string result_name_;  // Either generated or custom alias

    static std::string AggFunctionToString(AggregateType type, bool uppercase = false) {
        switch (type) {
            case AggregateType::Count:
                return uppercase ? "COUNT" : "count";
            case AggregateType::Sum:
                return uppercase ? "SUM" : "sum";
            case AggregateType::Avg:
                return uppercase ? "AVG" : "avg";
            case AggregateType::Min:
                return uppercase ? "MIN" : "min";
            case AggregateType::Max:
                return uppercase ? "MAX" : "max";
            default:
                return uppercase ? "UNKNOWN" : "unknown";
        }
    }
};

using AggregateExpressionPtr = std::shared_ptr<AggregateExpression>;

/**
 * @brief Represents a star (*) in an expression, used for SELECT *
 */
class StarExpression : public Expression {
public:
    StarExpression() : Expression(ExprType::Star) {}

    std::string ToString() const override { return "*"; }
};

using StarExpressionPtr = std::shared_ptr<StarExpression>;

// Convenience functions for creating expressions
inline BinaryExpressionPtr MakeAndExpression(ExpressionPtr left, ExpressionPtr right) {
    return std::make_shared<BinaryExpression>(BinaryOpType::And, std::move(left), std::move(right));
}

inline BinaryExpressionPtr MakeOrExpression(ExpressionPtr left, ExpressionPtr right) {
    return std::make_shared<BinaryExpression>(BinaryOpType::Or, std::move(left), std::move(right));
}

inline BinaryExpressionPtr MakeComparisonExpression(BinaryOpType op, ExpressionPtr left, ExpressionPtr right) {
    return std::make_shared<BinaryExpression>(op, std::move(left), std::move(right));
}

inline ColumnExpressionPtr MakeColumnExpression(const std::string& table_name, const std::string& column_name) {
    return std::make_shared<ColumnExpression>(table_name, column_name);
}

inline StarExpressionPtr MakeStarExpression() {
    return std::make_shared<StarExpression>();
}

inline ConstantExpressionPtr MakeIntegerConstant(int64_t value) {
    return common::ConstantExpression::CreateInteger(value);
}

inline ConstantExpressionPtr MakeFloatConstant(float value) {
    return common::ConstantExpression::CreateFloat(value);
}

inline ConstantExpressionPtr MakeStringConstant(const std::string& value) {
    return common::ConstantExpression::CreateString(value);
}

inline ConstantExpressionPtr MakeBooleanConstant(bool value) {
    return common::ConstantExpression::CreateBoolean(value);
}

inline ConstantExpressionPtr MakeNullConstant() {
    return common::ConstantExpression::CreateNull();
}

}  // namespace pond::common