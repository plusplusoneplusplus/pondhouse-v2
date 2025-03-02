#pragma once

namespace pond::common {

enum class ExprType { Invalid, Column, Constant, BinaryOp, Aggregate, Star };

enum class BinaryOpType {
    Add,
    Subtract,
    Multiply,
    Divide,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
    Like
};

enum class AggregateType { Count, Sum, Avg, Min, Max };

}  // namespace pond::common