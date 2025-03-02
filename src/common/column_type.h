#pragma once

namespace pond::common {

enum class ColumnType {
    INVALID,
    INT32,
    INT64,
    UINT32,
    UINT64,
    FLOAT,
    DOUBLE,
    STRING,
    BINARY,
    BOOLEAN,
    TIMESTAMP,
    UUID
};

}  // namespace pond::common