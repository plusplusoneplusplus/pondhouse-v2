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

inline std::string ColumnTypeToString(ColumnType type) {
    switch (type) {
        case ColumnType::INVALID:
            return "INVALID";
        case ColumnType::INT32:
            return "INT32";
        case ColumnType::INT64:
            return "INT64";
        case ColumnType::UINT32:
            return "UINT32";
        case ColumnType::UINT64:
            return "UINT64";
        case ColumnType::FLOAT:
            return "FLOAT";
        case ColumnType::DOUBLE:
            return "DOUBLE";
        case ColumnType::STRING:
            return "STRING";
        case ColumnType::BINARY:
            return "BINARY";
        case ColumnType::BOOLEAN:
            return "BOOLEAN";
        case ColumnType::TIMESTAMP:
            return "TIMESTAMP";
        case ColumnType::UUID:
            return "UUID";
        default:
            return "UNKNOWN";
    }
}

}  // namespace pond::common
