#pragma once

#include <unordered_map>

#include <arrow/api.h>

#include "common/error.h"
#include "common/expression.h"
#include "common/result.h"
#include "common/types.h"
#include "query/data/arrow_util.h"

namespace pond::query {

// Create a type-safe key struct
struct JoinKey {
    enum class Type { Int32, Int64, UInt32, UInt64, Float, Double, String, Bool, Timestamp };

    union Value {
        int32_t i32;
        int64_t i64;
        uint32_t u32;
        uint64_t u64;
        float f32;
        double f64;
        bool b;
        int64_t ts;  // for timestamp

        Value() : i64(0) {}  // Initialize to prevent undefined behavior
    };

    Type type;
    Value value;
    std::string str;  // for string type only

    bool operator==(const JoinKey& other) const {
        if (type != other.type)
            return false;

        switch (type) {
            case Type::Int32:
                return value.i32 == other.value.i32;
            case Type::Int64:
                return value.i64 == other.value.i64;
            case Type::UInt32:
                return value.u32 == other.value.u32;
            case Type::UInt64:
                return value.u64 == other.value.u64;
            case Type::Float:
                return value.f32 == other.value.f32;
            case Type::Double:
                return value.f64 == other.value.f64;
            case Type::Bool:
                return value.b == other.value.b;
            case Type::Timestamp:
                return value.ts == other.value.ts;
            case Type::String:
                return str == other.str;
        }
        return false;
    }
};

// Add hash function for JoinKey
struct JoinKeyHash {
    std::size_t operator()(const JoinKey& key) const {
        switch (key.type) {
            case JoinKey::Type::Int32:
                return std::hash<int32_t>{}(key.value.i32);
            case JoinKey::Type::Int64:
                return std::hash<int64_t>{}(key.value.i64);
            case JoinKey::Type::UInt32:
                return std::hash<uint32_t>{}(key.value.u32);
            case JoinKey::Type::UInt64:
                return std::hash<uint64_t>{}(key.value.u64);
            case JoinKey::Type::Float:
                return std::hash<float>{}(key.value.f32);
            case JoinKey::Type::Double:
                return std::hash<double>{}(key.value.f64);
            case JoinKey::Type::Bool:
                return std::hash<bool>{}(key.value.b);
            case JoinKey::Type::Timestamp:
                return std::hash<int64_t>{}(key.value.ts);
            case JoinKey::Type::String:
                return std::hash<std::string>{}(key.str);
        }
        return 0;
    }
};

/**
 * @brief Utility class for performing hash join on two ArrowDataBatches
 */
class HashJoinContext {
public:
    HashJoinContext(ArrowDataBatchSharedPtr left_batch,
                    ArrowDataBatchSharedPtr right_batch,
                    const common::Expression& condition,
                    common::JoinType join_type);

    /**
     * @brief Build the hash table from the right batch
     * @return Result<void>
     */
    common::Result<void> Build();

    /**
     * @brief Probe the hash table with the left batch
     * @return Result<ArrowDataBatchSharedPtr>
     */
    common::Result<ArrowDataBatchSharedPtr> Probe();

    common::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema();

private:
    ArrowDataBatchSharedPtr left_batch_;
    ArrowDataBatchSharedPtr right_batch_;
    const common::Expression& condition_;
    common::JoinType join_type_;

    std::shared_ptr<arrow::Array> left_array_;
    std::shared_ptr<arrow::Array> right_array_;

    std::string left_col_name_;
    std::string right_col_name_;

    std::shared_ptr<arrow::Schema> output_arrow_schema_;
    std::unordered_multimap<JoinKey, int64_t, JoinKeyHash> hash_table_;
};

}  // namespace pond::query