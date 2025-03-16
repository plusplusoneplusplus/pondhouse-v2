#pragma once

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include "common/expression.h"
#include "common/result.h"
#include "common/schema.h"

namespace pond::query {

using ArrowDataBatchSharedPtr = std::shared_ptr<arrow::RecordBatch>;

class GroupKey {
public:
    // Add a typed value to the key
    template <typename T>
    void AddValue(const T& value, bool is_null = false);

    // Compare operators for sorting/uniqueness
    bool operator<(const GroupKey& other) const;
    bool operator==(const GroupKey& other) const;

    // Convert to string representation if needed
    std::string ToString() const;

private:
    // Store values in their native type using std::variant
    std::vector<std::variant<int32_t, int64_t, uint32_t, uint64_t, float, double, std::string, bool>> values_;
    std::vector<bool> null_flags_;
};


    // Add a template helper to handle the type mapping
    template <typename T>
    struct AggregateTypeMap {
        using ArrayType = void;
        using BuilderType = void;
        using ValueType = void;
    };

// Specializations for each type
template <>
struct AggregateTypeMap<arrow::Int32Type> {
    using ArrayType = arrow::Int32Array;
    using BuilderType = arrow::Int32Builder;
    using ValueType = int32_t;
};

template <>
struct AggregateTypeMap<arrow::Int64Type> {
    using ArrayType = arrow::Int64Array;
    using BuilderType = arrow::Int64Builder;
    using ValueType = int64_t;
};

template <>
struct AggregateTypeMap<arrow::UInt32Type> {
    using ArrayType = arrow::UInt32Array;
    using BuilderType = arrow::UInt32Builder;
    using ValueType = uint32_t;
};

template <>
struct AggregateTypeMap<arrow::UInt64Type> {
    using ArrayType = arrow::UInt64Array;
    using BuilderType = arrow::UInt64Builder;
    using ValueType = uint64_t;
};

template <>
struct AggregateTypeMap<arrow::FloatType> {
    using ArrayType = arrow::FloatArray;
    using BuilderType = arrow::FloatBuilder;
    using ValueType = float;
};

template <>
struct AggregateTypeMap<arrow::DoubleType> {
    using ArrayType = arrow::DoubleArray;
    using BuilderType = arrow::DoubleBuilder;
    using ValueType = double;
};

template <>
struct AggregateTypeMap<arrow::StringType> {
    using ArrayType = arrow::StringArray;
    using BuilderType = arrow::StringBuilder;
    using ValueType = std::string;
};


/**
 * @brief Utility functions for working with Arrow data
 */
class ArrowUtil {
public:
    /**
     * @brief Create an empty array of the appropriate type for a column
     * @param type The column type
     * @return An empty array of the appropriate type
     */
    static arrow::Result<std::shared_ptr<arrow::Array>> CreateEmptyArray(common::ColumnType type);

    /**
     * @brief Create an array builder for a column type
     * @param type The column type
     * @return An array builder for the column type
     */
    static common::Result<std::shared_ptr<arrow::ArrayBuilder>> CreateArrayBuilder(common::ColumnType type);

    /**
     * @brief Create an empty record batch with the given schema
     * @param schema The schema for the record batch
     * @return An empty record batch with the given schema
     */
    static common::Result<ArrowDataBatchSharedPtr> CreateEmptyBatch(const common::Schema& schema);

    /**
     * @brief Create an empty record batch with an empty schema
     * @return An empty record batch with an empty schema
     */
    static ArrowDataBatchSharedPtr CreateEmptyBatch();

    /**
     * @brief Append a value to an array builder
     * @param input_array The input array
     * @param builder The array builder
     * @param row_idx The row index
     * @return A result containing the void or an error
     */
    static common::Result<void> AppendGroupValue(const std::shared_ptr<arrow::Array>& input_array,
                                                 std::shared_ptr<arrow::ArrayBuilder> builder,
                                                 int row_idx);

    /**
     * @brief Append a value to an array builder
     * @param builder The array builder
     * @param value The value to append
     * @param is_null Whether the value is null
     * @return A result containing the void or an error
     */
    template <typename BuilderType, typename ValueType>
    static common::Result<void> AppendValue(std::shared_ptr<arrow::ArrayBuilder> builder,
                                            const ValueType& value,
                                            bool is_null = false) {
        auto typed_builder = static_cast<BuilderType*>(builder.get());
        if (is_null) {
            auto status = typed_builder->AppendNull();
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append null: " + status.ToString());
            }
        } else {
            auto status = typed_builder->Append(value);
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append value: " + status.ToString());
            }
        }

        return common::Result<void>::success();
    }

    /**
     * @brief Compute the sum of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder, must match the type of the input array
     * @return A result containing the void or an error
     */
    static common::Result<void> ComputeSum(const std::shared_ptr<arrow::Array>& input_array,
                                           const std::vector<int>& row_indices,
                                           std::shared_ptr<arrow::ArrayBuilder> builder) {
        return ComputeAggregate<SumOperation>(input_array, row_indices, builder);
    }

    /**
     * @brief Compute the average of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder, must match the type of the input array
     * @return A result containing the void or an error
     */
    static common::Result<void> ComputeAverage(const std::shared_ptr<arrow::Array>& input_array,
                                               const std::vector<int>& row_indices,
                                               std::shared_ptr<arrow::ArrayBuilder> builder) {
        return ComputeAggregate<AverageOperation>(input_array, row_indices, builder);
    }

    /**
     * @brief Compute the minimum of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder, must match the type of the input array
     * @return A result containing the void or an error
     */
    static common::Result<void> ComputeMin(const std::shared_ptr<arrow::Array>& input_array,
                                           const std::vector<int>& row_indices,
                                           std::shared_ptr<arrow::ArrayBuilder> builder) {
        return ComputeAggregate<MinOperation>(input_array, row_indices, builder);
    }

    /**
     * @brief Compute the maximum of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder, must match the type of the input array
     * @return A result containing the void or an error
     */
    static common::Result<void> ComputeMax(const std::shared_ptr<arrow::Array>& input_array,
                                           const std::vector<int>& row_indices,
                                           std::shared_ptr<arrow::ArrayBuilder> builder) {
        return ComputeAggregate<MaxOperation>(input_array, row_indices, builder);
    }

    /**
     * @brief Compute the count of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder, must be an Int64Builder
     */
    static common::Result<void> ComputeCount(const std::shared_ptr<arrow::Array>& input_array,
                                             const std::vector<int>& row_indices,
                                             std::shared_ptr<arrow::Int64Builder> builder) {
        return ArrowUtil::AppendValue<arrow::Int64Builder>(builder, row_indices.size());
    }

    /**
     * @brief Append a key value to a group key
     * @param array The input array
     * @param row_idx The row index
     * @param group_key The group key
     */
    static common::Result<void> AppendGroupKeyValue(const std::shared_ptr<arrow::Array>& array,
                                                    int row_idx,
                                                    std::string& group_key);
    /**
     * @brief Apply a filter expression to a record batch
     * @param batch The record batch to filter
     * @param predicate The filter expression
     * @return The filtered record batch
     */
    static common::Result<ArrowDataBatchSharedPtr> ApplyPredicate(const ArrowDataBatchSharedPtr& batch,
                                                                  const std::shared_ptr<common::Expression>& predicate);

    /**
     * @brief Concatenate multiple record batches into a single batch
     * @param batches Vector of record batches to concatenate
     * @return Result containing the concatenated batch or an error
     *
     * All batches must have the same schema. If the vector is empty,
     * an empty record batch will be returned with a null schema.
     * If there's only one batch, it will be returned directly without copying.
     */
    static common::Result<ArrowDataBatchSharedPtr> ConcatenateBatches(
        const std::vector<ArrowDataBatchSharedPtr>& batches);

    /**
     * @brief Convert JSON string to Arrow RecordBatch
     * @param json_str The JSON string to convert
     * @param schema The schema for the record batch
     * @return Result containing the converted record batch or an error
     *
     * The JSON string should be an array of objects, where each object represents a row.
     * Each object should have keys matching the column names in the schema.
     * Example: [{"col1": 1, "col2": "value"}, {"col1": 2, "col2": "another value"}]
     */
    static common::Result<ArrowDataBatchSharedPtr> JsonToRecordBatch(const std::string& json_str,
                                                                     const common::Schema& schema);

    /**
     * @brief Convert JSON document to Arrow RecordBatch
     * @param json_doc The RapidJSON document to convert
     * @param schema The schema for the record batch
     * @return Result containing the converted record batch or an error
     */
    static common::Result<ArrowDataBatchSharedPtr> JsonToRecordBatch(const rapidjson::Document& json_doc,
                                                                     const common::Schema& schema);

    /**
     * @brief Extract unique group keys from specified columns in a record batch
     * @param batch The input record batch
     * @param group_by_columns Names of columns to group by
     * @return Result containing vector of unique group keys or an error
     */
    static common::Result<std::vector<GroupKey>> ExtractGroupKeys(const ArrowDataBatchSharedPtr& batch,
                                                                  const std::vector<std::string>& group_by_columns);

    /**
     * @brief Perform hash-based aggregation on a record batch
     * @param batch The input record batch
     * @param group_by_columns Names of columns to group by
     * @param agg_columns Names of columns to aggregate
     * @param agg_types Types of aggregation to perform for each column, must be the same size as agg_columns
     * @param output_columns_override Names of aggregate columns to include in the output, must be the same size as
     * agg_types or empty. if empty, the output columns will be the same as the input columns.
     * @return Result containing the aggregated record batch or an error
     *
     * The output batch will contain:
     * 1. All group by columns in their original order
     * 2. All aggregated columns in their original order
     *
     * Note:
     * 1. Null values will be included as a separate group and will be aggregated as well.
     * 2. The count function still counts NULL values unless explicitly filtered out.
     */
    static common::Result<ArrowDataBatchSharedPtr> HashAggregate(
        const ArrowDataBatchSharedPtr& batch,
        const std::vector<std::string>& group_by_columns,
        const std::vector<std::string>& agg_columns,
        const std::vector<common::AggregateType>& agg_types,
        const std::vector<std::string>& output_columns_override = {});

    /**
     * @brief Convert a record batch to a human-readable string format
     * @param batch The record batch to format
     * @param max_rows Maximum number of rows to include (0 for all rows)
     * @return A formatted string representation of the record batch
     *
     * Example output:
     * RecordBatch with 3 columns, 2 rows:
     * col1 (int32)    | col2 (string)  | col3 (double)
     * ----------------------------------------------
     * 1              | "hello"        | 3.14
     * 2              | null          | 2.718
     */
    static std::string BatchToString(const ArrowDataBatchSharedPtr& batch, size_t max_rows = 0);

private:
    /**
     * @brief Append a value to an array builder
     * @param input_array The input array
     * @param builder The array builder
     * @param row_idx The row index
     * @return A result containing the void or an error
     */
    template <typename ArrayType, typename BuilderType>
    static common::Result<void> AppendGroupValueInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                         std::shared_ptr<arrow::ArrayBuilder> builder,
                                                         int row_idx) {
        auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
        auto typed_builder = static_cast<BuilderType*>(builder.get());
        if (typed_array->IsNull(row_idx)) {
            auto status = typed_builder->AppendNull();
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append null: " + status.ToString());
            }
        } else {
            if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
                auto status = typed_builder->Append(typed_array->GetString(row_idx));
                if (!status.ok()) {
                    return common::Error(common::ErrorCode::Failure, "Failed to append string: " + status.ToString());
                }
            } else {
                auto status = typed_builder->Append(typed_array->Value(row_idx));
                if (!status.ok()) {
                    return common::Error(common::ErrorCode::Failure, "Failed to append value: " + status.ToString());
                }
            }
        }
        return common::Result<void>::success();
    }

    /**
     * @brief Append a key value to a group key
     * @param array The input array
     * @param row_idx The row index
     * @param group_key The group key
     */
    template <typename ArrayType>
    static void AppendGroupKeyValue(const std::shared_ptr<arrow::Array>& array, int row_idx, std::string& group_key) {
        auto typed_array = std::static_pointer_cast<ArrayType>(array);
        if (typed_array->IsNull(row_idx)) {
            group_key += "NULL";
        } else {
            if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
                group_key += typed_array->GetString(row_idx);
            } else if constexpr (std::is_same_v<ArrayType, arrow::BooleanArray>) {
                group_key += typed_array->Value(row_idx) ? "1" : "0";
            } else {
                group_key += std::to_string(typed_array->Value(row_idx));
            }
        }
    }

    /**
     * @brief Compute the sum of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder
     * @return A result containing the void or an error
     */
    template <typename ArrayType, typename BuilderType, typename SumType>
    static common::Result<void> ComputeSumInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                   const std::vector<int>& row_indices,
                                                   std::shared_ptr<arrow::ArrayBuilder> builder) {
        auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
        SumType sum = 0;
        bool all_null = true;

        for (int row_idx : row_indices) {
            if (!typed_array->IsNull(row_idx)) {
                sum += typed_array->Value(row_idx);
                all_null = false;
            }
        }

        return AppendValue<BuilderType>(builder, sum, all_null);
    }

    /**
     * @brief Compute the average of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder
     * @return A result containing the void or an error
     */
    template <typename ArrayType, typename BuilderType, typename ValueType>
    static common::Result<void> ComputeAverageInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                       const std::vector<int>& row_indices,
                                                       std::shared_ptr<arrow::ArrayBuilder> builder) {
        auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
        auto typed_builder = static_cast<arrow::DoubleBuilder*>(builder.get());

        double sum = 0.0;
        int count = 0;

        for (int row_idx : row_indices) {
            if (!typed_array->IsNull(row_idx)) {
                sum += typed_array->Value(row_idx);
                count++;
            }
        }

        if (count == 0) {
            auto status = typed_builder->AppendNull();
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append null: " + status.ToString());
            }
        } else {
            auto status = typed_builder->Append(sum / count);
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append value: " + status.ToString());
            }
        }

        return common::Result<void>::success();
    }

    // Template for Sum operation
    template <typename ArrayType, typename BuilderType, typename ValueType>
    struct SumOperation {
        static common::Result<void> compute(const std::shared_ptr<arrow::Array>& input_array,
                                            const std::vector<int>& row_indices,
                                            std::shared_ptr<arrow::ArrayBuilder> builder) {
            return ComputeSumInternal<ArrayType, BuilderType, ValueType>(input_array, row_indices, builder);
        }
    };

    // Template for Average operation
    template <typename ArrayType, typename BuilderType, typename ValueType>
    struct AverageOperation {
        static common::Result<void> compute(const std::shared_ptr<arrow::Array>& input_array,
                                            const std::vector<int>& row_indices,
                                            std::shared_ptr<arrow::ArrayBuilder> builder) {
            // Average is special because it always uses DoubleBuilder
            auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builder.get());

            double sum = 0.0;
            int count = 0;

            for (int row_idx : row_indices) {
                if (!typed_array->IsNull(row_idx)) {
                    sum += typed_array->Value(row_idx);
                    count++;
                }
            }

            if (count == 0) {
                auto status = typed_builder->AppendNull();
                if (!status.ok()) {
                    return common::Error(common::ErrorCode::Failure, "Failed to append null: " + status.ToString());
                }
            } else {
                auto status = typed_builder->Append(sum / count);
                if (!status.ok()) {
                    return common::Error(common::ErrorCode::Failure, "Failed to append value: " + status.ToString());
                }
            }

            return common::Result<void>::success();
        }
    };

    // Helper trait to detect numeric-only operations
    template <template <typename, typename, typename> class>
    struct IsNumericOnly : std::false_type {};

    // Helper function to handle the switch cases
    template <template <typename, typename, typename> class AggregateFunc>
    static common::Result<void> ComputeAggregate(const std::shared_ptr<arrow::Array>& input_array,
                                                 const std::vector<int>& row_indices,
                                                 std::shared_ptr<arrow::ArrayBuilder> builder) {
        constexpr bool numeric_only = IsNumericOnly<AggregateFunc>::value;

        switch (input_array->type_id()) {
            case arrow::Type::INT32:
                return AggregateFunc<typename AggregateTypeMap<arrow::Int32Type>::ArrayType,
                                     typename AggregateTypeMap<arrow::Int32Type>::BuilderType,
                                     typename AggregateTypeMap<arrow::Int32Type>::ValueType>::compute(input_array,
                                                                                                      row_indices,
                                                                                                      builder);
            case arrow::Type::INT64:
                return AggregateFunc<typename AggregateTypeMap<arrow::Int64Type>::ArrayType,
                                     typename AggregateTypeMap<arrow::Int64Type>::BuilderType,
                                     typename AggregateTypeMap<arrow::Int64Type>::ValueType>::compute(input_array,
                                                                                                      row_indices,
                                                                                                      builder);
            case arrow::Type::UINT32:
                return AggregateFunc<typename AggregateTypeMap<arrow::UInt32Type>::ArrayType,
                                     typename AggregateTypeMap<arrow::UInt32Type>::BuilderType,
                                     typename AggregateTypeMap<arrow::UInt32Type>::ValueType>::compute(input_array,
                                                                                                       row_indices,
                                                                                                       builder);
            case arrow::Type::UINT64:
                return AggregateFunc<typename AggregateTypeMap<arrow::UInt64Type>::ArrayType,
                                     typename AggregateTypeMap<arrow::UInt64Type>::BuilderType,
                                     typename AggregateTypeMap<arrow::UInt64Type>::ValueType>::compute(input_array,
                                                                                                       row_indices,
                                                                                                       builder);
            case arrow::Type::FLOAT:
                return AggregateFunc<typename AggregateTypeMap<arrow::FloatType>::ArrayType,
                                     typename AggregateTypeMap<arrow::FloatType>::BuilderType,
                                     typename AggregateTypeMap<arrow::FloatType>::ValueType>::compute(input_array,
                                                                                                      row_indices,
                                                                                                      builder);
            case arrow::Type::DOUBLE:
                return AggregateFunc<typename AggregateTypeMap<arrow::DoubleType>::ArrayType,
                                     typename AggregateTypeMap<arrow::DoubleType>::BuilderType,
                                     typename AggregateTypeMap<arrow::DoubleType>::ValueType>::compute(input_array,
                                                                                                       row_indices,
                                                                                                       builder);
            case arrow::Type::STRING:
                if constexpr (numeric_only) {
                    return common::Error(common::ErrorCode::NotImplemented, "Operation only supports numeric types");
                } else {
                    return AggregateFunc<typename AggregateTypeMap<arrow::StringType>::ArrayType,
                                         typename AggregateTypeMap<arrow::StringType>::BuilderType,
                                         typename AggregateTypeMap<arrow::StringType>::ValueType>::compute(input_array,
                                                                                                           row_indices,
                                                                                                           builder);
                }
            default:
                return common::Error(common::ErrorCode::NotImplemented,
                                     numeric_only ? "Operation only supports numeric types"
                                                  : "Operation only supports numeric and string types");
        }
    }

    // Template for Min operation
    template <typename ArrayType, typename BuilderType, typename ValueType>
    struct MinOperation {
        static common::Result<void> compute(const std::shared_ptr<arrow::Array>& input_array,
                                            const std::vector<int>& row_indices,
                                            std::shared_ptr<arrow::ArrayBuilder> builder) {
            return ComputeMinInternal<ArrayType, BuilderType, ValueType>(input_array, row_indices, builder);
        }
    };

    // Template for Max operation
    template <typename ArrayType, typename BuilderType, typename ValueType>
    struct MaxOperation {
        static common::Result<void> compute(const std::shared_ptr<arrow::Array>& input_array,
                                            const std::vector<int>& row_indices,
                                            std::shared_ptr<arrow::ArrayBuilder> builder) {
            return ComputeMaxInternal<ArrayType, BuilderType, ValueType>(input_array, row_indices, builder);
        }
    };

    /**
     * @brief Compute the min or max of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder
     * @param compute_min Whether to compute the min or max
     */
    template <typename ArrayType, typename BuilderType, typename ValueType>
    static common::Result<void> ComputeMinMaxInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                      const std::vector<int>& row_indices,
                                                      std::shared_ptr<arrow::ArrayBuilder> builder,
                                                      bool compute_min) {
        auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
        auto typed_builder = static_cast<BuilderType*>(builder.get());

        bool found_value = false;
        ValueType val;
        if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
            val = std::string();
        } else {
            val = compute_min ? std::numeric_limits<ValueType>::max() : std::numeric_limits<ValueType>::min();
        }

        for (int row_idx : row_indices) {
            if (!typed_array->IsNull(row_idx)) {
                if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
                    std::string current = typed_array->GetString(row_idx);
                    if (!found_value || (compute_min ? current < val : current > val)) {
                        val = current;
                        found_value = true;
                    }
                } else {
                    val = compute_min ? std::min(val, typed_array->Value(row_idx))
                                      : std::max(val, typed_array->Value(row_idx));
                    found_value = true;
                }
            }
        }

        if (!found_value) {
            auto status = typed_builder->AppendNull();
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append null");
            }
        } else {
            auto status = typed_builder->Append(val);
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append value");
            }
        }

        return common::Result<void>::success();
    }

    /**
     * @brief Compute the min of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder
     * @return A result containing the void or an error
     */
    template <typename ArrayType, typename BuilderType, typename ValueType>
    static common::Result<void> ComputeMinInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                   const std::vector<int>& row_indices,
                                                   std::shared_ptr<arrow::ArrayBuilder> builder) {
        return ComputeMinMaxInternal<ArrayType, BuilderType, ValueType>(input_array, row_indices, builder, true);
    }

    /**
     * @brief Compute the max of a column
     * @param input_array The input array
     * @param row_indices The row indices from the input array
     * @param builder The array builder
     * @return A result containing the void or an error
     */
    template <typename ArrayType, typename BuilderType, typename ValueType>
    static common::Result<void> ComputeMaxInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                   const std::vector<int>& row_indices,
                                                   std::shared_ptr<arrow::ArrayBuilder> builder) {
        return ComputeMinMaxInternal<ArrayType, BuilderType, ValueType>(input_array, row_indices, builder, false);
    }

    /**
     * @brief Helper function to create array builder for aggregation results
     * @param type Column type
     * @param agg_type Aggregation type
     * @return Result containing the array builder or an error
     */
    static common::Result<std::shared_ptr<arrow::ArrayBuilder>> CreateAggregateBuilder(common::ColumnType type,
                                                                                       common::AggregateType agg_type);

    /**
     * @brief Helper function to format a single cell value
     * @param array The array containing the value
     * @param row_idx The row index
     * @return Formatted string representation of the cell value
     */
    static std::string FormatCellValue(const std::shared_ptr<arrow::Array>& array, int row_idx);

    /**
     * @brief Helper function to get the width needed for a column
     * @param array The array containing the values
     * @param header The column header string
     * @param max_rows Maximum number of rows to consider
     * @return Required width for the column
     */
    static size_t GetColumnWidth(const std::shared_ptr<arrow::Array>& array,
                                 const std::string& header,
                                 size_t max_rows);
};

template <>
struct ArrowUtil::IsNumericOnly<ArrowUtil::SumOperation> : std::true_type {};

template <>
struct ArrowUtil::IsNumericOnly<ArrowUtil::AverageOperation> : std::true_type {};


}  // namespace pond::query