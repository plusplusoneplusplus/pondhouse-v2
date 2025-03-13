#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include "common/expression.h"
#include "common/result.h"
#include "common/schema.h"

namespace pond::query {

using ArrowDataBatchSharedPtr = std::shared_ptr<arrow::RecordBatch>;

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
                                                 int row_idx) {
        switch (input_array->type_id()) {
            case arrow::Type::INT32:
                ArrowUtil::AppendGroupValueInternal<arrow::Int32Array, arrow::Int32Builder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::INT64:
                ArrowUtil::AppendGroupValueInternal<arrow::Int64Array, arrow::Int64Builder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::UINT32:
                ArrowUtil::AppendGroupValueInternal<arrow::UInt32Array, arrow::UInt32Builder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::UINT64:
                ArrowUtil::AppendGroupValueInternal<arrow::UInt64Array, arrow::UInt64Builder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::FLOAT:
                ArrowUtil::AppendGroupValueInternal<arrow::FloatArray, arrow::FloatBuilder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::DOUBLE:
                ArrowUtil::AppendGroupValueInternal<arrow::DoubleArray, arrow::DoubleBuilder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::STRING:
                ArrowUtil::AppendGroupValueInternal<arrow::StringArray, arrow::StringBuilder>(
                    input_array, builder, row_idx);
                break;
            case arrow::Type::BOOL:
                ArrowUtil::AppendGroupValueInternal<arrow::BooleanArray, arrow::BooleanBuilder>(
                    input_array, builder, row_idx);
                break;
            default: {
                return common::Error(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY");
            }
        }
        return common::Result<void>::success();
    }

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
     * @param builder The array builder
     * @return A result containing the void or an error
     */
    static common::Result<void> ComputeSum(const std::shared_ptr<arrow::Array>& input_array,
                                           const std::vector<int>& row_indices,
                                           std::shared_ptr<arrow::ArrayBuilder> builder) {
        switch (input_array->type_id()) {
            case arrow::Type::INT32: {
                return ArrowUtil::ComputeSumInternal<arrow::Int32Array, arrow::Int64Builder, int64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::INT64: {
                return ArrowUtil::ComputeSumInternal<arrow::Int64Array, arrow::Int64Builder, int64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::UINT32: {
                return ArrowUtil::ComputeSumInternal<arrow::UInt32Array, arrow::Int64Builder, int64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::UINT64: {
                return ArrowUtil::ComputeSumInternal<arrow::UInt64Array, arrow::Int64Builder, int64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::FLOAT: {
                return ArrowUtil::ComputeSumInternal<arrow::FloatArray, arrow::FloatBuilder, float>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::DOUBLE: {
                return ArrowUtil::ComputeSumInternal<arrow::DoubleArray, arrow::DoubleBuilder, double>(
                    input_array, row_indices, builder);
            }
            default: {
                return common::Error(common::ErrorCode::NotImplemented, "SUM only supports numeric types");
            }
        }
    }

    static common::Result<void> ComputeAverage(const std::shared_ptr<arrow::Array>& input_array,
                                               const std::vector<int>& row_indices,
                                               std::shared_ptr<arrow::ArrayBuilder> builder) {
        switch (input_array->type_id()) {
            case arrow::Type::INT32: {
                return ArrowUtil::ComputeAverageInternal<arrow::Int32Array>(input_array, row_indices, builder);
            }
            case arrow::Type::INT64: {
                return ArrowUtil::ComputeAverageInternal<arrow::Int64Array>(input_array, row_indices, builder);
            }
            case arrow::Type::UINT32: {
                return ArrowUtil::ComputeAverageInternal<arrow::UInt32Array>(input_array, row_indices, builder);
            }
            case arrow::Type::UINT64: {
                return ArrowUtil::ComputeAverageInternal<arrow::UInt64Array>(input_array, row_indices, builder);
            }
            case arrow::Type::FLOAT: {
                return ArrowUtil::ComputeAverageInternal<arrow::FloatArray>(input_array, row_indices, builder);
            }
            case arrow::Type::DOUBLE: {
                return ArrowUtil::ComputeAverageInternal<arrow::DoubleArray>(input_array, row_indices, builder);
            }
            default: {
                return common::Error(common::ErrorCode::NotImplemented, "AVG only supports numeric types");
            }
        }
    }

    static common::Result<void> ComputeMin(const std::shared_ptr<arrow::Array>& input_array,
                                           const std::vector<int>& row_indices,
                                           std::shared_ptr<arrow::ArrayBuilder> builder) {
        switch (input_array->type_id()) {
            case arrow::Type::INT32: {
                return ArrowUtil::ComputeMinInternal<arrow::Int32Array, arrow::Int32Builder, int32_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::INT64: {
                return ArrowUtil::ComputeMinInternal<arrow::Int64Array, arrow::Int64Builder, int64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::UINT32: {
                return ArrowUtil::ComputeMinInternal<arrow::UInt32Array, arrow::UInt32Builder, uint32_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::UINT64: {
                return ArrowUtil::ComputeMinInternal<arrow::UInt64Array, arrow::UInt64Builder, uint64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::FLOAT: {
                return ArrowUtil::ComputeMinInternal<arrow::FloatArray, arrow::FloatBuilder, float>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::DOUBLE: {
                return ArrowUtil::ComputeMinInternal<arrow::DoubleArray, arrow::DoubleBuilder, double>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::STRING: {
                return ArrowUtil::ComputeMinInternal<arrow::StringArray, arrow::StringBuilder, std::string>(
                    input_array, row_indices, builder);
            }
            default: {
                return common::Error(common::ErrorCode::NotImplemented, "MIN only supports numeric types");
            }
        }
    }

    static common::Result<void> ComputeMax(const std::shared_ptr<arrow::Array>& input_array,
                                           const std::vector<int>& row_indices,
                                           std::shared_ptr<arrow::ArrayBuilder> builder) {
        switch (input_array->type_id()) {
            case arrow::Type::INT32: {
                return ArrowUtil::ComputeMaxInternal<arrow::Int32Array, arrow::Int32Builder, int32_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::INT64: {
                return ArrowUtil::ComputeMaxInternal<arrow::Int64Array, arrow::Int64Builder, int64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::UINT32: {
                return ArrowUtil::ComputeMaxInternal<arrow::UInt32Array, arrow::UInt32Builder, uint32_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::UINT64: {
                return ArrowUtil::ComputeMaxInternal<arrow::UInt64Array, arrow::UInt64Builder, uint64_t>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::FLOAT: {
                return ArrowUtil::ComputeMaxInternal<arrow::FloatArray, arrow::FloatBuilder, float>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::DOUBLE: {
                return ArrowUtil::ComputeMaxInternal<arrow::DoubleArray, arrow::DoubleBuilder, double>(
                    input_array, row_indices, builder);
            }
            case arrow::Type::STRING: {
                return ArrowUtil::ComputeMaxInternal<arrow::StringArray, arrow::StringBuilder, std::string>(
                    input_array, row_indices, builder);
            }
            default: {
                return common::Error(common::ErrorCode::NotImplemented, "MAX only supports numeric types");
            }
        }
    }

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
                                                    std::string& group_key) {
        switch (array->type_id()) {
            case arrow::Type::INT32:
                ArrowUtil::AppendGroupKeyValue<arrow::Int32Array>(array, row_idx, group_key);
                break;
            case arrow::Type::INT64:
                ArrowUtil::AppendGroupKeyValue<arrow::Int64Array>(array, row_idx, group_key);
                break;
            case arrow::Type::UINT32:
                ArrowUtil::AppendGroupKeyValue<arrow::UInt32Array>(array, row_idx, group_key);
                break;
            case arrow::Type::UINT64:
                ArrowUtil::AppendGroupKeyValue<arrow::UInt64Array>(array, row_idx, group_key);
                break;
            case arrow::Type::FLOAT:
                ArrowUtil::AppendGroupKeyValue<arrow::FloatArray>(array, row_idx, group_key);
                break;
            case arrow::Type::DOUBLE:
                ArrowUtil::AppendGroupKeyValue<arrow::DoubleArray>(array, row_idx, group_key);
                break;
            case arrow::Type::STRING:
                ArrowUtil::AppendGroupKeyValue<arrow::StringArray>(array, row_idx, group_key);
                break;
            case arrow::Type::BOOL:
                ArrowUtil::AppendGroupKeyValue<arrow::BooleanArray>(array, row_idx, group_key);
                break;
            default: {
                return common::Error(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY");
            }
        }

        return common::Result<void>::success();
    }

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
    template <typename ArrayType>
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

    template <typename ArrayType, typename BuilderType, typename ValueType>
    static common::Result<void> ComputeMinInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                   const std::vector<int>& row_indices,
                                                   std::shared_ptr<arrow::ArrayBuilder> builder) {
        auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
        auto typed_builder = static_cast<BuilderType*>(builder.get());

        bool found_value = false;
        ValueType min_val;
        if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
            min_val = std::string();
        } else {
            min_val = std::numeric_limits<ValueType>::max();
        }

        for (int row_idx : row_indices) {
            if (!typed_array->IsNull(row_idx)) {
                if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
                    std::string current = typed_array->GetString(row_idx);
                    if (!found_value || current < min_val) {
                        min_val = current;
                        found_value = true;
                    }
                } else {
                    min_val = std::min(min_val, typed_array->Value(row_idx));
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
            auto status = typed_builder->Append(min_val);
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append value");
            }
        }

        return common::Result<void>::success();
    }

    template <typename ArrayType, typename BuilderType, typename ValueType>
    static common::Result<void> ComputeMaxInternal(const std::shared_ptr<arrow::Array>& input_array,
                                                   const std::vector<int>& row_indices,
                                                   std::shared_ptr<arrow::ArrayBuilder> builder) {
        auto typed_array = std::static_pointer_cast<ArrayType>(input_array);
        auto typed_builder = static_cast<BuilderType*>(builder.get());

        bool found_value = false;
        ValueType max_val;
        if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
            max_val = std::string();
        } else {
            max_val = std::numeric_limits<ValueType>::min();
        }

        for (int row_idx : row_indices) {
            if (!typed_array->IsNull(row_idx)) {
                if constexpr (std::is_same_v<ArrayType, arrow::StringArray>) {
                    std::string current = typed_array->GetString(row_idx);
                    if (!found_value || current > max_val) {
                        max_val = current;
                        found_value = true;
                    }
                } else {
                    max_val = std::max(max_val, typed_array->Value(row_idx));
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
            auto status = typed_builder->Append(max_val);
            if (!status.ok()) {
                return common::Error(common::ErrorCode::Failure, "Failed to append value");
            }
        }

        return common::Result<void>::success();
    }
};

}  // namespace pond::query