#include "catalog/data_ingestor_util.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

#include "common/log.h"

namespace pond::catalog {

/*static*/ common::Result<std::unordered_map<std::string, std::string>> DataIngestorUtil::ExtractPartitionValues(
    const TableMetadata& metadata, const std::shared_ptr<arrow::RecordBatch>& batch) {
    using ReturnType = common::Result<std::unordered_map<std::string, std::string>>;

    std::unordered_map<std::string, std::string> partition_values;

    if (batch->num_rows() == 0 || metadata.partition_specs.empty()) {
        // just return empty partition values
        return ReturnType::success(partition_values);
    }

    // Extract partition values based on partition spec
    for (const auto& partition_field : metadata.partition_specs[0].fields) {
        if (!partition_field.IsValid()) {
            return common::Error(common::ErrorCode::InvalidOperation,
                                 "Invalid partition field: " + partition_field.name);
        }

        // Get the source field name from schema
        std::string source_field_name = metadata.schema->Fields()[partition_field.source_id].name;

        // Find the field in the input batch
        int field_idx = batch->schema()->GetFieldIndex(source_field_name);
        if (field_idx == -1) {
            return common::Error(common::ErrorCode::SchemaMismatch,
                                 "Partition field not found in input: " + source_field_name);
        }

        // Get the value from the first row (assuming all rows in batch have same partition value)
        auto array = batch->column(field_idx);
        if (array->null_count() > 0) {
            return common::Error(common::ErrorCode::InvalidOperation,
                                 "Partition field cannot be null: " + source_field_name);
        }

        // Extract raw value based on type
        std::string raw_value;
        int32_t int32_value = 0;
        int64_t int64_value = 0;
        double double_value = 0.0;
        std::string string_value;
        bool have_int32 = false;
        bool have_int64 = false;
        bool have_double = false;
        bool have_string = false;

        switch (array->type_id()) {
            case arrow::Type::INT32: {
                auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                int32_value = int_array->Value(0);
                raw_value = std::to_string(int32_value);
                have_int32 = true;
                break;
            }
            case arrow::Type::INT64: {
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
                int64_value = int_array->Value(0);
                raw_value = std::to_string(int64_value);
                have_int64 = true;
                break;
            }
            case arrow::Type::DOUBLE: {
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                double_value = double_array->Value(0);
                raw_value = std::to_string(double_value);
                have_double = true;
                break;
            }
            case arrow::Type::STRING: {
                auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
                string_value = str_array->GetString(0);
                raw_value = string_value;
                have_string = true;
                break;
            }
            case arrow::Type::DATE32: {
                auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
                int32_value = date_array->Value(0);  // days since epoch
                raw_value = std::to_string(int32_value);
                have_int32 = true;
                break;
            }
            case arrow::Type::TIMESTAMP: {
                auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
                int64_value = ts_array->Value(0);  // timestamp value
                raw_value = std::to_string(int64_value);
                have_int64 = true;
                break;
            }
            default:
                return common::Error(common::ErrorCode::NotImplemented,
                                     "Unsupported partition field type: " + array->type()->ToString());
        }

        // Apply transform to get partition value
        std::string partition_value;

        switch (partition_field.transform) {
            case catalog::Transform::IDENTITY: {
                // Use raw value as-is
                partition_value = raw_value;
                break;
            }
            case catalog::Transform::BUCKET: {
                // Apply a hash function and take modulo with num_buckets
                size_t hash_value = 0;

                if (have_int32) {
                    hash_value = std::hash<int32_t>{}(int32_value);
                } else if (have_int64) {
                    hash_value = std::hash<int64_t>{}(int64_value);
                } else if (have_double) {
                    hash_value = std::hash<double>{}(double_value);
                } else if (have_string) {
                    hash_value = std::hash<std::string>{}(string_value);
                }

                // Take modulo and ensure non-negative result
                size_t bucket = hash_value % partition_field.GetTransformParam();
                partition_value = std::to_string(bucket);
                break;
            }
            case catalog::Transform::TRUNCATE: {
                // Truncate numeric value to specified precision
                if (have_int32) {
                    // Truncate to nearest multiple of mod_n
                    int32_t truncated =
                        (int32_value / partition_field.GetTransformParam()) * partition_field.GetTransformParam();
                    partition_value = std::to_string(truncated);
                } else if (have_int64) {
                    int64_t truncated =
                        (int64_value / partition_field.GetTransformParam()) * partition_field.GetTransformParam();
                    partition_value = std::to_string(truncated);
                } else if (have_double) {
                    double truncated = std::floor(double_value / partition_field.GetTransformParam())
                                       * partition_field.GetTransformParam();
                    partition_value = std::to_string(static_cast<int64_t>(truncated));
                } else {
                    return common::Error(common::ErrorCode::InvalidOperation,
                                         "TRUNCATE transform only applies to numeric types");
                }
                break;
            }
            case catalog::Transform::YEAR: {
                // Extract year from timestamp or date
                if (array->type_id() == arrow::Type::DATE32) {
                    // Convert days since epoch to year
                    time_t epoch_seconds = int32_value * 86400;  // days to seconds
                    struct tm* date_tm = gmtime(&epoch_seconds);
                    partition_value = std::to_string(date_tm->tm_year + 1900);
                } else if (array->type_id() == arrow::Type::TIMESTAMP) {
                    // Convert timestamp to year
                    time_t epoch_seconds = int64_value / 1000000000;  // nanoseconds to seconds
                    struct tm* date_tm = gmtime(&epoch_seconds);
                    partition_value = std::to_string(date_tm->tm_year + 1900);
                } else if (have_string) {
                    // Assuming ISO format YYYY-MM-DD or similar
                    if (string_value.length() >= 4) {
                        partition_value = string_value.substr(0, 4);
                    } else {
                        return common::Error(common::ErrorCode::InvalidOperation,
                                             "String value not in expected date format");
                    }
                } else {
                    return common::Error(common::ErrorCode::InvalidOperation,
                                         "YEAR transform only applies to date/timestamp types");
                }
                break;
            }
            case catalog::Transform::MONTH: {
                // Extract month from timestamp or date
                if (array->type_id() == arrow::Type::DATE32) {
                    // Convert days since epoch to month
                    time_t epoch_seconds = int32_value * 86400;  // days to seconds
                    struct tm* date_tm = gmtime(&epoch_seconds);
                    partition_value = std::to_string(date_tm->tm_mon + 1);
                } else if (array->type_id() == arrow::Type::TIMESTAMP) {
                    // Convert timestamp to month
                    time_t epoch_seconds = int64_value / 1000000000;  // nanoseconds to seconds
                    struct tm* date_tm = gmtime(&epoch_seconds);
                    partition_value = std::to_string(date_tm->tm_mon + 1);
                } else if (have_string) {
                    // Assuming ISO format YYYY-MM-DD or similar
                    if (string_value.length() >= 7 && string_value[4] == '-') {
                        partition_value = string_value.substr(5, 2);
                    } else {
                        return common::Error(common::ErrorCode::InvalidOperation,
                                             "String value not in expected date format");
                    }
                } else {
                    return common::Error(common::ErrorCode::InvalidOperation,
                                         "MONTH transform only applies to date/timestamp types");
                }
                break;
            }
            case catalog::Transform::DAY: {
                // Extract day from timestamp or date
                if (array->type_id() == arrow::Type::DATE32) {
                    // Convert days since epoch to day
                    time_t epoch_seconds = int32_value * 86400;  // days to seconds
                    struct tm* date_tm = gmtime(&epoch_seconds);
                    partition_value = std::to_string(date_tm->tm_mday);
                } else if (array->type_id() == arrow::Type::TIMESTAMP) {
                    // Convert timestamp to day
                    time_t epoch_seconds = int64_value / 1000000000;  // nanoseconds to seconds
                    struct tm* date_tm = gmtime(&epoch_seconds);
                    partition_value = std::to_string(date_tm->tm_mday);
                } else if (have_string) {
                    // Assuming ISO format YYYY-MM-DD or similar
                    if (string_value.length() >= 10 && string_value[4] == '-' && string_value[7] == '-') {
                        partition_value = string_value.substr(8, 2);
                    } else {
                        return common::Error(common::ErrorCode::InvalidOperation,
                                             "String value not in expected date format");
                    }
                } else {
                    return common::Error(common::ErrorCode::InvalidOperation,
                                         "DAY transform only applies to date/timestamp types");
                }
                break;
            }
            default:
                return common::Error(
                    common::ErrorCode::NotImplemented,
                    "Unsupported transform type: " + std::to_string(static_cast<int>(partition_field.transform)));
        }

        // Add to partition values map
        partition_values[partition_field.name] = partition_value;
    }

    return ReturnType::success(partition_values);
}

/*static*/ common::Result<std::vector<PartitionedBatch>> DataIngestorUtil::PartitionRecordBatch(
    const TableMetadata& metadata, const std::shared_ptr<arrow::RecordBatch>& batch) {
    using ReturnType = common::Result<std::vector<PartitionedBatch>>;

    if (batch->num_rows() == 0 || metadata.partition_specs.empty()) {
        // No rows or no partition specs, just return the original batch with empty partition values
        PartitionedBatch result;
        result.batch = batch;
        result.partition_values = {};
        return ReturnType::success({result});
    }

    // For each unique set of partition values, we'll track which rows belong to it
    std::unordered_map<std::string, arrow::Datum> partition_masks;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> partition_values_lookup;

    // First pass: Generate partition values for each column and build partition groups
    for (const auto& spec : metadata.partition_specs) {
        for (const auto& field : spec.fields) {
            // Find the source column in the batch
            int column_idx = -1;

            // First, find the field name in the schema by source_id
            std::string field_name;
            for (size_t i = 0; i < metadata.schema->columns().size(); i++) {
                // In a real implementation, we would need a way to map field IDs to column indices
                // For now, we'll assume the field ID matches the column index
                if (static_cast<int>(i) == field.source_id) {
                    field_name = metadata.schema->columns()[i].name;
                    break;
                }
            }

            if (!field_name.empty()) {
                // Now find the column in the batch schema by name
                for (int i = 0; i < batch->schema()->num_fields(); i++) {
                    if (batch->schema()->field(i)->name() == field_name) {
                        column_idx = i;
                        break;
                    }
                }
            }

            if (column_idx < 0) {
                // Field not found in the schema, skip it
                continue;
            }

            auto column = batch->column(column_idx);

            // Generate partition values for the entire column in one vectorized operation
            auto partition_values_result = ExtractPartitionValuesVectorized(field, column);
            if (!partition_values_result.ok()) {
                return common::Error(
                    common::ErrorCode::InvalidArgument,
                    "Failed to extract partition values: " + partition_values_result.error().message());
            }

            auto partition_values_array = partition_values_result.value();

            // Update the partition masks based on these values
            UpdatePartitionMasks(partition_values_array, partition_masks, partition_values_lookup, field.name);
        }
    }

    // Second pass: Use the masks to slice the batch into partitioned batches
    std::vector<PartitionedBatch> result;
    for (const auto& [key, mask] : partition_masks) {
        // Use Arrow's Filter function to extract rows based on the mask
        auto filtered_batch_result = arrow::compute::Filter(batch, mask);
        if (!filtered_batch_result.ok()) {
            return common::Error(common::ErrorCode::InvalidArgument,
                                 "Failed to filter batch: " + filtered_batch_result.status().ToString());
        }

        // Convert the filtered result to a RecordBatch
        std::shared_ptr<arrow::RecordBatch> filtered_batch;

        // Create a table from the filtered data
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (int i = 0; i < batch->num_columns(); i++) {
            auto filtered_array = arrow::compute::Filter(batch->column(i), mask).ValueOrDie().make_array();
            arrays.push_back(filtered_array);
        }

        // Create a record batch from the filtered arrays
        filtered_batch = arrow::RecordBatch::Make(batch->schema(), arrays[0]->length(), arrays);

        if (filtered_batch->num_rows() == 0) {
            continue;  // Skip empty partitions
        }

        PartitionedBatch p_batch;
        p_batch.batch = filtered_batch;
        p_batch.partition_values = partition_values_lookup[key];
        result.push_back(std::move(p_batch));
    }

    return ReturnType::success(result);
}

/*static*/ common::Result<std::shared_ptr<arrow::StringArray>> DataIngestorUtil::ExtractPartitionValuesVectorized(
    const PartitionField& field, const std::shared_ptr<arrow::Array>& array) {
    using ReturnType = common::Result<std::shared_ptr<arrow::StringArray>>;

    // Create a string array builder to store the partition values
    arrow::StringBuilder builder;
    auto reserve_status = builder.Reserve(array->length());
    if (!reserve_status.ok()) {
        return common::Error(common::ErrorCode::InvalidArgument,
                             "Failed to reserve memory: " + reserve_status.ToString());
    }

    // Process based on the transform type
    switch (field.transform) {
        case Transform::IDENTITY: {
            // For identity transform, simply convert the values to strings
            auto cast_options = arrow::compute::CastOptions::Safe(arrow::utf8());
            auto cast_result = arrow::compute::Cast(array, arrow::utf8(), cast_options);
            if (!cast_result.ok()) {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "Failed to cast array to string: " + cast_result.status().ToString());
            }
            return ReturnType::success(
                std::static_pointer_cast<arrow::StringArray>(cast_result.ValueOrDie().make_array()));
        }

        case Transform::YEAR:
        case Transform::MONTH:
        case Transform::DAY:
        case Transform::HOUR: {
            // Handle date/timestamp transforms
            if (array->type_id() == arrow::Type::TIMESTAMP || array->type_id() == arrow::Type::DATE32
                || array->type_id() == arrow::Type::DATE64) {
                // Extract the timestamp values
                std::shared_ptr<arrow::Array> timestamp_array;
                if (array->type_id() != arrow::Type::TIMESTAMP) {
                    // Convert to timestamp if it's a date type
                    auto cast_options = arrow::compute::CastOptions::Safe(arrow::timestamp(arrow::TimeUnit::MILLI));
                    auto cast_result =
                        arrow::compute::Cast(array, arrow::timestamp(arrow::TimeUnit::MILLI), cast_options);
                    if (!cast_result.ok()) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to cast date to timestamp: " + cast_result.status().ToString());
                    }
                    timestamp_array = cast_result.ValueOrDie().make_array();
                } else {
                    timestamp_array = array;
                }

                // Get the numerical values based on transform type
                auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(timestamp_array);
                arrow::Int32Builder int_builder;
                auto reserve_status = int_builder.Reserve(array->length());
                if (!reserve_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to reserve memory: " + reserve_status.ToString());
                }

                for (int64_t i = 0; i < ts_array->length(); i++) {
                    if (ts_array->IsNull(i)) {
                        auto status = int_builder.AppendNull();
                        if (!status.ok()) {
                            return common::Error(common::ErrorCode::InvalidArgument,
                                                 "Failed to append null: " + status.ToString());
                        }
                        continue;
                    }

                    int64_t timestamp_ms = ts_array->Value(i);
                    time_t epoch_seconds = timestamp_ms / 1000;
                    struct tm* date_tm = gmtime(&epoch_seconds);

                    int value;
                    if (field.transform == Transform::YEAR) {
                        value = date_tm->tm_year + 1900;
                    } else if (field.transform == Transform::MONTH) {
                        value = date_tm->tm_mon + 1;
                    } else if (field.transform == Transform::DAY) {
                        value = date_tm->tm_mday;
                    } else {  // HOUR
                        value = date_tm->tm_hour;
                    }

                    auto status = int_builder.Append(value);
                    if (!status.ok()) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to append value: " + status.ToString());
                    }
                }

                std::shared_ptr<arrow::Int32Array> int_array;
                auto finish_status = int_builder.Finish(&int_array);
                if (!finish_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to finish int array: " + finish_status.ToString());
                }

                // Convert the int array to string
                auto cast_options = arrow::compute::CastOptions::Safe(arrow::utf8());
                auto cast_result = arrow::compute::Cast(int_array, arrow::utf8(), cast_options);
                if (!cast_result.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to cast int to string: " + cast_result.status().ToString());
                }
                return ReturnType::success(
                    std::static_pointer_cast<arrow::StringArray>(cast_result.ValueOrDie().make_array()));
            }
            // Add support for string dates (e.g., "2023-01-01")
            else if (array->type_id() == arrow::Type::STRING) {
                auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                arrow::StringBuilder string_builder;
                auto reserve_status = string_builder.Reserve(array->length());
                if (!reserve_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to reserve memory: " + reserve_status.ToString());
                }

                for (int64_t i = 0; i < string_array->length(); i++) {
                    if (string_array->IsNull(i)) {
                        auto status = string_builder.AppendNull();
                        if (!status.ok()) {
                            return common::Error(common::ErrorCode::InvalidArgument,
                                                 "Failed to append null: " + status.ToString());
                        }
                        continue;
                    }

                    std::string date_str = string_array->GetString(i);

                    // Try to parse the date string - support common formats
                    struct tm date_tm = {};
                    bool parsed = false;

                    // Try ISO format: YYYY-MM-DD
                    if (strptime(date_str.c_str(), "%Y-%m-%d", &date_tm)) {
                        parsed = true;
                    }
                    // Try format: MM/DD/YYYY
                    else if (strptime(date_str.c_str(), "%m/%d/%Y", &date_tm)) {
                        parsed = true;
                    }
                    // Try format: YYYY/MM/DD
                    else if (strptime(date_str.c_str(), "%Y/%m/%d", &date_tm)) {
                        parsed = true;
                    }
                    // Try format: DD-MM-YYYY
                    else if (strptime(date_str.c_str(), "%d-%m-%Y", &date_tm)) {
                        parsed = true;
                    }

                    if (!parsed) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to parse date string: " + date_str);
                    }

                    std::string value;
                    if (field.transform == Transform::YEAR) {
                        value = std::to_string(date_tm.tm_year + 1900);
                    } else if (field.transform == Transform::MONTH) {
                        value = std::to_string(date_tm.tm_mon + 1);
                    } else {  // DAY
                        value = std::to_string(date_tm.tm_mday);
                    }

                    auto status = string_builder.Append(value);
                    if (!status.ok()) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to append value: " + status.ToString());
                    }
                }

                std::shared_ptr<arrow::StringArray> result_array;
                auto finish_status = string_builder.Finish(&result_array);
                if (!finish_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to finish string array: " + finish_status.ToString());
                }
                return ReturnType::success(result_array);
            } else {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "YEAR/MONTH/DAY transform only applies to date/timestamp types or date strings");
            }
        }

        case Transform::BUCKET: {
            if (!field.transform_param.has_value() || field.transform_param.value() <= 0) {
                return common::Error(common::ErrorCode::InvalidArgument, "BUCKET transform parameter must be positive");
            }

            int32_t num_buckets = field.transform_param.value();

            // For BUCKET transform, we'll manually compute a hash and modulo
            arrow::Int32Builder int_builder;
            auto reserve_status = int_builder.Reserve(array->length());
            if (!reserve_status.ok()) {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "Failed to reserve memory: " + reserve_status.ToString());
            }

            // First convert the array to a string array
            std::shared_ptr<arrow::StringArray> string_array;
            if (array->type_id() != arrow::Type::STRING) {
                auto cast_options = arrow::compute::CastOptions::Safe(arrow::utf8());
                auto cast_result = arrow::compute::Cast(array, arrow::utf8(), cast_options);
                if (!cast_result.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to cast array to string: " + cast_result.status().ToString());
                }
                string_array = std::static_pointer_cast<arrow::StringArray>(cast_result.ValueOrDie().make_array());
            } else {
                string_array = std::static_pointer_cast<arrow::StringArray>(array);
            }

            // Simple string hash for all values
            for (int64_t i = 0; i < string_array->length(); i++) {
                if (string_array->IsNull(i)) {
                    auto status = int_builder.AppendNull();
                    if (!status.ok()) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to append null: " + status.ToString());
                    }
                    continue;
                }

                std::string value = string_array->GetString(i);
                // Simple string hash - consistent for same string values
                size_t hash_value = 0;
                for (char c : value) {
                    hash_value = hash_value * 31 + c;
                }

                // Apply modulo to get bucket number (always non-negative)
                int32_t bucket = hash_value % num_buckets;

                // Debug log to see hash and bucket values
                LOG_STATUS("BUCKET hash for value '%s': hash=%zu, bucket=%d", value.c_str(), hash_value, bucket);

                auto status = int_builder.Append(bucket);
                if (!status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to append value: " + status.ToString());
                }
            }

            std::shared_ptr<arrow::Int32Array> int_array;
            auto finish_status = int_builder.Finish(&int_array);
            if (!finish_status.ok()) {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "Failed to finish int array: " + finish_status.ToString());
            }

            // Convert the int array to string
            auto cast_options = arrow::compute::CastOptions::Safe(arrow::utf8());
            auto cast_result = arrow::compute::Cast(int_array, arrow::utf8(), cast_options);
            if (!cast_result.ok()) {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "Failed to cast int to string: " + cast_result.status().ToString());
            }
            return ReturnType::success(
                std::static_pointer_cast<arrow::StringArray>(cast_result.ValueOrDie().make_array()));
        }

        case Transform::TRUNCATE: {
            if (!field.transform_param.has_value()) {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "TRUNCATE transform requires a transform parameter");
            }

            int32_t width = field.transform_param.value();
            if (width <= 0) {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "TRUNCATE transform parameter must be positive");
            }

            // Handle string truncation
            if (array->type_id() == arrow::Type::STRING) {
                auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                arrow::StringBuilder string_builder;
                auto reserve_status = string_builder.Reserve(array->length());
                if (!reserve_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to reserve memory: " + reserve_status.ToString());
                }

                for (int64_t i = 0; i < string_array->length(); i++) {
                    if (string_array->IsNull(i)) {
                        auto status = string_builder.AppendNull();
                        if (!status.ok()) {
                            return common::Error(common::ErrorCode::InvalidArgument,
                                                 "Failed to append null: " + status.ToString());
                        }
                        continue;
                    }

                    std::string value = string_array->GetString(i);
                    if (value.length() > static_cast<size_t>(width)) {
                        value = value.substr(0, width);
                    }
                    auto status = string_builder.Append(value);
                    if (!status.ok()) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to append value: " + status.ToString());
                    }
                }

                std::shared_ptr<arrow::StringArray> result_array;
                auto finish_status = string_builder.Finish(&result_array);
                if (!finish_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to finish string array: " + finish_status.ToString());
                }
                return ReturnType::success(result_array);
            }
            // Handle numeric truncation
            else if (arrow::is_integer(array->type_id())) {
                // For numeric truncation, we divide by 10^width and then multiply back
                auto cast_options = arrow::compute::CastOptions::Safe(arrow::int64());
                auto cast_result = arrow::compute::Cast(array, arrow::int64(), cast_options);
                if (!cast_result.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to cast to int64: " + cast_result.status().ToString());
                }

                int64_t divisor = static_cast<int64_t>(std::pow(10, width));
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(cast_result.ValueOrDie().make_array());

                arrow::Int64Builder int_builder;
                auto reserve_status = int_builder.Reserve(array->length());
                if (!reserve_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to reserve memory: " + reserve_status.ToString());
                }

                for (int64_t i = 0; i < int_array->length(); i++) {
                    if (int_array->IsNull(i)) {
                        auto status = int_builder.AppendNull();
                        if (!status.ok()) {
                            return common::Error(common::ErrorCode::InvalidArgument,
                                                 "Failed to append null: " + status.ToString());
                        }
                        continue;
                    }

                    int64_t value = int_array->Value(i);
                    value = (value / divisor) * divisor;
                    auto append_status = int_builder.Append(value);
                    if (!append_status.ok()) {
                        return common::Error(common::ErrorCode::InvalidArgument,
                                             "Failed to append value: " + append_status.ToString());
                    }
                }

                std::shared_ptr<arrow::Int64Array> truncated_array;
                auto finish_status = int_builder.Finish(&truncated_array);
                if (!finish_status.ok()) {
                    return common::Error(common::ErrorCode::InvalidArgument,
                                         "Failed to finish int array: " + finish_status.ToString());
                }

                // Convert to string
                auto string_cast_options = arrow::compute::CastOptions::Safe(arrow::utf8());
                auto string_cast_result = arrow::compute::Cast(truncated_array, arrow::utf8(), string_cast_options);
                if (!string_cast_result.ok()) {
                    return common::Error(
                        common::ErrorCode::InvalidArgument,
                        "Failed to cast truncated int to string: " + string_cast_result.status().ToString());
                }

                return ReturnType::success(
                    std::static_pointer_cast<arrow::StringArray>(string_cast_result.ValueOrDie().make_array()));
            } else {
                return common::Error(common::ErrorCode::InvalidArgument,
                                     "TRUNCATE transform only applies to string and numeric types");
            }
        }

        default:
            return common::Error(common::ErrorCode::InvalidOperation,
                                 "Unsupported transform type: " + std::to_string(static_cast<int>(field.transform)));
    }
}

/*static*/ void DataIngestorUtil::UpdatePartitionMasks(
    const std::shared_ptr<arrow::StringArray>& partition_values_array,
    std::unordered_map<std::string, arrow::Datum>& partition_masks,
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>>& partition_values_lookup,
    const std::string& field_name) {
    const int64_t length = partition_values_array->length();

    if (partition_masks.empty()) {
        // First field being processed, initialize masks
        // Group rows by their partition value
        std::unordered_map<std::string, std::vector<int64_t>> value_to_rows;

        for (int64_t i = 0; i < length; i++) {
            if (partition_values_array->IsNull(i)) {
                continue;  // Skip null values
            }

            std::string value = partition_values_array->GetString(i);
            value_to_rows[value].push_back(i);
        }

        // Create masks for each unique partition value
        for (const auto& [value, row_indices] : value_to_rows) {
            std::string key = field_name + "=" + value;

            // Debug log for first field partitioning
            LOG_STATUS("Creating initial partition key: %s", key.c_str());

            // Create a boolean array with true at all rows with this value
            std::vector<bool> mask_values(length, false);
            for (int64_t idx : row_indices) {
                mask_values[idx] = true;
            }

            auto bool_builder = std::make_shared<arrow::BooleanBuilder>();
            auto status = bool_builder->AppendValues(mask_values);
            if (!status.ok()) {
                // Just log an error and continue - we're in a void function
                LOG_ERROR("Failed to append values to boolean builder: %s", status.ToString().c_str());
                continue;
            }

            std::shared_ptr<arrow::BooleanArray> mask_array;
            status = bool_builder->Finish(&mask_array);
            if (!status.ok()) {
                LOG_ERROR("Failed to finish boolean array: %s", status.ToString().c_str());
                continue;
            }

            partition_masks[key] = arrow::Datum(mask_array);
            partition_values_lookup[key][field_name] = value;
        }
    } else {
        // Update existing masks
        std::unordered_map<std::string, arrow::Datum> new_masks;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> new_values_lookup;

        // Group by existing key + new value
        std::unordered_map<std::string, std::vector<bool>> new_key_to_mask;
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> new_key_to_values;

        for (const auto& [existing_key, existing_mask] : partition_masks) {
            auto existing_values = partition_values_lookup[existing_key];

            // Convert mask to boolean array for processing
            std::shared_ptr<arrow::BooleanArray> mask_array =
                std::static_pointer_cast<arrow::BooleanArray>(existing_mask.make_array());

            // For each row in the existing partition, add it to the appropriate new partition
            for (int64_t i = 0; i < length; i++) {
                if (!mask_array->Value(i) || partition_values_array->IsNull(i)) {
                    continue;
                }

                std::string value = partition_values_array->GetString(i);
                std::string new_key = existing_key + "," + field_name + "=" + value;

                // Debug log for combined keys
                LOG_STATUS("Creating combined partition key: %s", new_key.c_str());

                // Initialize the mask if we haven't seen this key before
                if (new_key_to_mask.find(new_key) == new_key_to_mask.end()) {
                    new_key_to_mask[new_key] = std::vector<bool>(length, false);

                    // Copy and update partition values
                    auto new_values = existing_values;
                    new_values[field_name] = value;
                    new_key_to_values[new_key] = new_values;
                }

                // Set this row in the mask
                new_key_to_mask[new_key][i] = true;
            }
        }

        // Create arrow::Datum objects for each mask
        for (const auto& [new_key, mask_values] : new_key_to_mask) {
            auto bool_builder = std::make_shared<arrow::BooleanBuilder>();
            auto status = bool_builder->AppendValues(mask_values);
            if (!status.ok()) {
                LOG_ERROR("Failed to append values to boolean builder: %s", status.ToString().c_str());
                continue;
            }

            std::shared_ptr<arrow::BooleanArray> new_mask_array;
            status = bool_builder->Finish(&new_mask_array);
            if (!status.ok()) {
                LOG_ERROR("Failed to finish boolean array: %s", status.ToString().c_str());
                continue;
            }

            // Add to new masks
            new_masks[new_key] = arrow::Datum(new_mask_array);
            new_values_lookup[new_key] = new_key_to_values[new_key];
        }

        // Replace old masks with new ones
        partition_masks = std::move(new_masks);
        partition_values_lookup = std::move(new_values_lookup);

        // Debug log the final partitions
        LOG_STATUS("Final partition count: %zu", partition_masks.size());
        for (const auto& [key, _] : partition_masks) {
            LOG_STATUS("Final partition key: %s", key.c_str());
        }
    }
}
}  // namespace pond::catalog