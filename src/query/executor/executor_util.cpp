#include "query/executor/executor_util.h"

#include "format/parquet/schema_converter.h"

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

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateProjectionBatch(PhysicalProjectionNode& node,
                                                                            ArrowDataBatchSharedPtr input_batch) {
    const auto& projections = node.Projections();
    // Create the output schema based on projection expressions
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    for (const auto& expr : projections) {
        // For column references, use the original field name
        if (expr->Type() == common::ExprType::Column) {
            auto col_expr = std::static_pointer_cast<common::ColumnExpression>(expr);
            const std::string& col_name = col_expr->ColumnName();

            // Find the field in the input schema
            int col_idx = -1;
            auto input_schema = input_batch->schema();
            for (int i = 0; i < input_schema->num_fields(); ++i) {
                if (input_schema->field(i)->name() == col_name) {
                    col_idx = i;
                    break;
                }
            }

            if (col_idx == -1) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Column not found: " + col_name));
            }

            // Add the field to output schema
            output_fields.push_back(input_schema->field(col_idx));
        } else if (expr->Type() == common::ExprType::Aggregate) {
            auto agg_expr = std::static_pointer_cast<common::AggregateExpression>(expr);
            auto agg_type = agg_expr->AggType();
            auto input_expr = agg_expr->Input();

            // For now, only support column references in aggregate input
            if (input_expr->Type() != common::ExprType::Column && input_expr->Type() != common::ExprType::Star) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::NotImplemented,
                                  std::format("Only column references or star are supported in aggregate input: {}",
                                              input_expr->ToString())));
            }

            auto result_name = agg_expr->ResultName();
            auto result_type = agg_expr->ResultType();

            output_fields.push_back(format::SchemaConverter::ToArrowField(
                common::ColumnSchema(result_name, result_type, common::Nullability::NOT_NULL)));
        } else {
            // For now, only support column references and star
            return common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only column references and star are supported in projections"));
        }
    }

    // Create output schema
    auto output_schema = arrow::schema(output_fields);

    // Create arrays for the output batch using output_schema
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    auto input_schema = input_batch->schema();

    // For each field in the output schema, find the corresponding column in the input batch
    for (int i = 0; i < output_schema->num_fields(); ++i) {
        const std::string& output_col_name = output_schema->field(i)->name();

        // Find the column in the input batch
        int col_idx = -1;
        for (int j = 0; j < input_schema->num_fields(); ++j) {
            if (input_schema->field(j)->name() == output_col_name) {
                col_idx = j;
                break;
            }
        }

        // If column not found, return error
        if (col_idx == -1) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::InvalidArgument, "Column not found: " + output_col_name));
        }

        // Add the column to the output arrays
        output_arrays.push_back(input_batch->column(col_idx));
    }

    // Create the output batch
    return common::Result<ArrowDataBatchSharedPtr>::success(
        arrow::RecordBatch::Make(output_schema, input_batch->num_rows(), output_arrays));
}

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateSortBatch(ArrowDataBatchSharedPtr input_batch,
                                                                      const std::vector<SortSpec>& sort_specs) {
    if (input_batch->num_rows() == 0) {
        return common::Result<ArrowDataBatchSharedPtr>::success(input_batch);
    }

    if (sort_specs.empty()) {
        return common::Result<ArrowDataBatchSharedPtr>::success(input_batch);
    }

    // Extract sort columns and directions from SortSpecs
    std::vector<std::string> sort_columns;
    std::vector<SortDirection> sort_directions;

    for (const auto& spec : sort_specs) {
        if (spec.expr->Type() != common::ExprType::Column) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::NotImplemented, "Only column references are supported in ORDER BY"));
        }
        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(spec.expr);
        sort_columns.push_back(col_expr->ColumnName());
        sort_directions.push_back(spec.direction);
    }

    // Create a vector of indices for the rows
    std::vector<int64_t> indices(input_batch->num_rows());
    for (int64_t i = 0; i < input_batch->num_rows(); i++) {
        indices[i] = i;
    }

    // Sort the indices based on the sort columns and directions
    std::sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
        for (size_t i = 0; i < sort_columns.size(); i++) {
            const std::string& col_name = sort_columns[i];
            bool ascending = sort_directions[i] == SortDirection::Ascending;

            // Get the column index
            int col_idx = input_batch->schema()->GetFieldIndex(col_name);
            if (col_idx < 0) {
                // Column not found, skip it
                continue;
            }

            auto array = input_batch->column(col_idx);

            // Handle nulls - nulls are considered greater than non-nulls
            bool a_is_null = array->IsNull(a);
            bool b_is_null = array->IsNull(b);

            if (a_is_null && !b_is_null) {
                return ascending;  // If ascending, nulls first (true), else nulls last (false)
            } else if (!a_is_null && b_is_null) {
                return !ascending;  // If ascending, non-nulls last (false), else non-nulls first (true)
            } else if (a_is_null && b_is_null) {
                continue;  // Both null, move to next column
            }

            // Compare non-null values based on type
            int comparison = 0;
            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    comparison = typed_array->Value(a) - typed_array->Value(b);
                    break;
                }
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                    int64_t val_a = typed_array->Value(a);
                    int64_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::UINT32: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt32Array>(array);
                    comparison = typed_array->Value(a) - typed_array->Value(b);
                    break;
                }
                case arrow::Type::UINT64: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt64Array>(array);
                    uint64_t val_a = typed_array->Value(a);
                    uint64_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    float val_a = typed_array->Value(a);
                    float val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                    double val_a = typed_array->Value(a);
                    double val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                    comparison = typed_array->GetString(a).compare(typed_array->GetString(b));
                    break;
                }
                case arrow::Type::BOOL: {
                    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                    bool val_a = typed_array->Value(a);
                    bool val_b = typed_array->Value(b);
                    comparison = val_a - val_b;
                    break;
                }
                case arrow::Type::TIMESTAMP: {
                    auto typed_array = std::static_pointer_cast<arrow::TimestampArray>(array);
                    comparison = typed_array->Value(a) - typed_array->Value(b);
                    break;
                }
                default:
                    // Unsupported type, skip this column
                    continue;
            }

            if (comparison != 0) {
                return ascending ? (comparison < 0) : (comparison > 0);
            }
        }

        // If all columns are equal, maintain original order
        return a < b;
    });

    // Create a new batch with the sorted rows
    std::vector<std::shared_ptr<arrow::Array>> sorted_arrays;
    for (int col_idx = 0; col_idx < input_batch->num_columns(); col_idx++) {
        auto input_array = input_batch->column(col_idx);
        auto array_type = input_array->type();

        auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
        if (!column_type_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
        }

        // Create a builder for this column
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Append values in sorted order
        for (int64_t idx : indices) {
            auto result = ArrowUtil::AppendGroupValue(input_array, builder, idx);
            if (!result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(result.error());
            }
        }

        // Finish the array
        std::shared_ptr<arrow::Array> sorted_array;
        auto status = builder->Finish(&sorted_array);
        if (!status.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finish array: " + status.ToString()));
        }

        sorted_arrays.push_back(sorted_array);
    }

    // Create the sorted batch
    auto sorted_batch = arrow::RecordBatch::Make(input_batch->schema(), input_batch->num_rows(), sorted_arrays);

    return common::Result<ArrowDataBatchSharedPtr>::success(sorted_batch);
}

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateLimitBatch(ArrowDataBatchSharedPtr input_batch,
                                                                       size_t limit,
                                                                       size_t offset) {
    if (!input_batch) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Input batch is null"));
    }

    // If offset is beyond the input size, return empty batch
    if (offset >= input_batch->num_rows()) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (int i = 0; i < input_batch->num_columns(); i++) {
            auto array_type = input_batch->column(i)->type();
            auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
            if (!column_type_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
            }
            auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
            if (!builder_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
            }
            std::shared_ptr<arrow::Array> empty_array;
            if (!builder_result.value()->Finish(&empty_array).ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::Failure, "Failed to finish array"));
            }
            empty_arrays.push_back(empty_array);
        }
        return common::Result<ArrowDataBatchSharedPtr>::success(
            arrow::RecordBatch::Make(input_batch->schema(), 0, empty_arrays));
    }

    // Calculate actual number of rows to include
    size_t available_rows = input_batch->num_rows() - offset;
    size_t actual_limit = std::min(limit, available_rows);

    // Create arrays for the output batch
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    for (int i = 0; i < input_batch->num_columns(); i++) {
        auto input_array = input_batch->column(i);
        auto array_type = input_array->type();

        // Get column type
        auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
        if (!column_type_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
        }

        // Create builder
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Append values from offset to offset + actual_limit
        for (size_t j = offset; j < offset + actual_limit; j++) {
            auto result = ArrowUtil::AppendGroupValue(input_array, builder, j);
            if (!result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(result.error());
            }
        }

        // Finish array
        std::shared_ptr<arrow::Array> output_array;
        if (!builder->Finish(&output_array).ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finish array"));
        }
        output_arrays.push_back(output_array);
    }

    // Create output batch
    return common::Result<ArrowDataBatchSharedPtr>::success(
        arrow::RecordBatch::Make(input_batch->schema(), actual_limit, output_arrays));
}

common::Result<JoinKey> GetJoinKeyFromArray(const std::shared_ptr<arrow::Array>& array, int64_t index) {
    JoinKey key;
    switch (array->type_id()) {
        case arrow::Type::INT32: {
            auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
            key.type = JoinKey::Type::Int32;
            key.value.i32 = typed_array->Value(index);
            break;
        }
        case arrow::Type::INT64: {
            auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
            key.type = JoinKey::Type::Int64;
            key.value.i64 = typed_array->Value(index);
            break;
        }
        case arrow::Type::UINT32: {
            auto typed_array = std::static_pointer_cast<arrow::UInt32Array>(array);
            key.type = JoinKey::Type::UInt32;
            key.value.u32 = typed_array->Value(index);
            break;
        }
        case arrow::Type::UINT64: {
            auto typed_array = std::static_pointer_cast<arrow::UInt64Array>(array);
            key.type = JoinKey::Type::UInt64;
            key.value.u64 = typed_array->Value(index);
            break;
        }
        case arrow::Type::FLOAT: {
            auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
            key.type = JoinKey::Type::Float;
            key.value.f32 = typed_array->Value(index);
            break;
        }
        case arrow::Type::DOUBLE: {
            auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
            key.type = JoinKey::Type::Double;
            key.value.f64 = typed_array->Value(index);
            break;
        }
        case arrow::Type::STRING: {
            auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
            key.type = JoinKey::Type::String;
            key.str = typed_array->GetString(index);
            break;
        }
        case arrow::Type::BOOL: {
            auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
            key.type = JoinKey::Type::Bool;
            key.value.b = typed_array->Value(index);
            break;
        }
        case arrow::Type::TIMESTAMP: {
            auto typed_array = std::static_pointer_cast<arrow::TimestampArray>(array);
            key.type = JoinKey::Type::Timestamp;
            key.value.ts = typed_array->Value(index);
            break;
        }
        default:
            return common::Result<JoinKey>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Unsupported join column type: " + array->type()->ToString()));
    }
    return common::Result<JoinKey>::success(key);
}

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateHashJoinBatch(ArrowDataBatchSharedPtr left_batch,
                                                                          ArrowDataBatchSharedPtr right_batch,
                                                                          const common::Expression& condition) {
    // Validate input batches
    if (!left_batch) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Left batch is null"));
    }
    if (!right_batch) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Right batch is null"));
    }

    // For now, only support equality join conditions
    if (condition.Type() != common::ExprType::BinaryOp) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
            common::ErrorCode::NotImplemented, "Only binary operations are supported in join conditions"));
    }

    auto binary_op = static_cast<const common::BinaryExpression&>(condition);
    if (binary_op.OpType() != common::BinaryOpType::Equal) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::NotImplemented, "Only equality joins are supported"));
    }

    // Extract join columns
    if (binary_op.Left()->Type() != common::ExprType::Column || binary_op.Right()->Type() != common::ExprType::Column) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::NotImplemented, "Join condition must be between two columns"));
    }

    auto left_col_expr = static_cast<const common::ColumnExpression&>(*binary_op.Left());
    auto right_col_expr = static_cast<const common::ColumnExpression&>(*binary_op.Right());

    std::string left_col_name = left_col_expr.ColumnName();
    std::string right_col_name = right_col_expr.ColumnName();

    // Find column indices
    int left_col_idx = left_batch->schema()->GetFieldIndex(left_col_name);
    int right_col_idx = right_batch->schema()->GetFieldIndex(right_col_name);

    if (left_col_idx < 0) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Left join column not found: " + left_col_name));
    }
    if (right_col_idx < 0) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Right join column not found: " + right_col_name));
    }

    // Get join column arrays
    auto left_array = left_batch->column(left_col_idx);
    auto right_array = right_batch->column(right_col_idx);

    // Check if column types are compatible for join
    if (left_array->type_id() != right_array->type_id()) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument,
                          "Join column types do not match: " + left_array->type()->ToString() + " vs "
                              + right_array->type()->ToString()));
    }

    // Build hash table from right side
    std::unordered_multimap<JoinKey, int64_t, JoinKeyHash> hash_table;
    for (int64_t i = 0; i < right_batch->num_rows(); i++) {
        if (right_array->IsNull(i))
            continue;

        auto key_result = GetJoinKeyFromArray(right_array, i);
        if (!key_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(key_result.error());
        }
        JoinKey key = key_result.value();
        hash_table.insert({key, i});
    }

    // Create output schema by combining left and right schemas
    std::vector<std::shared_ptr<arrow::Field>> output_fields;

    // Add left fields
    for (int i = 0; i < left_batch->schema()->num_fields(); i++) {
        output_fields.push_back(left_batch->schema()->field(i));
    }

    // Add right fields (except join column to avoid duplication)
    for (int i = 0; i < right_batch->schema()->num_fields(); i++) {
        if (i != right_col_idx) {
            // Avoid name collisions by prefixing with "right_" if needed
            auto field = right_batch->schema()->field(i);
            std::string field_name = field->name();
            if (left_batch->schema()->GetFieldIndex(field_name) >= 0) {
                field_name = "right_" + field_name;
            }
            output_fields.push_back(arrow::field(field_name, field->type(), field->nullable()));
        }
    }

    auto output_schema = arrow::schema(output_fields);

    // Probe phase - find matches for each row in left batch
    std::vector<std::vector<std::shared_ptr<arrow::Scalar>>> output_rows;

    for (int64_t i = 0; i < left_batch->num_rows(); i++) {
        if (left_array->IsNull(i))
            continue;

        auto key_result = GetJoinKeyFromArray(left_array, i);
        if (!key_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(key_result.error());
        }
        JoinKey key = key_result.value();
        auto range = hash_table.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            int64_t right_row_idx = it->second;

            // Create a new output row by combining left and right rows
            std::vector<std::shared_ptr<arrow::Scalar>> output_row;

            // Add values from left row
            for (int j = 0; j < left_batch->num_columns(); j++) {
                auto result = ArrowUtil::GetScalar(left_batch->column(j), i);
                if (!result.ok()) {
                    return common::Result<ArrowDataBatchSharedPtr>::failure(result.error());
                }
                output_row.push_back(result.value());
            }

            // Add values from right row (except join column)
            for (int j = 0; j < right_batch->num_columns(); j++) {
                if (j != right_col_idx) {
                    auto result = ArrowUtil::GetScalar(right_batch->column(j), right_row_idx);
                    if (!result.ok()) {
                        return common::Result<ArrowDataBatchSharedPtr>::failure(result.error());
                    }
                    output_row.push_back(result.value());
                }
            }

            output_rows.push_back(output_row);
        }
    }

    // If no matches found, return empty batch with correct schema
    if (output_rows.empty()) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (int i = 0; i < output_schema->num_fields(); i++) {
            auto field = output_schema->field(i);
            auto column_type_result = format::SchemaConverter::FromArrowDataType(field->type());
            if (!column_type_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
            }
            auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
            if (!builder_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
            }
            std::shared_ptr<arrow::Array> empty_array;
            if (!builder_result.value()->Finish(&empty_array).ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::Failure, "Failed to finish array"));
            }
            empty_arrays.push_back(empty_array);
        }
        return common::Result<ArrowDataBatchSharedPtr>::success(
            arrow::RecordBatch::Make(output_schema, 0, empty_arrays));
    }

    // Create arrays for output batch
    std::vector<std::shared_ptr<arrow::Array>> output_arrays(output_schema->num_fields());
    for (int i = 0; i < output_schema->num_fields(); i++) {
        auto field = output_schema->field(i);
        auto column_type_result = format::SchemaConverter::FromArrowDataType(field->type());
        if (!column_type_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
        }
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Add values from each output row
        for (const auto& row : output_rows) {
            auto status = builder->AppendScalar(*row[i]);
            if (!status.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::Failure, "Failed to append value: " + status.ToString()));
            }
        }

        // Finish array
        std::shared_ptr<arrow::Array> output_array;
        if (!builder->Finish(&output_array).ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finish array"));
        }
        output_arrays[i] = output_array;
    }

    // Create output batch
    return common::Result<ArrowDataBatchSharedPtr>::success(
        arrow::RecordBatch::Make(output_schema, output_rows.size(), output_arrays));
}

}  // namespace pond::query