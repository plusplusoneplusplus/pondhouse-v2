#include "query/executor/hash_join.h"

#include <unordered_set>

#include "format/parquet/schema_converter.h"

namespace pond::query {

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

HashJoinContext::HashJoinContext(ArrowDataBatchSharedPtr left_batch,
                                 ArrowDataBatchSharedPtr right_batch,
                                 const common::Expression& condition,
                                 common::JoinType join_type)
    : left_batch_(left_batch), right_batch_(right_batch), condition_(condition), join_type_(join_type) {}

common::Result<void> HashJoinContext::Build() {
    using ReturnType = common::Result<void>;

    // Validate input batches
    if (!left_batch_) {
        return ReturnType::failure(common::Error(common::ErrorCode::InvalidArgument, "Left batch is null"));
    }
    if (!right_batch_) {
        return ReturnType::failure(common::Error(common::ErrorCode::InvalidArgument, "Right batch is null"));
    }

    // For now, only support equality join conditions
    if (condition_.Type() != common::ExprType::BinaryOp) {
        return ReturnType::failure(common::Error(common::ErrorCode::NotImplemented,
                                                 "Only binary operations are supported in join conditions"));
    }

    auto binary_op = static_cast<const common::BinaryExpression&>(condition_);
    if (binary_op.OpType() != common::BinaryOpType::Equal) {
        return ReturnType::failure(
            common::Error(common::ErrorCode::NotImplemented, "Only equality joins are supported"));
    }

    // Extract join columns
    if (binary_op.Left()->Type() != common::ExprType::Column || binary_op.Right()->Type() != common::ExprType::Column) {
        return ReturnType::failure(
            common::Error(common::ErrorCode::NotImplemented, "Join condition must be between two columns"));
    }

    if (join_type_ != common::JoinType::Inner) {
        return ReturnType::failure(common::Error(common::ErrorCode::NotImplemented, "Only inner join is supported"));
    }

    auto left_col_expr = static_cast<const common::ColumnExpression&>(*binary_op.Left());
    auto right_col_expr = static_cast<const common::ColumnExpression&>(*binary_op.Right());

    left_col_name_ = left_col_expr.ColumnName();
    right_col_name_ = right_col_expr.ColumnName();

    // Find column indices
    int left_col_idx = left_batch_->schema()->GetFieldIndex(left_col_name_);
    int right_col_idx = right_batch_->schema()->GetFieldIndex(right_col_name_);

    if (left_col_idx < 0) {
        return ReturnType::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Left join column not found: " + left_col_name_));
    }
    if (right_col_idx < 0) {
        return ReturnType::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Right join column not found: " + right_col_name_));
    }

    // Get join column arrays
    left_array_ = left_batch_->column(left_col_idx);
    right_array_ = right_batch_->column(right_col_idx);

    // Check if column types are compatible for join
    if (left_array_->type_id() != right_array_->type_id()) {
        return ReturnType::failure(common::Error(common::ErrorCode::InvalidArgument,
                                                 "Join column types do not match: " + left_array_->type()->ToString()
                                                     + " vs " + right_array_->type()->ToString()));
    }

    // Create output schema by combining left and right schemas
    std::vector<std::shared_ptr<arrow::Field>> output_fields;

    // Track column names to detect conflicts
    std::unordered_set<std::string> used_column_names;

    // Add left fields first
    for (int i = 0; i < left_batch_->schema()->num_fields(); i++) {
        auto field = left_batch_->schema()->field(i);
        std::string field_name = field->name();

        // Add the field with its original name
        output_fields.push_back(arrow::field(field_name, field->type(), field->nullable()));
        used_column_names.insert(field_name);
    }

    // Add right fields, handling conflicts
    for (int i = 0; i < right_batch_->schema()->num_fields(); i++) {
        auto field = right_batch_->schema()->field(i);
        std::string field_name = field->name();

        // Skip the join column from right side if it has the same name as the left join column
        if (field_name == right_col_name_ && left_col_name_ == right_col_name_) {
            continue;
        }

        // If this is the join column with a different name, keep it as is
        if (field_name == right_col_name_ && left_col_name_ != right_col_name_) {
            // No prefix needed for join columns with different names
        }
        // For non-join columns with name conflicts, add prefix
        else if (used_column_names.find(field_name) != used_column_names.end()) {
            field_name = "right_" + field_name;
        }

        output_fields.push_back(arrow::field(field_name, field->type(), field->nullable()));
        used_column_names.insert(field_name);
    }

    output_arrow_schema_ = arrow::schema(output_fields);

    // Build hash table from right side
    for (int64_t i = 0; i < right_batch_->num_rows(); i++) {
        if (right_array_->IsNull(i))
            continue;

        auto key_result = GetJoinKeyFromArray(right_array_, i);
        if (!key_result.ok()) {
            return ReturnType::failure(key_result.error());
        }
        JoinKey key = key_result.value();
        hash_table_.insert({key, i});
    }

    return ReturnType::success();
}

common::Result<ArrowDataBatchSharedPtr> HashJoinContext::Probe() {
    using ReturnType = common::Result<ArrowDataBatchSharedPtr>;

    // Probe phase - find matches for each row in left batch
    std::vector<std::vector<std::shared_ptr<arrow::Scalar>>> output_rows;

    for (int64_t i = 0; i < left_batch_->num_rows(); i++) {
        if (left_array_->IsNull(i))
            continue;

        auto key_result = GetJoinKeyFromArray(left_array_, i);
        if (!key_result.ok()) {
            return ReturnType::failure(key_result.error());
        }
        JoinKey key = key_result.value();
        auto range = hash_table_.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            int64_t right_row_idx = it->second;

            // Create a new output row by combining left and right rows
            std::vector<std::shared_ptr<arrow::Scalar>> output_row;

            // Add all values from left row
            for (int j = 0; j < left_batch_->num_columns(); j++) {
                auto result = ArrowUtil::GetScalar(left_batch_->column(j), i);
                if (!result.ok()) {
                    return ReturnType::failure(result.error());
                }
                output_row.push_back(result.value());
            }

            // Add values from right row, skipping the join column if it has the same name
            for (int j = 0; j < right_batch_->num_columns(); j++) {
                auto field_name = right_batch_->schema()->field(j)->name();

                // Skip the join column from right side if it has the same name as the left join column
                if (field_name == right_col_name_ && left_col_name_ == right_col_name_) {
                    continue;
                }

                auto result = ArrowUtil::GetScalar(right_batch_->column(j), right_row_idx);
                if (!result.ok()) {
                    return ReturnType::failure(result.error());
                }
                output_row.push_back(result.value());
            }

            output_rows.push_back(output_row);
        }
    }

    // If no matches found, return empty batch with correct schema
    if (output_rows.empty()) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (int i = 0; i < output_arrow_schema_->num_fields(); i++) {
            auto field = output_arrow_schema_->field(i);
            auto column_type_result = format::SchemaConverter::FromArrowDataType(field->type());
            if (!column_type_result.ok()) {
                return ReturnType::failure(column_type_result.error());
            }
            auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
            if (!builder_result.ok()) {
                return ReturnType::failure(builder_result.error());
            }
            std::shared_ptr<arrow::Array> empty_array;
            if (!builder_result.value()->Finish(&empty_array).ok()) {
                return ReturnType::failure(common::Error(common::ErrorCode::Failure, "Failed to finish array"));
            }
            empty_arrays.push_back(empty_array);
        }
        return ReturnType::success(arrow::RecordBatch::Make(output_arrow_schema_, 0, empty_arrays));
    }

    // Create arrays for output batch
    std::vector<std::shared_ptr<arrow::Array>> output_arrays(output_arrow_schema_->num_fields());
    for (int i = 0; i < output_arrow_schema_->num_fields(); i++) {
        auto field = output_arrow_schema_->field(i);
        auto column_type_result = format::SchemaConverter::FromArrowDataType(field->type());
        if (!column_type_result.ok()) {
            return ReturnType::failure(column_type_result.error());
        }
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return ReturnType::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Add values from each output row
        for (const auto& row : output_rows) {
            auto status = builder->AppendScalar(*row[i]);
            if (!status.ok()) {
                return ReturnType::failure(
                    common::Error(common::ErrorCode::Failure, "Failed to append value: " + status.ToString()));
            }
        }

        // Finish array
        std::shared_ptr<arrow::Array> output_array;
        if (!builder->Finish(&output_array).ok()) {
            return ReturnType::failure(common::Error(common::ErrorCode::Failure, "Failed to finish array"));
        }
        output_arrays[i] = output_array;
    }

    // Create output batch
    return ReturnType::success(arrow::RecordBatch::Make(output_arrow_schema_, output_rows.size(), output_arrays));
}

common::Result<std::shared_ptr<arrow::Schema>> HashJoinContext::GetOutputSchema() {
    // If Build() hasn't been called yet, output_arrow_schema_ will be null
    if (!output_arrow_schema_) {
        auto build_result = Build();
        if (!build_result.ok()) {
            return common::Result<std::shared_ptr<arrow::Schema>>::failure(build_result.error());
        }
    }

    return common::Result<std::shared_ptr<arrow::Schema>>::success(output_arrow_schema_);
}

}  // namespace pond::query