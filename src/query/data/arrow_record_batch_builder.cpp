#include "query/data/arrow_record_batch_builder.h"

#include <arrow/builder.h>
#include <arrow/type.h>

namespace pond::query {

namespace detail {

// Helper function to append values to a string builder
template <typename T>
arrow::Status AppendValuesWithValidity(arrow::ArrayBuilder* builder,
                                       const std::vector<T>& values,
                                       const std::vector<bool>& validity) {
    if constexpr (std::is_same_v<T, std::string>) {
        auto string_builder = static_cast<arrow::StringBuilder*>(builder);
        // For strings, we need to handle each value individually due to Arrow's API
        for (size_t i = 0; i < values.size(); ++i) {
            if (validity.empty() || (i < validity.size() && validity[i])) {
                ARROW_RETURN_NOT_OK(string_builder->Append(values[i]));
            } else {
                ARROW_RETURN_NOT_OK(string_builder->AppendNull());
            }
        }
        return arrow::Status::OK();
    } else {
        // For numeric types, we can use AppendValues directly
        auto typed_builder =
            static_cast<typename arrow::TypeTraits<typename arrow::CTypeTraits<T>::ArrowType>::BuilderType*>(builder);
        if (validity.empty()) {
            return typed_builder->AppendValues(values);
        } else {
            return typed_builder->AppendValues(values, validity);
        }
    }
}

}  // namespace detail

template <typename T, typename BuilderType>
ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddTypedColumn(const std::string& name,
                                                                 const std::vector<T>& values,
                                                                 const std::vector<bool>& validity,
                                                                 common::Nullability nullability) {
    // Create builder
    std::unique_ptr<BuilderType> builder = std::make_unique<BuilderType>();

    // Append values
    if (!validity.empty() && validity.size() > values.size()) {
        // Invalid validity vector size, add null array
        fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Use the helper function to append values
    auto status = detail::AppendValuesWithValidity<T>(builder.get(), values, validity);
    if (!status.ok()) {
        // Failed to append values, add null array
        fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Finish building the array
    std::shared_ptr<arrow::Array> array;
    status = builder->Finish(&array);
    if (!status.ok()) {
        // Failed to finish array, add null array
        fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Add field and array
    fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
    arrays_.push_back(array);

    // Update number of rows
    if (num_rows_ == -1) {
        num_rows_ = array->length();
    }

    return *this;
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddInt32Column(const std::string& name,
                                                                 const std::vector<int32_t>& values,
                                                                 const std::vector<bool>& validity,
                                                                 common::Nullability nullability) {
    return AddTypedColumn<int32_t, arrow::Int32Builder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddInt64Column(const std::string& name,
                                                                 const std::vector<int64_t>& values,
                                                                 const std::vector<bool>& validity,
                                                                 common::Nullability nullability) {
    return AddTypedColumn<int64_t, arrow::Int64Builder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddUInt32Column(const std::string& name,
                                                                  const std::vector<uint32_t>& values,
                                                                  const std::vector<bool>& validity,
                                                                  common::Nullability nullability) {
    return AddTypedColumn<uint32_t, arrow::UInt32Builder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddUInt64Column(const std::string& name,
                                                                  const std::vector<uint64_t>& values,
                                                                  const std::vector<bool>& validity,
                                                                  common::Nullability nullability) {
    return AddTypedColumn<uint64_t, arrow::UInt64Builder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddFloatColumn(const std::string& name,
                                                                 const std::vector<float>& values,
                                                                 const std::vector<bool>& validity,
                                                                 common::Nullability nullability) {
    return AddTypedColumn<float, arrow::FloatBuilder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddDoubleColumn(const std::string& name,
                                                                  const std::vector<double>& values,
                                                                  const std::vector<bool>& validity,
                                                                  common::Nullability nullability) {
    return AddTypedColumn<double, arrow::DoubleBuilder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddStringColumn(const std::string& name,
                                                                  const std::vector<std::string>& values,
                                                                  const std::vector<bool>& validity,
                                                                  common::Nullability nullability) {
    return AddTypedColumn<std::string, arrow::StringBuilder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddBooleanColumn(const std::string& name,
                                                                   const std::vector<bool>& values,
                                                                   const std::vector<bool>& validity,
                                                                   common::Nullability nullability) {
    return AddTypedColumn<bool, arrow::BooleanBuilder>(name, values, validity, nullability);
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddColumn(const std::string& name,
                                                            const std::shared_ptr<arrow::Array>& array,
                                                            common::Nullability nullability) {
    if (!array) {
        // Null array provided, add null array
        fields_.push_back(arrow::field(name, arrow::null(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Add field and array
    fields_.push_back(arrow::field(name, array->type(), nullability == common::Nullability::NULLABLE));
    arrays_.push_back(array);

    // Update number of rows
    if (num_rows_ == -1) {
        num_rows_ = array->length();
    }

    return *this;
}

ArrowRecordBatchBuilder& ArrowRecordBatchBuilder::AddTimestampColumn(const std::string& name,
                                                                     const std::vector<int64_t>& values,
                                                                     const std::vector<bool>& validity,
                                                                     common::Nullability nullability) {
    using BuilderType = arrow::TimestampBuilder;
    auto type = arrow::timestamp(arrow::TimeUnit::MILLI);
    std::unique_ptr<BuilderType> builder = std::make_unique<BuilderType>(type, arrow::default_memory_pool());

    // Append values
    if (!validity.empty() && validity.size() > values.size()) {
        // Invalid validity vector size, add null array
        fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Use the helper function to append values
    auto status = detail::AppendValuesWithValidity<int64_t>(builder.get(), values, validity);
    if (!status.ok()) {
        // Failed to append values, add null array
        fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Finish building the array
    std::shared_ptr<arrow::Array> array;
    status = builder->Finish(&array);
    if (!status.ok()) {
        // Failed to finish array, add null array
        fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
        arrays_.push_back(nullptr);
        return *this;
    }

    // Add field and array
    fields_.push_back(arrow::field(name, builder->type(), nullability == common::Nullability::NULLABLE));
    arrays_.push_back(array);

    // Update number of rows
    if (num_rows_ == -1) {
        num_rows_ = array->length();
    }

    return *this;
}

common::Result<void> ArrowRecordBatchBuilder::ValidateColumnLengths() const {
    if (arrays_.empty()) {
        return common::Result<void>::success();
    }

    for (size_t i = 0; i < arrays_.size(); ++i) {
        if (!arrays_[i]) {
            return common::Result<void>::failure(common::Error(common::ErrorCode::InvalidArgument,
                                                               "Column '" + fields_[i]->name() + "' has null array"));
        }
        if (arrays_[i]->length() != num_rows_) {
            return common::Result<void>::failure(common::Error(
                common::ErrorCode::InvalidArgument,
                "Column '" + fields_[i]->name() + "' has different length (" + std::to_string(arrays_[i]->length())
                    + ") than expected (" + std::to_string(num_rows_) + ")"));
        }
    }

    return common::Result<void>::success();
}

common::Result<ArrowDataBatchSharedPtr> ArrowRecordBatchBuilder::Build() {
    // Handle empty case
    if (arrays_.empty()) {
        return common::Result<ArrowDataBatchSharedPtr>::success(ArrowUtil::CreateEmptyBatch());
    }

    // Validate column lengths
    auto validate_result = ValidateColumnLengths();
    if (!validate_result.ok()) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(validate_result.error());
    }

    // Create schema
    auto schema = arrow::schema(fields_);

    // Create record batch
    return common::Result<ArrowDataBatchSharedPtr>::success(arrow::RecordBatch::Make(schema, num_rows_, arrays_));
}

}  // namespace pond::query