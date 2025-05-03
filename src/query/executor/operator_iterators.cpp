#include "query/executor/operator_iterators.h"

#include <arrow/builder.h>
#include <arrow/table.h>

#include "common/error.h"
#include "common/expression.h"
#include "common/log.h"
#include "query/data/arrow_predicate.h"
#include "query/data/arrow_util.h"
#include "query/executor/hash_join.h"

namespace pond::query {

// SequentialScanIterator implementation

common::Result<bool> SequentialScanIterator::InitializeImpl() {
    // List the files for the table
    auto files_result = data_accessor_->ListTableFiles(table_name_);
    if (!files_result.ok()) {
        return common::Result<bool>::failure(files_result.error());
    }

    files_ = std::move(files_result.value());

    // If there are no files, we have no data
    if (files_.empty()) {
        return common::Result<bool>::success(false);
    }

    // Prepare the first file
    return PrepareNextFile();
}

common::Result<bool> SequentialScanIterator::PrepareNextFile() {
    // If we've processed all files, there are no more batches
    if (current_file_index_ >= files_.size()) {
        has_more_batches_ = false;
        current_batch_reader_.reset();
        current_reader_.reset();
        return common::Result<bool>::success(false);
    }

    // Get a reader for the current file
    auto reader_result = data_accessor_->GetReader(files_[current_file_index_]);
    if (!reader_result.ok()) {
        return common::Result<bool>::failure(reader_result.error());
    }

    current_reader_ = std::move(reader_result).value();

    // Get a batch reader with column selection if specified
    auto batch_reader_result = projection_column_names_.empty()
                                   ? current_reader_->GetBatchReader()
                                   : current_reader_->GetBatchReader(projection_column_names_);

    if (!batch_reader_result.ok()) {
        return common::Result<bool>::failure(batch_reader_result.error());
    }

    current_batch_reader_ = std::move(batch_reader_result.value());
    current_file_index_++;

    // Read the first batch to see if there's data
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = current_batch_reader_->ReadNext(&batch);
    if (!status.ok()) {
        return common::Result<bool>::failure(common::ErrorCode::Failure, "Failed to read batch: " + status.ToString());
    }

    // If no batch in this file, try the next file
    if (batch == nullptr) {
        return PrepareNextFile();
    }

    // If predicate is present, apply it
    if (predicate_) {
        auto filtered_batch_result = ApplyPredicate(batch);
        if (!filtered_batch_result.ok()) {
            return common::Result<bool>::failure(filtered_batch_result.error());
        }
        batch = filtered_batch_result.value();

        // If batch is empty after filtering, get the next batch
        if (batch->num_rows() == 0) {
            return Next().ok();
        }
    }

    // Store the current batch and indicate we have data
    current_batch_ = std::move(batch);
    has_more_batches_ = true;
    has_next_ = true;

    return common::Result<bool>::success(true);
}

common::Result<ArrowDataBatchSharedPtr> SequentialScanIterator::Next() {
    if (!initialized_) {
        auto init_result = Initialize();
        if (!init_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(init_result.error());
        }
    }

    if (!has_next_) {
        return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
    }

    // Save the current batch to return
    auto result_batch = current_batch_;

    // Try to get the next batch
    std::shared_ptr<arrow::RecordBatch> next_batch;
    auto status = current_batch_reader_->ReadNext(&next_batch);
    if (!status.ok()) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(common::ErrorCode::Failure,
                                                                "Failed to read next batch: " + status.ToString());
    }

    // If we got a batch, store it (after applying predicate if needed)
    if (next_batch != nullptr) {
        if (predicate_) {
            auto filtered_batch_result = ApplyPredicate(next_batch);
            if (!filtered_batch_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(filtered_batch_result.error());
            }
            next_batch = filtered_batch_result.value();

            // If batch is empty after filtering, recursively get next
            if (next_batch->num_rows() == 0) {
                current_batch_ = nullptr;
                auto next_result = Next();
                if (!next_result.ok()) {
                    return next_result;
                }

                // If Next() returns a batch, update has_next_
                has_next_ = next_result.value() != nullptr;
                return result_batch;
            }
        }

        current_batch_ = std::move(next_batch);
    } else {
        // No more batches in current file, try next file
        current_batch_ = nullptr;
        auto prepare_result = PrepareNextFile();
        if (!prepare_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(prepare_result.error());
        }

        has_next_ = prepare_result.value();
    }

    return common::Result<ArrowDataBatchSharedPtr>::success(result_batch);
}

common::Result<std::shared_ptr<arrow::RecordBatch>> SequentialScanIterator::ApplyPredicate(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!predicate_ || batch->num_rows() == 0) {
        return common::Result<std::shared_ptr<arrow::RecordBatch>>::success(batch);
    }

    return ArrowPredicate::Apply(batch, predicate_);
}

// FilterIterator implementation

common::Result<bool> FilterIterator::InitializeImpl() {
    // Initialize the child iterator
    auto init_result = child_->Initialize();
    if (!init_result.ok()) {
        return init_result;
    }

    // If child has no data, we have no data
    if (!child_->HasNext()) {
        return common::Result<bool>::success(false);
    }

    return common::Result<bool>::success(true);
}

common::Result<ArrowDataBatchSharedPtr> FilterIterator::Next() {
    if (!initialized_) {
        auto init_result = Initialize();
        if (!init_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(init_result.error());
        }
    }

    if (!has_next_) {
        return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
    }

    // Get the next batch from the child iterator
    auto batch_result = child_->Next();
    if (!batch_result.ok()) {
        return batch_result;
    }

    auto batch = batch_result.value();

    // Update has_next_ based on child's state
    has_next_ = child_->HasNext();

    // If we reached the end of the child iterator, return nullptr
    if (batch == nullptr) {
        return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
    }

    // Apply the filter predicate
    auto filter_result = ArrowPredicate::Apply(batch, predicate_);
    if (!filter_result.ok()) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(filter_result.error());
    }

    auto filtered_batch = filter_result.value();

    // If the filtered batch is empty, recursively get the next one
    if (filtered_batch->num_rows() == 0 && has_next_) {
        return Next();
    }

    // Return the filtered batch
    return common::Result<ArrowDataBatchSharedPtr>::success(filtered_batch);
}

// ProjectionIterator implementation

common::Result<bool> ProjectionIterator::InitializeImpl() {
    // Initialize the child iterator
    auto init_result = child_->Initialize();
    if (!init_result.ok()) {
        return init_result;
    }

    // If child has no data, we have no data
    if (!child_->HasNext()) {
        return common::Result<bool>::success(false);
    }

    return common::Result<bool>::success(true);
}

common::Result<ArrowDataBatchSharedPtr> ProjectionIterator::Next() {
    if (!initialized_) {
        auto init_result = Initialize();
        if (!init_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(init_result.error());
        }
    }

    if (!has_next_) {
        return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
    }

    // Get the next batch from the child iterator
    auto batch_result = child_->Next();
    if (!batch_result.ok()) {
        return batch_result;
    }

    auto batch = batch_result.value();

    // Update has_next_ based on child's state
    has_next_ = child_->HasNext();

    // If we reached the end of the child iterator, return nullptr
    if (batch == nullptr) {
        return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
    }

    // If no projections, just return the batch as is
    if (projections_.empty()) {
        return batch_result;
    }

    // Get the input schema from the batch
    auto input_schema = batch->schema();
    auto input_batch = batch;

    // Create the output columns based on projections
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    std::vector<std::shared_ptr<arrow::Field>> output_fields;

    for (const auto& expr : projections_) {
        // For now, only handle column expressions (field references)
        if (auto col_expr = std::dynamic_pointer_cast<common::ColumnExpression>(expr)) {
            const std::string& col_name = col_expr->ColumnName();

            // Find the column in the input batch
            int col_idx = input_schema->GetFieldIndex(col_name);
            if (col_idx == -1) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(common::ErrorCode::InvalidArgument,
                                                                        "Column not found: " + col_name);
            }

            // Add the column to the output
            output_arrays.push_back(input_batch->column(col_idx));
            output_fields.push_back(input_schema->field(col_idx));
        } else {
            // For now, only support column references
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::ErrorCode::NotImplemented, "Only column references are supported in projections");
        }
    }

    // Create the output schema and batch
    auto output_schema = arrow::schema(output_fields);
    auto output_batch = arrow::RecordBatch::Make(output_schema, input_batch->num_rows(), output_arrays);

    return common::Result<ArrowDataBatchSharedPtr>::success(output_batch);
}

}  // namespace pond::query