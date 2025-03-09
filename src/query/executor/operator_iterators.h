#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>
#include <arrow/record_batch.h>

#include "catalog/metadata.h"
#include "common/expression.h"
#include "common/schema.h"
#include "format/parquet/parquet_reader.h"
#include "query/data/data_accessor.h"
#include "query/executor/vectorized_executor.h"

namespace pond::query {

/**
 * @brief Base class for all operator iterators
 *
 * Provides common functionality for operator iterators, including
 * initialization and schema management.
 */
class BaseOperatorIterator : public OperatorIterator {
public:
    explicit BaseOperatorIterator(common::Schema output_schema)
        : output_schema_(std::move(output_schema)), initialized_(false), has_next_(false) {}

    ~BaseOperatorIterator() override = default;

    common::Result<bool> Initialize() override {
        if (initialized_) {
            return common::Result<bool>::success(true);
        }

        auto result = InitializeImpl();
        if (result.ok()) {
            initialized_ = true;
            has_next_ = result.value();
        }
        return result;
    }

    common::Result<common::Schema> GetSchema() const override {
        return common::Result<common::Schema>::success(output_schema_);
    }

    bool HasNext() const override { return initialized_ && has_next_; }

protected:
    // Implementation-specific initialization
    virtual common::Result<bool> InitializeImpl() = 0;

    common::Schema output_schema_;
    bool initialized_;
    bool has_next_;
};

/**
 * @brief Iterator for sequential scan operations
 *
 * Reads data from storage files and applies filtering and projection.
 */
class SequentialScanIterator : public BaseOperatorIterator {
public:
    SequentialScanIterator(std::shared_ptr<DataAccessor> data_accessor,
                           const std::string& table_name,
                           std::shared_ptr<common::Expression> predicate,
                           const std::vector<std::string>& projection_column_names,
                           common::Schema output_schema)
        : BaseOperatorIterator(std::move(output_schema)),
          data_accessor_(std::move(data_accessor)),
          table_name_(table_name),
          predicate_(std::move(predicate)),
          projection_column_names_(projection_column_names),
          current_file_index_(0),
          current_batch_reader_(),
          has_more_batches_(false) {}

    ~SequentialScanIterator() override = default;

    common::Result<ArrowDataBatchSharedPtr> Next() override;

protected:
    common::Result<bool> InitializeImpl() override;

private:
    // Load the next file and prepare its batch reader
    common::Result<bool> PrepareNextFile();

    // Apply predicate to a batch
    common::Result<std::shared_ptr<arrow::RecordBatch>> ApplyPredicate(
        const std::shared_ptr<arrow::RecordBatch>& batch);

    // State
    std::shared_ptr<DataAccessor> data_accessor_;
    std::string table_name_;
    std::shared_ptr<common::Expression> predicate_;
    std::vector<std::string> projection_column_names_;

    // Execution state
    std::vector<catalog::DataFile> files_;
    size_t current_file_index_;
    std::shared_ptr<format::ParquetReader> current_reader_;
    std::shared_ptr<arrow::RecordBatchReader> current_batch_reader_;
    std::shared_ptr<arrow::RecordBatch> current_batch_;
    bool has_more_batches_;
};

/**
 * @brief Iterator for filter operations
 *
 * Applies a predicate to filter rows from its child iterator.
 */
class FilterIterator : public BaseOperatorIterator {
public:
    FilterIterator(std::unique_ptr<OperatorIterator> child,
                   std::shared_ptr<common::Expression> predicate,
                   common::Schema output_schema)
        : BaseOperatorIterator(std::move(output_schema)), child_(std::move(child)), predicate_(std::move(predicate)) {}

    ~FilterIterator() override = default;

    common::Result<ArrowDataBatchSharedPtr> Next() override;

protected:
    common::Result<bool> InitializeImpl() override;

private:
    std::unique_ptr<OperatorIterator> child_;
    std::shared_ptr<common::Expression> predicate_;
};

/**
 * @brief Iterator for projection operations
 *
 * Projects columns from its child iterator based on expressions.
 */
class ProjectionIterator : public BaseOperatorIterator {
public:
    ProjectionIterator(std::unique_ptr<OperatorIterator> child,
                       const std::vector<std::shared_ptr<common::Expression>>& projections,
                       common::Schema output_schema)
        : BaseOperatorIterator(std::move(output_schema)), child_(std::move(child)), projections_(projections) {}

    ~ProjectionIterator() override = default;

    common::Result<ArrowDataBatchSharedPtr> Next() override;

protected:
    common::Result<bool> InitializeImpl() override;

private:
    std::unique_ptr<OperatorIterator> child_;
    std::vector<std::shared_ptr<common::Expression>> projections_;
};

// Additional operator iterators would be defined here:
// - JoinIterator
// - AggregateIterator
// - SortIterator
// - LimitIterator
// etc.

}  // namespace pond::query