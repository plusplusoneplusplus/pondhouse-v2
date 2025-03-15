#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <arrow/record_batch.h>

#include "common/schema.h"
#include "query/data/arrow_util.h"

namespace pond::query {

/**
 * @brief A builder class for creating Arrow RecordBatches with a fluent interface.
 *
 * Example usage:
 * ```cpp
 * auto batch = ArrowRecordBatchBuilder()
 *     .AddInt32Column("id", {1, 2, 3}, {true, true, false})
 *     .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
 *     .AddDoubleColumn("salary", {50000.0, 60000.0, 70000.0})
 *     .Build();
 * ```
 *
 * Notes:
 *  - The size of the values vector must match the number of rows in the batch.
 *  - The size of the validity vector must match or be smaller than the size of the values vector. The extra elements
 *    in the values vector are considered invalid.
 *  - If the validity vector is not provided, all values are considered valid.
 */
class ArrowRecordBatchBuilder {
public:
    ArrowRecordBatchBuilder() = default;

    /**
     * @brief Add an Int32 column to the batch
     * @param name Column name
     * @param values Vector of values
     * @param validity Optional vector of validity flags (true = valid, false = null)
     * @param nullability Whether the column is nullable
     * @return Reference to this builder for chaining
     */
    ArrowRecordBatchBuilder& AddInt32Column(const std::string& name,
                                            const std::vector<int32_t>& values,
                                            const std::vector<bool>& validity = {},
                                            common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add an Int64 column to the batch
     */
    ArrowRecordBatchBuilder& AddInt64Column(const std::string& name,
                                            const std::vector<int64_t>& values,
                                            const std::vector<bool>& validity = {},
                                            common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a UInt32 column to the batch
     */
    ArrowRecordBatchBuilder& AddUInt32Column(const std::string& name,
                                             const std::vector<uint32_t>& values,
                                             const std::vector<bool>& validity = {},
                                             common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a UInt64 column to the batch
     */
    ArrowRecordBatchBuilder& AddUInt64Column(const std::string& name,
                                             const std::vector<uint64_t>& values,
                                             const std::vector<bool>& validity = {},
                                             common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a Float column to the batch
     */
    ArrowRecordBatchBuilder& AddFloatColumn(const std::string& name,
                                            const std::vector<float>& values,
                                            const std::vector<bool>& validity = {},
                                            common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a Double column to the batch
     */
    ArrowRecordBatchBuilder& AddDoubleColumn(const std::string& name,
                                             const std::vector<double>& values,
                                             const std::vector<bool>& validity = {},
                                             common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a String column to the batch
     */
    ArrowRecordBatchBuilder& AddStringColumn(const std::string& name,
                                             const std::vector<std::string>& values,
                                             const std::vector<bool>& validity = {},
                                             common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a Boolean column to the batch
     */
    ArrowRecordBatchBuilder& AddBooleanColumn(const std::string& name,
                                              const std::vector<bool>& values,
                                              const std::vector<bool>& validity = {},
                                              common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a column from an existing Arrow array
     */
    ArrowRecordBatchBuilder& AddColumn(const std::string& name,
                                       const std::shared_ptr<arrow::Array>& array,
                                       common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a Timestamp column to the batch (millisecond precision)
     * @param name Column name
     * @param values Vector of timestamps (in milliseconds since epoch)
     * @param validity Optional vector of validity flags (true = valid, false = null)
     * @param nullability Whether the column is nullable
     * @return Reference to this builder for chaining
     */
    ArrowRecordBatchBuilder& AddTimestampColumn(const std::string& name,
                                                const std::vector<int64_t>& values,
                                                const std::vector<bool>& validity = {},
                                                common::Nullability nullability = common::Nullability::NULLABLE);

    /**
     * @brief Add a Timestamp column to the batch using std::chrono::time_point
     * @param name Column name
     * @param values Vector of time_points
     * @param validity Optional vector of validity flags (true = valid, false = null)
     * @param nullability Whether the column is nullable
     * @return Reference to this builder for chaining
     */
    template <typename Clock = std::chrono::system_clock>
    ArrowRecordBatchBuilder& AddTimestampColumn(const std::string& name,
                                                const std::vector<std::chrono::time_point<Clock>>& values,
                                                const std::vector<bool>& validity = {},
                                                common::Nullability nullability = common::Nullability::NULLABLE) {
        std::vector<int64_t> ms_values;
        ms_values.reserve(values.size());

        for (const auto& tp : values) {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
            ms_values.push_back(ms);
        }

        return AddTimestampColumn(name, ms_values, validity, nullability);
    }

    /**
     * @brief Build and return the record batch
     * @return Result containing the record batch or an error
     */
    common::Result<ArrowDataBatchSharedPtr> Build();

    /**
     * @brief Get the number of rows in the batch
     */
    int64_t NumRows() const { return num_rows_; }

private:
    /**
     * @brief Template method to add a column of any supported type
     */
    template <typename T, typename BuilderType>
    ArrowRecordBatchBuilder& AddTypedColumn(const std::string& name,
                                            const std::vector<T>& values,
                                            const std::vector<bool>& validity,
                                            common::Nullability nullability);

    /**
     * @brief Validate that all columns have the same length
     */
    common::Result<void> ValidateColumnLengths() const;

    std::vector<std::shared_ptr<arrow::Field>> fields_;
    std::vector<std::shared_ptr<arrow::Array>> arrays_;
    int64_t num_rows_ = -1;  // -1 means no columns added yet
};

}  // namespace pond::query