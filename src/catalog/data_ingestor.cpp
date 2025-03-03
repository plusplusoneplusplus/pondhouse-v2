#include "catalog/data_ingestor.h"

#include "common/result.h"
#include "format/parquet/schema_converter.h"

namespace pond::catalog {

common::Result<std::unique_ptr<DataIngestor>> DataIngestor::Create(std::shared_ptr<Catalog> catalog,
                                                                   std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                                   const std::string& table_name) {
    using ReturnType = common::Result<std::unique_ptr<DataIngestor>>;

    auto metadata_result = catalog->LoadTable(table_name);
    RETURN_IF_ERROR_T(ReturnType, metadata_result);

    return ReturnType::success(
        std::unique_ptr<DataIngestor>(new DataIngestor(catalog, fs, table_name, metadata_result.value())));
}

DataIngestor::DataIngestor(std::shared_ptr<Catalog> catalog,
                           std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                           const std::string& table_name,
                           TableMetadata metadata)
    : catalog_(std::move(catalog)),
      fs_(std::move(fs)),
      table_name_(table_name),
      current_metadata_(std::move(metadata)) {}

common::Result<DataFile> DataIngestor::IngestBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                   bool commit_after_write) {
    using ReturnType = common::Result<DataFile>;

    // Validate schema before proceeding
    auto schema_validation = ValidateSchema(batch->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    // Extract partition values from the batch
    auto partition_values_result = ExtractPartitionValues(batch);
    RETURN_IF_ERROR_T(ReturnType, partition_values_result);
    auto partition_values = partition_values_result.value();

    auto file_path_result = GenerateDataFilePath(partition_values);
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(batch);
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->num_rows();

    auto close_result = writer->close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    auto data_file_result = FinalizeDataFile(file_path, num_records, partition_values);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    auto data_file = std::move(data_file_result).value();
    pending_files_.push_back(data_file);

    if (commit_after_write) {
        RETURN_IF_ERROR_T(ReturnType, Commit());
    }
    return ReturnType::success(data_file);
}

common::Result<std::vector<DataFile>> DataIngestor::IngestBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, bool commit_after_write) {
    using ReturnType = common::Result<std::vector<DataFile>>;

    if (batches.empty()) {
        return ReturnType::success({});
    }

    // Validate schema using the first batch's schema
    auto schema_validation = ValidateSchema(batches[0]->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    // Extract partition values from the first batch
    auto partition_values_result = ExtractPartitionValues(batches[0]);
    RETURN_IF_ERROR_T(ReturnType, partition_values_result);
    auto partition_values = partition_values_result.value();

    auto file_path_result = GenerateDataFilePath(partition_values);
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(batches);
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->num_rows();

    auto close_result = writer->close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    auto data_file_result = FinalizeDataFile(file_path, num_records, partition_values);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    auto data_file = std::move(data_file_result).value();
    pending_files_.push_back(data_file);

    if (commit_after_write) {
        RETURN_IF_ERROR_T(ReturnType, Commit());
    }
    return ReturnType::success(pending_files_);
}

common::Result<DataFile> DataIngestor::IngestTable(const std::shared_ptr<arrow::Table>& table,
                                                   bool commit_after_write) {
    using ReturnType = common::Result<DataFile>;

    // Validate schema before proceeding
    auto schema_validation = ValidateSchema(table->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    // Check if table has rows
    if (table->num_rows() == 0) {
        return common::Error(common::ErrorCode::InvalidOperation, "Cannot ingest empty table");
    }

    // Create a record batch from the first row of the table
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int i = 0; i < table->num_columns(); i++) {
        auto chunked_array = table->column(i);
        if (chunked_array->num_chunks() == 0) {
            return common::Error(common::ErrorCode::InvalidOperation, "Column " + std::to_string(i) + " has no chunks");
        }
        // Get the first chunk and slice to get just the first row
        columns.push_back(chunked_array->chunk(0)->Slice(0, 1));
    }
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(table->schema(), 1, columns);

    auto partition_values_result = ExtractPartitionValues(batch);
    RETURN_IF_ERROR_T(ReturnType, partition_values_result);
    auto partition_values = partition_values_result.value();

    auto file_path_result = GenerateDataFilePath(partition_values);
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(table);
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->num_rows();

    auto close_result = writer->close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    auto data_file_result = FinalizeDataFile(file_path, num_records, partition_values);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    auto data_file = std::move(data_file_result).value();
    pending_files_.push_back(data_file);

    if (commit_after_write) {
        RETURN_IF_ERROR_T(ReturnType, Commit());
    }
    return ReturnType::success(std::move(data_file));
}

common::Result<bool> DataIngestor::Commit() {
    using ReturnType = common::Result<bool>;

    if (pending_files_.empty()) {
        return ReturnType::success(true);
    }

    auto snapshot_result = catalog_->CreateSnapshot(table_name_,
                                                    pending_files_,
                                                    {},  // No deleted files
                                                    Operation::APPEND);

    RETURN_IF_ERROR_T(ReturnType, snapshot_result);
    current_metadata_ = snapshot_result.value();

    LOG_VERBOSE("Committed ingested files for table %s, snapshot id: %d, first file: %s, pending files: %d",
                table_name_.c_str(),
                current_metadata_.current_snapshot_id,
                pending_files_.empty() ? "None" : pending_files_.front().file_path.c_str(),
                pending_files_.size());

    pending_files_.clear();

    return ReturnType::success(true);
}

common::Result<std::string> DataIngestor::GenerateDataFilePath(
    const std::unordered_map<std::string, std::string>& partition_values) {
    std::string base_path = current_metadata_.location;
    if (!base_path.empty() && base_path.back() != '/') {
        base_path += '/';
    }

    // Create partition path
    std::string partition_path = "data";
    for (const auto& [key, value] : partition_values) {
        partition_path += "/" + key + "=" + value;
    }

    std::string file_name = "part_" + std::to_string(current_metadata_.current_snapshot_id) + "_"
                            + std::to_string(pending_files_.size()) + ".parquet";
    return common::Result<std::string>::success(base_path + partition_path + "/" + file_name);
}

common::Result<std::unique_ptr<format::ParquetWriter>> DataIngestor::CreateWriter(const std::string& file_path) {
    auto arrow_schema_result = format::SchemaConverter::ToArrowSchema(*current_metadata_.schema);
    RETURN_IF_ERROR_T(common::Result<std::unique_ptr<format::ParquetWriter>>, arrow_schema_result);
    return format::ParquetWriter::create(fs_, file_path, arrow_schema_result.value());
}

common::Result<DataFile> DataIngestor::FinalizeDataFile(
    const std::string& file_path,
    int64_t num_records,
    const std::unordered_map<std::string, std::string>& partition_values) {
    using ReturnType = common::Result<DataFile>;

    DataFile file;
    file.file_path = file_path;
    file.format = FileFormat::PARQUET;
    file.record_count = num_records;
    file.partition_values = partition_values;

    // Get file size from filesystem
    auto handle_result = fs_->OpenFile(file_path);
    RETURN_IF_ERROR_T(ReturnType, handle_result);
    auto handle = handle_result.value();

    auto size_result = fs_->Size(handle);
    RETURN_IF_ERROR_T(ReturnType, size_result);
    file.file_size_bytes = size_result.value();

    auto close_result = fs_->CloseFile(handle);
    RETURN_IF_ERROR_T(ReturnType, close_result);
    return ReturnType::success(std::move(file));
}

common::Result<void> DataIngestor::ValidateSchema(const std::shared_ptr<arrow::Schema>& input_schema) const {
    return format::SchemaConverter::ValidateSchema(input_schema, current_metadata_.schema);
}

common::Result<std::unordered_map<std::string, std::string>> DataIngestor::ExtractPartitionValues(
    const std::shared_ptr<arrow::RecordBatch>& batch) const {
    using ReturnType = common::Result<std::unordered_map<std::string, std::string>>;

    std::unordered_map<std::string, std::string> partition_values;

    // Extract partition values based on partition spec
    for (const auto& partition_field : current_metadata_.partition_specs[0].fields) {
        if (!partition_field.IsValid()) {
            return common::Error(common::ErrorCode::InvalidOperation,
                                 "Invalid partition field: " + partition_field.name);
        }

        // Get the source field name from schema
        std::string source_field_name = current_metadata_.schema->Fields()[partition_field.source_id].name;

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

}  // namespace pond::catalog