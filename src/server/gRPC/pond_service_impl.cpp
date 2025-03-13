#include "pond_service_impl.h"

#include <string>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <grpcpp/grpcpp.h>

#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "common/data_chunk.h"
#include "common/log.h"
#include "common/result.h"
#include "common/schema.h"
#include "common/time.h"
#include "format/parquet/parquet_reader.h"
#include "format/parquet/parquet_writer.h"
#include "kv/db.h"
#include "query/data/arrow_util.h"
#include "query/executor/sql_table_executor.h"
#include "server/gRPC/type_converter.h"

namespace pond::server {

// Default table name for key-value operations
static const char* DEFAULT_TABLE = "default";

PondServiceImpl::PondServiceImpl(std::shared_ptr<pond::kv::DB> db,
                                 std::shared_ptr<pond::common::IAppendOnlyFileSystem> fs)
    : db_(std::move(db)), fs_(std::move(fs)) {
    // Ensure default table exists
    auto schema = std::make_shared<pond::common::Schema>(
        std::vector<pond::common::ColumnSchema>{{"value", pond::common::ColumnType::STRING}});

    // Try to create the default table if it doesn't exist
    auto create_result = db_->CreateTable(DEFAULT_TABLE, schema);
    // Ignore error if table already exists

    // Initialize the catalog
    auto catalog_db_result = pond::kv::DB::Create(fs_, "catalog");
    if (!catalog_db_result.ok()) {
        LOG_ERROR("Failed to create catalog database: %s", catalog_db_result.error().message().c_str());
        return;
    }
    catalog_db_ = std::move(catalog_db_result.value());
    catalog_ = std::make_shared<pond::catalog::KVCatalog>(catalog_db_);
}

// Helper method to get the default table
common::Result<std::shared_ptr<pond::kv::Table>> PondServiceImpl::GetDefaultTable() {
    return db_->GetTable(DEFAULT_TABLE);
}

grpc::Status PondServiceImpl::Get(grpc::ServerContext* context, const pond::proto::GetRequest* request,
                                 pond::proto::GetResponse* response) {
    try {
        std::string key(request->key().begin(), request->key().end());
        
        // Get the table to use (default if not specified)
        std::string table_name = request->table_name().empty() ? DEFAULT_TABLE : request->table_name();
        LOG_STATUS("Received Get request for key '%s' from table '%s'", key.c_str(), table_name.c_str());
        
        // Get the table
        auto table_result = db_->GetTable(table_name);
        if (!table_result.ok()) {
            response->set_found(false);
            response->set_error(table_result.error().message());
            return grpc::Status::OK;
        }
        
        auto table = table_result.value();
        auto get_result = table->Get(key);
        
        if (get_result.ok()) {
            auto record_ptr = std::move(get_result).value();
            if (record_ptr) {
                // Value found, get the string value from the record
                auto value_result = record_ptr->Get<std::string>(0);
                if (value_result.ok()) {
                    response->set_found(true);
                    response->set_value(value_result.value());
                } else {
                    response->set_found(false);
                    response->set_error(value_result.error().message());
                }
            } else {
                // Key not found
                response->set_found(false);
            }
        } else {
            // Error getting value
            response->set_found(false);
            response->set_error(get_result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_found(false);
        response->set_error(std::string("Error getting value: ") + e.what());
    }
    
    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::Put(grpc::ServerContext* context, const pond::proto::PutRequest* request,
                                 pond::proto::PutResponse* response) {
    try {
        std::string key(request->key().begin(), request->key().end());
        std::string value(request->value().begin(), request->value().end());
        
        // Get the table to use (default if not specified)
        std::string table_name = request->table_name().empty() ? DEFAULT_TABLE : request->table_name();
        LOG_STATUS("Received Put request for key '%s' in table '%s'", key.c_str(), table_name.c_str());
        
        // Get the table
        auto table_result = db_->GetTable(table_name);
        if (!table_result.ok()) {
            response->set_success(false);
            response->set_error(table_result.error().message());
            return grpc::Status::OK;
        }
        
        auto table = table_result.value();
        
        // Create a record with the value
        auto record = std::make_unique<pond::kv::Record>(table->schema());
        record->Set(0, value);
        
        // Put the record in the table
        auto put_result = table->Put(key, std::move(record));
        
        if (put_result.ok()) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error(put_result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error putting value: ") + e.what());
    }
    
    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::Delete(grpc::ServerContext* context, const pond::proto::DeleteRequest* request,
                                    pond::proto::DeleteResponse* response) {
    try {
        std::string key(request->key().begin(), request->key().end());
        
        // Get the table to use (default if not specified)
        std::string table_name = request->table_name().empty() ? DEFAULT_TABLE : request->table_name();
        LOG_STATUS("Received Delete request for key '%s' from table '%s'", key.c_str(), table_name.c_str());
        
        // Get the table
        auto table_result = db_->GetTable(table_name);
        if (!table_result.ok()) {
            response->set_success(false);
            response->set_error(table_result.error().message());
            return grpc::Status::OK;
        }
        
        auto table = table_result.value();
        auto delete_result = table->Delete(key);
        
        if (delete_result.ok()) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error(delete_result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error deleting key: ") + e.what());
    }
    
    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::Scan(grpc::ServerContext* context, const pond::proto::ScanRequest* request,
                                  grpc::ServerWriter<pond::proto::ScanResponse>* writer) {
    try {
        std::string start_key(request->start_key().begin(), request->start_key().end());
        std::string end_key(request->end_key().begin(), request->end_key().end());
        int32_t limit = request->limit();
        
        // Get the table to use (default if not specified)
        std::string table_name = request->table_name().empty() ? DEFAULT_TABLE : request->table_name();
        LOG_STATUS("Received Scan request from table '%s'", table_name.c_str());
        
        // Get the table
        auto table_result = db_->GetTable(table_name);
        if (!table_result.ok()) {
            pond::proto::ScanResponse response;
            response.set_error(table_result.error().message());
            writer->Write(response);
            return grpc::Status::OK;
        }
        
        auto table = table_result.value();
        
        // Get an iterator for the table
        auto iter_result = table->NewIterator();
        if (!iter_result.ok()) {
            pond::proto::ScanResponse response;
            response.set_error(iter_result.error().message());
            writer->Write(response);
            return grpc::Status::OK;
        }
        
        auto iter = std::move(iter_result).value();
        
        // Seek to the start key if specified
        if (!start_key.empty()) {
            iter->Seek(start_key);
        } else {
            iter->Seek("");
        }
        
        // Scan keys and values
        int count = 0;
        while (iter->Valid() && (limit <= 0 || count < limit)) {
            // Check if we've reached the end key
            if (!end_key.empty() && iter->key() >= end_key) {
                break;
            }
            
            pond::proto::ScanResponse response;
            response.set_key(iter->key());
            
            // Get the value from the record
            auto& record = iter->value();
            auto value_result = record->Get<std::string>(0);
            if (value_result.ok()) {
                response.set_value(value_result.value());
            }
            
            if (!writer->Write(response)) {
                // Client disconnected
                return grpc::Status(grpc::StatusCode::CANCELLED, "Client disconnected");
            }
            
            iter->Next();
            count++;
        }
    } catch (const std::exception& e) {
        pond::proto::ScanResponse response;
        response.set_error(std::string("Error scanning keys: ") + e.what());
        writer->Write(response);
    }
    
    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::ListDetailedFiles(grpc::ServerContext* context,
                                                const pond::proto::ListDetailedFilesRequest* request,
                                                pond::proto::ListDetailedFilesResponse* response) {
    LOG_STATUS("Received ListDetailedFiles request for path: %s", request->path().c_str());

    std::string path = request->path();
    bool recursive = request->recursive();

    // Call the file system's ListDetailed method
    auto result = fs_->ListDetailed(path, recursive);

    if (result.ok()) {
        response->set_success(true);
        for (const auto& entry : result.value()) {
            auto* proto_entry = response->add_entries();
            proto_entry->set_path(entry.path);
            proto_entry->set_is_directory(entry.is_directory);
        }
    } else {
        response->set_success(false);
        response->set_error(result.error().message());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::GetDirectoryInfo(grpc::ServerContext* context,
                                               const pond::proto::DirectoryInfoRequest* request,
                                               pond::proto::DirectoryInfoResponse* response) {
    LOG_STATUS("Received GetDirectoryInfo request for path: %s", request->path().c_str());

    std::string path = request->path();

    // Call the file system's GetDirectoryInfo method
    auto result = fs_->GetDirectoryInfo(path);

    if (result.ok()) {
        const auto& dir_info = result.value();
        response->set_success(true);
        response->set_exists(dir_info.exists);
        response->set_is_directory(dir_info.is_directory);
        response->set_num_files(dir_info.num_files);
        response->set_total_size(dir_info.total_size);
    } else {
        response->set_success(false);
        response->set_error(result.error().message());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::ExecuteSQL(grpc::ServerContext* context,
                                         const pond::proto::ExecuteSQLRequest* request,
                                         pond::proto::ExecuteSQLResponse* response) {
    LOG_STATUS("Received ExecuteSQL request with query: %s", request->sql_query().c_str());

    try {
        // Get the SQL query
        std::string sql_query = request->sql_query();

        // Create a SQL table executor
        pond::query::SQLTableExecutor executor(catalog_);

        // Currently only supports CREATE TABLE statements
        auto result = executor.ExecuteCreateTable(sql_query);

        if (result.ok()) {
            auto metadata = result.value();
            response->set_success(true);
            response->set_message("Table '" + metadata.table_uuid + "' created successfully");
        } else {
            response->set_success(false);
            response->set_error(result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error executing SQL: ") + e.what());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::ListTables(grpc::ServerContext* context,
                                         const pond::proto::ListTablesRequest* request,
                                         pond::proto::ListTablesResponse* response) {
    LOG_STATUS("Received ListTables request");

    try {
        // Use the catalog directly
        auto result = catalog_->ListTables();

        if (result.ok()) {
            response->set_success(true);
            for (const auto& table_name : result.value()) {
                response->add_table_names(table_name);
            }
        } else {
            response->set_success(false);
            response->set_error(result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error listing tables: ") + e.what());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::GetTableMetadata(grpc::ServerContext* context,
                                               const pond::proto::GetTableMetadataRequest* request,
                                               pond::proto::GetTableMetadataResponse* response) {
    LOG_STATUS("Received GetTableMetadata request for table: %s", request->table_name().c_str());

    try {
        // Use the catalog directly
        auto table_name = request->table_name();
        auto result = catalog_->LoadTable(table_name);

        if (result.ok()) {
            const auto& table_metadata = result.value();

            // Use TypeConverter to convert table metadata
            auto metadata_info = TypeConverter::ConvertToTableMetadataInfo(table_metadata);

            // Get data files
            auto files_result = catalog_->ListDataFiles(table_name);
            if (files_result.ok()) {
                // Add data files using TypeConverter
                TypeConverter::AddDataFilesToTableMetadataInfo(&metadata_info, files_result.value());
            }

            // Set the metadata in the response
            response->mutable_metadata()->CopyFrom(metadata_info);
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error(result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error getting table metadata: ") + e.what());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::IngestJsonData(grpc::ServerContext* context,
                                             const pond::proto::IngestJsonDataRequest* request,
                                             pond::proto::IngestJsonDataResponse* response) {
    LOG_STATUS("Received IngestJsonData request for table: %s", request->table_name().c_str());

    try {
        // Get the table name and JSON data from the request
        const std::string& table_name = request->table_name();
        const std::string& json_data = request->json_data();

        // Load the table metadata to get the schema
        auto table_result = catalog_->LoadTable(table_name);
        if (!table_result.ok()) {
            response->set_success(false);
            response->set_error_message("Failed to load table: " + table_result.error().message());
            return grpc::Status::OK;
        }

        const auto& table_metadata = table_result.value();
        const auto& schema = table_metadata.schema;

        if (!schema) {
            response->set_success(false);
            response->set_error_message("Table schema is null");
            return grpc::Status::OK;
        }

        // Convert JSON to Arrow RecordBatch
        auto batch_result = pond::query::ArrowUtil::JsonToRecordBatch(json_data, *schema);
        if (!batch_result.ok()) {
            response->set_success(false);
            response->set_error_message("Failed to convert JSON to Arrow RecordBatch: "
                                        + batch_result.error().message());
            return grpc::Status::OK;
        }

        auto record_batch = batch_result.value();

        // If the record batch is empty, return success with 0 rows
        if (record_batch->num_rows() == 0) {
            response->set_success(true);
            response->set_rows_ingested(0);
            return grpc::Status::OK;
        }

        // Create a DataIngestor to handle writing the data to the table
        auto ingestor_result = catalog::DataIngestor::Create(catalog_, fs_, table_name);
        if (!ingestor_result.ok()) {
            response->set_success(false);
            response->set_error_message("Failed to create data ingestor: " + ingestor_result.error().message());
            return grpc::Status::OK;
        }

        auto ingestor = std::move(ingestor_result).value();

        // Add the record batch to the ingestor
        auto add_result = ingestor->IngestBatch(record_batch);
        if (!add_result.ok()) {
            response->set_success(false);
            response->set_error_message("Failed to add record batch to ingestor: " + add_result.error().message());
            return grpc::Status::OK;
        }

        // Commit the data to the table
        auto commit_result = ingestor->Commit();
        if (!commit_result.ok()) {
            response->set_success(false);
            response->set_error_message("Failed to commit data to table: " + commit_result.error().message());
            return grpc::Status::OK;
        }

        // Set response values
        response->set_success(true);
        response->set_rows_ingested(record_batch->num_rows());

    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(std::string("Error ingesting JSON data: ") + e.what());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::CreateKVTable(grpc::ServerContext* context,
                                            const pond::proto::CreateKVTableRequest* request,
                                            pond::proto::CreateKVTableResponse* response) {
    LOG_STATUS("Received CreateKVTable request for table: %s", request->table_name().c_str());

    try {
        // Create a schema for the KV table (single 'value' column of type STRING)
        auto schema = std::make_shared<pond::common::Schema>(
            std::vector<pond::common::ColumnSchema>{{"value", pond::common::ColumnType::STRING}});

        // Try to create the table in the DB
        auto create_result = db_->CreateTable(request->table_name(), schema);
        
        if (create_result.ok()) {
            // Also register the table in the catalog for consistency
            if (catalog_) {
                auto catalog_result = catalog_->CreateTable(
                    request->table_name(),
                    schema,
                    {},  // empty partition spec
                    ""   // let catalog decide the location
                );
                
                if (!catalog_result.ok()) {
                    // Log the warning but don't fail the operation
                    LOG_WARNING("Created KV table in DB but failed to register in catalog: %s", 
                               catalog_result.error().message().c_str());
                }
            }
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error(create_result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error creating KV table: ") + e.what());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::ListKVTables(grpc::ServerContext* context,
                                           const pond::proto::ListKVTablesRequest* request,
                                           pond::proto::ListKVTablesResponse* response) {
    LOG_STATUS("Received ListKVTables request");

    try {
        // Get all tables from the KV DB
        auto list_result = db_->ListTables();
        
        if (list_result.ok()) {
            response->set_success(true);
            
            // For each table, check if it has KV structure (single 'value' column of type STRING)
            for (const auto& table_name : list_result.value()) {
                // Preload the table to ensure it's in memory
                auto table_result = db_->GetTable(table_name);
                if (table_result.ok()) {
                    auto table = table_result.value();
                    auto& schema = table->schema();
                    
                    // Check if the schema matches a KV table
                    const auto& columns = schema->Columns();
                    if (columns.size() == 1 && 
                        columns[0].name == "value" && 
                        columns[0].type == pond::common::ColumnType::STRING) {
                        response->add_table_names(table_name);
                    }
                }
            }
        } else {
            response->set_success(false);
            response->set_error(list_result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error listing KV tables: ") + e.what());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::ReadParquetFile(grpc::ServerContext* context,
                                              const pond::proto::ReadParquetFileRequest* request,
                                              grpc::ServerWriter<pond::proto::ReadParquetFileResponse>* writer) {
    LOG_STATUS("Received ReadParquetFile request for file: %s", request->file_path().c_str());

    try {
        // Create a ParquetReader instance
        auto reader_result = pond::format::ParquetReader::Create(fs_, request->file_path());
        if (!reader_result.ok()) {
            pond::proto::ReadParquetFileResponse error_response;
            error_response.set_success(false);
            error_response.set_error(reader_result.error().message());
            writer->Write(error_response);
            return grpc::Status::OK;
        }
        auto reader = std::move(reader_result).value();

        // Get the schema
        auto schema_result = reader->Schema();
        if (!schema_result.ok()) {
            pond::proto::ReadParquetFileResponse error_response;
            error_response.set_success(false);
            error_response.set_error(schema_result.error().message());
            writer->Write(error_response);
            return grpc::Status::OK;
        }
        auto schema = schema_result.value();

        // Get number of row groups
        auto num_row_groups_result = reader->NumRowGroups();
        if (!num_row_groups_result.ok()) {
            pond::proto::ReadParquetFileResponse error_response;
            error_response.set_success(false);
            error_response.set_error(num_row_groups_result.error().message());
            writer->Write(error_response);
            return grpc::Status::OK;
        }
        int num_row_groups = num_row_groups_result.value();

        // Convert column names to indices if specific columns are requested
        std::vector<std::string> column_names;
        if (!request->columns().empty()) {
            column_names.assign(request->columns().begin(), request->columns().end());
        }

        // Read the entire table to get total row count and handle pagination
        auto table_result = column_names.empty() ? reader->Read() : reader->Read(column_names);
        if (!table_result.ok()) {
            pond::proto::ReadParquetFileResponse error_response;
            error_response.set_success(false);
            error_response.set_error(table_result.error().message());
            writer->Write(error_response);
            return grpc::Status::OK;
        }
        auto table = table_result.value();

        // Get total number of rows
        int64_t total_rows = table->num_rows();

        // Calculate start and end rows for pagination
        int64_t start_row = request->start_row();
        int64_t num_rows = request->num_rows() > 0 ? request->num_rows() : total_rows;
        int64_t end_row = std::min(start_row + num_rows, total_rows);

        // Prepare response
        pond::proto::ReadParquetFileResponse response;
        response.set_success(true);
        response.set_total_rows(total_rows);
        response.set_has_more(end_row < total_rows);

        // Convert columns to response format for the requested page
        for (int i = 0; i < table->num_columns(); i++) {
            auto column = table->column(i);
            auto field = table->schema()->field(i);
            
            auto* col_data = response.add_columns();
            col_data->set_name(field->name());
            col_data->set_type(field->type()->ToString());

            // Add only the rows for the current page
            auto chunk = column->chunk(0);
            for (int64_t row = start_row; row < end_row; row++) {
                if (chunk->IsNull(row)) {
                    col_data->add_values("");
                    col_data->add_nulls(true);
                } else {
                    std::string str_val;
                    switch (field->type()->id()) {
                        case arrow::Type::INT32:
                            str_val = std::to_string(std::static_pointer_cast<arrow::Int32Array>(chunk)->Value(row));
                            break;
                        case arrow::Type::INT64:
                            str_val = std::to_string(std::static_pointer_cast<arrow::Int64Array>(chunk)->Value(row));
                            break;
                        case arrow::Type::FLOAT:
                            str_val = std::to_string(std::static_pointer_cast<arrow::FloatArray>(chunk)->Value(row));
                            break;
                        case arrow::Type::DOUBLE:
                            str_val = std::to_string(std::static_pointer_cast<arrow::DoubleArray>(chunk)->Value(row));
                            break;
                        case arrow::Type::STRING:
                            str_val = std::static_pointer_cast<arrow::StringArray>(chunk)->GetString(row);
                            break;
                        case arrow::Type::BOOL:
                            str_val = std::static_pointer_cast<arrow::BooleanArray>(chunk)->Value(row) ? "true" : "false";
                            break;
                        default:
                            str_val = "Unsupported type";
                    }
                    col_data->add_values(str_val);
                    col_data->add_nulls(false);
                }
            }
        }

        // Send the batch
        if (!writer->Write(response)) {
            return grpc::Status(grpc::StatusCode::CANCELLED, "Failed to write response");
        }

        return grpc::Status::OK;
    } catch (const std::exception& e) {
        pond::proto::ReadParquetFileResponse error_response;
        error_response.set_success(false);
        error_response.set_error(std::string("Error reading parquet file: ") + e.what());
        writer->Write(error_response);
        return grpc::Status::OK;
    }
}

grpc::Status PondServiceImpl::UpdatePartitionSpec(grpc::ServerContext* context,
                                                  const pond::proto::UpdatePartitionSpecRequest* request,
                                                  pond::proto::UpdatePartitionSpecResponse* response) {
    LOG_STATUS("Received UpdatePartitionSpec request for table: %s", request->table_name().c_str());

    try {
        // Convert protobuf partition spec to catalog partition spec
        catalog::PartitionSpec spec;
        spec.spec_id = 0;  // New spec gets ID 0

        for (const auto& pb_field : request->partition_spec().fields()) {
            std::string transform = pb_field.transform();
            std::transform(transform.begin(), transform.end(), transform.begin(), ::tolower);

            catalog::Transform transform_type;
            try {
                transform_type = catalog::TransformFromString(transform);
            } catch (const std::exception& e) {
                response->set_success(false);
                response->set_error("Invalid transform type: " + transform);
                return grpc::Status::OK;
            }

            catalog::PartitionField field(
                pb_field.source_id(),
                pb_field.field_id(),
                pb_field.name(),
                transform_type
            );
            
            // Validate transform parameters
            if (transform_type == catalog::Transform::BUCKET || transform_type == catalog::Transform::TRUNCATE) {
                if (pb_field.transform_param().empty()) {
                    response->set_success(false);
                    response->set_error("Transform parameter is required for " + transform + " transform");
                    return grpc::Status::OK;
                }
                
                try {
                    int param = std::stoi(pb_field.transform_param());
                    if (param <= 0) {
                        response->set_success(false);
                        response->set_error("Transform parameter must be positive for " + transform + " transform");
                        return grpc::Status::OK;
                    }
                    field.transform_param = param;
                } catch (const std::exception& e) {
                    response->set_success(false);
                    response->set_error("Invalid transform parameter for " + transform + " transform: " + pb_field.transform_param());
                    return grpc::Status::OK;
                }
            }
            
            spec.fields.push_back(std::move(field));
        }

        // Update the partition spec in the catalog
        auto result = catalog_->UpdatePartitionSpec(request->table_name(), spec);

        if (result.ok()) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_error(result.error().message());
        }
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error(std::string("Error updating partition spec: ") + e.what());
    }
    
    return grpc::Status::OK;
}

}  // namespace pond::server