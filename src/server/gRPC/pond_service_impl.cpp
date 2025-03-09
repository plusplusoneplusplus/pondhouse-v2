#include "pond_service_impl.h"

#include <string>

#include <grpcpp/grpcpp.h>

#include "common/data_chunk.h"
#include "common/log.h"
#include "common/result.h"
#include "common/schema.h"
#include "kv/db.h"
#include "catalog/kv_catalog.h"
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

grpc::Status PondServiceImpl::Get(grpc::ServerContext* context,
                                  const pond::proto::GetRequest* request,
                                  pond::proto::GetResponse* response) {
    LOG_STATUS("Received Get request for key: %s", request->key().c_str());

    std::string key = request->key();

    // Get the default table
    auto table_result = GetDefaultTable();
    if (!table_result.ok()) {
        response->set_found(false);
        response->set_error(table_result.error().message());
        return grpc::Status::OK;
    }

    // Get the value from the table
    auto result = table_result.value()->Get(key);

    if (result.ok()) {
        response->set_found(true);
        auto record = std::move(result).value();
        auto value_result = record->Get<std::string>(0);
        if (value_result.ok()) {
            response->set_value(value_result.value());
        } else {
            response->set_value("");
        }
    } else {
        response->set_found(false);
        response->set_error(result.error().message());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::Put(grpc::ServerContext* context,
                                  const pond::proto::PutRequest* request,
                                  pond::proto::PutResponse* response) {
    LOG_STATUS("Received Put request for key: %s", request->key().c_str());

    std::string key = request->key();
    std::string value = request->value();

    // Get the default table
    auto table_result = GetDefaultTable();
    if (!table_result.ok()) {
        response->set_success(false);
        response->set_error(table_result.error().message());
        return grpc::Status::OK;
    }

    // Create a record with the value
    auto table = table_result.value();
    auto record = std::make_unique<pond::kv::Record>(table->schema());
    record->Set(0, value);

    // Put the record in the table
    auto result = table->Put(key, std::move(record));

    if (result.ok()) {
        response->set_success(true);
    } else {
        response->set_success(false);
        response->set_error(result.error().message());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::Delete(grpc::ServerContext* context,
                                     const pond::proto::DeleteRequest* request,
                                     pond::proto::DeleteResponse* response) {
    LOG_STATUS("Received Delete request for key: %s", request->key().c_str());

    std::string key = request->key();

    // Get the default table
    auto table_result = GetDefaultTable();
    if (!table_result.ok()) {
        response->set_success(false);
        response->set_error(table_result.error().message());
        return grpc::Status::OK;
    }

    // Delete the key from the table
    auto result = table_result.value()->Delete(key);

    if (result.ok()) {
        response->set_success(true);
    } else {
        response->set_success(false);
        response->set_error(result.error().message());
    }

    return grpc::Status::OK;
}

grpc::Status PondServiceImpl::Scan(grpc::ServerContext* context,
                                   const pond::proto::ScanRequest* request,
                                   grpc::ServerWriter<pond::proto::ScanResponse>* writer) {
    LOG_STATUS("Received Scan request from %s to %s", request->start_key().c_str(), request->end_key().c_str());

    std::string start_key = request->start_key();
    std::string end_key = request->end_key();
    int32_t limit = request->limit();

    // Get the default table
    auto table_result = GetDefaultTable();
    if (!table_result.ok()) {
        pond::proto::ScanResponse scan_response;
        scan_response.set_has_more(false);
        scan_response.set_error(table_result.error().message());
        writer->Write(scan_response);
        return grpc::Status::OK;
    }

    // Get an iterator for the table
    auto iter_result = table_result.value()->NewIterator();
    if (!iter_result.ok()) {
        pond::proto::ScanResponse scan_response;
        scan_response.set_has_more(false);
        scan_response.set_error(iter_result.error().message());
        writer->Write(scan_response);
        return grpc::Status::OK;
    }

    auto iter = std::move(iter_result).value();

    if (!start_key.empty()) {
        iter->Seek(start_key);
    }

    int count = 0;
    while (iter->Valid() && (limit <= 0 || count < limit)) {
        // Check if we've reached the end key
        if (!end_key.empty() && iter->key() >= end_key) {
            break;
        }

        pond::proto::ScanResponse scan_response;
        scan_response.set_key(iter->key());

        // Get the value from the record
        auto& record = iter->value();
        auto value_result = record->Get<std::string>(0);
        if (value_result.ok()) {
            scan_response.set_value(value_result.value());
        } else {
            scan_response.set_value("");
        }

        scan_response.set_has_more(true);

        if (!writer->Write(scan_response)) {
            // Client disconnected
            return grpc::Status(grpc::StatusCode::CANCELLED, "Client disconnected");
        }

        iter->Next();
        count++;
    }

    // Send a final message indicating no more results
    if (iter->Valid() && (limit <= 0 || count < limit) && (end_key.empty() || iter->key() < end_key)) {
        pond::proto::ScanResponse scan_response;
        scan_response.set_has_more(true);
        writer->Write(scan_response);
    } else {
        pond::proto::ScanResponse scan_response;
        scan_response.set_has_more(false);
        writer->Write(scan_response);
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
                TypeConverter::AddDataFilesToTableMetadataInfo(
                    &metadata_info, 
                    files_result.value()
                );
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

}  // namespace pond::server