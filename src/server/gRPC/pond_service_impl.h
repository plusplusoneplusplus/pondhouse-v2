#pragma once

#include <memory>
#include <string>

#include "catalog/catalog.h"
#include "common/append_only_fs.h"
#include "common/result.h"
#include "kv/db.h"
#include "proto/build/proto/pond_service.grpc.pb.h"
#include "query/executor/executor.h"
#include "query/data/data_accessor.h"

namespace pond::server {

// Implementation of the PondService gRPC service
class PondServiceImpl final : public pond::proto::PondService::Service {
public:
    explicit PondServiceImpl(std::shared_ptr<pond::kv::DB> db, std::shared_ptr<pond::common::IAppendOnlyFileSystem> fs);
    ~PondServiceImpl() override = default;

    // Get a value by key
    grpc::Status Get(grpc::ServerContext* context,
                     const pond::proto::GetRequest* request,
                     pond::proto::GetResponse* response) override;

    // Put a key-value pair
    grpc::Status Put(grpc::ServerContext* context,
                     const pond::proto::PutRequest* request,
                     pond::proto::PutResponse* response) override;

    // Delete a key
    grpc::Status Delete(grpc::ServerContext* context,
                        const pond::proto::DeleteRequest* request,
                        pond::proto::DeleteResponse* response) override;

    // Scan a range of keys
    grpc::Status Scan(grpc::ServerContext* context,
                      const pond::proto::ScanRequest* request,
                      grpc::ServerWriter<pond::proto::ScanResponse>* writer) override;

    // List detailed files in a directory
    grpc::Status ListDetailedFiles(grpc::ServerContext* context,
                                   const pond::proto::ListDetailedFilesRequest* request,
                                   pond::proto::ListDetailedFilesResponse* response) override;

    // Get directory information
    grpc::Status GetDirectoryInfo(grpc::ServerContext* context,
                                  const pond::proto::DirectoryInfoRequest* request,
                                  pond::proto::DirectoryInfoResponse* response) override;

    // Execute SQL query (primarily for table creation)
    grpc::Status ExecuteSQL(grpc::ServerContext* context,
                            const pond::proto::ExecuteSQLRequest* request,
                            pond::proto::ExecuteSQLResponse* response) override;

    // List all tables in the catalog
    grpc::Status ListTables(grpc::ServerContext* context,
                            const pond::proto::ListTablesRequest* request,
                            pond::proto::ListTablesResponse* response) override;

    // Get table metadata
    grpc::Status GetTableMetadata(grpc::ServerContext* context,
                                  const pond::proto::GetTableMetadataRequest* request,
                                  pond::proto::GetTableMetadataResponse* response) override;

    // Ingest data from JSON
    grpc::Status IngestJsonData(grpc::ServerContext* context,
                                const pond::proto::IngestJsonDataRequest* request,
                                pond::proto::IngestJsonDataResponse* response) override;

    // Create a new KV table
    grpc::Status CreateKVTable(grpc::ServerContext* context,
                               const pond::proto::CreateKVTableRequest* request,
                               pond::proto::CreateKVTableResponse* response) override;
    
    // List all KV tables
    grpc::Status ListKVTables(grpc::ServerContext* context,
                              const pond::proto::ListKVTablesRequest* request,
                              pond::proto::ListKVTablesResponse* response) override;

    // Read content from a parquet file
    grpc::Status ReadParquetFile(grpc::ServerContext* context,
                                 const pond::proto::ReadParquetFileRequest* request,
                                 grpc::ServerWriter<pond::proto::ReadParquetFileResponse>* writer) override;

    // Update partition specification for a table
    grpc::Status UpdatePartitionSpec(grpc::ServerContext* context,
                                    const pond::proto::UpdatePartitionSpecRequest* request,
                                    pond::proto::UpdatePartitionSpecResponse* response) override;

    // Execute a SQL query and stream results back
    grpc::Status ExecuteQuery(grpc::ServerContext* context,
                            const pond::proto::ExecuteQueryRequest* request,
                            grpc::ServerWriter<pond::proto::ExecuteQueryResponse>* writer) override;

protected:
    // Helper method to get the default table
    common::Result<std::shared_ptr<pond::kv::Table>> GetDefaultTable();

    std::shared_ptr<pond::kv::DB> db_;
    std::shared_ptr<pond::common::IAppendOnlyFileSystem> fs_;
    std::shared_ptr<pond::catalog::Catalog> catalog_;
    std::shared_ptr<pond::kv::DB> catalog_db_;
    std::shared_ptr<pond::query::Executor> query_executor_;
    std::shared_ptr<pond::query::DataAccessor> data_accessor_;

    // Friend test declarations for unit tests
    friend class PondServiceImplTest;
    FRIEND_TEST(PondServiceImplTest, IngestJsonData_Success);
    FRIEND_TEST(PondServiceImplTest, IngestJsonData_TableNotFound);
    FRIEND_TEST(PondServiceImplTest, IngestJsonData_InvalidJson);
    FRIEND_TEST(PondServiceImplTest, IngestJsonData_SchemaTypeMismatch);
    FRIEND_TEST(PondServiceImplTest, IngestJsonData_EmptyJsonArray);
    FRIEND_TEST(PondServiceImplTest, IngestJsonData_MultipleBatches);
};

}  // namespace pond::server