#pragma once

#include <memory>
#include <string>

#include "common/result.h"
#include "kv/db.h"
#include "proto/build/proto/pond_service.grpc.pb.h"

namespace pond::server {

// Implementation of the PondService gRPC service
class PondServiceImpl final : public pond::proto::PondService::Service {
public:
    explicit PondServiceImpl(std::shared_ptr<pond::kv::DB> db);
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

private:
    // Helper method to get the default table
    common::Result<std::shared_ptr<pond::kv::Table>> GetDefaultTable();

    std::shared_ptr<pond::kv::DB> db_;
};

}  // namespace pond::server