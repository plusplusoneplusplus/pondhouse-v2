#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "common/append_only_fs.h"
#include "common/log.h"
#include "kv/db.h"
#include "pond_service_impl.h"

int main(int argc, char** argv) {
    // Initialize logging
    pond::common::Logger::instance().init("server.log");

    // Parse command line arguments
    std::string server_address = "0.0.0.0:50051";
    std::string db_name = "pondhouse_db";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--address" && i + 1 < argc) {
            server_address = argv[++i];
        } else if (arg == "--db_name" && i + 1 < argc) {
            db_name = argv[++i];
        }
    }

    LOG_STATUS("Starting Pond gRPC server on %s", server_address.c_str());
    LOG_STATUS("Using database name: %s", db_name.c_str());

    // Create the database
    auto fs = std::make_shared<pond::common::LocalAppendOnlyFileSystem>();
    auto db_result = pond::kv::DB::Create(fs, db_name);
    if (!db_result.ok()) {
        LOG_ERROR("Failed to create database: %s", db_result.error().message().c_str());
        return 1;
    }

    auto db = std::move(db_result.value());

    // Create the service implementation
    auto service = std::make_unique<pond::server::PondServiceImpl>(db, fs);

    // Build the server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());

    // Start the server
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    LOG_STATUS("Server started, listening on %s", server_address.c_str());

    // Wait for the server to shutdown
    server->Wait();

    return 0;
}