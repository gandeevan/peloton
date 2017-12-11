//
// Created by Gandeevan R on 11/29/17.
//



#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>

#include "logging/wal_recovery.h"
#include "logging/wal_replicator_server.h"
#include "peloton/proto/wal_service.pb.h"
#include "peloton/proto/wal_service.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;



namespace peloton {
namespace logging {

Status WalReplicatorService::ReplayTransaction(ServerContext* context,
                const LogReplayRequest* request, LogReplayResponse* response){

  (void) context;
  (void) request;
  (void) response;
  LOG_DEBUG("rEaching here !!");

  WalRecovery wr(0, "/tmp/log");
  FileHandle fh;

  Status status_code;
  
  std::cout<<" recv len: "<<*((int *)request->data().c_str())<<" "<<std::endl;
  // char *buffer = new char [request->data().length()+1];
  // std::strcpy (buffer, request->data().c_str());
  std::cout<<"R ecaching here console"<<std::endl;

  char *buffer = (char *)request->data().c_str();

  if(wr.ReplayLogFileOrReceivedBuffer(false, fh, buffer, request->len())){
    status_code = Status::OK;
  } else{
    status_code = Status::CANCELLED;
  }

  return status_code;

  return Status::OK;
}

void WalReplicatorServer::RunServer() {
    std::string server_address = GetServerAddress();

    WalReplicatorService *service = new WalReplicatorService();
    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service);
    std::unique_ptr<Server> server(builder.BuildAndStart());

    server->Wait();
}

}
}
