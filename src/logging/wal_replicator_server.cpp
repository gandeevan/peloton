//
// Created by Gandeevan R on 11/29/17.
//



#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>

//#include "logging/wal_recovery.h"
#include "logging/wal_replicator_server.h"
#include "peloton/proto/wal_service.pb.h"
#include "peloton/proto/wal_service.grpc.pb.h"
#include "threadpool/replay_queue_pool.h"
#include "logging/wal_secondary_replay.h"

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

  Status status_code = Status::CANCELLED;
  
  std::cout<<" recv len: "<<*((int *)request->data().c_str())<<" "<<std::endl;
  // char *buffer = new char [request->data().length()+1];
  // std::strcpy (buffer, request->data().c_str());
  std::cout<<"R ecaching here console"<<std::endl;
  bool is_async_ = true;
  char *buffer = (char *)request->data().c_str();;

  if (is_async_){

    // make callback arguments
    void (*task_callback_)(void *) = nullptr;
    void *task_callback_arg_ = nullptr;

    char *buffer_cpy = (char *)malloc(sizeof(char) * request->len());
    if (buffer_cpy == nullptr){
      LOG_ERROR("Can not allocate memory !!");
      exit(EXIT_FAILURE);
    }

    // copy data into buffer
    memcpy(buffer_cpy,buffer,request->len());

    ReplayTransactionArg *arg = new ReplayTransactionArg(false, fh, buffer_cpy, request->len());
    // TODO: add class
    std::cout<<"Before making aysnc call"<<std::endl;
    threadpool::ReplayQueuePool::GetInstance().SubmitTask(WalSecondaryReplay::ReplayTransactionWrapper,arg,task_callback_,task_callback_arg_);
    status_code = Status::OK;
  }
  else{
    if(wr.ReplayLogFileOrReceivedBuffer(false, fh, buffer, request->len())){
      status_code = Status::OK;
    }
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
