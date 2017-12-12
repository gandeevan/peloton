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

  WalRecovery wr(0, "/tmp/log");
  FileHandle fh;

  Status status_code = Status::CANCELLED;
  
  int request_length = request->len();

  int32_t rep_mode_ = peloton::settings::SettingsManager::GetInt(peloton::settings::SettingId::replication_mode);
  char *buffer = (char *)request->data().c_str();;

  if (rep_mode_ == 2){

    // make callback arguments
    //void (*task_callback_)(void *) = nullptr;
    //void *task_callback_arg_ = nullptr;

    char *buffer_cpy = (char *)malloc(sizeof(char) * request_length);
    if (buffer_cpy == nullptr){
      LOG_ERROR("Can not allocate memory !!");
      exit(EXIT_FAILURE);
    }

    // copy data into buffer
    memcpy(buffer_cpy,buffer,request_length);

    ReplayTransactionArg *arg = new ReplayTransactionArg(buffer_cpy, request_length);
    // TODO: add class
    //threadpool::ReplayQueuePool::GetInstance().SubmitTask(WalSecondaryReplay::ReplayTransactionWrapper,arg,task_callback_,task_callback_arg_);
    std::thread t1(WalSecondaryReplay::ReplayTransactionWrapper,(void *)arg);
    t1.detach();
    status_code = Status::OK;
  }
  else if (rep_mode_ ==  1){
    if( wr.ReplayLogFileOrReceivedBuffer(false, fh, buffer, request_length) ) {
      status_code = Status::OK;
    }
  }
  return status_code;
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
