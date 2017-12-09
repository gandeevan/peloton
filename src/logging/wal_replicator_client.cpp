/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/logging_util.cpp
 *
 *-------------------------------------------------------------------------
 */


#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>

#include "common/logger.h"
#include "logging/log_record.h"
#include "logging/wal_logger.h"
#include "logging/wal_replicator_client.h"
#include "peloton/proto/wal_service.pb.h"
#include "peloton/proto/wal_service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;


namespace peloton{
namespace logging{

void WalReplicatorClient::ReplayTransaction(std::vector<LogRecord> log_records){

    std::string buffer;
    LogReplayRequest replay_request;
    LogReplayResponse replay_response;
    ClientContext context;

    WalLogger* wl = new WalLogger(0, "/tmp/log");

    for(LogRecord log_record : log_records){
        CopySerializeOutput *output = wl->WriteRecordToBuffer(log_record);
        buffer.append(output->Data(), output->Size());
        delete output;
    }


    replay_request.set_len(buffer.size());
    replay_request.set_data(buffer);

    std::cout<<"length = "<<*((int *)(replay_request.data().c_str()))<<std::flush;


    // actual rpc call
    Status status = stub_->ReplayTransaction(&context, replay_request, &replay_response);

    if(status.ok()){
      LOG_INFO("GRPC REQUEST SUCCESS");
    } else{
      // TODO: what to do when rpc request fails ?
      LOG_ERROR("GRPC REQUEST FAILED - %s", status.error_message().c_str());
    }

    delete wl;
}



}
}


