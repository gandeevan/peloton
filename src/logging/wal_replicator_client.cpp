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
#include "peloton/proto/wal_service.grpc.pb.h"
//#include "peloton/proto/wal_service.grpc.pb.cc"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;


namespace peloton{
namespace logging{


Status WalReplicatorClient::ReplayRecord(LogRecord log_record){

    WalLogger* wl = new WalLogger(0, "/tmp/log");

    // TODO: Allocate request on heap maybe ?
    LogReplayRequest replay_request;
    LogReplayResponse replay_response;
    ClientContext context;

    CopySerializeOutput *output = wl->WriteRecordToBuffer(log_record);

    replay_request.set_len(output->Size());
    replay_request.set_data(output->Data());

    /* grpc request */
    Status status = stub_->ReplayRecord(&context, replay_request, &replay_response);

    delete wl;
    delete output;

    return status;
}


void WalReplicatorClient::ReplayTransaction(std::vector<LogRecord> log_records){
    for(LogRecord log_record : log_records){
        Status status = ReplayRecord(log_record);

        if(status.ok()){
            LOG_INFO("GRPC REQUEST SUCCESS: commit_id = %lu, epoch_id = %lu", log_record.GetCommitId(), log_record.GetEpochId());
        } else{
            // TODO: what to do when rpc request fails ?
            LOG_ERROR("GRPC REQUEST FAILED");
        }
    }
}



}
}


