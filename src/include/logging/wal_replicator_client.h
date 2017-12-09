//
// Created by Gandeevan R on 11/29/17.
//


#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>

#include "logging/log_record.h"
#include "peloton/proto/wal_service.grpc.pb.h"

using grpc::Channel;
using grpc::Status;

namespace peloton{
namespace logging{

class WalReplicatorClient{
  public:
    WalReplicatorClient(std::shared_ptr<Channel> channel)
            : stub_(WalReplicatorService::NewStub(channel)) {}

    void ReplayTransaction(std::vector<LogRecord> log_records);

  private:
    Status ReplayRecord(LogRecord log_record);

  private:
    std::unique_ptr<WalReplicatorService::Stub> stub_;

};

}
}