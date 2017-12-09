//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_log_replicator.cpp
//
// Identification: src/logging/wal_log_replicator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <vector>
#include <cstdio>
#include <iostream>
#include <string>
#include <grpc++/grpc++.h>

#include "logging/wal_replicator_server.h"
#include "logging/wal_replication_manager.h"
#include "logging/wal_replicator_client.h"

using grpc::Channel;

namespace peloton{
namespace logging{

// invoked by the primary server
void WalReplicationManager::SendReplayTransactionRequest(std::vector<LogRecord> log_records){
    WalReplicatorClient *wrc = new WalReplicatorClient(grpc::CreateChannel(
            "localhost:50051", grpc::InsecureChannelCredentials()));

    wrc->ReplayTransaction(log_records);

    delete wrc;
}

// invoked by the secondary server
void WalReplicationManager::AcceptReplayTransactionRequests(){
    WalReplicatorServer *wrs = new WalReplicatorServer();
    wrs->RunServer();
    delete wrs;
}

}
}