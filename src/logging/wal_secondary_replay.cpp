//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_secondary_replay.cpp
//
// Identification: src/logging/wal_secondary_replay.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>
#include <thread>

#include "logging/wal_replication_manager.h"
#include "logging/wal_secondary_replay.h"
#include "logging/log_record.h"
#include "logging/wal_recovery.h"

namespace peloton {
namespace logging {


void WalSecondaryReplay::RunReplayThread(){
  replay_thread_ = new std::thread(WalReplicationManager::AcceptReplayTransactionRequests);
}

void WalSecondaryReplay::ReplayTransactionWrapper(void *arg){

	ReplayTransactionArg *arg = (ReplayTransactionArg *) arg;

	wr->ReplayLogFileOrReceivedBuffer(arg->from_log_file_, arg->file_handle_, arg->received_buf_, arg->len_);

	delete (arg);
}

}
}
