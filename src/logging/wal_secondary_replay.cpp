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

void WalSecondaryReplay::ReplayTransactionWrapper(void *arg_ptr){
	
	WalRecovery wr(0, "/tmp/log");
	FileHandle fh;

	ReplayTransactionArg *arg = (ReplayTransactionArg *) arg_ptr;
	
	wr.ReplayLogFileOrReceivedBuffer(false,fh, arg->received_buf_, arg->len_);
	std::cout<<"replayed the request"<<std::endl;
	
	// first delete the allocated buffer
	delete arg->received_buf_;
	delete (arg);
}

}
}
