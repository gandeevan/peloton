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
#include "logging/wal_logger.h"

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

  PersistReplayLog(arg->received_buf_, arg->len_);
  // first delete the allocated buffer
  delete arg->received_buf_;
  delete (arg);
}

void WalSecondaryReplay::PersistReplayLog(const char *received_buf, size_t len) {
  size_t offset, step_size = 1024*500;
  std::cout << "PersistReplayLog" << std::endl;
  WalLogger wl(0, "/tmp/log");
  for (offset = 0; offset < len; offset += step_size) {
    LogBuffer *buffer = new LogBuffer();
    size_t to_write = std::min(len - offset, step_size);
    buffer->WriteData(received_buf, to_write);
    std::cout << "persisting to buffer" << std::endl;
    wl.PersistLogBuffer(buffer);
    std::cout << "Persisted to buffer " << std::endl;
  }
}

}
}
