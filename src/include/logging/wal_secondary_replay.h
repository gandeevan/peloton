//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_secondary_replay.h
//
// Identification: src/include/logging/wal_secondary_replay.h
//
// Recovery component, called on init.
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "type/types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
#include "common/logger.h"
#include "type/abstract_pool.h"
#include "logging/wal_recovery.h"

namespace peloton {

namespace logging {

class WalSecondaryReplay {
 public:
  
  void RunReplayThread();

  static void ReplayTransactionWrapper(void *arg);
 
 private:
  std::thread *replay_thread_;
  WalRecovery wr(0, "/tmp/log");
};

struct ReplayTransactionArg {
  inline ReplayTransactionArg(bool from_log_file, FileHandle &file_handle, char *received_buf, size_t len)
      : from_log_file_(from_log_file),file_handle_(file_handle),received_buf_(received_buf),len_(len) {}

  bool from_log_file_;
  FileHandle &file_handle_;
  char *received_buf_;
  size_t len_;
};


}
}
