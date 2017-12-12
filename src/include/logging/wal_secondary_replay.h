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
  WalSecondaryReplay() {
  	//wr = new WalRecovery(0, "/tmp/log");
  }

  ~WalSecondaryReplay() {
  	//delete wr;
  }

  void RunReplayThread();

  static void ReplayTransactionWrapper(void *arg_ptr);
  static void PersistReplayLog(const char *buf, size_t len);
 
 private:
  std::thread *replay_thread_;
  //static WalRecovery *wr;
};

struct ReplayTransactionArg {
  inline ReplayTransactionArg(char *received_buf, size_t len)
      : received_buf_(received_buf),len_(len) {}
 
  char *received_buf_;
  size_t len_;
};


}
}
