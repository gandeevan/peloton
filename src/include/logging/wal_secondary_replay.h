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

namespace peloton {

namespace logging {

class WalSecondaryReplay {
 public:
  void RunReplayThread();
  static void RunServer();
 private:
  std::thread *replay_thread_;
};
}
}
