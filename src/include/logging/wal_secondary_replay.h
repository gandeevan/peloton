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

/*
namespace storage {
class TileGroupHeader;
}*/

namespace logging {

class WalSecondaryReplay {
 public:
  WalSecondaryReplay(){}
  WalSecondaryReplay(const size_t &logger_id, const std::string &log_dir)
      : logger_id_(logger_id), log_dir_(log_dir) {}

  ~WalSecondaryReplay() {}

  void ReplayInsert(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t tg_block, oid_t tg_offset);
  void ReplayCatalogInsert(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t tg_block, oid_t tg_offset);
  void ReplayDelete(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t tg_block, oid_t tg_offset);
  void ReplayUpdate(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t old_tg_block ,oid_t tg_block, old_tg_offset ,oid_t tg_offset);

  void RunReplayThread();
  void RunServer();

 private:
  // void Run();
  std::thread *replay_thread_;

};
}
}
