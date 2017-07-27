//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cmath>

#include "common/macros.h"
#include "concurrency/transaction.h"
#include "logging/worker_context.h"

namespace peloton {

namespace storage {
  class TileGroupHeader;
}

namespace logging {


/* Per worker thread local context */
extern thread_local WorkerContext* tl_worker_ctx;

// loggers are created before workers.
class LogManager {
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;
  LogManager(LogManager &&) = delete;
  LogManager &operator=(LogManager &&) = delete;

public:
  LogManager() {}

  virtual ~LogManager() {}

  virtual void SetDirectory(const std::string logging_dirs) = 0;

  virtual const std::string GetDirectory() = 0;

  void SetRecoveryThreadCount(const size_t &recovery_thread_count) {
    recovery_thread_count_ = recovery_thread_count;
  }

  virtual void RegisterWorker() = 0;
  virtual void DeregisterWorker() = 0;

  //virtual void DoRecovery(const size_t &begin_eid) = 0;

  virtual void StartLoggers() = 0;
  virtual void StopLoggers() = 0;

  virtual void StartTxn(concurrency::Transaction *txn);

  //virtual void FinishTxn(concurrency::Transaction *txn);

  void MarkTupleCommitEpochId(storage::TileGroupHeader *tg_header, oid_t tuple_slot);


protected:
  size_t logger_count_=1;

  size_t recovery_thread_count_ = 1;
};

}
}
