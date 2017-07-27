//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_log_manager.h
//
// Identification: src/backend/logging/reordered_phylog_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>
#include <unordered_map>

#include "libcuckoo/cuckoohash_map.hh"
#include "concurrency/transaction.h"
#include "concurrency/epoch_manager.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/log_buffer_pool.h"
#include "logging/log_manager.h"
#include "logging/logging_util.h"
#include "logging/worker_context.h"
#include "logging/reordered_phylog_logger.h"
#include "type/types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
#include "common/logger.h"


namespace peloton {
namespace logging {

/**
 * logging file name layout :
 *
 * dir_name + "/" + prefix + "_" + epoch_id
 *
 *
 * logging file layout :
 *
 *  -----------------------------------------------------------------------------
 *  | txn_cid | database_id | table_id | operation_type | data | ... | txn_end_flag
 *  -----------------------------------------------------------------------------
 *
 * NOTE: this layout is designed for physiological logging.
 *
 * NOTE: tuple length can be obtained from the table schema.
 *
 */

class ReorderedPhyLogLogManager : public LogManager {
  ReorderedPhyLogLogManager(const ReorderedPhyLogLogManager &) = delete;
  ReorderedPhyLogLogManager &operator=(const ReorderedPhyLogLogManager &) = delete;
  ReorderedPhyLogLogManager(ReorderedPhyLogLogManager &&) = delete;
  ReorderedPhyLogLogManager &operator=(ReorderedPhyLogLogManager &&) = delete;

protected:

  ReorderedPhyLogLogManager()
    : worker_count_(0),
      is_running_(false) {}

public:
  static ReorderedPhyLogLogManager &GetInstance() {
    static ReorderedPhyLogLogManager log_manager;
    return log_manager;
  }
  virtual ~ReorderedPhyLogLogManager() {}

  virtual void SetDirectory(const std::string logging_dir) {
    logger_dir_ = logging_dir;

    // check the existence of logging directories.
    // if not exists, then create the directory.
    if (LoggingUtil::CheckDirectoryExistence(logger_dir_.c_str()) == false) {
        LOG_INFO("Logging directory %s is not accessible or does not exist", logger_dir_.c_str());
        bool res = LoggingUtil::CreateDirectory(logger_dir_.c_str(), 0700);
        if (res == false) {
          LOG_ERROR("Cannot create directory: %s", logger_dir_.c_str());
          //throw new Exception();
        }
      }

    logger_count_ = 1;
      loggers_.emplace_back(new ReorderedPhyLogLogger(0, logger_dir_));
    }

  virtual const std::string GetDirectory() {
    return logger_dir_;
  }

  // Worker side logic
  virtual void RegisterWorker() override;
  virtual void DeregisterWorker() override;

  virtual void StartTxn(concurrency::Transaction *txn) override ;

  void LogInsert(const ItemPointer &tuple_pos);
  void LogUpdate(const ItemPointer &tuple_pos);
  void LogDelete(const ItemPointer &tuple_pos_deleted);

  void StartPersistTxn(concurrency::Transaction *txn);
  void EndPersistTxn(concurrency::Transaction *txn);


  // Logger side logic
//  virtual void DoRecovery(const size_t &begin_eid) override;
  virtual void StartLoggers() override;
  virtual void StopLoggers() override;

//  void RunPepochLogger();

private:
//  size_t RecoverPepoch();

  void WriteRecordToBuffer(LogRecord &record);

private:
  std::atomic<oid_t> worker_count_;

  std::string logger_dir_;

  std::vector<std::shared_ptr<ReorderedPhyLogLogger>> loggers_;

 // std::unique_ptr<std::thread> pepoch_thread_;
  volatile bool is_running_;

  //std::string pepoch_dir_;

  //const std::string pepoch_filename_ = "pepoch";
};

}
}
