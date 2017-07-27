//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "logging/durability_factory.h"
#include "logging/log_manager.h"
#include "storage/tile_group_header.h"

namespace peloton {
namespace logging {

  thread_local WorkerContext* tl_worker_ctx = nullptr;

  void LogManager::StartTxn(concurrency::Transaction *txn) {
    PL_ASSERT(tl_worker_ctx);
 //   size_t txn_id = txn->GetTransactionId();

    // Record the txn timer
//    DurabilityFactory::StartTxnTimer(txn_eid, tl_worker_ctx);

//    PL_ASSERT(tl_worker_ctx->current_commit_eid == 999 || tl_worker_ctx->current_commit_eid <= txn_eid); //MAX_EPOCH_ID


    // Handle the commit id
    cid_t txn_cid = txn->GetCommitId();
    tl_worker_ctx->current_cid = txn_cid;
  }

//  void LogManager::FinishPendingTxn() {
//    PL_ASSERT(tl_worker_ctx);
//    size_t glob_peid = global_persist_epoch_id_.load();
//    DurabilityFactory::StopTimersByPepoch(glob_peid, tl_worker_ctx);
//  }

  /*void LogManager::MarkTupleCommitEpochId(storage::TileGroupHeader *tg_header, oid_t tuple_slot) {
    if (DurabilityFactory::GetLoggingType() == LoggingType::INVALID) {
      return;
    }

    PL_ASSERT(tl_worker_ctx != nullptr);
    PL_ASSERT(tl_worker_ctx->current_commit_eid != 999 && tl_worker_ctx->current_commit_eid != 999999); //MAX_EPOCH_ID INVALID_EPOCH_ID
    //tg_header->SetLoggingCommitEpochId(tuple_slot, tl_worker_ctx->current_commit_eid);
  }*/

}
}
