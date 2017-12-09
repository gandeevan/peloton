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
#include "gc/gc_manager_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index_factory.h"

#include "catalog/catalog_defaults.h"
#include "catalog/column.h"
#include "catalog/manager.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/database_catalog.h"
#include "concurrency/epoch_manager_factory.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/tile_group_header.h"
#include "type/ephemeral_pool.h"
#include "storage/storage_manager.h"
#include "logging/log_record.h"
#include "logging/wal_recovery.h"

namespace peloton {
namespace logging {

void WalSecondaryReplay::ReplayInsert(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t tg_block, oid_t tg_offset){
  (void) log_record;
  (void) database_id;
  (void) table_id;
  (void) tg_block;
  (void) tg_offset;

  /*
  cid_t current_cid = log_record.GetCommitId();
  cid_t current_eid = log_record.GetEpochId();
  
  // Get item pointer , place where to insert the tuple
  ItemPointer location(tg_block, tg_offset);
  
  // table, schema and Tile Group
  auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
      database_id, table_id);
  auto schema = table->GetSchema();
  auto tg = table->GetTileGroupById(tg_block);
  
  // The required tile group might not have been created yet
  if (tg.get() == nullptr) {
    table->AddTileGroupWithOidForRecovery(tg_block);
    tg = table->GetTileGroupById(tg_block);
    catalog::Manager::GetInstance().GetNextTileGroupId();
  }

  // Major issue here - Tuple
  
  // create the required tuple - think what data can be put here -- TODO 
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  for (oid_t oid = 0; oid < schema->GetColumns().size(); oid++) {
    type::Value val = type::Value::DeserializeFrom(
        record_decode, schema->GetColumn(oid).GetType());
    tuple->SetValue(oid, val, pool.get());
  }


  // Simply insert the tuple in the tilegroup directly
  auto tuple_id =
      tg->InsertTupleFromRecovery(current_cid, tg_offset, tuple.get());
  table->IncreaseTupleCount(1);
  if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
    if (table->GetTileGroupById(tg->GetTileGroupId() + 1).get() ==
        nullptr)
      table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId() + 1);
    
    // Why is this line written?
    catalog::Manager::GetInstance().GetNextTileGroupId();
  }*/
}

void WalSecondaryReplay::ReplayDelete(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t tg_block, oid_t tg_offset){
  (void) log_record;
  (void) database_id;
  (void) table_id;
  (void) tg_block;
  (void) tg_offset;

  /*
  cid_t current_cid = log_record.GetCommitId();
  cid_t current_eid = log_record.GetEpochId();

  // Get item pointer , place where to insert the tuple
  ItemPointer location(tg_block, tg_offset);
  
  // table, schema and Tile Group
  auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
      database_id, table_id);
  auto schema = table->GetSchema();
  auto tg = table->GetTileGroupById(tg_block);
  // This code might be useful on drop
  if (database_id == CATALOG_DATABASE_OID) {  // catalog database oid
    switch (table_id) {
      case TABLE_CATALOG_OID:  // pg_table
      {
        auto db_oid = tg->GetValue(tg_offset, 2).GetAs<oid_t>();
        auto table_oid = tg->GetValue(tg_offset, 0).GetAs<oid_t>();
        auto database =
            storage::StorageManager::GetInstance()->GetDatabaseWithOid(
                db_oid);  // Getting database oid from pg_table
        database->DropTableWithOid(table_oid);
        LOG_DEBUG("\n\n\nPG_TABLE\n\n\n");
        break;
      }
      case INDEX_CATALOG_OID:  // pg_index
      {
        std::vector<std::unique_ptr<storage::Tuple>>::iterator pos =
            std::find_if(indexes.begin(), indexes.end(),
                         [&tg, &tg_offset](
                             const std::unique_ptr<storage::Tuple> &arg) {
                           return arg->GetValue(0)
                               .CompareEquals(tg->GetValue(tg_offset, 0));
                         });
        if (pos != indexes.end()) indexes.erase(pos);
      }
    }
  }
  auto tuple_id = tg->DeleteTupleFromRecovery(current_cid, tg_offset);
  table->IncreaseTupleCount(1);
  if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
    if (table->GetTileGroupById(tg->GetTileGroupId() + 1).get() ==
        nullptr)
      table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId() + 1);
    catalog::Manager::GetInstance().GetNextTileGroupId();
  }*/
}

void WalSecondaryReplay::ReplayUpdate(LogRecord &log_record, oid_t database_id, oid_t table_id, oid_t old_tg_block ,oid_t tg_block, old_tg_offset ,oid_t tg_offset){
  
  (void) log_record;
  (void) database_id;
  (void) table_id;
  (void) tg_block;
  (void) tg_offset;

  /*
  cid_t current_cid = log_record.GetCommitId();
  cid_t current_eid = log_record.GetEpochId();

  // Get item pointer , place where to insert the tuple
  ItemPointer location(tg_block, tg_offset);

  auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
            database_id, table_id);
  auto old_tg = table->GetTileGroupById(old_tg_block);
  auto tg = table->GetTileGroupById(tg_block);

  if (tg.get() == nullptr) {
    table->AddTileGroupWithOidForRecovery(tg_block);
    tg = table->GetTileGroupById(tg_block);
  }

  // XXX: We still rely on an alive catalog manager
  auto schema = table->GetSchema();
  
  // Decode the tuple from the record
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  for (oid_t oid = 0; oid < schema->GetColumns().size(); oid++) {
    type::Value val = type::Value::DeserializeFrom(
        record_decode, schema->GetColumn(oid).GetType());
    tuple->SetValue(oid, val, pool.get());
  }
  auto tuple_id =
      tg->InsertTupleFromRecovery(current_cid, tg_offset, tuple.get());
  old_tg->UpdateTupleFromRecovery(current_cid, old_tg_offset, location);
  table->IncreaseTupleCount(1);
  if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
    if (table->GetTileGroupById(tg->GetTileGroupId() + 1).get() ==
        nullptr)
      table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId() + 1);
    catalog::Manager::GetInstance().GetNextTileGroupId();
  }*/

}

void WalSecondaryReplay::RunServer(){
  std::cout<<" Runing server:  "<<std::endl;
  /*
  std::string server_address("0.0.0.0:15821");
  RouteGuideImpl service();

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();*/
}

void WalSecondaryReplay::RunReplayThread(){
  replay_thread_ = new std::thread(&WalSecondaryReplay::RunServer,this);
}


}
}
