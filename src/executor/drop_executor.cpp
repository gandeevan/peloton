//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_executor.cpp
//
// Identification: src/executor/drop_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/drop_executor.h"

#include "catalog/catalog.h"
#include "catalog/trigger_catalog.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "executor/executor_context.h"
#include "planner/drop_plan.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/storage_manager.h"


namespace peloton {
namespace executor {

// Constructor for drop executor
DropExecutor::DropExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {
  context_ = executor_context;
}

// Initialize executer
// Nothing to initialize for now
bool DropExecutor::DInit() {
  LOG_TRACE("Initializing Drop Executer...");

  LOG_TRACE("Create Executer initialized!");
  return true;
}

bool DropExecutor::DExecute() {
  LOG_TRACE("Executing Drop...");
  auto storage_manager = storage::StorageManager::GetInstance();
  auto database = storage_manager->GetDatabaseWithOid(16777316);

  for (oid_t i = 0; i < database->GetTableCount(); i++){
      auto table = database->GetTable(i);
      LOG_DEBUG("Table: %s, iterating through tilegroups", table->GetName().c_str());
      for (oid_t j = 0; j < table->GetTileGroupCount(); j++){
          auto tile_group = table->GetTileGroup(j);
          LOG_DEBUG("Tilegroup %d: %d active tuples of %d total tuples", j, tile_group->GetActiveTupleCount(), tile_group->GetAllocatedTupleCount());
      }
  }
/*
  const planner::DropPlan &node = GetPlanNode<planner::DropPlan>();
  DropType dropType = node.GetDropType();
  switch (dropType) {
    case DropType::TABLE: {
      auto table_name = node.GetTableName();
      auto current_txn = context_->GetTransaction();

      if (node.IsMissing()) {
        try {
          auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
              DEFAULT_DB_NAME, table_name, current_txn);
        } catch (CatalogException &e) {
          LOG_TRACE("Table %s does not exist.", table_name.c_str());
          return false;
        }
      }

      ResultType result = catalog::Catalog::GetInstance()->DropTable(
          DEFAULT_DB_NAME, table_name, current_txn);
      current_txn->SetResult(result);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping table succeeded!");
      } else {
        LOG_TRACE("Result is: %s",
                  ResultTypeToString(current_txn->GetResult()).c_str());
      }
      break;
    }
    case DropType::TRIGGER: {
      std::string table_name = node.GetTableName();
      std::string trigger_name = node.GetTriggerName();

      auto current_txn = context_->GetTransaction();

      ResultType result = catalog::TriggerCatalog::GetInstance()->DropTrigger(
          DEFAULT_DB_NAME, table_name, trigger_name, current_txn);
      current_txn->SetResult(result);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping trigger succeeded!");
      } else if (current_txn->GetResult() == ResultType::FAILURE &&
                 node.IsMissing()) {
        current_txn->SetResult(ResultType::SUCCESS);
        LOG_TRACE("Dropping trigger Succeeded!");
      } else if (current_txn->GetResult() == ResultType::FAILURE &&
                 !node.IsMissing()) {
        LOG_TRACE("Dropping trigger Failed!");
      } else {
        LOG_TRACE("Result is: %s",
                  ResultTypeToString(current_txn->GetResult()).c_str());
      }
      break;
    }
    default: {
      throw NotImplementedException(
          StringUtil::Format("Drop type %d not supported yet.\n", dropType));
    }
  }
  */

  return false;
}

}  // namespace executor
}  // namespace peloton
