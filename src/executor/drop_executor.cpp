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
#include "eviction/evicter.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"

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
  const planner::DropPlan &node UNUSED_ATTRIBUTE = GetPlanNode<planner::DropPlan>();
  //DropType dropType = node.GetDropType();
 // switch (dropType) {
 //   case DropType::TABLE: {
      auto table_name UNUSED_ATTRIBUTE= node.GetTableName();
      auto current_txn UNUSED_ATTRIBUTE= context_->GetTransaction();


          auto table_object UNUSED_ATTRIBUTE = catalog::Catalog::GetInstance()->GetTableWithName(
              DEFAULT_DB_NAME, table_name, current_txn);
          auto evicter = new eviction::Evicter();

        for (int i = 1; i<50; i++)
        table_object->GetTileGroup(i)->GetHeader()->SetEvictable(true);
        evicter->EvictDataFromTable(table_object);
      delete evicter;
      current_txn->SetResult(ResultType::SUCCESS);

      if (current_txn->GetResult() == ResultType::SUCCESS) {
        LOG_TRACE("Dropping table succeeded!");
      } else {
        LOG_TRACE("Result is: %s",
                  ResultTypeToString(current_txn->GetResult()).c_str());
      }

    /*case DropType::TRIGGER: {
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
