//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_executor.cpp
//
// Identification: src/executor/hash_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <utility>
#include <vector>

#include "common/logger.h"
#include "type/value.h"
#include "executor/logical_tile.h"
#include "executor/populate_index_executor.h"
#include "planner/populate_index_plan.h"
#include "expression/tuple_value_expression.h"
#include "storage/data_table.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor
 */
PopulateIndexExecutor::PopulateIndexExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Do some basic checks and initialize executor state.
 * @return true on success, false otherwise.
 */
bool PopulateIndexExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);

  // Initialize executor state
  done_ = false;
  result_itr = 0;

  return true;
}

bool PopulateIndexExecutor::DExecute() {
  LOG_TRACE("Populate Index Executor");

  if (done_ == false) {
    const planner::PopulateIndexPlan &node = GetPlanNode<planner::PopulateIndexPlan>();

    // First, get all the input logical tiles
    while (children_[0]->Execute()) {
      child_tiles_.emplace_back(children_[0]->GetOutput());
    }

    if (child_tiles_.size() == 0) {
      LOG_TRACE("PopulateIndex Executor : false -- no child tiles ");
       return false;
    }




    // Construct the hash table by going over each child logical tile and
    // hashing
    for (size_t child_tile_itr = 0; child_tile_itr < child_tiles_.size();
         child_tile_itr++) {
      auto tile = child_tiles_[child_tile_itr].get();

      // Go over all tuples in the logical tile
      for (oid_t tuple_id : *tile) {
        storage::
        // Key : container tuple with a subset of tuple attributes
        // Value : < child_tile offset, tuple offset >
        auto key = HashMapType::key_type(tile, tuple_id, &column_ids_);
        if (hash_table_.find(key) != hash_table_.end()){
           //If data is already present, remove from output
           //but leave data for hash joins.
           tile->RemoveVisibility(tuple_id);
        }
        hash_table_[key].insert(
                    std::make_pair(child_tile_itr, tuple_id));
      }
    }

    done_ = true;
  }
/*
  // Return logical tiles one at a time
  while (result_itr < child_tiles_.size()) {
    if (child_tiles_[result_itr]->GetTupleCount() == 0) {
      result_itr++;
      continue;
    } else {
      SetOutput(child_tiles_[result_itr++].release());
      LOG_TRACE("Hash Executor : true -- return tile one at a time ");
      return true;
    }
  }
*/
  LOG_TRACE("Hash Executor : false -- done ");
  return false;
}

} /* namespace executor */
} /* namespace peloton */
