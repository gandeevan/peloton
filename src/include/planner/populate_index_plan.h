//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_plan.h
//
// Identification: src/include/planner/order_by_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {

namespace storage {
class DataTable;
//class Tuple;
}

namespace planner {

/**
 * IMPORTANT All tiles got from child must have the same physical schema.
 */

class PopulateIndexPlan : public AbstractPlan {
 public:
  PopulateIndexPlan(const PopulateIndexPlan &) = delete;
  PopulateIndexPlan &operator=(const PopulateIndexPlan &) = delete;
  PopulateIndexPlan(const PopulateIndexPlan &&) = delete;
  PopulateIndexPlan &operator=(const PopulateIndexPlan &&) = delete;

  explicit PopulateIndexPlan(storage::DataTable *table, std::vector<oid_t> column_ids);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::POPULATE_INDEX; }

  inline const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const std::string GetInfo() const { return "PopulateIndex"; }

  storage::DataTable *GetTable() const { return target_table_; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new PopulateIndexPlan(target_table_, column_ids_));
  }

 private:
   /** @brief Target table. */
  storage::DataTable *target_table_ = nullptr;
  std::vector<oid_t> column_ids_;
  /** @brief Tuple */
//  std::vector<std::unique_ptr<storage::Tuple>> tuples_;

  // <tuple_index, tuple_column_index, parameter_index>
 // std::unique_ptr<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
  //    parameter_vector_;

  // Parameter values
  //std::unique_ptr<std::vector<type::Type::TypeId>> params_value_type_;

};
}
}

