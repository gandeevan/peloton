//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_plan.h
//
// Identification: src/include/planner/drop_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {
namespace storage {
class DataTable;
}
namespace parser {
class AlterStatement;
}
namespace catalog {
class Schema;
}

namespace planner {

class AlterPlan : public AbstractPlan {
 public:
  AlterPlan() = delete;

  explicit AlterPlan(storage::DataTable *table);

//  explicit AlterPlan(std::string name);

  explicit AlterPlan(parser::AlterStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::ALTER; }

  const std::string GetInfo() const {
    std::string returned_string = "AlterPlan:\n";
    returned_string += "\tTable name: " + table_name + "\n";
    return returned_string;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new AlterPlan(target_table_));
  }

  std::string GetTableName() const { return table_name; }

  bool IsMissing() const { return missing; }

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;
  std::string table_name;
  bool missing;

 private:
  DISALLOW_COPY_AND_MOVE(AlterPlan);
};

}  // namespace planner
}  // namespace peloton
