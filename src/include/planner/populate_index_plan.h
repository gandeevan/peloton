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

  PopulateIndexPlan();

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::INSERT; }

  const std::string GetInfo() const { return "PopulateIndex"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new PopulateIndexPlan());
  }

 private:
  

};
}
}

