//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager_factory.h
//
// Identification: src/include/gc/gc_manager_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "ltm/ltm_manager.h"
#include "ltm/transaction_level_ltm_manager.h"


namespace peloton {
namespace ltm {

class LTMManagerFactory {
 public:
  static LTMManager &GetInstance() {
    switch (ltm_type_) {

      case LTMType::ON:
        return TransactionLevelLTMManager::GetInstance(ltm_thread_count_);

      default:
        return LTMManager::GetInstance();
    }
  }

  static void Configure(const int thread_count = 1) { 
    if (thread_count == 0) {
      ltm_type_ = LTMType::OFF;
    } else {
      ltm_type_ = LTMType::ON;
      ltm_thread_count_ = thread_count;
    }
  }

  static LTMType GetLTMType() { return ltm_type_; }

 private:
  // GC type
  static LTMType ltm_type_;

  static int ltm_thread_count_;
};

}  // namespace gc
}  // namespace peloton
