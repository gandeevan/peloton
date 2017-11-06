//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_level_gc_manager.h
//
// Identification: src/include/gc/transaction_level_gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>
#include <map>
#include <vector>
#include <list>

#include "sys/types.h"
#include "sys/sysinfo.h"

#include "type/types.h"
#include "common/logger.h"
#include "common/init.h"
#include "common/thread_pool.h"


namespace peloton {
namespace ltm {

class ResourceTracker {
public:
  ResourceTracker() {} 
   
  virtual ~ResourceTracker() { }

  // this function cleans up all the member variables in the class object.

  static ResourceTracker& GetInstance() {
    static ResourceTracker resource_tracker;
     return resource_tracker;
  }

   void StartRT();

   void StopRT();

bool IsThreshold() {
  return false; 
}

private:

struct sysinfo memInfo;
  
void Running();

int GetValue();

int ParseLine(char* line);

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

long long total_memory_;

long long used_memory_;

bool is_running_;

};
}
}

