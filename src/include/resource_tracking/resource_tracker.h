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


struct sysinfo memInfo;


class ResourceTracker {
public:
  ResourceTracker() {} 
   
  virtual ~ResourceTracker() { }

  // this function cleans up all the member variables in the class object.

  static ResourceTracker& GetInstance() {
    static ResourceTracker resource_tracker;
     return resource_tracker;
  }

   void StartRT()  {
    LOG_TRACE("Starting Resource Tracker");
    this->is_running_ = true;
      thread_pool.SubmitDedicatedTask(&ResourceTracker::Running, this);
    
  };

   void StopRT() {
    
    this->is_running_ = false;
  }

bool IsThreshold() {
  return false; 
}

private:


  
  void Running() {
     sysinfo (&memInfo);
     total_memory_ = memInfo.totalram;
     total_memory_ *= memInfo.mem_unit;
     while(is_running_) {
      std::this_thread::sleep_for(std::chrono::microseconds(60000000));
      used_memory_ = GetValue();
      LOG_DEBUG("Used memory: %lld",used_memory_);
      LOG_DEBUG("Total memory: %lld",total_memory_);
       
    }
  }

int GetValue(){ //Note: this value is in KB!
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = ParseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

int ParseLine(char* line){
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}
  
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

