#include "ltm/resource_tracker.h"

namespace peloton {
namespace ltm {

void ResourceTracker::StartRT()  {
 LOG_TRACE("Starting Resource Tracker");
 this->is_running_ = true;
   thread_pool.SubmitDedicatedTask(&ResourceTracker::Running, this);

}

void ResourceTracker::StopRT() {

 this->is_running_ = false;
}

void ResourceTracker::Running() {
   sysinfo (&memInfo);
   total_memory_ = memInfo.totalram;
   total_memory_ *= memInfo.mem_unit;
   while(is_running_) {
    std::this_thread::sleep_for(std::chrono::microseconds(60000000));
    used_memory_ = GetValue() * 1024;
    LOG_DEBUG("Used memory: %lld",used_memory_);
    LOG_DEBUG("Total memory: %lld",total_memory_);

  }
}

int ResourceTracker::GetValue(){ //Note: this value is in KB!
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

int ResourceTracker::ParseLine(char* line){
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}

}
}
