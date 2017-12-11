//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// replay_queue_pool.h
//
// Identification: src/include/threadpool/logger_queue_pool.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "task_queue.h"
#include "worker_pool.h"

// TODO: tune these variables
#define DEFAULT_LOGGER_POOL_SIZE 1
#define DEFAULT_TASK_QUEUE_SIZE 2000

namespace peloton {
namespace threadpool {
/**
 * @class LoggerQueuePool
 * @brief Wrapper class for single queue and single pool
 * One should use this if possible.
 */
class ReplayQueuePool {
 public:
  ReplayQueuePool() : task_queue_(DEFAULT_TASK_QUEUE_SIZE),
                    logger_pool_(DEFAULT_LOGGER_POOL_SIZE, &task_queue_),
                    startup_(false) {}
  ~ReplayQueuePool() {
    if (startup_ == true)
      Shutdown();
  }

  void Startup() {
    logger_pool_.Startup();
    startup_ = true;
  }

  void Shutdown() {
    logger_pool_.Shutdown();
    startup_ = false;
  }

  void SubmitTask(void (*task_ptr)(void *), void *task_arg,
                  void (*callback_ptr)(void *), void *callback_arg) {
    if (startup_ == false)
      Startup();
    task_queue_.Enqueue(task_ptr, task_arg, callback_ptr, callback_arg);
  }

  static ReplayQueuePool &GetInstance() {
    static ReplayQueuePool replay_queue_pool;
    return replay_queue_pool;
  }

 private:
  TaskQueue task_queue_;
  WorkerPool logger_pool_;
  bool startup_;
};

} // namespace threadpool
} // namespace peloton
