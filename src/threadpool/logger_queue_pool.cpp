#include "threadpool/logger_queue_pool.h"
#include "common/container/lock_free_queue.h"



namespace peloton{
namespace threadpool{

void LoggerFunc(std::atomic_bool *is_running, LoggerQueue *logger_queue) {
  constexpr auto kMinPauseTime = std::chrono::microseconds(1);
  constexpr auto kMaxPauseTime = std::chrono::microseconds(1000);
  auto pause_time = kMinPauseTime;

  logging::WalLogger logger;

  while (is_running->load() || !logger_queue->IsEmpty()) {

    logging::LogBuffer *log_buffer;

    if (!logger_queue->Dequeue(log_buffer)) {
      // Polling with exponential backoff
      std::this_thread::sleep_for(pause_time);
      pause_time = std::min(pause_time * 2, kMaxPauseTime);
    } else {
      LOG_DEBUG("Logger dequeued a log buffer %p", (void *) log_buffer);

      pause_time = kMinPauseTime;
    }
  }

}

}
}