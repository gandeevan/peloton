

#pragma once

#include <fstream>
#include "logging/wal_recovery.h"


namespace peloton{
namespace logging{

class LogManager{
public:

  LogManager() {}
  ~LogManager() {}

  //TODO(graghura): refactor using log_util.h
  inline bool init(std::string logging_dir) {
    directory_ = logging_dir;

    if(!logger_ofstream_.is_open()){
      logger_ofstream_.open(logging_dir+"/log", std::ofstream::out | std::ofstream::app);

      if(!logger_ofstream_.fail()) {
        enable_logging_ = true;
        return true;
      }
    }
    enable_logging_ = false;
    return false;
  }

  inline static LogManager &GetInstance(){
    static LogManager log_manager;
    return log_manager;
  }

  inline void DoRecovery(){
    WalRecovery* wr = new WalRecovery();
    wr->StartRecovery();
    delete wr;
  }

  inline static size_t GetTransactionBufferSize() { return transaction_buffer_threshold_; }
  inline static size_t GetLoggerBufferSize() { return logger_buffer_threshold_; }

  inline bool IsLoggingEnabled() { return enable_logging_; }
  inline std::ofstream *GetFileStream() { return &logger_ofstream_; }
//  inline std::string GetDirectory() { return directory_; }
//  inline std::string GetLogFilePath() { return directory_+"/tmp"; }

//  TODO(gandeevan): support StartLogging and StopLogging
//  static void StartLogging() {}
//  static void StopLogging() {}


private:

  const static size_t logger_buffer_threshold_ = 1024 * 512;  // 512 KB
  const static size_t transaction_buffer_threshold_ = 1024 * 16; // 16 KB

        // NOTE: ofstream is not thread safe, might need to change it if more than one logger thread is used
  bool enable_logging_;
  std::string directory_;
  std::ofstream logger_ofstream_;


};

}
}