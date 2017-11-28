//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_log_replicator.h
//
// Identification: src/include/logging/wal_log_replicator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/asio.hpp>
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "type/serializeio.h"
#include "type/types.h"
#include "common/logger.h"



namespace peloton{
namespace logging {

class WalReplicator{

  private:


    // TODO : maintain a vector of endpoints
    // vector<pair<string, string>> endpoints;

  public:

    WalReplicator() {}
    ~WalReplicator() {}

    void ReplicateTransactionSync(std::vector<LogRecord> log_records);
    void ReplicateTransactionAsync(std::vector<LogRecord> log_records);



};

}
}
