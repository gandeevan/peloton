//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_log_replicator.cpp
//
// Identification: src/logging/wal_log_replicator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <string>
#include <boost/asio.hpp>


#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"

#include "logging/wal_logger.h"
#include "logging/wal_log_manager.h"
#include "logging/wal_log_replicator.h"


//TODO : maintain a vector of secondary servers
#define SECONDARY_HOST "127.0.0.1"
#define SECONDARY_PORT "15712"

namespace peloton{
namespace logging{

void WalReplicator::ReplicateTransactionAsync(std::vector<LogRecord> log_records){

    try{
        (void) log_records;
        boost::asio::io_service aios;
        boost::asio::ip::tcp::resolver resolver(aios);

        boost::asio::ip::tcp::resolver::iterator endpoint = resolver.resolve(
                boost::asio::ip::tcp::resolver::query(SECONDARY_HOST, SECONDARY_PORT));

        time_t now = time(0);
        std::string message = ctime(&now);

        boost::asio::ip::tcp::socket socket(aios);
        boost::asio::connect(socket, endpoint);

        boost::asio::write(socket, boost::asio::buffer(message));

    }

    catch(std::exception& e){
        LOG_ERROR("WAL replication failed: %s", e.what());
    }

}

void WalReplicator::ReplicateTransactionSync(std::vector<LogRecord> log_records){
    (void) log_records;
    LOG_DEBUG("Synchronous replication not implemented");
}








}
}