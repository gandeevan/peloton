//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_secondary_replay.cpp
//
// Identification: src/logging/wal_secondary_replay.cpp
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
#include <thread>
#include "logging/wal_secondary_replay.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index_factory.h"

#include "catalog/catalog_defaults.h"
#include "catalog/column.h"
#include "catalog/manager.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/database_catalog.h"
#include "concurrency/epoch_manager_factory.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/tile_group_header.h"
#include "type/ephemeral_pool.h"
#include "storage/storage_manager.h"
#include "logging/log_record.h"
#include "logging/wal_recovery.h"

namespace peloton {
namespace logging {

void WalSecondaryReplay::RunServer(){
  while (1) {
    LOG_INFO("Runing Secondary Replay server!!!");
    sleep(1);
  }
  /*
  std::string server_address("0.0.0.0:15821");
  RouteGuideImpl service();

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();*/
}

void WalSecondaryReplay::RunReplayThread(){
  replay_thread_ = new std::thread(WalSecondaryReplay::RunServer);
}


}
}
