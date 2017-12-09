//
// Created by Gandeevan R on 11/29/17.
//

#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>

#include "peloton/proto/wal_service.pb.h"
#include "peloton/proto/wal_service.grpc.pb.h"


using grpc::Status;
using grpc::ServerContext;


namespace peloton{
namespace logging{

class WalReplicatorService final : public WalReplicator::Service{
public:
    WalReplicatorService() {}
    ~WalReplicatorService() {}

    Status ReplayTransaction(ServerContext* context, const LogReplayRequest* request, LogReplayResponse* response);
};

class WalReplicatorServer {

  public:
    WalReplicatorServer()
      : port_num_(0) {}

    WalReplicatorServer(int port_num)
      : port_num_(port_num) {}

    ~WalReplicatorServer() {}

    std::string GetServerAddress(){
      return std::string("0.0.0.0:")+std::to_string(port_num_);
    }

    void RunServer();

  private:
    int port_num_;
};

}
}