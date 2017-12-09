## Finds gRPC library
##set(gRPC_ROOT_DIR /home/vagrant/grpc/*)
#if (NOT __GLOG_INCLUDED)
#find_package(gRPC REQUIRED)
#include_directories(SYSTEM ${gRPC_INCLUDE_DIR})
#list(APPEND Peloton_LINKER_LIBS ${gRPC_LIBRARIES})

if (NOT __GRPC_INCLUDED)
    set(__GRPC_INCLUDED TRUE)

    # try the system-wide glog first
    find_package(grpc)

    if (GRPC_LIBRARIES)
        Message("DEBUG: GRPC FOUND")
        set(GLOG_EXTERNAL FALSE)
    else()
        Message("DEBUG: GRPC NOT FOUND")
    endif()

endif()