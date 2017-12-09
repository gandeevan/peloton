find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin) # Get full path to plugin

find_library(GRPC_LIBRARY NAMES grpc)
find_library(GRPCPP_LIBRARY NAMES grpc++)
find_library(GPR_LIBRARY NAMES gpr)
set(GRPC_LIBRARIES ${GRPCPP_LIBRARY} ${GRPC_LIBRARY} ${GPR_LIBRARY})

if(GRPC_LIBRARIES)
    message(STATUS "Found GRPC: ${GRPC_LIBRARIES}; plugin - ${GRPC_CPP_PLUGIN}")
endif()
