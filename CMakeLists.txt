cmake_minimum_required(VERSION 3.22)
project(financial_pipeline CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)

# Dependencies provided by Conan
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)

# Protobuf & gRPC code generation
set(PROTO_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_FILE "${PROTO_SRC_DIR}/financial_pipeline.proto")
set(PROTO_SRCS "${PROTO_GEN_DIR}/financial_pipeline.pb.cc")
set(PROTO_HDRS "${PROTO_GEN_DIR}/financial_pipeline.pb.h")
set(GRPC_SRCS "${PROTO_GEN_DIR}/financial_pipeline.grpc.pb.cc")
set(GRPC_HDRS "${PROTO_GEN_DIR}/financial_pipeline.grpc.pb.h")

# Determine protoc and grpc plugin paths
if(TARGET protobuf::protoc)
    set(PROTOC_EXEC $<TARGET_FILE:protobuf::protoc>)
else()
    find_program(PROTOC_EXEC protoc REQUIRED)
endif()

if(TARGET gRPC::grpc_cpp_plugin)
    set(GRPC_CPP_PLUGIN_EXEC $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
    find_program(GRPC_CPP_PLUGIN_EXEC grpc_cpp_plugin REQUIRED)
endif()

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${PROTOC_EXEC}
        --proto_path=${PROTO_SRC_DIR}
        --cpp_out=${PROTO_GEN_DIR}
        --grpc_out=${PROTO_GEN_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXEC}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Compiling Protocol Buffer: financial_pipeline.proto"
    VERBATIM
)

# Generated Proto Library
add_library(financial_pipeline_proto STATIC
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${GRPC_SRCS}
    ${GRPC_HDRS}
)
target_include_directories(financial_pipeline_proto PUBLIC
    ${PROTO_GEN_DIR}
)
target_link_libraries(financial_pipeline_proto PUBLIC
    gRPC::grpc++
    protobuf::libprotobuf
)

# Core Pipeline Library
set(CORE_SOURCES
    src/common/checksum.cpp
    src/domain/validator.cpp
    src/domain/session.cpp
    src/wire/wire_mapper.cpp
    src/metrics/metrics_collector.cpp
    src/transport/channel_config.cpp
    src/transport/server.cpp
    src/transport/client.cpp
    src/transport/service_impl.cpp
    src/pipeline/data_generator.cpp
    src/pipeline/receiver.cpp
    src/pipeline/streamer.cpp
)

add_library(financial_pipeline_core STATIC
    ${CORE_SOURCES}
)
target_include_directories(financial_pipeline_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PROTO_GEN_DIR}
)
target_link_libraries(financial_pipeline_core PUBLIC
    financial_pipeline_proto
    gRPC::grpc++
    protobuf::libprotobuf
)

if(WIN32)
    target_link_libraries(financial_pipeline_core PUBLIC psapi)
endif()

# Server Application
add_executable(financial_pipeline_server
    src/server_main.cpp
)
target_link_libraries(financial_pipeline_server PRIVATE
    financial_pipeline_core
)

# Client Application
add_executable(financial_pipeline_client
    src/client_main.cpp
)
target_link_libraries(financial_pipeline_client PRIVATE
    financial_pipeline_core
)

# Benchmark Suite
add_executable(financial_pipeline_benchmark
    benchmarks/benchmark_pipeline.cpp
)
target_link_libraries(financial_pipeline_benchmark PRIVATE
    financial_pipeline_core
)

# Test Suite
enable_testing()
add_executable(financial_pipeline_tests
    tests/unit/test_checksum.cpp
    tests/unit/test_trade_record.cpp
    tests/unit/test_wire_mapper.cpp
    tests/unit/test_session.cpp
    tests/integration/test_pipeline_streaming.cpp
)
target_link_libraries(financial_pipeline_tests PRIVATE
    financial_pipeline_core
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(financial_pipeline_tests)
