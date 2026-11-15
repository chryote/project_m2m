#pragma once

#include <grpcpp/grpcpp.h>
#include <string>
#include <cstdint>

namespace financial_pipeline {
namespace transport {

struct GrpcConfig {
    std::string host{"127.0.0.1"};
    int port{50051};
    int max_message_size_bytes{32 * 1024 * 1024}; // 32 MB
    int keepalive_time_ms{30000};
    int keepalive_timeout_ms{10000};
    bool enable_bdp_probe{true};
    bool permit_keepalive_without_calls{true};
    size_t chunk_size_bytes{2 * 1024 * 1024};      // 2 MB chunking default

    std::string address() const {
        return host + ":" + std::to_string(port);
    }
};

/**
 * @brief Factory and Builder for tuned gRPC Channel and Server arguments.
 */
class ChannelConfigBuilder {
public:
    static grpc::ChannelArguments build_channel_args(const GrpcConfig& config);
    static void apply_server_builder_options(grpc::ServerBuilder& builder, const GrpcConfig& config);
};

} // namespace transport
} // namespace financial_pipeline
