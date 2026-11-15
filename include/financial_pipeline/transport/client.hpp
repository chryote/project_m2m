#pragma once

#include "financial_pipeline/transport/channel_config.hpp"
#include "financial_pipeline/pipeline/streamer.hpp"
#include "financial_pipeline/common/status.hpp"
#include "financial_pipeline.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace financial_pipeline {
namespace transport {

/**
 * @brief High-throughput M2M client managing channel lifecycle, health checks, and streaming stubs.
 */
class PipelineClient {
public:
    explicit PipelineClient(GrpcConfig config);

    /// Connect to remote gRPC server endpoint
    common::PipelineStatus connect();

    /// Check if target server is healthy and ready
    common::Result<financial_pipeline::HealthCheckResponse> check_health(
        const std::string& client_id = "client-m2m",
        int timeout_ms = 5000);

    /// Query the status of a specific session on the server
    common::Result<financial_pipeline::SessionStatusResponse> query_session(
        const std::string& session_id,
        int timeout_ms = 5000);

    /// Obtain raw stub for streaming calls
    std::shared_ptr<financial_pipeline::FinancialDataPipeline::Stub> stub() const noexcept {
        return stub_;
    }

    /// Factory method to create a streamer bound to this client
    std::unique_ptr<pipeline::FinancialDataStreamer> create_streamer();

    bool is_connected() const noexcept { return stub_ != nullptr; }
    const GrpcConfig& config() const noexcept { return config_; }

private:
    GrpcConfig config_;
    std::shared_ptr<grpc::Channel> channel_;
    std::shared_ptr<financial_pipeline::FinancialDataPipeline::Stub> stub_;
};

} // namespace transport
} // namespace financial_pipeline
