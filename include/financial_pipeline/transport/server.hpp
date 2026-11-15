#pragma once

#include "financial_pipeline/transport/channel_config.hpp"
#include "financial_pipeline/transport/service_impl.hpp"
#include "financial_pipeline/common/status.hpp"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>
#include <atomic>

namespace financial_pipeline {
namespace transport {

/**
 * @brief Manages lifecycle of the gRPC high-throughput pipeline server.
 */
class PipelineServer {
public:
    explicit PipelineServer(
        GrpcConfig config,
        std::shared_ptr<FinancialPipelineServiceImpl> service);

    ~PipelineServer();

    // Prevent copying
    PipelineServer(const PipelineServer&) = delete;
    PipelineServer& operator=(const PipelineServer&) = delete;

    /// Start the server listening on the configured port
    common::PipelineStatus start();

    /// Block calling thread until server shuts down
    void wait();

    /// Graceful shutdown with deadline in milliseconds
    void stop(int timeout_ms = 5000);

    bool is_running() const noexcept { return is_running_.load(); }
    int bound_port() const noexcept { return selected_port_; }
    const GrpcConfig& config() const noexcept { return config_; }

private:
    GrpcConfig config_;
    std::shared_ptr<FinancialPipelineServiceImpl> service_;
    std::unique_ptr<grpc::Server> server_;
    int selected_port_{0};
    std::atomic<bool> is_running_{false};
};

} // namespace transport
} // namespace financial_pipeline
