#include "financial_pipeline/transport/server.hpp"
#include <chrono>

namespace financial_pipeline {
namespace transport {

PipelineServer::PipelineServer(
    GrpcConfig config,
    std::shared_ptr<FinancialPipelineServiceImpl> service)
    : config_(std::move(config)), service_(std::move(service)) {}

PipelineServer::~PipelineServer() {
    stop();
}

common::PipelineStatus PipelineServer::start() {
    if (is_running_.load()) {
        return common::PipelineStatus::ok();
    }

    if (!service_) {
        return common::PipelineStatus::invalid_argument("Service implementation cannot be null");
    }

    grpc::ServerBuilder builder;
    ChannelConfigBuilder::apply_server_builder_options(builder, config_);

    std::string server_address = config_.address();
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &selected_port_);
    builder.RegisterService(service_.get());

    server_ = builder.BuildAndStart();
    if (!server_) {
        return common::PipelineStatus::network_error("Failed to bind and start gRPC server on " + server_address);
    }

    is_running_.store(true);
    return common::PipelineStatus::ok();
}

void PipelineServer::wait() {
    if (server_) {
        server_->Wait();
    }
}

void PipelineServer::stop(int timeout_ms) {
    if (!is_running_.exchange(false)) {
        return;
    }

    if (server_) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms);
        server_->Shutdown(deadline);
        server_.reset();
    }
}

} // namespace transport
} // namespace financial_pipeline
