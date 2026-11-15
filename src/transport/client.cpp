#include "financial_pipeline/transport/client.hpp"
#include <chrono>

namespace financial_pipeline {
namespace transport {

PipelineClient::PipelineClient(GrpcConfig config)
    : config_(std::move(config)) {}

common::PipelineStatus PipelineClient::connect() {
    auto channel_args = ChannelConfigBuilder::build_channel_args(config_);
    channel_ = grpc::CreateCustomChannel(
        config_.address(),
        grpc::InsecureChannelCredentials(),
        channel_args);

    if (!channel_) {
        return common::PipelineStatus::network_error("Failed to create gRPC channel to " + config_.address());
    }

    stub_ = financial_pipeline::FinancialDataPipeline::NewStub(channel_);
    return common::PipelineStatus::ok();
}

common::Result<financial_pipeline::HealthCheckResponse> PipelineClient::check_health(
    const std::string& client_id,
    int timeout_ms) {
    if (!stub_) {
        return common::PipelineStatus::internal_error("Client not connected");
    }

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));

    financial_pipeline::HealthCheckRequest request;
    request.set_client_id(client_id);

    financial_pipeline::HealthCheckResponse response;
    grpc::Status status = stub_->HealthCheck(&context, request, &response);

    if (!status.ok()) {
        return common::PipelineStatus::network_error(
            "HealthCheck failed: " + status.error_message() + " (code " + std::to_string(status.error_code()) + ")");
    }

    return response;
}

common::Result<financial_pipeline::SessionStatusResponse> PipelineClient::query_session(
    const std::string& session_id,
    int timeout_ms) {
    if (!stub_) {
        return common::PipelineStatus::internal_error("Client not connected");
    }

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));

    financial_pipeline::SessionQuery request;
    request.set_session_id(session_id);

    financial_pipeline::SessionStatusResponse response;
    grpc::Status status = stub_->QuerySessionStatus(&context, request, &response);

    if (!status.ok()) {
        return common::PipelineStatus::network_error(
            "QuerySessionStatus failed: " + status.error_message());
    }

    return response;
}

std::unique_ptr<pipeline::FinancialDataStreamer> PipelineClient::create_streamer() {
    return std::make_unique<pipeline::FinancialDataStreamer>(stub_);
}

} // namespace transport
} // namespace financial_pipeline
