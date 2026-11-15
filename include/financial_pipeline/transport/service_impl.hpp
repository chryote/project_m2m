#pragma once

#include "financial_pipeline.grpc.pb.h"
#include "financial_pipeline/pipeline/receiver.hpp"
#include "financial_pipeline/domain/session.hpp"
#include <memory>
#include <chrono>

namespace financial_pipeline {
namespace transport {

/**
 * @brief gRPC Service implementation delegating streaming pipelines and queries to domain/pipeline services.
 */
class FinancialPipelineServiceImpl final : public financial_pipeline::FinancialDataPipeline::Service {
public:
    explicit FinancialPipelineServiceImpl(
        std::shared_ptr<pipeline::FinancialDataReceiver> receiver,
        std::shared_ptr<domain::SessionManager> session_mgr);

    grpc::Status StreamFinancialData(
        grpc::ServerContext* context,
        grpc::ServerReader<financial_pipeline::FinancialDataChunk>* reader,
        financial_pipeline::StreamSummary* response) override;

    grpc::Status StreamWithFlowControl(
        grpc::ServerContext* context,
        grpc::ServerReaderWriter<financial_pipeline::StreamAck, financial_pipeline::FinancialDataChunk>* stream) override;

    grpc::Status QuerySessionStatus(
        grpc::ServerContext* context,
        const financial_pipeline::SessionQuery* request,
        financial_pipeline::SessionStatusResponse* response) override;

    grpc::Status HealthCheck(
        grpc::ServerContext* context,
        const financial_pipeline::HealthCheckRequest* request,
        financial_pipeline::HealthCheckResponse* response) override;

private:
    std::shared_ptr<pipeline::FinancialDataReceiver> receiver_;
    std::shared_ptr<domain::SessionManager> session_mgr_;
    std::chrono::steady_clock::time_point startup_time_;
};

} // namespace transport
} // namespace financial_pipeline
