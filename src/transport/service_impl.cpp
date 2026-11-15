#include "financial_pipeline/transport/service_impl.hpp"
#include "financial_pipeline/wire/wire_mapper.hpp"

namespace financial_pipeline {
namespace transport {

FinancialPipelineServiceImpl::FinancialPipelineServiceImpl(
    std::shared_ptr<pipeline::FinancialDataReceiver> receiver,
    std::shared_ptr<domain::SessionManager> session_mgr)
    : receiver_(std::move(receiver)),
      session_mgr_(std::move(session_mgr)),
      startup_time_(std::chrono::steady_clock::now()) {}

grpc::Status FinancialPipelineServiceImpl::StreamFinancialData(
    grpc::ServerContext* context,
    grpc::ServerReader<financial_pipeline::FinancialDataChunk>* reader,
    financial_pipeline::StreamSummary* response) {
    if (!reader || !response) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Null reader or response");
    }

    financial_pipeline::FinancialDataChunk chunk;
    std::string session_id;
    bool is_first = true;

    while (reader->Read(&chunk)) {
        if (context->IsCancelled()) {
            receiver_->fail_stream(session_id, common::PipelineStatus::cancelled("RPC cancelled by client"), response);
            return grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled stream");
        }

        if (is_first) {
            session_id = chunk.session_id();
            is_first = false;
        }

        auto status = receiver_->process_chunk(chunk);
        if (!status.is_ok()) {
            receiver_->fail_stream(session_id, status, response);
            return grpc::Status(grpc::StatusCode::DATA_LOSS, status.to_string());
        }
    }

    receiver_->complete_stream(session_id, response);
    return grpc::Status::OK;
}

grpc::Status FinancialPipelineServiceImpl::StreamWithFlowControl(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<financial_pipeline::StreamAck, financial_pipeline::FinancialDataChunk>* stream) {
    if (!stream) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Null stream");
    }

    financial_pipeline::FinancialDataChunk chunk;
    std::string session_id;
    uint64_t chunks_since_ack = 0;
    constexpr uint64_t ack_interval = 10; // Send ACK every 10 chunks

    while (stream->Read(&chunk)) {
        if (context->IsCancelled()) {
            return grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled stream");
        }

        session_id = chunk.session_id();
        auto status = receiver_->process_chunk(chunk);
        if (!status.is_ok()) {
            financial_pipeline::StreamAck nack;
            nack.set_session_id(session_id);
            nack.set_acknowledged_chunk_index(chunk.chunk_index());
            nack.set_can_continue(false);
            nack.set_status_message(status.to_string());
            stream->Write(nack);
            return grpc::Status(grpc::StatusCode::DATA_LOSS, status.to_string());
        }

        if (chunk.request_ack() || chunk.is_last_chunk()) {
            auto session = session_mgr_->find(session_id);
            auto stats = session ? session->get_statistics() : domain::SessionStatistics{};

            financial_pipeline::StreamAck ack;
            ack.set_session_id(session_id);
            ack.set_acknowledged_chunk_index(chunk.chunk_index());
            ack.set_records_received_so_far(stats.total_records);
            ack.set_bytes_received_so_far(stats.total_bytes);
            ack.set_can_continue(true);
            ack.set_status_message("OK");
            stream->Write(ack);
        }

        if (chunk.is_last_chunk()) {
            financial_pipeline::StreamSummary dummy_summary;
            receiver_->complete_stream(session_id, &dummy_summary);
            break;
        }
    }

    return grpc::Status::OK;
}

grpc::Status FinancialPipelineServiceImpl::QuerySessionStatus(
    grpc::ServerContext* /*context*/,
    const financial_pipeline::SessionQuery* request,
    financial_pipeline::SessionStatusResponse* response) {
    if (!request || !response) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Null query or response");
    }

    auto session = session_mgr_->find(request->session_id());
    if (!session) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Session not found: " + request->session_id());
    }

    auto stats = session->get_statistics();
    response->set_session_id(stats.session_id);
    response->set_status(wire::WireMapper::to_proto_status(stats.final_status.code()));
    response->set_chunks_processed(stats.total_chunks);
    response->set_records_processed(stats.total_records);
    response->set_bytes_processed(stats.total_bytes);
    response->set_is_active(stats.state == domain::SessionState::Active);
    response->set_elapsed_seconds(stats.elapsed_seconds());

    return grpc::Status::OK;
}

grpc::Status FinancialPipelineServiceImpl::HealthCheck(
    grpc::ServerContext* /*context*/,
    const financial_pipeline::HealthCheckRequest* /*request*/,
    financial_pipeline::HealthCheckResponse* response) {
    if (!response) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Null response");
    }

    response->set_healthy(true);
    response->set_version("1.0.0");
    auto now = std::chrono::steady_clock::now();
    auto uptime_sec = std::chrono::duration_cast<std::chrono::seconds>(now - startup_time_).count();
    response->set_uptime_seconds(static_cast<uint64_t>(uptime_sec));
    response->set_active_sessions(static_cast<uint32_t>(session_mgr_->active_count()));

    return grpc::Status::OK;
}

} // namespace transport
} // namespace financial_pipeline
