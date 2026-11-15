#include "financial_pipeline/pipeline/receiver.hpp"
#include "financial_pipeline/common/checksum.hpp"
#include "financial_pipeline/wire/wire_mapper.hpp"

namespace financial_pipeline {
namespace pipeline {

FinancialDataReceiver::FinancialDataReceiver(
    std::shared_ptr<domain::SessionManager> session_mgr,
    std::shared_ptr<IDataSink> sink,
    std::shared_ptr<metrics::PipelineMetricsCollector> metrics)
    : session_mgr_(std::move(session_mgr)),
      sink_(std::move(sink)),
      metrics_(std::move(metrics)) {
    if (!session_mgr_) {
        session_mgr_ = std::make_shared<domain::SessionManager>();
    }
    if (!sink_) {
        sink_ = std::make_shared<NullDataSink>();
    }
    if (!metrics_) {
        metrics_ = std::make_shared<metrics::PipelineMetricsCollector>();
    }
}

common::PipelineStatus FinancialDataReceiver::process_chunk(const financial_pipeline::FinancialDataChunk& chunk) {
    const std::string& session_id = chunk.session_id();
    auto session = session_mgr_->get_or_create(session_id);

    // 1. Checksum integrity verification
    const std::string& payload = chunk.payload();
    uint32_t calculated_crc = common::Crc32cCalculator::calculate(payload.data(), payload.size());
    if (calculated_crc != chunk.chunk_checksum()) {
        metrics_->record_error();
        auto err = common::PipelineStatus::checksum_mismatch(
            "CRC32C mismatch for chunk " + std::to_string(chunk.chunk_index()) +
            ": expected " + std::to_string(chunk.chunk_checksum()) +
            ", got " + std::to_string(calculated_crc));
        session->fail(err);
        return err;
    }

    // 2. Session sequence continuity and rolling checksum update
    auto session_status = session->on_chunk_received(
        chunk.chunk_index(),
        chunk.record_count(),
        payload.size(),
        chunk.chunk_checksum());

    if (!session_status.is_ok()) {
        metrics_->record_error();
        return session_status;
    }

    // 3. Sink ingestion: deserialize and dispatch if non-null
    if (sink_) {
        auto batch_res = wire::WireMapper::deserialize_batch(payload);
        if (!batch_res.is_ok()) {
            metrics_->record_error();
            auto err = common::PipelineStatus::invalid_argument(
                "Failed to deserialize batch for chunk " + std::to_string(chunk.chunk_index()) +
                ": " + batch_res.status().message());
            session->fail(err);
            return err;
        }
        auto sink_status = sink_->on_batch(batch_res.value());
        if (!sink_status.is_ok()) {
            metrics_->record_error();
            session->fail(sink_status);
            return sink_status;
        }
    }

    // 4. Update metrics
    metrics_->record_chunk(payload.size(), chunk.record_count());

    return common::PipelineStatus::ok();
}

void FinancialDataReceiver::complete_stream(
    const std::string& session_id,
    financial_pipeline::StreamSummary* summary) {
    auto session = session_mgr_->find(session_id);
    if (session) {
        session->complete();
        auto stats = session->get_statistics();
        if (summary) {
            summary->set_session_id(session_id);
            summary->set_status(financial_pipeline::STATUS_SUCCESS);
            summary->set_total_chunks(stats.total_chunks);
            summary->set_total_records(stats.total_records);
            summary->set_total_bytes(stats.total_bytes);
            summary->set_composite_checksum(common::Crc32cCalculator::finalize(stats.composite_checksum));
            summary->set_duration_ms(stats.elapsed_seconds() * 1000.0);
            summary->set_throughput_mb_per_sec(stats.throughput_mb_per_sec());
            summary->set_records_per_sec(stats.records_per_sec());
        }
    }
    if (sink_) {
        sink_->on_completed();
    }
}

void FinancialDataReceiver::fail_stream(
    const std::string& session_id,
    const common::PipelineStatus& error,
    financial_pipeline::StreamSummary* summary) {
    auto session = session_mgr_->find(session_id);
    if (session) {
        session->fail(error);
        auto stats = session->get_statistics();
        if (summary) {
            summary->set_session_id(session_id);
            summary->set_status(wire::WireMapper::to_proto_status(error.code()));
            summary->set_total_chunks(stats.total_chunks);
            summary->set_total_records(stats.total_records);
            summary->set_total_bytes(stats.total_bytes);
            summary->set_composite_checksum(common::Crc32cCalculator::finalize(stats.composite_checksum));
            summary->set_duration_ms(stats.elapsed_seconds() * 1000.0);
            summary->set_throughput_mb_per_sec(stats.throughput_mb_per_sec());
            summary->set_records_per_sec(stats.records_per_sec());
            summary->set_error_message(error.message());
        }
    }
    if (sink_) {
        sink_->on_error(error);
    }
}

} // namespace pipeline
} // namespace financial_pipeline
