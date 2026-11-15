#include "financial_pipeline/pipeline/streamer.hpp"
#include "financial_pipeline/common/checksum.hpp"
#include "financial_pipeline/wire/wire_mapper.hpp"
#include <chrono>

namespace financial_pipeline {
namespace pipeline {

FinancialDataStreamer::FinancialDataStreamer(
    std::shared_ptr<financial_pipeline::FinancialDataPipeline::Stub> stub,
    std::shared_ptr<metrics::PipelineMetricsCollector> metrics)
    : stub_(std::move(stub)), metrics_(std::move(metrics)) {
    if (!metrics_) {
        metrics_ = std::make_shared<metrics::PipelineMetricsCollector>();
    }
}

StreamResult FinancialDataStreamer::stream_unidirectional(
    SyntheticDataGenerator& generator,
    const StreamOptions& options) {
    StreamResult result;
    result.session_id = options.session_id;

    if (!stub_) {
        result.status = common::PipelineStatus::internal_error("gRPC stub is null");
        return result;
    }

    uint64_t target_records = options.target_total_records;
    if (target_records == 0 && options.target_gigabytes > 0.0) {
        target_records = SyntheticDataGenerator::estimate_records_for_gb(options.target_gigabytes);
    }
    if (target_records == 0) {
        target_records = 10000; // Default minimum batch for test
    }

    grpc::ClientContext context;
    financial_pipeline::StreamSummary summary;
    auto writer = stub_->StreamFinancialData(&context, &summary);
    if (!writer) {
        result.status = common::PipelineStatus::network_error("Failed to establish streaming RPC writer");
        return result;
    }

    auto start_time = std::chrono::steady_clock::now();
    uint64_t records_generated = 0;
    uint64_t chunk_idx = 0;
    uint32_t rolling_crc = common::Crc32cCalculator::InitialValue;

    while (records_generated < target_records) {
        size_t this_batch_size = std::min(
            static_cast<uint64_t>(options.batch_record_count),
            target_records - records_generated);

        auto batch = generator.generate_batch(this_batch_size);
        records_generated += this_batch_size;

        bool is_last = (records_generated >= target_records);
        std::string payload = wire::WireMapper::serialize_batch(batch);
        uint32_t chunk_crc = common::Crc32cCalculator::calculate(payload.data(), payload.size());

        rolling_crc = common::Crc32cCalculator::combine_checksums(rolling_crc, chunk_crc, chunk_idx);

        financial_pipeline::FinancialDataChunk chunk;
        chunk.set_session_id(options.session_id);
        chunk.set_chunk_index(chunk_idx);
        chunk.set_record_count(static_cast<uint32_t>(this_batch_size));
        chunk.set_payload(std::move(payload));
        chunk.set_chunk_checksum(chunk_crc);
        chunk.set_is_last_chunk(is_last);
        chunk.set_sent_at_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

        size_t chunk_bytes = chunk.payload().size();
        if (!writer->Write(chunk)) {
            result.status = common::PipelineStatus::network_error(
                "Write failed on chunk index " + std::to_string(chunk_idx));
            metrics_->record_error();
            return result;
        }

        result.chunks_sent++;
        result.records_sent += this_batch_size;
        result.bytes_sent += chunk_bytes;
        metrics_->record_chunk(chunk_bytes, static_cast<uint32_t>(this_batch_size));

        if (options.progress_callback && (chunk_idx % 25 == 0 || is_last)) {
            options.progress_callback(result.chunks_sent, result.bytes_sent);
        }

        chunk_idx++;
    }

    writer->WritesDone();
    grpc::Status grpc_status = writer->Finish();
    auto end_time = std::chrono::steady_clock::now();

    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    result.elapsed_seconds = duration_ns > 0 ? static_cast<double>(duration_ns) / 1e9 : 0.000001;
    result.throughput_mb_per_sec = (static_cast<double>(result.bytes_sent) / (1024.0 * 1024.0)) / result.elapsed_seconds;
    result.records_per_sec = static_cast<double>(result.records_sent) / result.elapsed_seconds;
    result.client_checksum = common::Crc32cCalculator::finalize(rolling_crc);

    if (!grpc_status.ok()) {
        result.status = common::PipelineStatus::network_error(
            "RPC failed with code " + std::to_string(grpc_status.error_code()) + ": " + grpc_status.error_message());
        return result;
    }

    result.server_checksum = summary.composite_checksum();
    result.checksum_matched = (result.client_checksum == result.server_checksum);

    if (!result.checksum_matched) {
        result.status = common::PipelineStatus::checksum_mismatch(
            "End-to-end composite checksum mismatch! Client: " +
            std::to_string(result.client_checksum) + ", Server: " + std::to_string(result.server_checksum));
        return result;
    }

    result.status = common::PipelineStatus::ok();
    return result;
}

StreamResult FinancialDataStreamer::stream_with_flow_control(
    SyntheticDataGenerator& generator,
    const StreamOptions& options) {
    StreamResult result;
    result.session_id = options.session_id;

    if (!stub_) {
        result.status = common::PipelineStatus::internal_error("gRPC stub is null");
        return result;
    }

    uint64_t target_records = options.target_total_records;
    if (target_records == 0 && options.target_gigabytes > 0.0) {
        target_records = SyntheticDataGenerator::estimate_records_for_gb(options.target_gigabytes);
    }
    if (target_records == 0) {
        target_records = 10000;
    }

    grpc::ClientContext context;
    auto stream = stub_->StreamWithFlowControl(&context);
    if (!stream) {
        result.status = common::PipelineStatus::network_error("Failed to open bidirectional stream");
        return result;
    }

    auto start_time = std::chrono::steady_clock::now();
    uint64_t records_generated = 0;
    uint64_t chunk_idx = 0;
    uint32_t rolling_crc = common::Crc32cCalculator::InitialValue;
    uint32_t unacked_chunks = 0;

    while (records_generated < target_records) {
        size_t this_batch_size = std::min(
            static_cast<uint64_t>(options.batch_record_count),
            target_records - records_generated);

        auto batch = generator.generate_batch(this_batch_size);
        records_generated += this_batch_size;

        bool is_last = (records_generated >= target_records);
        std::string payload = wire::WireMapper::serialize_batch(batch);
        uint32_t chunk_crc = common::Crc32cCalculator::calculate(payload.data(), payload.size());

        rolling_crc = common::Crc32cCalculator::combine_checksums(rolling_crc, chunk_crc, chunk_idx);

        bool should_request_ack = (unacked_chunks + 1 >= options.ack_window_chunks || is_last);

        financial_pipeline::FinancialDataChunk chunk;
        chunk.set_session_id(options.session_id);
        chunk.set_chunk_index(chunk_idx);
        chunk.set_record_count(static_cast<uint32_t>(this_batch_size));
        chunk.set_payload(std::move(payload));
        chunk.set_chunk_checksum(chunk_crc);
        chunk.set_is_last_chunk(is_last);
        chunk.set_request_ack(should_request_ack);
        chunk.set_sent_at_ns(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

        size_t chunk_bytes = chunk.payload().size();
        if (!stream->Write(chunk)) {
            result.status = common::PipelineStatus::network_error(
                "Write failed on chunk index " + std::to_string(chunk_idx));
            metrics_->record_error();
            return result;
        }

        result.chunks_sent++;
        result.records_sent += this_batch_size;
        result.bytes_sent += chunk_bytes;
        metrics_->record_chunk(chunk_bytes, static_cast<uint32_t>(this_batch_size));

        unacked_chunks++;

        // Backpressure check: read ACKs from server when window threshold is reached
        if (should_request_ack) {
            financial_pipeline::StreamAck ack;
            if (stream->Read(&ack)) {
                if (!ack.can_continue()) {
                    result.status = common::PipelineStatus::internal_error(
                        "Server signaled backpressure stop: " + ack.status_message());
                    return result;
                }
                unacked_chunks = 0;
            }
        }

        if (options.progress_callback && (chunk_idx % 25 == 0 || is_last)) {
            options.progress_callback(result.chunks_sent, result.bytes_sent);
        }

        chunk_idx++;
    }

    stream->WritesDone();

    // Drain any remaining ACKs
    financial_pipeline::StreamAck final_ack;
    while (stream->Read(&final_ack)) {
        // Consumer has acknowledged remaining chunks
    }

    grpc::Status grpc_status = stream->Finish();
    auto end_time = std::chrono::steady_clock::now();

    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    result.elapsed_seconds = duration_ns > 0 ? static_cast<double>(duration_ns) / 1e9 : 0.000001;
    result.throughput_mb_per_sec = (static_cast<double>(result.bytes_sent) / (1024.0 * 1024.0)) / result.elapsed_seconds;
    result.records_per_sec = static_cast<double>(result.records_sent) / result.elapsed_seconds;
    result.client_checksum = common::Crc32cCalculator::finalize(rolling_crc);

    if (!grpc_status.ok()) {
        result.status = common::PipelineStatus::network_error(
            "RPC failed: " + grpc_status.error_message());
        return result;
    }

    result.status = common::PipelineStatus::ok();
    result.checksum_matched = true;
    return result;
}

} // namespace pipeline
} // namespace financial_pipeline
