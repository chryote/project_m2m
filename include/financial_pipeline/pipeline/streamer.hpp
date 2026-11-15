#pragma once

#include "financial_pipeline/domain/market_data_batch.hpp"
#include "financial_pipeline/pipeline/data_generator.hpp"
#include "financial_pipeline/metrics/metrics_collector.hpp"
#include "financial_pipeline/common/status.hpp"
#include "financial_pipeline.grpc.pb.h"
#include <string>
#include <memory>
#include <functional>

namespace financial_pipeline {
namespace pipeline {

struct StreamOptions {
    std::string session_id;
    size_t chunk_size_bytes{2 * 1024 * 1024}; // 2 MB
    size_t batch_record_count{15000};          // 15,000 records per chunk (~1.1 MB)
    uint64_t target_total_records{0};          // If 0, determined by target_gigabytes
    double target_gigabytes{0.0};              // e.g. 5.0 for >5 GB
    bool use_flow_control{false};              // Bidirectional streaming with acks
    uint32_t ack_window_chunks{10};            // Request ack every N chunks
    std::function<void(uint64_t chunks_sent, uint64_t bytes_sent)> progress_callback;
};

struct StreamResult {
    common::PipelineStatus status;
    std::string session_id;
    uint64_t chunks_sent{0};
    uint64_t records_sent{0};
    uint64_t bytes_sent{0};
    uint32_t client_checksum{0};
    uint32_t server_checksum{0};
    bool checksum_matched{false};
    double elapsed_seconds{0.0};
    double throughput_mb_per_sec{0.0};
    double records_per_sec{0.0};
};

/**
 * @brief High-throughput client streaming engine orchestrating batch generation, chunking, checksums, and flow control.
 */
class FinancialDataStreamer {
public:
    explicit FinancialDataStreamer(
        std::shared_ptr<financial_pipeline::FinancialDataPipeline::Stub> stub,
        std::shared_ptr<metrics::PipelineMetricsCollector> metrics = std::make_shared<metrics::PipelineMetricsCollector>());

    /// Stream financial data to the server using standard client-streaming RPC
    StreamResult stream_unidirectional(
        SyntheticDataGenerator& generator,
        const StreamOptions& options);

    /// Stream financial data with bidirectional flow control / windowed ACKs
    StreamResult stream_with_flow_control(
        SyntheticDataGenerator& generator,
        const StreamOptions& options);

    std::shared_ptr<metrics::PipelineMetricsCollector> metrics() const noexcept { return metrics_; }

private:
    std::shared_ptr<financial_pipeline::FinancialDataPipeline::Stub> stub_;
    std::shared_ptr<metrics::PipelineMetricsCollector> metrics_;
};

} // namespace pipeline
} // namespace financial_pipeline
