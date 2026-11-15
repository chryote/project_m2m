#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace financial_pipeline {
namespace metrics {

struct PipelineMetricsSnapshot {
    uint64_t total_bytes{0};
    uint64_t total_records{0};
    uint64_t total_chunks{0};
    uint64_t error_count{0};
    double elapsed_seconds{0.0};
    double throughput_mb_per_sec{0.0};
    double records_per_sec{0.0};
    double memory_rss_mb{0.0};
};

/**
 * @brief Thread-safe telemetry collector monitoring pipeline throughput, latency, and memory footprint.
 */
class PipelineMetricsCollector {
public:
    PipelineMetricsCollector();

    void record_chunk(size_t byte_count, uint32_t record_count) noexcept;
    void record_error() noexcept;
    void reset() noexcept;

    PipelineMetricsSnapshot get_snapshot() const noexcept;
    std::string format_summary() const;

    /// Read Resident Set Size (RSS) memory consumption of current process in MB
    static double get_process_memory_mb() noexcept;

private:
    std::atomic<uint64_t> total_bytes_{0};
    std::atomic<uint64_t> total_records_{0};
    std::atomic<uint64_t> total_chunks_{0};
    std::atomic<uint64_t> error_count_{0};
    std::chrono::steady_clock::time_point start_time_;
};

} // namespace metrics
} // namespace financial_pipeline
