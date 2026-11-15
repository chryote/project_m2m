#include "financial_pipeline/metrics/metrics_collector.hpp"
#include <sstream>
#include <iomanip>

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <psapi.h>
#pragma comment(lib, "psapi.lib")
#else
#include <sys/resource.h>
#include <unistd.h>
#endif

namespace financial_pipeline {
namespace metrics {

PipelineMetricsCollector::PipelineMetricsCollector() {
    reset();
}

void PipelineMetricsCollector::record_chunk(size_t byte_count, uint32_t record_count) noexcept {
    total_bytes_.fetch_add(byte_count, std::memory_order_relaxed);
    total_records_.fetch_add(record_count, std::memory_order_relaxed);
    total_chunks_.fetch_add(1, std::memory_order_relaxed);
}

void PipelineMetricsCollector::record_error() noexcept {
    error_count_.fetch_add(1, std::memory_order_relaxed);
}

void PipelineMetricsCollector::reset() noexcept {
    total_bytes_.store(0, std::memory_order_relaxed);
    total_records_.store(0, std::memory_order_relaxed);
    total_chunks_.store(0, std::memory_order_relaxed);
    error_count_.store(0, std::memory_order_relaxed);
    start_time_ = std::chrono::steady_clock::now();
}

PipelineMetricsSnapshot PipelineMetricsCollector::get_snapshot() const noexcept {
    PipelineMetricsSnapshot snap;
    snap.total_bytes = total_bytes_.load(std::memory_order_relaxed);
    snap.total_records = total_records_.load(std::memory_order_relaxed);
    snap.total_chunks = total_chunks_.load(std::memory_order_relaxed);
    snap.error_count = error_count_.load(std::memory_order_relaxed);

    auto now = std::chrono::steady_clock::now();
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_).count();
    snap.elapsed_seconds = duration_ns > 0 ? static_cast<double>(duration_ns) / 1e9 : 0.000001;

    double mb = static_cast<double>(snap.total_bytes) / (1024.0 * 1024.0);
    snap.throughput_mb_per_sec = mb / snap.elapsed_seconds;
    snap.records_per_sec = static_cast<double>(snap.total_records) / snap.elapsed_seconds;
    snap.memory_rss_mb = get_process_memory_mb();

    return snap;
}

std::string PipelineMetricsCollector::format_summary() const {
    auto snap = get_snapshot();
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "=== Pipeline Telemetry Summary ===\n"
        << "  Total Transferred : " << (static_cast<double>(snap.total_bytes) / (1024.0 * 1024.0 * 1024.0)) << " GB ("
        << (static_cast<double>(snap.total_bytes) / (1024.0 * 1024.0)) << " MB)\n"
        << "  Total Records     : " << snap.total_records << " trades\n"
        << "  Total Chunks      : " << snap.total_chunks << " chunks\n"
        << "  Elapsed Time      : " << snap.elapsed_seconds << " sec\n"
        << "  Wire Throughput   : " << snap.throughput_mb_per_sec << " MB/s\n"
        << "  Record Rate       : " << snap.records_per_sec << " records/sec\n"
        << "  Process Memory RSS: " << snap.memory_rss_mb << " MB\n"
        << "  Errors Encountered: " << snap.error_count << "\n"
        << "==================================";
    return oss.str();
}

double PipelineMetricsCollector::get_process_memory_mb() noexcept {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS_EX pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&pmc), sizeof(pmc))) {
        return static_cast<double>(pmc.WorkingSetSize) / (1024.0 * 1024.0);
    }
    return 0.0;
#else
    struct rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        // ru_maxrss is in kilobytes on Linux, bytes on macOS
#if defined(__APPLE__)
        return static_cast<double>(usage.ru_maxrss) / (1024.0 * 1024.0);
#else
        return static_cast<double>(usage.ru_maxrss) / 1024.0;
#endif
    }
    return 0.0;
#endif
}

} // namespace metrics
} // namespace financial_pipeline
