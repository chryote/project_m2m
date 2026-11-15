#pragma once

#include "financial_pipeline/domain/session.hpp"
#include "financial_pipeline/domain/market_data_batch.hpp"
#include "financial_pipeline/pipeline/data_sink.hpp"
#include "financial_pipeline/metrics/metrics_collector.hpp"
#include "financial_pipeline/common/status.hpp"
#include "financial_pipeline.pb.h"
#include <memory>

namespace financial_pipeline {
namespace pipeline {

/**
 * @brief Server-side streaming receiver processing chunks, verifying checksums, and dispatching to sinks.
 */
class FinancialDataReceiver {
public:
    explicit FinancialDataReceiver(
        std::shared_ptr<domain::SessionManager> session_mgr,
        std::shared_ptr<IDataSink> sink = std::make_shared<NullDataSink>(),
        std::shared_ptr<metrics::PipelineMetricsCollector> metrics = std::make_shared<metrics::PipelineMetricsCollector>());

    /// Process a single incoming chunk
    common::PipelineStatus process_chunk(const financial_pipeline::FinancialDataChunk& chunk);

    /// Finalize stream session and populate summary
    void complete_stream(const std::string& session_id, financial_pipeline::StreamSummary* summary);

    /// Fail stream session with error and populate summary
    void fail_stream(
        const std::string& session_id,
        const common::PipelineStatus& error,
        financial_pipeline::StreamSummary* summary);

    std::shared_ptr<IDataSink> sink() const noexcept { return sink_; }
    std::shared_ptr<metrics::PipelineMetricsCollector> metrics() const noexcept { return metrics_; }
    std::shared_ptr<domain::SessionManager> session_manager() const noexcept { return session_mgr_; }

private:
    std::shared_ptr<domain::SessionManager> session_mgr_;
    std::shared_ptr<IDataSink> sink_;
    std::shared_ptr<metrics::PipelineMetricsCollector> metrics_;
};

} // namespace pipeline
} // namespace financial_pipeline
