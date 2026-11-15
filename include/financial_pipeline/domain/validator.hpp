#pragma once

#include "financial_pipeline/common/status.hpp"
#include "financial_pipeline/domain/trade_record.hpp"
#include "financial_pipeline/domain/market_data_batch.hpp"

namespace financial_pipeline {
namespace domain {

/**
 * @brief Validates financial records and batches against strict market data rules.
 */
class TradeRecordValidator {
public:
    /// Validates an individual trade transaction
    static common::PipelineStatus validate(const TradeRecord& record) noexcept;

    /// Validates an entire batch of records
    static common::PipelineStatus validate(const MarketDataBatch& batch) noexcept;

    /// Validates monotonic sequence ordering
    static common::PipelineStatus validate_sequence(
        common::SequenceNumber expected_seq,
        common::SequenceNumber actual_seq) noexcept;
};

} // namespace domain
} // namespace financial_pipeline
