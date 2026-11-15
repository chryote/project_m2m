#include "financial_pipeline/domain/validator.hpp"
#include <string>

namespace financial_pipeline {
namespace domain {

common::PipelineStatus TradeRecordValidator::validate(const TradeRecord& record) noexcept {
    if (record.symbol.empty()) {
        return common::PipelineStatus::invalid_argument("TradeRecord validation failed: symbol cannot be empty");
    }
    if (record.symbol.size() > 32) {
        return common::PipelineStatus::invalid_argument("TradeRecord validation failed: symbol exceeds max length 32");
    }
    if (record.price <= 0.0) {
        return common::PipelineStatus::invalid_argument(
            "TradeRecord validation failed: price must be positive, got " + std::to_string(record.price));
    }
    if (record.quantity <= 0.0) {
        return common::PipelineStatus::invalid_argument(
            "TradeRecord validation failed: quantity must be positive, got " + std::to_string(record.quantity));
    }
    if (record.side == common::TradeSide::Unspecified) {
        return common::PipelineStatus::invalid_argument("TradeRecord validation failed: trade side cannot be UNSPECIFIED");
    }
    if (record.timestamp_ns == 0) {
        return common::PipelineStatus::invalid_argument("TradeRecord validation failed: timestamp_ns cannot be 0");
    }
    return common::PipelineStatus::ok();
}

common::PipelineStatus TradeRecordValidator::validate(const MarketDataBatch& batch) noexcept {
    for (size_t i = 0; i < batch.size(); ++i) {
        auto status = validate(batch[i]);
        if (!status.is_ok()) {
            return common::PipelineStatus::invalid_argument(
                "Batch validation error at index " + std::to_string(i) + ": " + status.message());
        }
    }
    return common::PipelineStatus::ok();
}

common::PipelineStatus TradeRecordValidator::validate_sequence(
    common::SequenceNumber expected_seq,
    common::SequenceNumber actual_seq) noexcept {
    if (expected_seq != actual_seq) {
        return common::PipelineStatus::sequence_gap(
            "Sequence mismatch: expected " + std::to_string(expected_seq) +
            ", but received " + std::to_string(actual_seq));
    }
    return common::PipelineStatus::ok();
}

} // namespace domain
} // namespace financial_pipeline
