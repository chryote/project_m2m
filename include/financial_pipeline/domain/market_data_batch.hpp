#pragma once

#include "financial_pipeline/domain/trade_record.hpp"
#include <vector>
#include <cstddef>
#include <algorithm>

namespace financial_pipeline {
namespace domain {

/**
 * @brief Aggregate collection of TradeRecords optimized for batched processing and chunking.
 */
class MarketDataBatch {
public:
    MarketDataBatch() = default;
    explicit MarketDataBatch(size_t reserve_capacity) {
        records_.reserve(reserve_capacity);
    }

    void add_record(TradeRecord record) {
        records_.push_back(std::move(record));
    }

    void reserve(size_t capacity) {
        records_.reserve(capacity);
    }

    void clear() noexcept {
        records_.clear();
    }

    [[nodiscard]] bool empty() const noexcept {
        return records_.empty();
    }

    [[nodiscard]] size_t size() const noexcept {
        return records_.size();
    }

    const std::vector<TradeRecord>& records() const noexcept {
        return records_;
    }

    std::vector<TradeRecord>& records() noexcept {
        return records_;
    }

    const TradeRecord& operator[](size_t index) const {
        return records_[index];
    }

    TradeRecord& operator[](size_t index) {
        return records_[index];
    }

    auto begin() noexcept { return records_.begin(); }
    auto end() noexcept { return records_.end(); }
    auto begin() const noexcept { return records_.begin(); }
    auto end() const noexcept { return records_.end(); }
    auto cbegin() const noexcept { return records_.cbegin(); }
    auto cend() const noexcept { return records_.cend(); }

    /// Estimated memory footprint of records in bytes
    [[nodiscard]] size_t estimated_byte_size() const noexcept {
        size_t bytes = sizeof(MarketDataBatch);
        for (const auto& r : records_) {
            bytes += sizeof(TradeRecord) + r.symbol.capacity() + r.venue.capacity() + r.order_id.capacity();
        }
        return bytes;
    }

private:
    std::vector<TradeRecord> records_;
};

} // namespace domain
} // namespace financial_pipeline
