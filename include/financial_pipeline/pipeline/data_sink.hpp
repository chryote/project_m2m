#pragma once

#include "financial_pipeline/domain/market_data_batch.hpp"
#include "financial_pipeline/common/status.hpp"
#include <memory>
#include <mutex>
#include <vector>
#include <fstream>
#include <atomic>

namespace financial_pipeline {
namespace pipeline {

/**
 * @brief Abstract interface for consuming streaming market data batches on the receiving server.
 */
class IDataSink {
public:
    virtual ~IDataSink() = default;

    virtual common::PipelineStatus on_batch(const domain::MarketDataBatch& batch) = 0;
    virtual void on_completed() = 0;
    virtual void on_error(const common::PipelineStatus& status) = 0;
};

/**
 * @brief High-performance Null sink that discards data after verification.
 * 
 * Essential for testing raw network + serialization throughput without disk I/O bottlenecks.
 */
class NullDataSink : public IDataSink {
public:
    common::PipelineStatus on_batch(const domain::MarketDataBatch& batch) override {
        records_consumed_.fetch_add(batch.size(), std::memory_order_relaxed);
        batches_consumed_.fetch_add(1, std::memory_order_relaxed);
        return common::PipelineStatus::ok();
    }

    void on_completed() override {}
    void on_error(const common::PipelineStatus&) override {}

    uint64_t records_consumed() const noexcept { return records_consumed_.load(std::memory_order_relaxed); }
    uint64_t batches_consumed() const noexcept { return batches_consumed_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> records_consumed_{0};
    std::atomic<uint64_t> batches_consumed_{0};
};

/**
 * @brief Thread-safe in-memory sink that stores batches up to a maximum capacity (useful for tests).
 */
class MemoryDataSink : public IDataSink {
public:
    explicit MemoryDataSink(size_t max_records = 100000) : max_records_(max_records) {}

    common::PipelineStatus on_batch(const domain::MarketDataBatch& batch) override {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& rec : batch) {
            if (stored_records_.size() < max_records_) {
                stored_records_.push_back(rec);
            }
        }
        total_records_seen_ += batch.size();
        return common::PipelineStatus::ok();
    }

    void on_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        completed_ = true;
    }

    void on_error(const common::PipelineStatus& status) override {
        std::lock_guard<std::mutex> lock(mutex_);
        error_status_ = status;
    }

    std::vector<domain::TradeRecord> get_records() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return stored_records_;
    }

    size_t total_records_seen() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return total_records_seen_;
    }

    bool is_completed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return completed_;
    }

private:
    mutable std::mutex mutex_;
    size_t max_records_;
    std::vector<domain::TradeRecord> stored_records_;
    size_t total_records_seen_{0};
    bool completed_{false};
    common::PipelineStatus error_status_{common::PipelineStatus::ok()};
};

} // namespace pipeline
} // namespace financial_pipeline
