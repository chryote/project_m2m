#pragma once

#include "financial_pipeline/domain/trade_record.hpp"
#include "financial_pipeline/domain/market_data_batch.hpp"
#include <random>
#include <vector>
#include <string>
#include <cstdint>

namespace financial_pipeline {
namespace pipeline {

/**
 * @brief High-throughput synthetic market data generator modeling realistic multi-asset trading activity.
 */
class SyntheticDataGenerator {
public:
    explicit SyntheticDataGenerator(uint32_t seed = 42);

    /// Generate an individual trade record
    domain::TradeRecord generate_record();

    /// Generate a batch of trade records
    domain::MarketDataBatch generate_batch(size_t count);

    /// Estimate approximate number of records required to reach a target size in gigabytes
    static uint64_t estimate_records_for_gb(double target_gb) noexcept;

    /// Reset sequence counters
    void reset(uint64_t starting_seq = 1) noexcept;

private:
    std::mt19937_64 rng_;
    uint64_t current_seq_{1};
    uint64_t base_timestamp_ns_{1700000000000000000ULL};
    std::vector<std::string> symbols_;
    std::vector<std::string> venues_;
    std::vector<double> base_prices_;
};

} // namespace pipeline
} // namespace financial_pipeline
