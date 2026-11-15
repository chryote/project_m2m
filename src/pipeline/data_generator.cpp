#include "financial_pipeline/pipeline/data_generator.hpp"

namespace financial_pipeline {
namespace pipeline {

SyntheticDataGenerator::SyntheticDataGenerator(uint32_t seed)
    : rng_(seed),
      symbols_{"AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "GS", "BTCUSD"},
      venues_{"NASDAQ", "NYSE", "BATS", "CME", "IEX"},
      base_prices_{185.50, 420.25, 175.80, 180.10, 890.00, 490.50, 210.00, 195.40, 410.00, 65000.00} {}

domain::TradeRecord SyntheticDataGenerator::generate_record() {
    std::uniform_int_distribution<size_t> sym_dist(0, symbols_.size() - 1);
    std::uniform_int_distribution<size_t> venue_dist(0, venues_.size() - 1);
    std::uniform_real_distribution<double> price_delta(-0.02, 0.02);
    std::uniform_real_distribution<double> qty_dist(1.0, 500.0);
    std::uniform_int_distribution<int> side_dist(1, 2);
    std::uniform_int_distribution<uint64_t> time_step(100, 5000); // 100ns to 5000ns increments

    size_t sym_idx = sym_dist(rng_);
    double price = base_prices_[sym_idx] * (1.0 + price_delta(rng_));
    if (price <= 0.01) price = 0.01;

    base_timestamp_ns_ += time_step(rng_);

    domain::TradeRecord record;
    record.trade_id = current_seq_;
    record.sequence_number = current_seq_;
    record.symbol = symbols_[sym_idx];
    record.price = price;
    record.quantity = qty_dist(rng_);
    record.side = (side_dist(rng_) == 1) ? common::TradeSide::Buy : common::TradeSide::Sell;
    record.timestamp_ns = base_timestamp_ns_;
    record.venue = venues_[venue_dist(rng_)];
    record.order_id = "ORD-" + std::to_string(current_seq_);
    record.execution_flags = 0x01; // Regular execution

    current_seq_++;
    return record;
}

domain::MarketDataBatch SyntheticDataGenerator::generate_batch(size_t count) {
    domain::MarketDataBatch batch(count);
    for (size_t i = 0; i < count; ++i) {
        batch.add_record(generate_record());
    }
    return batch;
}

uint64_t SyntheticDataGenerator::estimate_records_for_gb(double target_gb) noexcept {
    // Each serialized Protobuf trade record in a batch averages ~70 bytes on the wire.
    // 1 GB = 1024 * 1024 * 1024 bytes = 1,073,741,824 bytes.
    // ~15,000,000 records is approximately 1.05 GB.
    // >5 GB is ~75,000,000 records.
    constexpr double avg_bytes_per_record = 72.0;
    double total_bytes = target_gb * 1024.0 * 1024.0 * 1024.0;
    return static_cast<uint64_t>(total_bytes / avg_bytes_per_record);
}

void SyntheticDataGenerator::reset(uint64_t starting_seq) noexcept {
    current_seq_ = starting_seq;
    base_timestamp_ns_ = 1700000000000000000ULL;
}

} // namespace pipeline
} // namespace financial_pipeline
