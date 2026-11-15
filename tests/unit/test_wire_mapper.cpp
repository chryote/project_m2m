#include <gtest/gtest.h>
#include "financial_pipeline/wire/wire_mapper.hpp"
#include "financial_pipeline/domain/trade_record.hpp"
#include "financial_pipeline/domain/market_data_batch.hpp"

using namespace financial_pipeline::wire;
using namespace financial_pipeline::domain;
using namespace financial_pipeline::common;

TEST(WireMapperTest, TradeRecordRoundTrip) {
    TradeRecord src;
    src.trade_id = 998877;
    src.sequence_number = 42;
    src.symbol = "ETHUSD";
    src.price = 3500.75;
    src.quantity = 4.5;
    src.side = TradeSide::Buy;
    src.timestamp_ns = 1710000000123456789ULL;
    src.venue = "BINANCE";
    src.order_id = "ORD-9988";
    src.execution_flags = 0x05;

    financial_pipeline::TradeRecord proto_rec;
    WireMapper::to_proto(src, &proto_rec);

    TradeRecord dst;
    WireMapper::from_proto(proto_rec, &dst);

    EXPECT_EQ(src, dst);
}

TEST(WireMapperTest, BatchSerializationRoundTrip) {
    MarketDataBatch batch;
    for (uint64_t i = 0; i < 50; ++i) {
        TradeRecord r;
        r.trade_id = i;
        r.sequence_number = i;
        r.symbol = (i % 2 == 0) ? "AAPL" : "GOOGL";
        r.price = 150.0 + static_cast<double>(i);
        r.quantity = 10.0 + static_cast<double>(i);
        r.side = (i % 2 == 0) ? TradeSide::Buy : TradeSide::Sell;
        r.timestamp_ns = 1700000000000000000ULL + i * 1000;
        r.venue = "NASDAQ";
        batch.add_record(std::move(r));
    }

    std::string bytes = WireMapper::serialize_batch(batch);
    EXPECT_FALSE(bytes.empty());

    auto result = WireMapper::deserialize_batch(bytes);
    ASSERT_TRUE(result.is_ok());

    const auto& deserialized = result.value();
    ASSERT_EQ(deserialized.size(), batch.size());
    for (size_t i = 0; i < batch.size(); ++i) {
        EXPECT_EQ(deserialized[i], batch[i]);
    }
}

TEST(WireMapperTest, InvalidBytesFailsGracefully) {
    std::string invalid_bytes = "\xFF\xFF\xFF\xFFinvalid_proto_bytes";
    auto result = WireMapper::deserialize_batch(invalid_bytes);
    EXPECT_FALSE(result.is_ok());
}
