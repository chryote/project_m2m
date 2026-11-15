#include <gtest/gtest.h>
#include "financial_pipeline/domain/trade_record.hpp"
#include "financial_pipeline/domain/market_data_batch.hpp"
#include "financial_pipeline/domain/validator.hpp"

using namespace financial_pipeline::domain;
using namespace financial_pipeline::common;

TEST(TradeRecordTest, EqualityAndProperties) {
    TradeRecord r1;
    r1.trade_id = 1001;
    r1.sequence_number = 1;
    r1.symbol = "AAPL";
    r1.price = 185.25;
    r1.quantity = 50.0;
    r1.side = TradeSide::Buy;
    r1.timestamp_ns = 1700000000000000000ULL;
    r1.venue = "NASDAQ";
    r1.order_id = "ORD-001";
    r1.execution_flags = 0x01;

    TradeRecord r2 = r1;
    EXPECT_EQ(r1, r2);

    r2.price = 185.30;
    EXPECT_NE(r1, r2);

    std::string str = r1.to_string();
    EXPECT_NE(str.find("AAPL"), std::string::npos);
    EXPECT_NE(str.find("BUY"), std::string::npos);
}

TEST(TradeRecordValidatorTest, ValidRecordPasses) {
    TradeRecord r;
    r.trade_id = 1;
    r.sequence_number = 1;
    r.symbol = "NVDA";
    r.price = 850.0;
    r.quantity = 10.0;
    r.side = TradeSide::Sell;
    r.timestamp_ns = 1700000000000000000ULL;
    r.venue = "NASDAQ";

    auto status = TradeRecordValidator::validate(r);
    EXPECT_TRUE(status.is_ok());
}

TEST(TradeRecordValidatorTest, RejectsInvalidRecord) {
    TradeRecord r;
    r.symbol = ""; // Empty symbol
    r.price = 100.0;
    r.quantity = 10.0;
    r.side = TradeSide::Buy;
    r.timestamp_ns = 1000;
    EXPECT_FALSE(TradeRecordValidator::validate(r).is_ok());

    r.symbol = "AAPL";
    r.price = -10.0; // Negative price
    EXPECT_FALSE(TradeRecordValidator::validate(r).is_ok());

    r.price = 100.0;
    r.quantity = 0.0; // Zero quantity
    EXPECT_FALSE(TradeRecordValidator::validate(r).is_ok());

    r.quantity = 10.0;
    r.side = TradeSide::Unspecified; // Unspecified side
    EXPECT_FALSE(TradeRecordValidator::validate(r).is_ok());

    r.side = TradeSide::Buy;
    r.timestamp_ns = 0; // Zero timestamp
    EXPECT_FALSE(TradeRecordValidator::validate(r).is_ok());
}

TEST(MarketDataBatchTest, BatchOperations) {
    MarketDataBatch batch;
    EXPECT_TRUE(batch.empty());
    EXPECT_EQ(batch.size(), 0);

    TradeRecord r;
    r.trade_id = 1;
    r.symbol = "MSFT";
    r.price = 420.0;
    r.quantity = 25.0;
    r.side = TradeSide::Buy;
    r.timestamp_ns = 1000;

    batch.add_record(r);
    EXPECT_FALSE(batch.empty());
    EXPECT_EQ(batch.size(), 1);
    EXPECT_GT(batch.estimated_byte_size(), 0);

    EXPECT_EQ(batch[0].symbol, "MSFT");
    batch.clear();
    EXPECT_TRUE(batch.empty());
}
