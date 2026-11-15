#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace financial_pipeline {
namespace common {

using TradeId = uint64_t;
using SequenceNumber = uint64_t;
using TimestampNs = uint64_t;
using SessionId = std::string;
using ChecksumValue = uint32_t;

enum class TradeSide : uint8_t {
    Unspecified = 0,
    Buy = 1,
    Sell = 2
};

enum class CompressionMode : uint8_t {
    None = 0,
    Zlib = 1
};

inline const char* trade_side_to_string(TradeSide side) noexcept {
    switch (side) {
        case TradeSide::Buy: return "BUY";
        case TradeSide::Sell: return "SELL";
        default: return "UNSPECIFIED";
    }
}

inline TradeSide trade_side_from_string(std::string_view sv) noexcept {
    if (sv == "BUY" || sv == "buy") return TradeSide::Buy;
    if (sv == "SELL" || sv == "sell") return TradeSide::Sell;
    return TradeSide::Unspecified;
}

} // namespace common
} // namespace financial_pipeline
