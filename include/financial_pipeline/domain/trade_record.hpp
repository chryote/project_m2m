#pragma once

#include "financial_pipeline/common/types.hpp"
#include <string>
#include <string_view>
#include <sstream>
#include <iomanip>

namespace financial_pipeline {
namespace domain {

/**
 * @brief Canonical Domain Model for an executed trade transaction.
 */
struct TradeRecord {
    common::TradeId trade_id{0};
    common::SequenceNumber sequence_number{0};
    std::string symbol{};
    double price{0.0};
    double quantity{0.0};
    common::TradeSide side{common::TradeSide::Unspecified};
    common::TimestampNs timestamp_ns{0};
    std::string venue{};
    std::string order_id{};
    uint32_t execution_flags{0};

    bool operator==(const TradeRecord& other) const noexcept {
        return trade_id == other.trade_id &&
               sequence_number == other.sequence_number &&
               symbol == other.symbol &&
               price == other.price &&
               quantity == other.quantity &&
               side == other.side &&
               timestamp_ns == other.timestamp_ns &&
               venue == other.venue &&
               order_id == other.order_id &&
               execution_flags == other.execution_flags;
    }

    bool operator!=(const TradeRecord& other) const noexcept {
        return !(*this == other);
    }

    std::string to_string() const {
        std::ostringstream oss;
        oss << "TradeRecord[ID=" << trade_id
            << ", Seq=" << sequence_number
            << ", Sym=" << symbol
            << ", Px=" << std::fixed << std::setprecision(4) << price
            << ", Qty=" << quantity
            << ", Side=" << common::trade_side_to_string(side)
            << ", Ts=" << timestamp_ns
            << ", Venue=" << venue
            << ", OrderID=" << order_id << "]";
        return oss.str();
    }
};

} // namespace domain
} // namespace financial_pipeline
