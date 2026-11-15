#pragma once

#include "financial_pipeline/domain/trade_record.hpp"
#include "financial_pipeline/domain/market_data_batch.hpp"
#include "financial_pipeline/common/status.hpp"
#include "financial_pipeline.pb.h"
#include <string>
#include <string_view>

namespace financial_pipeline {
namespace wire {

/**
 * @brief Anti-corruption mapping layer between Domain models and Protobuf wire messages.
 */
class WireMapper {
public:
    /// Map Domain TradeRecord to Protobuf TradeRecord
    static void to_proto(const domain::TradeRecord& src, financial_pipeline::TradeRecord* dst);

    /// Map Protobuf TradeRecord to Domain TradeRecord
    static void from_proto(const financial_pipeline::TradeRecord& src, domain::TradeRecord* dst);

    /// Map Domain MarketDataBatch to Protobuf TradeBatch
    static void to_proto(const domain::MarketDataBatch& src, financial_pipeline::TradeBatch* dst);

    /// Map Protobuf TradeBatch to Domain MarketDataBatch
    static void from_proto(const financial_pipeline::TradeBatch& src, domain::MarketDataBatch* dst);

    /// Serialize a MarketDataBatch to contiguous wire bytes
    static std::string serialize_batch(const domain::MarketDataBatch& batch);

    /// Deserialize a MarketDataBatch from contiguous wire bytes
    static common::Result<domain::MarketDataBatch> deserialize_batch(std::string_view bytes);

    /// Convert Domain TradeSide to Protobuf Side
    static financial_pipeline::Side to_proto_side(common::TradeSide side) noexcept;

    /// Convert Protobuf Side to Domain TradeSide
    static common::TradeSide from_proto_side(financial_pipeline::Side side) noexcept;

    /// Convert Domain Status code to Protobuf TransferStatus
    static financial_pipeline::TransferStatus to_proto_status(common::StatusCode code) noexcept;

    /// Convert Protobuf TransferStatus to Domain Status code
    static common::StatusCode from_proto_status(financial_pipeline::TransferStatus status) noexcept;
};

} // namespace wire
} // namespace financial_pipeline
