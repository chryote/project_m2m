#include "financial_pipeline/wire/wire_mapper.hpp"

namespace financial_pipeline {
namespace wire {

financial_pipeline::Side WireMapper::to_proto_side(common::TradeSide side) noexcept {
    switch (side) {
        case common::TradeSide::Buy: return financial_pipeline::BUY;
        case common::TradeSide::Sell: return financial_pipeline::SELL;
        default: return financial_pipeline::SIDE_UNSPECIFIED;
    }
}

common::TradeSide WireMapper::from_proto_side(financial_pipeline::Side side) noexcept {
    switch (side) {
        case financial_pipeline::BUY: return common::TradeSide::Buy;
        case financial_pipeline::SELL: return common::TradeSide::Sell;
        default: return common::TradeSide::Unspecified;
    }
}

financial_pipeline::TransferStatus WireMapper::to_proto_status(common::StatusCode code) noexcept {
    switch (code) {
        case common::StatusCode::Ok: return financial_pipeline::STATUS_SUCCESS;
        case common::StatusCode::ChecksumMismatch: return financial_pipeline::STATUS_CHECKSUM_ERROR;
        case common::StatusCode::SequenceGap: return financial_pipeline::STATUS_SEQUENCE_GAP;
        case common::StatusCode::InvalidArgument: return financial_pipeline::STATUS_INVALID_DATA;
        case common::StatusCode::Cancelled: return financial_pipeline::STATUS_CANCELLED;
        default: return financial_pipeline::STATUS_INTERNAL_ERROR;
    }
}

common::StatusCode WireMapper::from_proto_status(financial_pipeline::TransferStatus status) noexcept {
    switch (status) {
        case financial_pipeline::STATUS_SUCCESS: return common::StatusCode::Ok;
        case financial_pipeline::STATUS_CHECKSUM_ERROR: return common::StatusCode::ChecksumMismatch;
        case financial_pipeline::STATUS_SEQUENCE_GAP: return common::StatusCode::SequenceGap;
        case financial_pipeline::STATUS_INVALID_DATA: return common::StatusCode::InvalidArgument;
        case financial_pipeline::STATUS_CANCELLED: return common::StatusCode::Cancelled;
        default: return common::StatusCode::InternalError;
    }
}

void WireMapper::to_proto(const domain::TradeRecord& src, financial_pipeline::TradeRecord* dst) {
    if (!dst) return;
    dst->set_trade_id(src.trade_id);
    dst->set_sequence_number(src.sequence_number);
    dst->set_symbol(src.symbol);
    dst->set_price(src.price);
    dst->set_quantity(src.quantity);
    dst->set_side(to_proto_side(src.side));
    dst->set_timestamp_ns(src.timestamp_ns);
    dst->set_venue(src.venue);
    dst->set_order_id(src.order_id);
    dst->set_execution_flags(src.execution_flags);
}

void WireMapper::from_proto(const financial_pipeline::TradeRecord& src, domain::TradeRecord* dst) {
    if (!dst) return;
    dst->trade_id = src.trade_id();
    dst->sequence_number = src.sequence_number();
    dst->symbol = src.symbol();
    dst->price = src.price();
    dst->quantity = src.quantity();
    dst->side = from_proto_side(src.side());
    dst->timestamp_ns = src.timestamp_ns();
    dst->venue = src.venue();
    dst->order_id = src.order_id();
    dst->execution_flags = src.execution_flags();
}

void WireMapper::to_proto(const domain::MarketDataBatch& src, financial_pipeline::TradeBatch* dst) {
    if (!dst) return;
    dst->clear_records();
    dst->mutable_records()->Reserve(static_cast<int>(src.size()));
    for (const auto& record : src) {
        to_proto(record, dst->add_records());
    }
}

void WireMapper::from_proto(const financial_pipeline::TradeBatch& src, domain::MarketDataBatch* dst) {
    if (!dst) return;
    dst->clear();
    dst->reserve(src.records_size());
    for (int i = 0; i < src.records_size(); ++i) {
        domain::TradeRecord rec;
        from_proto(src.records(i), &rec);
        dst->add_record(std::move(rec));
    }
}

std::string WireMapper::serialize_batch(const domain::MarketDataBatch& batch) {
    financial_pipeline::TradeBatch proto_batch;
    to_proto(batch, &proto_batch);
    std::string output;
    proto_batch.SerializeToString(&output);
    return output;
}

common::Result<domain::MarketDataBatch> WireMapper::deserialize_batch(std::string_view bytes) {
    financial_pipeline::TradeBatch proto_batch;
    if (!proto_batch.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
        return common::PipelineStatus::invalid_argument("Failed to parse TradeBatch from protobuf bytes");
    }
    domain::MarketDataBatch batch;
    from_proto(proto_batch, &batch);
    return batch;
}

} // namespace wire
} // namespace financial_pipeline
