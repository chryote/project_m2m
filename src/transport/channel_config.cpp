#include "financial_pipeline/transport/channel_config.hpp"

namespace financial_pipeline {
namespace transport {

grpc::ChannelArguments ChannelConfigBuilder::build_channel_args(const GrpcConfig& config) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(config.max_message_size_bytes);
    args.SetMaxSendMessageSize(config.max_message_size_bytes);

    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, config.keepalive_time_ms);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, config.keepalive_timeout_ms);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, config.permit_keepalive_without_calls ? 1 : 0);
    args.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, config.enable_bdp_probe ? 1 : 0);
    args.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);

    // Optimize for bulk data throughput
    args.SetString(GRPC_ARG_OPTIMIZATION_TARGET, "throughput");

    return args;
}

void ChannelConfigBuilder::apply_server_builder_options(grpc::ServerBuilder& builder, const GrpcConfig& config) {
    builder.SetMaxReceiveMessageSize(config.max_message_size_bytes);
    builder.SetMaxSendMessageSize(config.max_message_size_bytes);

    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, config.keepalive_time_ms);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, config.keepalive_timeout_ms);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, config.permit_keepalive_without_calls ? 1 : 0);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_BDP_PROBE, config.enable_bdp_probe ? 1 : 0);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
    builder.AddChannelArgument(GRPC_ARG_OPTIMIZATION_TARGET, "throughput");
}

} // namespace transport
} // namespace financial_pipeline
