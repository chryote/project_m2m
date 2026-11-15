#include <gtest/gtest.h>
#include "financial_pipeline/transport/server.hpp"
#include "financial_pipeline/transport/client.hpp"
#include "financial_pipeline/transport/service_impl.hpp"
#include "financial_pipeline/pipeline/data_generator.hpp"
#include "financial_pipeline/pipeline/data_sink.hpp"
#include "financial_pipeline/common/checksum.hpp"
#include "financial_pipeline/wire/wire_mapper.hpp"
#include <memory>
#include <thread>

using namespace financial_pipeline;

class PipelineIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.host = "127.0.0.1";
        config_.port = 50055;
        config_.max_message_size_bytes = 32 * 1024 * 1024;

        session_mgr_ = std::make_shared<domain::SessionManager>();
        sink_ = std::make_shared<pipeline::MemoryDataSink>(200000);
        metrics_ = std::make_shared<metrics::PipelineMetricsCollector>();
        receiver_ = std::make_shared<pipeline::FinancialDataReceiver>(session_mgr_, sink_, metrics_);
        service_ = std::make_shared<transport::FinancialPipelineServiceImpl>(receiver_, session_mgr_);

        server_ = std::make_unique<transport::PipelineServer>(config_, service_);
        auto start_status = server_->start();
        ASSERT_TRUE(start_status.is_ok()) << start_status.to_string();

        client_ = std::make_unique<transport::PipelineClient>(config_);
        auto conn_status = client_->connect();
        ASSERT_TRUE(conn_status.is_ok()) << conn_status.to_string();
    }

    void TearDown() override {
        if (server_) {
            server_->stop(1000);
        }
    }

    transport::GrpcConfig config_;
    std::shared_ptr<domain::SessionManager> session_mgr_;
    std::shared_ptr<pipeline::MemoryDataSink> sink_;
    std::shared_ptr<metrics::PipelineMetricsCollector> metrics_;
    std::shared_ptr<pipeline::FinancialDataReceiver> receiver_;
    std::shared_ptr<transport::FinancialPipelineServiceImpl> service_;
    std::unique_ptr<transport::PipelineServer> server_;
    std::unique_ptr<transport::PipelineClient> client_;
};

TEST_F(PipelineIntegrationTest, HealthCheckReturnsHealthy) {
    auto health_res = client_->check_health("test-runner", 3000);
    ASSERT_TRUE(health_res.is_ok());
    EXPECT_TRUE(health_res.value().healthy());
    EXPECT_EQ(health_res.value().version(), "1.0.0");
}

TEST_F(PipelineIntegrationTest, UnidirectionalStreamingSuccessAndBitExactChecksum) {
    auto streamer = client_->create_streamer();
    pipeline::SyntheticDataGenerator generator(777);

    pipeline::StreamOptions opts;
    opts.session_id = "integration-session-01";
    opts.target_total_records = 30000;
    opts.batch_record_count = 5000;
    opts.use_flow_control = false;

    auto result = streamer->stream_unidirectional(generator, opts);
    ASSERT_TRUE(result.status.is_ok()) << result.status.to_string();
    EXPECT_EQ(result.records_sent, 30000);
    EXPECT_EQ(result.chunks_sent, 6);
    EXPECT_TRUE(result.checksum_matched);
    EXPECT_EQ(result.client_checksum, result.server_checksum);
    EXPECT_GT(result.throughput_mb_per_sec, 0.0);

    // Verify sink
    EXPECT_EQ(sink_->total_records_seen(), 30000);

    // Query session
    auto query_res = client_->query_session(opts.session_id);
    ASSERT_TRUE(query_res.is_ok());
    EXPECT_EQ(query_res.value().records_processed(), 30000);
    EXPECT_EQ(query_res.value().chunks_processed(), 6);
}

TEST_F(PipelineIntegrationTest, BidirectionalFlowControlStreamingSuccess) {
    auto streamer = client_->create_streamer();
    pipeline::SyntheticDataGenerator generator(888);

    pipeline::StreamOptions opts;
    opts.session_id = "flow-control-session-02";
    opts.target_total_records = 25000;
    opts.batch_record_count = 5000;
    opts.use_flow_control = true;
    opts.ack_window_chunks = 2;

    auto result = streamer->stream_with_flow_control(generator, opts);
    ASSERT_TRUE(result.status.is_ok()) << result.status.to_string();
    EXPECT_EQ(result.records_sent, 25000);
    EXPECT_EQ(result.chunks_sent, 5);
}

TEST_F(PipelineIntegrationTest, DetectsTamperedChunkChecksum) {
    grpc::ClientContext context;
    financial_pipeline::StreamSummary summary;
    auto writer = client_->stub()->StreamFinancialData(&context, &summary);
    ASSERT_NE(writer, nullptr);

    pipeline::SyntheticDataGenerator generator(123);
    auto batch = generator.generate_batch(100);
    std::string payload = wire::WireMapper::serialize_batch(batch);

    financial_pipeline::FinancialDataChunk chunk;
    chunk.set_session_id("corrupt-session");
    chunk.set_chunk_index(0);
    chunk.set_record_count(100);
    chunk.set_payload(payload);
    chunk.set_chunk_checksum(0xDEADBEEF); // Intentional mismatch
    chunk.set_is_last_chunk(true);

    writer->Write(chunk);
    writer->WritesDone();
    grpc::Status status = writer->Finish();

    // Server should reject with DATA_LOSS or error
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::StatusCode::DATA_LOSS);
}
