#include <gtest/gtest.h>
#include "financial_pipeline/domain/session.hpp"

using namespace financial_pipeline::domain;
using namespace financial_pipeline::common;

TEST(SessionTest, InitialStateAndTransitions) {
    TransferSession session("session-test-01");
    auto stats = session.get_statistics();
    EXPECT_EQ(stats.session_id, "session-test-01");
    EXPECT_EQ(stats.state, SessionState::Initialized);
    EXPECT_EQ(stats.total_chunks, 0);

    // Feed chunk 0
    auto status = session.on_chunk_received(0, 100, 1024, 0x12345678);
    EXPECT_TRUE(status.is_ok());

    stats = session.get_statistics();
    EXPECT_EQ(stats.state, SessionState::Active);
    EXPECT_EQ(stats.total_chunks, 1);
    EXPECT_EQ(stats.total_records, 100);
    EXPECT_EQ(stats.total_bytes, 1024);

    // Feed chunk 1
    status = session.on_chunk_received(1, 200, 2048, 0x87654321);
    EXPECT_TRUE(status.is_ok());

    session.complete();
    stats = session.get_statistics();
    EXPECT_EQ(stats.state, SessionState::Completed);
    EXPECT_TRUE(stats.final_status.is_ok());
}

TEST(SessionTest, DetectsSequenceGap) {
    TransferSession session("session-gap-test");
    session.on_chunk_received(0, 50, 512, 0xAAAA);

    // Skip chunk 1, send chunk 2
    auto status = session.on_chunk_received(2, 50, 512, 0xBBBB);
    EXPECT_FALSE(status.is_ok());
    EXPECT_EQ(status.code(), StatusCode::SequenceGap);

    auto stats = session.get_statistics();
    EXPECT_EQ(stats.state, SessionState::Failed);
}

TEST(SessionManagerTest, GetCreateAndRemove) {
    SessionManager mgr;
    EXPECT_EQ(mgr.active_count(), 0);

    auto s1 = mgr.get_or_create("s1");
    EXPECT_NE(s1, nullptr);
    EXPECT_EQ(mgr.active_count(), 1);

    auto s1_again = mgr.find("s1");
    EXPECT_EQ(s1, s1_again);

    EXPECT_EQ(mgr.find("non-existent"), nullptr);

    EXPECT_TRUE(mgr.remove("s1"));
    EXPECT_EQ(mgr.active_count(), 0);
}
