#pragma once

#include "financial_pipeline/common/types.hpp"
#include "financial_pipeline/common/status.hpp"
#include "financial_pipeline/common/checksum.hpp"
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <memory>

namespace financial_pipeline {
namespace domain {

enum class SessionState {
    Initialized,
    Active,
    Completed,
    Failed,
    Cancelled
};

struct SessionStatistics {
    common::SessionId session_id;
    SessionState state{SessionState::Initialized};
    common::PipelineStatus final_status{common::PipelineStatus::ok()};
    uint64_t total_chunks{0};
    uint64_t total_records{0};
    uint64_t total_bytes{0};
    uint32_t composite_checksum{common::Crc32cCalculator::InitialValue};
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;

    [[nodiscard]] double elapsed_seconds() const noexcept;
    [[nodiscard]] double throughput_mb_per_sec() const noexcept;
    [[nodiscard]] double records_per_sec() const noexcept;
};

/**
 * @brief Thread-safe state tracker for an individual M2M streaming session.
 */
class TransferSession {
public:
    explicit TransferSession(common::SessionId session_id);

    const common::SessionId& id() const noexcept { return stats_.session_id; }

    /// Register the arrival of a chunk, verifying chunk sequence and updating rolling checksum
    common::PipelineStatus on_chunk_received(
        uint64_t chunk_index,
        uint32_t record_count,
        size_t byte_count,
        uint32_t chunk_crc);

    /// Mark the session as cleanly completed
    void complete();

    /// Mark the session as failed with error
    void fail(common::PipelineStatus status);

    /// Cancel the session
    void cancel();

    /// Get snapshot of current session statistics
    SessionStatistics get_statistics() const;

private:
    mutable std::mutex mutex_;
    SessionStatistics stats_;
    uint64_t next_expected_chunk_index_{0};
};

/**
 * @brief Registry and lifecycle manager for all active and historical M2M transfer sessions.
 */
class SessionManager {
public:
    SessionManager() = default;

    /// Get existing or create new transfer session
    std::shared_ptr<TransferSession> get_or_create(const common::SessionId& session_id);

    /// Find an existing session
    std::shared_ptr<TransferSession> find(const common::SessionId& session_id) const;

    /// Remove a session
    bool remove(const common::SessionId& session_id);

    /// Count of currently registered sessions
    size_t active_count() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<common::SessionId, std::shared_ptr<TransferSession>> sessions_;
};

} // namespace domain
} // namespace financial_pipeline
