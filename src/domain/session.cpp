#include "financial_pipeline/domain/session.hpp"

namespace financial_pipeline {
namespace domain {

double SessionStatistics::elapsed_seconds() const noexcept {
    auto end = (state == SessionState::Active || state == SessionState::Initialized)
                   ? std::chrono::steady_clock::now()
                   : end_time;
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_time).count();
    return duration_ns > 0 ? static_cast<double>(duration_ns) / 1e9 : 0.000001;
}

double SessionStatistics::throughput_mb_per_sec() const noexcept {
    double sec = elapsed_seconds();
    if (sec <= 0.0) return 0.0;
    double mb = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
    return mb / sec;
}

double SessionStatistics::records_per_sec() const noexcept {
    double sec = elapsed_seconds();
    if (sec <= 0.0) return 0.0;
    return static_cast<double>(total_records) / sec;
}

TransferSession::TransferSession(common::SessionId session_id) {
    stats_.session_id = std::move(session_id);
    stats_.state = SessionState::Initialized;
    stats_.start_time = std::chrono::steady_clock::now();
    stats_.composite_checksum = common::Crc32cCalculator::InitialValue;
}

common::PipelineStatus TransferSession::on_chunk_received(
    uint64_t chunk_index,
    uint32_t record_count,
    size_t byte_count,
    uint32_t chunk_crc) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (stats_.state == SessionState::Initialized) {
        stats_.state = SessionState::Active;
        stats_.start_time = std::chrono::steady_clock::now();
    } else if (stats_.state != SessionState::Active) {
        return common::PipelineStatus::invalid_argument("Session is not active; state is " +
            std::to_string(static_cast<int>(stats_.state)));
    }

    // Sequence verification
    if (chunk_index != next_expected_chunk_index_) {
        stats_.state = SessionState::Failed;
        stats_.final_status = common::PipelineStatus::sequence_gap(
            "Chunk sequence mismatch: expected " + std::to_string(next_expected_chunk_index_) +
            ", got " + std::to_string(chunk_index));
        stats_.end_time = std::chrono::steady_clock::now();
        return stats_.final_status;
    }

    // Update session metrics
    stats_.total_chunks++;
    stats_.total_records += record_count;
    stats_.total_bytes += byte_count;
    stats_.composite_checksum = common::Crc32cCalculator::combine_checksums(
        stats_.composite_checksum, chunk_crc, chunk_index);

    next_expected_chunk_index_++;
    return common::PipelineStatus::ok();
}

void TransferSession::complete() {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.state = SessionState::Completed;
    stats_.end_time = std::chrono::steady_clock::now();
    stats_.final_status = common::PipelineStatus::ok();
}

void TransferSession::fail(common::PipelineStatus status) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.state = SessionState::Failed;
    stats_.end_time = std::chrono::steady_clock::now();
    stats_.final_status = std::move(status);
}

void TransferSession::cancel() {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.state = SessionState::Cancelled;
    stats_.end_time = std::chrono::steady_clock::now();
    stats_.final_status = common::PipelineStatus::cancelled("Session cancelled by client or server");
}

SessionStatistics TransferSession::get_statistics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

std::shared_ptr<TransferSession> SessionManager::get_or_create(const common::SessionId& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        return it->second;
    }
    auto session = std::make_shared<TransferSession>(session_id);
    sessions_[session_id] = session;
    return session;
}

std::shared_ptr<TransferSession> SessionManager::find(const common::SessionId& session_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        return it->second;
    }
    return nullptr;
}

bool SessionManager::remove(const common::SessionId& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return sessions_.erase(session_id) > 0;
}

size_t SessionManager::active_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sessions_.size();
}

} // namespace domain
} // namespace financial_pipeline
