#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace financial_pipeline {
namespace common {

enum class StatusCode : int {
    Ok = 0,
    InvalidArgument = 1,
    ChecksumMismatch = 2,
    SequenceGap = 3,
    NetworkError = 4,
    IoError = 5,
    InternalError = 6,
    Cancelled = 7
};

class PipelineStatus {
public:
    PipelineStatus() : code_(StatusCode::Ok), message_("") {}
    explicit PipelineStatus(StatusCode code, std::string message = "")
        : code_(code), message_(std::move(message)) {}

    static PipelineStatus ok() {
        return PipelineStatus(StatusCode::Ok);
    }

    static PipelineStatus invalid_argument(std::string message) {
        return PipelineStatus(StatusCode::InvalidArgument, std::move(message));
    }

    static PipelineStatus checksum_mismatch(std::string message) {
        return PipelineStatus(StatusCode::ChecksumMismatch, std::move(message));
    }

    static PipelineStatus sequence_gap(std::string message) {
        return PipelineStatus(StatusCode::SequenceGap, std::move(message));
    }

    static PipelineStatus network_error(std::string message) {
        return PipelineStatus(StatusCode::NetworkError, std::move(message));
    }

    static PipelineStatus io_error(std::string message) {
        return PipelineStatus(StatusCode::IoError, std::move(message));
    }

    static PipelineStatus internal_error(std::string message) {
        return PipelineStatus(StatusCode::InternalError, std::move(message));
    }

    static PipelineStatus cancelled(std::string message) {
        return PipelineStatus(StatusCode::Cancelled, std::move(message));
    }

    bool is_ok() const noexcept { return code_ == StatusCode::Ok; }
    StatusCode code() const noexcept { return code_; }
    const std::string& message() const noexcept { return message_; }

    std::string to_string() const {
        if (is_ok()) return "OK";
        std::string res;
        switch (code_) {
            case StatusCode::InvalidArgument: res = "INVALID_ARGUMENT: "; break;
            case StatusCode::ChecksumMismatch: res = "CHECKSUM_MISMATCH: "; break;
            case StatusCode::SequenceGap: res = "SEQUENCE_GAP: "; break;
            case StatusCode::NetworkError: res = "NETWORK_ERROR: "; break;
            case StatusCode::IoError: res = "IO_ERROR: "; break;
            case StatusCode::InternalError: res = "INTERNAL_ERROR: "; break;
            case StatusCode::Cancelled: res = "CANCELLED: "; break;
            default: res = "UNKNOWN_ERROR: "; break;
        }
        res += message_;
        return res;
    }

private:
    StatusCode code_;
    std::string message_;
};

template <typename T>
class Result {
public:
    Result(T val) : storage_(std::move(val)) {}
    Result(PipelineStatus status) : storage_(std::move(status)) {}

    bool is_ok() const noexcept {
        return std::holds_alternative<T>(storage_);
    }

    const T& value() const & {
        return std::get<T>(storage_);
    }

    T& value() & {
        return std::get<T>(storage_);
    }

    T&& value() && {
        return std::get<T>(std::move(storage_));
    }

    const PipelineStatus& status() const noexcept {
        if (is_ok()) {
            static const PipelineStatus s_ok = PipelineStatus::ok();
            return s_ok;
        }
        return std::get<PipelineStatus>(storage_);
    }

    const T* operator->() const noexcept {
        return &value();
    }

    T* operator->() noexcept {
        return &value();
    }

    const T& operator*() const noexcept {
        return value();
    }

    T& operator*() noexcept {
        return value();
    }

private:
    std::variant<T, PipelineStatus> storage_;
};

} // namespace common
} // namespace financial_pipeline
