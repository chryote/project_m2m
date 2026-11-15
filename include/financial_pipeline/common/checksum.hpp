#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <array>

namespace financial_pipeline {
namespace common {

/**
 * @brief High-performance CRC32C (Castagnoli) calculator for stream integrity verification.
 * 
 * Polynomial: 0x82F63B78 (reversed Castagnoli).
 * Guarantees detection of transmission bit flips, sequence corruptions, and truncations.
 */
class Crc32cCalculator {
public:
    static constexpr uint32_t InitialValue = 0xFFFFFFFFu;

    /// Calculate CRC32C of a single contiguous buffer
    static uint32_t calculate(const void* data, size_t length) noexcept;

    /// Calculate CRC32C of a string_view
    static uint32_t calculate(std::string_view sv) noexcept {
        return calculate(sv.data(), sv.size());
    }

    /// Update an ongoing rolling CRC32C calculation across streaming chunks
    static uint32_t update(uint32_t previous_crc, const void* data, size_t length) noexcept;

    /// Finalize a rolling CRC32C (inverts bits)
    static uint32_t finalize(uint32_t rolling_crc) noexcept {
        return rolling_crc ^ InitialValue;
    }

    /// Combined rolling hash update for chunk checksums
    static uint32_t combine_checksums(uint32_t composite, uint32_t chunk_crc, uint64_t chunk_index) noexcept;

private:
    static const std::array<uint32_t, 256>& get_table() noexcept;
    static std::array<uint32_t, 256> generate_table() noexcept;
};

} // namespace common
} // namespace financial_pipeline
