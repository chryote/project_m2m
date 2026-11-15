#include "financial_pipeline/common/checksum.hpp"
#include <cstring>

namespace financial_pipeline {
namespace common {

std::array<uint32_t, 256> Crc32cCalculator::generate_table() noexcept {
    constexpr uint32_t polynomial = 0x82F63B78u;
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? ((crc >> 1) ^ polynomial) : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

const std::array<uint32_t, 256>& Crc32cCalculator::get_table() noexcept {
    static const auto table = generate_table();
    return table;
}

uint32_t Crc32cCalculator::calculate(const void* data, size_t length) noexcept {
    uint32_t crc = update(InitialValue, data, length);
    return finalize(crc);
}

uint32_t Crc32cCalculator::update(uint32_t previous_crc, const void* data, size_t length) noexcept {
    const auto& table = get_table();
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = previous_crc;

    for (size_t i = 0; i < length; ++i) {
        uint8_t index = static_cast<uint8_t>(crc ^ bytes[i]);
        crc = (crc >> 8) ^ table[index];
    }
    return crc;
}

uint32_t Crc32cCalculator::combine_checksums(uint32_t composite, uint32_t chunk_crc, uint64_t chunk_index) noexcept {
    // Pack exactly 12 bytes without struct alignment padding
    uint8_t packed[12];
    std::memcpy(packed, &chunk_crc, sizeof(chunk_crc));
    std::memcpy(packed + sizeof(chunk_crc), &chunk_index, sizeof(chunk_index));

    return update(composite, packed, sizeof(packed));
}

} // namespace common
} // namespace financial_pipeline
