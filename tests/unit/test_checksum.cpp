#include <gtest/gtest.h>
#include "financial_pipeline/common/checksum.hpp"
#include <string>
#include <vector>

using financial_pipeline::common::Crc32cCalculator;

TEST(ChecksumTest, EmptyBufferProducesZero) {
    uint32_t crc = Crc32cCalculator::calculate("", 0);
    EXPECT_EQ(crc, 0x00000000u);
}

TEST(ChecksumTest, KnownTestVectorStandard) {
    // Standard CRC32C vector for "123456789" is 0xE3069283
    const char* data = "123456789";
    uint32_t crc = Crc32cCalculator::calculate(data, 9);
    EXPECT_EQ(crc, 0xE3069283u);
}

TEST(ChecksumTest, IncrementalEqualsSinglePass) {
    std::string part1 = "FinancialMarketDataStreaming";
    std::string part2 = "HighThroughputPipelineValidation";
    std::string combined = part1 + part2;

    uint32_t single_pass = Crc32cCalculator::calculate(combined.data(), combined.size());

    uint32_t rolling = Crc32cCalculator::InitialValue;
    rolling = Crc32cCalculator::update(rolling, part1.data(), part1.size());
    rolling = Crc32cCalculator::update(rolling, part2.data(), part2.size());
    uint32_t multi_pass = Crc32cCalculator::finalize(rolling);

    EXPECT_EQ(single_pass, multi_pass);
}

TEST(ChecksumTest, DetectsSingleBitFlip) {
    std::vector<uint8_t> buffer = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    uint32_t crc1 = Crc32cCalculator::calculate(buffer.data(), buffer.size());

    // Flip 1 bit
    buffer[3] ^= 0x01;
    uint32_t crc2 = Crc32cCalculator::calculate(buffer.data(), buffer.size());

    EXPECT_NE(crc1, crc2);
}

TEST(ChecksumTest, CombineChecksumsDeterministic) {
    uint32_t c1 = 0x12345678;
    uint32_t c2 = 0x87654321;
    uint32_t comp1 = Crc32cCalculator::combine_checksums(c1, c2, 100);
    uint32_t comp2 = Crc32cCalculator::combine_checksums(c1, c2, 100);

    EXPECT_EQ(comp1, comp2);

    // Different index must produce different composite
    uint32_t comp3 = Crc32cCalculator::combine_checksums(c1, c2, 101);
    EXPECT_NE(comp1, comp3);
}
