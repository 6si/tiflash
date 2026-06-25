// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <IO/Compression/CompressionCodecLZ4.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <random>
#include <vector>

namespace DB::tests
{

class CodecRevenueComparisonTest : public ::testing::Test
{
protected:
    static constexpr size_t BLOCK_SIZE = 8192;

    static std::vector<Int64> generateRevenueData(size_t count, Int64 min_val = 50, Int64 max_val = 5000)
    {
        std::mt19937_64 gen(42);
        std::uniform_int_distribution<Int64> dist(min_val, max_val);
        std::vector<Int64> data(count);
        for (auto & v : data)
            v = dist(gen);
        return data;
    }

    static std::vector<Int64> generateTimestampData(size_t count)
    {
        std::vector<Int64> data(count);
        Int64 base = 1700000000;
        for (size_t i = 0; i < count; ++i)
            data[i] = base + static_cast<Int64>(i);
        return data;
    }
};

TEST_F(CodecRevenueComparisonTest, LightweightCompressesRevenueData)
{
    auto data = generateRevenueData(BLOCK_SIZE);
    const auto source_size = static_cast<UInt32>(data.size() * sizeof(Int64));

    CompressionCodecLightweight lw_codec(CompressionDataType::Int64, 3);
    std::vector<char> lw_dest(lw_codec.getCompressedReserveSize(source_size));
    auto lw_compressed = lw_codec.compress(reinterpret_cast<const char *>(data.data()), source_size, lw_dest.data());

    CompressionCodecLZ4 lz4_codec(1);
    std::vector<char> lz4_dest(lz4_codec.getCompressedReserveSize(source_size));
    auto lz4_compressed = lz4_codec.compress(reinterpret_cast<const char *>(data.data()), source_size, lz4_dest.data());

    double lw_ratio = static_cast<double>(lw_compressed) / source_size;
    double lz4_ratio = static_cast<double>(lz4_compressed) / source_size;

    // Lightweight (FOR) should compress better than LZ4 for narrow-range integers [50-5000]
    EXPECT_LT(lw_ratio, lz4_ratio) << "Lightweight should compress better than LZ4 for narrow-range integers";

    // Verify decompression correctness
    std::vector<Int64> decompressed(BLOCK_SIZE);
    lw_codec.decompress(
        lw_dest.data(),
        lw_compressed,
        reinterpret_cast<char *>(decompressed.data()),
        source_size);
    EXPECT_EQ(data, decompressed) << "Lightweight decompression must produce identical data";
}

TEST_F(CodecRevenueComparisonTest, LightweightCompressesTimestampData)
{
    auto data = generateTimestampData(BLOCK_SIZE);
    const auto source_size = static_cast<UInt32>(data.size() * sizeof(Int64));

    CompressionCodecLightweight lw_codec(CompressionDataType::Int64, 3);
    std::vector<char> lw_dest(lw_codec.getCompressedReserveSize(source_size));
    auto lw_compressed = lw_codec.compress(reinterpret_cast<const char *>(data.data()), source_size, lw_dest.data());

    CompressionCodecLZ4 lz4_codec(1);
    std::vector<char> lz4_dest(lz4_codec.getCompressedReserveSize(source_size));
    auto lz4_compressed = lz4_codec.compress(reinterpret_cast<const char *>(data.data()), source_size, lz4_dest.data());

    double lw_ratio = static_cast<double>(lw_compressed) / source_size;
    double lz4_ratio = static_cast<double>(lz4_compressed) / source_size;

    // Monotonic timestamps → ConstantDelta mode — extremely compact
    EXPECT_LT(lw_ratio, lz4_ratio) << "Lightweight should compress better than LZ4 for monotonic timestamps";
    EXPECT_LT(lw_ratio, 0.05) << "ConstantDelta should achieve <5% ratio for perfectly monotonic data";

    // Verify decompression
    std::vector<Int64> decompressed(BLOCK_SIZE);
    lw_codec.decompress(
        lw_dest.data(),
        lw_compressed,
        reinterpret_cast<char *>(decompressed.data()),
        source_size);
    EXPECT_EQ(data, decompressed) << "Lightweight decompression must produce identical data";
}

TEST_F(CodecRevenueComparisonTest, LightweightFallsBackForRandomData)
{
    std::mt19937_64 gen(42);
    std::vector<Int64> data(BLOCK_SIZE);
    for (auto & v : data)
        v = static_cast<Int64>(gen());

    const auto source_size = static_cast<UInt32>(data.size() * sizeof(Int64));

    CompressionCodecLightweight lw_codec(CompressionDataType::Int64, 3);
    std::vector<char> lw_dest(lw_codec.getCompressedReserveSize(source_size));
    auto lw_compressed = lw_codec.compress(reinterpret_cast<const char *>(data.data()), source_size, lw_dest.data());

    // Fully random data should fall back to LZ4 — no regression vs raw LZ4
    double lw_ratio = static_cast<double>(lw_compressed) / source_size;
    EXPECT_LT(lw_ratio, 1.10) << "Lightweight should not be significantly worse than no compression for random data";

    // Verify decompression
    std::vector<Int64> decompressed(BLOCK_SIZE);
    lw_codec.decompress(
        lw_dest.data(),
        lw_compressed,
        reinterpret_cast<char *>(decompressed.data()),
        source_size);
    EXPECT_EQ(data, decompressed) << "Lightweight decompression must produce identical data";
}

TEST(CompressionCodecLightweight, MisalignedDataFallback)
{
    // Simulate the crash scenario: Lightweight codec configured for Int64 (8-byte alignment)
    // but receives data that isn't aligned to 8 bytes (e.g., 5 bytes from a partial buffer flush
    // during PreHandleSnapshot). Previously this threw "data size 5 is not aligned to 8".
    // After the fix, it should gracefully fall back to non-integer compression.
    CompressionCodecLightweight codec(CompressionDataType::Int64, 3);

    // 5 bytes — not aligned to 8
    const char source[] = "hello";
    const UInt32 source_size = 5;

    std::vector<char> compressed(codec.getCompressedReserveSize(source_size));
    auto compressed_size = codec.compress(source, source_size, compressed.data());
    ASSERT_GT(compressed_size, 0u);

    // Verify round-trip decompression works
    std::vector<char> decompressed(source_size);
    codec.decompress(compressed.data(), compressed_size, decompressed.data(), source_size);
    EXPECT_EQ(std::string_view(decompressed.data(), source_size), std::string_view(source, source_size));
}

TEST(CompressionCodecLightweight, MisalignedInt32Fallback)
{
    // Int32 codec with 5 bytes (not aligned to 4)
    CompressionCodecLightweight codec(CompressionDataType::Int32, 3);

    const char source[] = "abcde";
    const UInt32 source_size = 5;

    std::vector<char> compressed(codec.getCompressedReserveSize(source_size));
    auto compressed_size = codec.compress(source, source_size, compressed.data());
    ASSERT_GT(compressed_size, 0u);

    std::vector<char> decompressed(source_size);
    codec.decompress(compressed.data(), compressed_size, decompressed.data(), source_size);
    EXPECT_EQ(std::string_view(decompressed.data(), source_size), std::string_view(source, source_size));
}

TEST(CompressionCodecLightweight, AlignedDataStillUsesIntegerPath)
{
    // Verify aligned data still uses the efficient integer compression path
    CompressionCodecLightweight codec(CompressionDataType::Int64, 3);

    // 8 identical Int64 values — should compress well with integer path (constant encoding)
    std::vector<Int64> data(8, 42);
    const auto source_size = static_cast<UInt32>(data.size() * sizeof(Int64));

    std::vector<char> compressed(codec.getCompressedReserveSize(source_size));
    auto compressed_size
        = codec.compress(reinterpret_cast<const char *>(data.data()), source_size, compressed.data());

    // Integer path for constant data should compress well (< 50% of original even with header overhead)
    double ratio = static_cast<double>(compressed_size) / source_size;
    EXPECT_LT(ratio, 0.50) << "Aligned constant data should use integer compression path";

    // Verify round-trip
    std::vector<Int64> decompressed(8);
    codec.decompress(
        compressed.data(),
        compressed_size,
        reinterpret_cast<char *>(decompressed.data()),
        source_size);
    EXPECT_EQ(data, decompressed);
}

} // namespace DB::tests
