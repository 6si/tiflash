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

#include <IO/Compression/CompressionCodecDictionary.h>
#include <IO/Compression/CompressionInfo.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <vector>

namespace DB::tests
{

class CompressionCodecDictionaryTest : public ::testing::Test
{
protected:
    template <typename T>
    void testIntegerRoundtrip(CompressionDataType dt, const std::vector<T> & data)
    {
        CompressionCodecDictionary codec(dt);

        UInt32 source_size = static_cast<UInt32>(data.size() * sizeof(T));
        const char * source = reinterpret_cast<const char *>(data.data());

        // Compress
        UInt32 max_compressed = codec.getCompressedReserveSize(source_size);
        std::vector<char> compressed(max_compressed);
        UInt32 compressed_size = codec.compress(source, source_size, compressed.data());
        ASSERT_GT(compressed_size, 0u);
        ASSERT_LE(compressed_size, max_compressed);

        // Decompress
        std::vector<char> decompressed(source_size);
        codec.decompress(compressed.data(), compressed_size, decompressed.data(), source_size);

        // Verify
        ASSERT_EQ(std::memcmp(source, decompressed.data(), source_size), 0)
            << "Roundtrip failed for " << typeid(T).name();
    }
};

TEST_F(CompressionCodecDictionaryTest, Int32LowCardinality)
{
    // 10 distinct values, 1000 rows — ideal for dictionary encoding
    std::vector<int32_t> data(1000);
    std::mt19937 rng(42);
    for (auto & v : data)
        v = rng() % 10;

    testIntegerRoundtrip<int32_t>(CompressionDataType::Int32, data);
}

TEST_F(CompressionCodecDictionaryTest, Int64LowCardinality)
{
    // 50 distinct values, 10000 rows
    std::vector<int64_t> data(10000);
    std::mt19937 rng(123);
    for (auto & v : data)
        v = static_cast<int64_t>(rng() % 50) * 1000000LL;

    testIntegerRoundtrip<int64_t>(CompressionDataType::Int64, data);
}

TEST_F(CompressionCodecDictionaryTest, Int8AllSameValue)
{
    // All same value — 1 distinct value, extreme case
    std::vector<int8_t> data(500, 42);
    testIntegerRoundtrip<int8_t>(CompressionDataType::Int8, data);
}

TEST_F(CompressionCodecDictionaryTest, Int16TwoValues)
{
    // Alternating two values
    std::vector<int16_t> data(1000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = (i % 2 == 0) ? 100 : -100;

    testIntegerRoundtrip<int16_t>(CompressionDataType::Int16, data);
}

TEST_F(CompressionCodecDictionaryTest, Int32HighCardinality)
{
    // High cardinality (> 4096 distinct values) — should fall back to raw storage
    std::vector<int32_t> data(5000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<int32_t>(i); // Each value is unique

    testIntegerRoundtrip<int32_t>(CompressionDataType::Int32, data);
}

TEST_F(CompressionCodecDictionaryTest, Int32ExactlyMaxCardinality)
{
    // Exactly at the threshold (4096 distinct values)
    std::vector<int32_t> data(8192);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<int32_t>(i % 4096);

    testIntegerRoundtrip<int32_t>(CompressionDataType::Int32, data);
}

TEST_F(CompressionCodecDictionaryTest, EmptyData)
{
    CompressionCodecDictionary codec(CompressionDataType::Int32);

    // ICompressionCodec::compress requires non-null source and source_size > 0.
    // Verify getCompressedReserveSize handles zero gracefully.
    UInt32 reserve = codec.getCompressedReserveSize(0);
    ASSERT_GT(reserve, 0u);
}

TEST_F(CompressionCodecDictionaryTest, SingleElement)
{
    std::vector<int32_t> data = {42};
    testIntegerRoundtrip<int32_t>(CompressionDataType::Int32, data);
}

TEST_F(CompressionCodecDictionaryTest, CompressionRatio)
{
    // With only 10 distinct values across 10000 int32 entries,
    // dictionary encoding should achieve significant compression.
    std::vector<int32_t> data(10000);
    std::mt19937 rng(99);
    for (auto & v : data)
        v = rng() % 10;

    CompressionCodecDictionary codec(CompressionDataType::Int32);
    UInt32 source_size = static_cast<UInt32>(data.size() * sizeof(int32_t));
    UInt32 max_compressed = codec.getCompressedReserveSize(source_size);
    std::vector<char> compressed(max_compressed);
    UInt32 compressed_size = codec.compress(
        reinterpret_cast<const char *>(data.data()),
        source_size,
        compressed.data());

    // With 10 distinct values, we need 4 bits per ID
    // 10000 * 4 bits = 5000 bytes for IDs + ~40 bytes dictionary + header
    // This should be much less than the raw 40000 bytes
    ASSERT_LT(compressed_size, source_size / 2)
        << "Expected significant compression for low-cardinality data";
}

TEST_F(CompressionCodecDictionaryTest, MethodByte)
{
    CompressionCodecDictionary codec(CompressionDataType::Int32);
    ASSERT_EQ(codec.getMethodByte(), static_cast<UInt8>(CompressionMethodByte::Dictionary));
}

TEST_F(CompressionCodecDictionaryTest, IsDataSuitableInt32Low)
{
    std::vector<int32_t> data(1000);
    std::mt19937 rng(42);
    for (auto & v : data)
        v = rng() % 50;

    ASSERT_TRUE(CompressionCodecDictionary::isDataSuitableForDictionary(
        reinterpret_cast<const char *>(data.data()),
        static_cast<UInt32>(data.size() * sizeof(int32_t)),
        CompressionDataType::Int32));
}

TEST_F(CompressionCodecDictionaryTest, IsDataSuitableInt32High)
{
    std::vector<int32_t> data(10000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<int32_t>(i); // All unique

    ASSERT_FALSE(CompressionCodecDictionary::isDataSuitableForDictionary(
        reinterpret_cast<const char *>(data.data()),
        static_cast<UInt32>(data.size() * sizeof(int32_t)),
        CompressionDataType::Int32));
}

TEST_F(CompressionCodecDictionaryTest, BackwardsCompatibility_UnknownMethodByte)
{
    // Verify that the dictionary method byte is 0x96
    CompressionCodecDictionary codec(CompressionDataType::Int32);
    ASSERT_EQ(codec.getMethodByte(), 0x96);
}

TEST_F(CompressionCodecDictionaryTest, BackwardsCompatibility_CompressDecompress)
{
    // Test that data compressed now can be decompressed correctly.
    // This simulates backward compatibility: if the format stays stable,
    // future versions can still read data written by this version.
    CompressionCodecDictionary codec(CompressionDataType::Int64);

    std::vector<int64_t> data = {100, 200, 100, 300, 200, 100, 300, 400, 200, 100};
    UInt32 source_size = static_cast<UInt32>(data.size() * sizeof(int64_t));
    const char * source = reinterpret_cast<const char *>(data.data());

    // Compress
    UInt32 max_compressed = codec.getCompressedReserveSize(source_size);
    std::vector<char> compressed(max_compressed);
    UInt32 compressed_size = codec.compress(source, source_size, compressed.data());
    ASSERT_GT(compressed_size, 0u);

    // The compressed output includes a header (method byte + sizes) prepended by ICompressionCodec.
    // Verify the method byte is correct at the start.
    EXPECT_EQ(static_cast<uint8_t>(compressed[0]), static_cast<uint8_t>(CompressionMethodByte::Dictionary));

    // Decompress — this is the core backward compat test: data written by this version
    // must be correctly readable by the same decompressor.
    std::vector<char> decompressed(source_size);
    codec.decompress(compressed.data(), compressed_size, decompressed.data(), source_size);

    ASSERT_EQ(std::memcmp(source, decompressed.data(), source_size), 0)
        << "Backward compatibility roundtrip failed";

    // Verify a second roundtrip with different data to ensure format stability
    std::vector<int64_t> data2 = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8};
    UInt32 source_size2 = static_cast<UInt32>(data2.size() * sizeof(int64_t));
    UInt32 max_compressed2 = codec.getCompressedReserveSize(source_size2);
    std::vector<char> compressed2(max_compressed2);
    UInt32 compressed_size2 = codec.compress(
        reinterpret_cast<const char *>(data2.data()), source_size2, compressed2.data());

    std::vector<char> decompressed2(source_size2);
    codec.decompress(compressed2.data(), compressed_size2, decompressed2.data(), source_size2);
    ASSERT_EQ(std::memcmp(reinterpret_cast<const char *>(data2.data()), decompressed2.data(), source_size2), 0)
        << "Second roundtrip failed";
}

TEST_F(CompressionCodecDictionaryTest, MixedNullLikeValues)
{
    // Test with values that include 0 (which could be confused with NULL sentinel)
    // and INT64_MIN (which could overflow in subtraction-based encoding)
    std::vector<int64_t> data = {0, INT64_MIN, INT64_MAX, 0, INT64_MIN, INT64_MAX, 42, 0};

    CompressionCodecDictionary codec(CompressionDataType::Int64);
    UInt32 source_size = static_cast<UInt32>(data.size() * sizeof(int64_t));
    const char * source = reinterpret_cast<const char *>(data.data());

    UInt32 max_compressed = codec.getCompressedReserveSize(source_size);
    std::vector<char> compressed(max_compressed);
    UInt32 compressed_size = codec.compress(source, source_size, compressed.data());
    ASSERT_GT(compressed_size, 0u);

    std::vector<char> decompressed(source_size);
    codec.decompress(compressed.data(), compressed_size, decompressed.data(), source_size);

    ASSERT_EQ(std::memcmp(source, decompressed.data(), source_size), 0)
        << "Extreme value roundtrip failed";
}

} // namespace DB::tests
