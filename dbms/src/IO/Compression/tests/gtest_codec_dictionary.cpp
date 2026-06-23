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

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/Compression/CompressionCodecDictionary.h>
#include <IO/VarInt.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace DB::tests
{

class CompressionCodecDictionaryTest : public ::testing::Test
{
protected:
    CompressionCodecDictionary codec;

    /// Build SizePrefix-format buffer from a vector of strings:
    ///   [VarUInt len][bytes] per string
    static std::string buildSizePrefixData(const std::vector<std::string> & strings)
    {
        std::string buf;
        for (const auto & s : strings)
        {
            char tmp[16];
            char * end = writeVarUInt(static_cast<UInt64>(s.size()), tmp);
            buf.append(tmp, end - tmp);
            buf.append(s);
        }
        return buf;
    }

    /// Parse SizePrefix-format buffer back to a vector of strings
    static std::vector<std::string> parseSizePrefixData(const char * data, size_t size)
    {
        std::vector<std::string> result;
        const char * pos = data;
        const char * end = data + size;
        while (pos < end)
        {
            UInt64 len = 0;
            pos = readVarUInt(len, pos, end - pos);
            result.emplace_back(pos, len);
            pos += len;
        }
        return result;
    }

    /// Compress using the public API (which adds a header)
    std::string compressWithHeader(const std::string & source)
    {
        UInt32 source_size = static_cast<UInt32>(source.size());
        UInt32 reserve = codec.getCompressedReserveSize(source_size);
        std::string compressed(reserve, '\0');
        UInt32 total = codec.compress(source.data(), source_size, compressed.data());
        compressed.resize(total);
        return compressed;
    }

    /// Decompress using the public API (reads the header)
    std::string decompressWithHeader(const std::string & compressed, UInt32 uncompressed_size)
    {
        std::string decompressed(uncompressed_size, '\0');
        codec.decompress(compressed.data(), static_cast<UInt32>(compressed.size()), decompressed.data(), uncompressed_size);
        return decompressed;
    }

    /// Get the compressed payload (after header) to inspect format details
    static const char * getPayload(const std::string & compressed)
    {
        return compressed.data() + ICompressionCodec::getHeaderSize();
    }
};

TEST_F(CompressionCodecDictionaryTest, RoundTrip_LowCardinality)
{
    std::vector<std::string> input = {
        "active", "inactive", "pending", "active", "deleted",
        "active", "inactive", "pending", "active", "suspended",
        "active", "inactive", "pending", "deleted", "active",
        "suspended", "active", "inactive", "pending", "active",
    };
    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);

    // Verify index_width = 1 (UInt8) since NDV = 5
    ASSERT_EQ(static_cast<UInt8>(*getPayload(compressed)), 1);

    auto decompressed = decompressWithHeader(compressed, source_size);
    auto output = parseSizePrefixData(decompressed.data(), source_size);
    ASSERT_EQ(output, input);
}

TEST_F(CompressionCodecDictionaryTest, AdaptiveWidth_UInt16)
{
    // 300 distinct values → UInt16 index width
    std::vector<std::string> input;
    input.reserve(600);
    for (int i = 0; i < 300; ++i)
        input.push_back("val_" + std::to_string(i));
    for (int i = 0; i < 300; ++i)
        input.push_back("val_" + std::to_string(i));

    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);

    // Verify index_width = 2 (UInt16) since NDV = 300 > 256
    ASSERT_EQ(static_cast<UInt8>(*getPayload(compressed)), 2);

    auto decompressed = decompressWithHeader(compressed, source_size);
    auto output = parseSizePrefixData(decompressed.data(), source_size);
    ASSERT_EQ(output, input);
}

TEST_F(CompressionCodecDictionaryTest, RawFallback_HighCardinality)
{
    // 70000 distinct values → exceeds MAX_DICT_SIZE (65536) → raw fallback
    std::vector<std::string> input;
    input.reserve(70000);
    for (int i = 0; i < 70000; ++i)
        input.push_back("unique_" + std::to_string(i));

    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);

    // Verify index_width = 0 (raw fallback)
    ASSERT_EQ(static_cast<UInt8>(*getPayload(compressed)), 0);

    auto decompressed = decompressWithHeader(compressed, source_size);
    auto output = parseSizePrefixData(decompressed.data(), source_size);
    ASSERT_EQ(output, input);
}

TEST_F(CompressionCodecDictionaryTest, DecompressAsColumnDictionary_Basic)
{
    // Need enough rows so dictionary encoding is smaller than raw fallback
    std::vector<std::string> input;
    std::vector<std::string> values = {"alpha", "bravo", "charlie"};
    for (int i = 0; i < 200; ++i)
        input.push_back(values[i % 3]);
    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);

    // Extract payload (skip header) for decompressAsColumnDictionary
    const char * payload = getPayload(compressed);
    UInt32 payload_size = static_cast<UInt32>(compressed.size()) - ICompressionCodec::getHeaderSize();

    auto value_type = std::make_shared<DataTypeString>();
    auto col = codec.decompressAsColumnDictionary(payload, payload_size, source_size, value_type);
    ASSERT_NE(col, nullptr);

    auto * dict_col = typeid_cast<const ColumnDictionary *>(col.get());
    ASSERT_NE(dict_col, nullptr);
    ASSERT_EQ(dict_col->size(), 200u);
    ASSERT_EQ(dict_col->getDictionarySize(), 3u); // 3 distinct values: alpha, bravo, charlie

    // Verify values via decode
    auto decoded = dict_col->decode();
    auto * str_col = typeid_cast<const ColumnString *>(decoded.get());
    ASSERT_NE(str_col, nullptr);
    ASSERT_EQ(str_col->size(), 200u);
    for (size_t i = 0; i < input.size(); ++i)
    {
        ASSERT_EQ(str_col->getDataAt(i).toString(), input[i]) << "Mismatch at row " << i;
    }
}

TEST_F(CompressionCodecDictionaryTest, DecompressAsColumnDictionary_RawFallbackReturnsNull)
{
    std::vector<std::string> input;
    for (int i = 0; i < 70000; ++i)
        input.push_back("u_" + std::to_string(i));

    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);
    const char * payload = getPayload(compressed);
    UInt32 payload_size = static_cast<UInt32>(compressed.size()) - ICompressionCodec::getHeaderSize();

    auto value_type = std::make_shared<DataTypeString>();
    auto col = codec.decompressAsColumnDictionary(payload, payload_size, source_size, value_type);
    ASSERT_EQ(col, nullptr);
}

TEST_F(CompressionCodecDictionaryTest, SingleDistinctValue)
{
    std::vector<std::string> input(1000, "active");
    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);

    // Compressed payload should be much smaller than source
    UInt32 payload_size = static_cast<UInt32>(compressed.size()) - ICompressionCodec::getHeaderSize();
    ASSERT_LT(payload_size, source_size / 3);

    auto decompressed = decompressWithHeader(compressed, source_size);
    auto output = parseSizePrefixData(decompressed.data(), source_size);
    ASSERT_EQ(output, input);
}

TEST_F(CompressionCodecDictionaryTest, EmptyStrings)
{
    std::vector<std::string> input = {"", "", "a", "", "b", ""};
    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);
    auto decompressed = decompressWithHeader(compressed, source_size);
    auto output = parseSizePrefixData(decompressed.data(), source_size);
    ASSERT_EQ(output, input);
}

TEST_F(CompressionCodecDictionaryTest, MethodByte)
{
    ASSERT_EQ(codec.getMethodByte(), static_cast<UInt8>(CompressionMethodByte::Dictionary));
}

TEST_F(CompressionCodecDictionaryTest, CompressionRatio_5NDV_100Krows)
{
    std::vector<std::string> values = {"active", "inactive", "pending", "deleted", "suspended"};
    std::vector<std::string> input;
    input.reserve(100000);
    for (int i = 0; i < 100000; ++i)
        input.push_back(values[i % 5]);

    auto source = buildSizePrefixData(input);
    UInt32 source_size = static_cast<UInt32>(source.size());

    auto compressed = compressWithHeader(source);
    UInt32 payload_size = static_cast<UInt32>(compressed.size()) - ICompressionCodec::getHeaderSize();

    double ratio = static_cast<double>(source_size) / payload_size;
    ASSERT_GT(ratio, 5.0) << "Compression ratio " << ratio << " is too low for 5 NDV / 100K rows";

    auto decompressed = decompressWithHeader(compressed, source_size);
    auto output = parseSizePrefixData(decompressed.data(), source_size);
    ASSERT_EQ(output, input);
}

} // namespace DB::tests
