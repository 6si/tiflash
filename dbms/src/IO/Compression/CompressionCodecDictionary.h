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

#pragma once

#include <Columns/ColumnDictionary.h>
#include <IO/Compression/ICompressionCodec.h>

#include <cstdint>
#include <vector>

namespace DB
{

/// Maximum number of distinct values to use dictionary encoding.
/// If cardinality exceeds this threshold, we fall back to LZ4.
static constexpr uint32_t DICTIONARY_ENCODING_MAX_CARDINALITY = 4096;

/**
 * @brief Dictionary compression codec for low-cardinality column data.
 *
 * Compressed format:
 *   [1 byte]  bit_width — number of bits per dictionary ID (1..16)
 *   [4 bytes] dict_size — number of dictionary entries (little-endian uint32)
 *   [4 bytes] num_values — number of encoded values (little-endian uint32)
 *   [4 bytes] avg_value_len — average serialized length of dict entries (only for string mode)
 *   [1 byte]  data_type_byte — CompressionDataType of the values
 *   [variable] dictionary entries (for integers: sizeof(T)*dict_size bytes;
 *              for strings: [4-byte len][data] per entry)
 *   [variable] packed IDs — bit-packed dictionary indices
 *
 * This codec supports:
 *   - Integer types (1/2/4/8 byte): entries stored as raw values
 *   - String/non-integer types: entries stored as length-prefixed byte arrays
 *
 * When cardinality > DICTIONARY_ENCODING_MAX_CARDINALITY, the codec falls back
 * to storing data uncompressed with a special flag.
 */
class CompressionCodecDictionary : public ICompressionCodec
{
public:
    explicit CompressionCodecDictionary(CompressionDataType data_type_);

    UInt8 getMethodByte() const override;

    ~CompressionCodecDictionary() override = default;

    bool isCompression() const override { return true; }

    /// Check if data is suitable for dictionary encoding (low cardinality).
    /// Returns true if the distinct count <= DICTIONARY_ENCODING_MAX_CARDINALITY.
    static bool isDataSuitableForDictionary(const char * source, UInt32 source_size, CompressionDataType data_type);

    /// Decompress a dictionary-compressed block directly into a ColumnDictionary,
    /// bypassing the string-rebuild step. Returns nullptr if the block uses the
    /// fallback marker (non-dictionary data) or is not String-typed.
    /// @param source     compressed data AFTER the ICompressionCodec 9-byte header
    /// @param source_size  size of that data
    /// @param value_type   DataType for the ColumnDictionary entries
    static ColumnPtr decompressToColumnDictionary(
        const char * source,
        UInt32 source_size,
        const DataTypePtr & value_type);

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

private:
    /// Compress integer data using dictionary encoding
    template <std::integral T>
    UInt32 compressDataInteger(const char * source, UInt32 source_size, char * dest) const;

    /// Decompress integer data using dictionary encoding
    template <std::integral T>
    void decompressDataInteger(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const;

    /// Compress string/non-integer data using dictionary encoding
    UInt32 compressDataString(const char * source, UInt32 source_size, char * dest) const;

    /// Decompress string/non-integer data using dictionary encoding
    void decompressDataString(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const;

    const CompressionDataType data_type;
};

} // namespace DB
