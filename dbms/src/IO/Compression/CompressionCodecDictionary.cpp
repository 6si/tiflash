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

#include <Common/BitpackingPrimitives.h>
#include <Common/Exception.h>
#include <IO/Compression/CompressionInfo.h>
#include <common/unaligned.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

namespace
{

/// Header layout:
///   [1] bit_width
///   [4] dict_size (uint32 LE)
///   [4] num_values (uint32 LE)
///   [1] data_type_byte
/// Total fixed header: 10 bytes
static constexpr size_t DICT_HEADER_SIZE = 10;

/// Fallback marker: bit_width == 0 means data is stored uncompressed (cardinality too high)
static constexpr uint8_t DICT_FALLBACK_MARKER = 0;

inline uint8_t bitsNeeded(uint32_t max_value)
{
    if (max_value == 0)
        return 1;
    return static_cast<uint8_t>(std::bit_width(max_value));
}

} // anonymous namespace

CompressionCodecDictionary::CompressionCodecDictionary(CompressionDataType data_type_)
    : data_type(data_type_)
{}

UInt8 CompressionCodecDictionary::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Dictionary);
}

UInt32 CompressionCodecDictionary::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // Worst case for dictionary path: header + dictionary entries + bit-packed IDs
    // Dictionary entries can be up to uncompressed_size bytes.
    // Bit-packed IDs: BitpackingPrimitives rounds up to groups of 32, with up to 32 bits each.
    // Safe upper bound: header + raw data + one extra bitpacking group (32 * 4 bytes).
    return DICT_HEADER_SIZE + uncompressed_size + 128;
}

bool CompressionCodecDictionary::isDataSuitableForDictionary(
    const char * source,
    UInt32 source_size,
    CompressionDataType data_type)
{
    if (source_size == 0)
        return false;

    switch (data_type)
    {
    case CompressionDataType::Int8:
    case CompressionDataType::Int16:
    case CompressionDataType::Int32:
    case CompressionDataType::Int64:
    {
        size_t elem_size = static_cast<size_t>(data_type);
        size_t count = source_size / elem_size;
        if (count == 0)
            return false;

        // Sample-based cardinality estimation for large data
        std::unordered_map<uint64_t, bool> seen;
        size_t sample_size = std::min(count, static_cast<size_t>(8192));
        size_t step = count / sample_size;
        if (step == 0)
            step = 1;

        for (size_t i = 0; i < count && seen.size() <= DICTIONARY_ENCODING_MAX_CARDINALITY; i += step)
        {
            uint64_t val = 0;
            std::memcpy(&val, source + i * elem_size, elem_size);
            seen[val] = true;
        }
        return seen.size() <= DICTIONARY_ENCODING_MAX_CARDINALITY;
    }
    case CompressionDataType::String:
    {
        // For string data, count distinct values by scanning length-prefixed entries
        std::unordered_map<std::string_view, bool> seen;
        size_t offset = 0;
        while (offset + 4 <= source_size && seen.size() <= DICTIONARY_ENCODING_MAX_CARDINALITY)
        {
            uint32_t len = unalignedLoad<uint32_t>(source + offset);
            offset += 4;
            if (offset + len > source_size)
                break;
            seen[std::string_view(source + offset, len)] = true;
            offset += len;
        }
        return seen.size() <= DICTIONARY_ENCODING_MAX_CARDINALITY;
    }
    default:
        return false;
    }
}

template <std::integral T>
UInt32 CompressionCodecDictionary::compressDataInteger(const char * source, UInt32 source_size, char * dest) const
{
    const size_t count = source_size / sizeof(T);
    if (count == 0)
    {
        // Fallback: store raw
        dest[0] = DICT_FALLBACK_MARKER;
        unalignedStore<uint32_t>(dest + 1, 0);
        unalignedStore<uint32_t>(dest + 5, 0);
        dest[9] = static_cast<uint8_t>(data_type);
        return DICT_HEADER_SIZE;
    }

    const T * values = reinterpret_cast<const T *>(source);

    // Build dictionary
    std::unordered_map<T, uint32_t> value_to_id;
    std::vector<T> dictionary;
    std::vector<uint32_t> ids(count);

    for (size_t i = 0; i < count; ++i)
    {
        auto it = value_to_id.find(values[i]);
        if (it == value_to_id.end())
        {
            if (dictionary.size() >= DICTIONARY_ENCODING_MAX_CARDINALITY)
            {
                // Cardinality too high — fallback to raw storage
                dest[0] = DICT_FALLBACK_MARKER;
                unalignedStore<uint32_t>(dest + 1, 0);
                unalignedStore<uint32_t>(dest + 5, static_cast<uint32_t>(count));
                dest[9] = static_cast<uint8_t>(data_type);
                std::memcpy(dest + DICT_HEADER_SIZE, source, source_size);
                return DICT_HEADER_SIZE + source_size;
            }
            uint32_t id = static_cast<uint32_t>(dictionary.size());
            value_to_id[values[i]] = id;
            dictionary.push_back(values[i]);
            ids[i] = id;
        }
        else
        {
            ids[i] = it->second;
        }
    }

    uint32_t dict_size = static_cast<uint32_t>(dictionary.size());
    uint8_t bit_width = bitsNeeded(dict_size - 1);

    // Write header
    char * out = dest;
    out[0] = bit_width;
    unalignedStore<uint32_t>(out + 1, dict_size);
    unalignedStore<uint32_t>(out + 5, static_cast<uint32_t>(count));
    out[9] = static_cast<uint8_t>(data_type);
    out += DICT_HEADER_SIZE;

    // Write dictionary entries
    for (const auto & entry : dictionary)
    {
        unalignedStore<T>(out, entry);
        out += sizeof(T);
    }

    // Write bit-packed IDs
    size_t packed_size = BitpackingPrimitives::getRequiredSize(count, bit_width);
    // Convert ids to the format expected by bitpacking (need uint32_t array)
    auto round_count = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    std::vector<uint32_t> padded_ids(round_count, 0);
    std::memcpy(padded_ids.data(), ids.data(), count * sizeof(uint32_t));

    BitpackingPrimitives::packBuffer(
        reinterpret_cast<unsigned char *>(out),
        padded_ids.data(),
        count,
        bit_width);
    out += packed_size;

    return static_cast<UInt32>(out - dest);
}

template <std::integral T>
void CompressionCodecDictionary::decompressDataInteger(
    const char * source,
    UInt32 /*source_size*/,
    char * dest,
    UInt32 uncompressed_size) const
{
    const size_t count = uncompressed_size / sizeof(T);
    const char * src = source;

    uint8_t bit_width = static_cast<uint8_t>(src[0]);
    uint32_t dict_size = unalignedLoad<uint32_t>(src + 1);
    uint32_t num_values = unalignedLoad<uint32_t>(src + 5);
    // uint8_t dt_byte = static_cast<uint8_t>(src[9]);
    src += DICT_HEADER_SIZE;

    if (bit_width == DICT_FALLBACK_MARKER)
    {
        // Fallback: data is stored raw after header
        std::memcpy(dest, src, uncompressed_size);
        return;
    }

    if (num_values != count)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Dictionary decompress: expected {} values but header says {}",
            count,
            num_values);

    // Read dictionary
    std::vector<T> dictionary(dict_size);
    for (uint32_t i = 0; i < dict_size; ++i)
    {
        dictionary[i] = unalignedLoad<T>(src);
        src += sizeof(T);
    }

    // Unpack IDs
    auto round_count = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
    std::vector<uint32_t> ids(round_count, 0);
    BitpackingPrimitives::unPackBuffer<uint32_t>(
        reinterpret_cast<unsigned char *>(ids.data()),
        reinterpret_cast<const unsigned char *>(src),
        count,
        bit_width);

    // Lookup dictionary and write output
    T * out = reinterpret_cast<T *>(dest);
    for (size_t i = 0; i < count; ++i)
    {
        if (ids[i] >= dict_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Dictionary decompress: ID {} exceeds dict_size {}",
                ids[i],
                dict_size);
        out[i] = dictionary[ids[i]];
    }
}

UInt32 CompressionCodecDictionary::compressDataString(const char * source, UInt32 source_size, char * dest) const
{
    // Parse length-prefixed strings and build dictionary
    std::unordered_map<std::string, uint32_t> value_to_id;
    std::vector<std::string> dictionary;
    std::vector<uint32_t> ids;

    size_t offset = 0;
    while (offset + 4 <= source_size)
    {
        uint32_t len = unalignedLoad<uint32_t>(source + offset);
        offset += 4;
        if (offset + len > source_size)
            break;

        std::string val(source + offset, len);
        offset += len;

        auto it = value_to_id.find(val);
        if (it == value_to_id.end())
        {
            if (dictionary.size() >= DICTIONARY_ENCODING_MAX_CARDINALITY)
            {
                // Fallback
                dest[0] = DICT_FALLBACK_MARKER;
                unalignedStore<uint32_t>(dest + 1, 0);
                unalignedStore<uint32_t>(dest + 5, static_cast<uint32_t>(ids.size() + 1));
                dest[9] = static_cast<uint8_t>(CompressionDataType::String);
                std::memcpy(dest + DICT_HEADER_SIZE, source, source_size);
                return DICT_HEADER_SIZE + source_size;
            }
            uint32_t id = static_cast<uint32_t>(dictionary.size());
            value_to_id[val] = id;
            dictionary.push_back(std::move(val));
            ids.push_back(id);
        }
        else
        {
            ids.push_back(it->second);
        }
    }

    uint32_t dict_size = static_cast<uint32_t>(dictionary.size());
    uint32_t num_values = static_cast<uint32_t>(ids.size());
    uint8_t bit_width = dict_size > 0 ? bitsNeeded(dict_size - 1) : 1;

    char * out = dest;
    out[0] = bit_width;
    unalignedStore<uint32_t>(out + 1, dict_size);
    unalignedStore<uint32_t>(out + 5, num_values);
    out[9] = static_cast<uint8_t>(CompressionDataType::String);
    out += DICT_HEADER_SIZE;

    // Write dictionary: [4-byte len][data] per entry
    for (const auto & entry : dictionary)
    {
        uint32_t entry_len = static_cast<uint32_t>(entry.size());
        unalignedStore<uint32_t>(out, entry_len);
        out += 4;
        std::memcpy(out, entry.data(), entry_len);
        out += entry_len;
    }

    // Write bit-packed IDs
    size_t packed_size = BitpackingPrimitives::getRequiredSize(num_values, bit_width);
    auto round_count = BitpackingPrimitives::roundUpToAlgorithmGroupSize(num_values);
    std::vector<uint32_t> padded_ids(round_count, 0);
    std::memcpy(padded_ids.data(), ids.data(), num_values * sizeof(uint32_t));

    BitpackingPrimitives::packBuffer(
        reinterpret_cast<unsigned char *>(out),
        padded_ids.data(),
        num_values,
        bit_width);
    out += packed_size;

    return static_cast<UInt32>(out - dest);
}

void CompressionCodecDictionary::decompressDataString(
    const char * source,
    UInt32 /*source_size*/,
    char * dest,
    UInt32 uncompressed_size) const
{
    const char * src = source;

    uint8_t bit_width = static_cast<uint8_t>(src[0]);
    uint32_t dict_size = unalignedLoad<uint32_t>(src + 1);
    uint32_t num_values = unalignedLoad<uint32_t>(src + 5);
    // uint8_t dt_byte = static_cast<uint8_t>(src[9]);
    src += DICT_HEADER_SIZE;

    if (bit_width == DICT_FALLBACK_MARKER)
    {
        std::memcpy(dest, src, uncompressed_size);
        return;
    }

    // Read dictionary entries
    std::vector<std::string> dictionary(dict_size);
    for (uint32_t i = 0; i < dict_size; ++i)
    {
        uint32_t entry_len = unalignedLoad<uint32_t>(src);
        src += 4;
        dictionary[i] = std::string(src, entry_len);
        src += entry_len;
    }

    // Unpack IDs
    auto round_count = BitpackingPrimitives::roundUpToAlgorithmGroupSize(num_values);
    std::vector<uint32_t> ids(round_count, 0);
    BitpackingPrimitives::unPackBuffer<uint32_t>(
        reinterpret_cast<unsigned char *>(ids.data()),
        reinterpret_cast<const unsigned char *>(src),
        num_values,
        bit_width);

    // Write output: [4-byte len][data] per value
    char * out = dest;
    for (uint32_t i = 0; i < num_values; ++i)
    {
        if (ids[i] >= dict_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Dictionary decompress string: ID {} exceeds dict_size {}",
                ids[i],
                dict_size);
        const auto & entry = dictionary[ids[i]];
        uint32_t entry_len = static_cast<uint32_t>(entry.size());
        unalignedStore<uint32_t>(out, entry_len);
        out += 4;
        std::memcpy(out, entry.data(), entry_len);
        out += entry_len;
    }
}

UInt32 CompressionCodecDictionary::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    switch (data_type)
    {
    case CompressionDataType::Int8:
        return compressDataInteger<int8_t>(source, source_size, dest);
    case CompressionDataType::Int16:
        return compressDataInteger<int16_t>(source, source_size, dest);
    case CompressionDataType::Int32:
        return compressDataInteger<int32_t>(source, source_size, dest);
    case CompressionDataType::Int64:
        return compressDataInteger<int64_t>(source, source_size, dest);
    case CompressionDataType::String:
        return compressDataString(source, source_size, dest);
    default:
        // Unsupported type — fallback to raw
        dest[0] = DICT_FALLBACK_MARKER;
        unalignedStore<uint32_t>(dest + 1, 0);
        unalignedStore<uint32_t>(dest + 5, 0);
        dest[9] = static_cast<uint8_t>(data_type);
        std::memcpy(dest + DICT_HEADER_SIZE, source, source_size);
        return DICT_HEADER_SIZE + source_size;
    }
}

void CompressionCodecDictionary::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    if (source_size < DICT_HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Dictionary decompress: source too small");

    uint8_t dt_byte = static_cast<uint8_t>(source[9]);
    auto stored_type = static_cast<CompressionDataType>(dt_byte);

    switch (stored_type)
    {
    case CompressionDataType::Int8:
        decompressDataInteger<int8_t>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::Int16:
        decompressDataInteger<int16_t>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::Int32:
        decompressDataInteger<int32_t>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::Int64:
        decompressDataInteger<int64_t>(source, source_size, dest, uncompressed_size);
        break;
    case CompressionDataType::String:
        decompressDataString(source, source_size, dest, uncompressed_size);
        break;
    default:
    {
        // Fallback: check if DICT_FALLBACK_MARKER
        uint8_t bit_width = static_cast<uint8_t>(source[0]);
        if (bit_width == DICT_FALLBACK_MARKER)
        {
            std::memcpy(dest, source + DICT_HEADER_SIZE, uncompressed_size);
        }
        else
        {
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Dictionary decompress: unsupported data type {}",
                dt_byte);
        }
        break;
    }
    }
}

// Explicit template instantiations
template UInt32 CompressionCodecDictionary::compressDataInteger<int8_t>(const char *, UInt32, char *) const;
template UInt32 CompressionCodecDictionary::compressDataInteger<int16_t>(const char *, UInt32, char *) const;
template UInt32 CompressionCodecDictionary::compressDataInteger<int32_t>(const char *, UInt32, char *) const;
template UInt32 CompressionCodecDictionary::compressDataInteger<int64_t>(const char *, UInt32, char *) const;
template void CompressionCodecDictionary::decompressDataInteger<int8_t>(const char *, UInt32, char *, UInt32) const;
template void CompressionCodecDictionary::decompressDataInteger<int16_t>(const char *, UInt32, char *, UInt32) const;
template void CompressionCodecDictionary::decompressDataInteger<int32_t>(const char *, UInt32, char *, UInt32) const;
template void CompressionCodecDictionary::decompressDataInteger<int64_t>(const char *, UInt32, char *, UInt32) const;

} // namespace DB
