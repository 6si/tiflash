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
#include <IO/VarInt.h>
#include <Common/Exception.h>
#include <common/likely.h>
#include <common/unaligned.h>
#include <lz4.h>

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

/// Non-throwing VarUInt reader. Returns false on truncated input instead of
/// throwing ATTEMPT_TO_READ_AFTER_EOF. Used by doCompressData to handle
/// compression buffer boundaries that may split mid-string.
static bool tryReadVarUInt(UInt64 & x, const char *& pos, const char * end)
{
    x = 0;
    for (size_t i = 0; i < 9; ++i)
    {
        if (pos == end)
            return false;
        UInt64 byte = static_cast<UInt8>(*pos);
        ++pos;
        x |= (byte & 0x7F) << (7 * i);
        if (!(byte & 0x80))
            return true;
    }
    return true;
}

static UInt32 writeRawFallback(const char * source, UInt32 source_size, char * dest)
{
    char * out = dest;
    *out = 0; // index_width = 0 = raw fallback
    out += sizeof(UInt8);
    memcpy(out, source, source_size);
    out += source_size;
    return static_cast<UInt32>(out - dest);
}

UInt8 CompressionCodecDictionary::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Dictionary);
}

UInt32 CompressionCodecDictionary::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    std::unordered_map<std::string_view, UInt16> dict_map;
    std::vector<std::string_view> dict_entries;
    std::vector<UInt16> ids;

    const char * pos = source;
    const char * end = source + source_size;

    while (pos < end)
    {
        UInt64 str_len = 0;
        if (unlikely(!tryReadVarUInt(str_len, pos, end) || pos + str_len > end))
        {
            // CompressedWriteBuffer flushed mid-string — the buffer boundary
            // split a VarUInt-prefixed string. Fall back to raw passthrough.
            return writeRawFallback(source, source_size, dest);
        }

        std::string_view sv(pos, str_len);
        auto it = dict_map.find(sv);
        UInt16 id;
        if (it == dict_map.end())
        {
            if (dict_entries.size() >= MAX_DICT_SIZE)
                return writeRawFallback(source, source_size, dest);
            id = static_cast<UInt16>(dict_entries.size());
            dict_entries.push_back(sv);
            dict_map[sv] = id;
        }
        else
        {
            id = it->second;
        }
        ids.push_back(id);
        pos += str_len;
    }

    // Determine base index width (1=UInt8, 2=UInt16)
    UInt8 base_width = (dict_entries.size() <= 256) ? 1 : 2;

    // Build the raw ID array
    size_t raw_ids_size = ids.size() * base_width;
    std::vector<char> raw_ids(raw_ids_size);
    if (base_width == 1)
    {
        for (size_t i = 0; i < ids.size(); ++i)
            raw_ids[i] = static_cast<char>(static_cast<UInt8>(ids[i]));
    }
    else
    {
        for (size_t i = 0; i < ids.size(); ++i)
            unalignedStore<UInt16>(&raw_ids[i * 2], ids[i]);
    }

    // LZ4-compress the ID array
    int lz4_bound = LZ4_compressBound(static_cast<int>(raw_ids_size));
    std::vector<char> lz4_ids(lz4_bound);
    int lz4_size = LZ4_compress_fast(raw_ids.data(), lz4_ids.data(), static_cast<int>(raw_ids_size), lz4_bound, 1);

    // Compute sizes: dict header + dict entries + num_rows + lz4 header + lz4 data
    size_t dict_header_size = sizeof(UInt8) + sizeof(UInt16); // index_width + dict_size
    size_t dict_entries_size = 0;
    for (const auto & entry : dict_entries)
    {
        size_t varint_len = 1;
        UInt64 tmp = entry.size();
        while (tmp >= 0x80) { ++varint_len; tmp >>= 7; }
        dict_entries_size += varint_len + entry.size();
    }
    size_t dict_total = dict_header_size + dict_entries_size + sizeof(UInt32) + sizeof(UInt32) + lz4_size;
    size_t raw_total = sizeof(UInt8) + source_size;

    if (dict_total >= raw_total)
        return writeRawFallback(source, source_size, dest);

    // Write v3 format: index_width 3 or 4 = LZ4-compressed IDs
    char * out = dest;

    // index_width: 3 = UInt8+LZ4, 4 = UInt16+LZ4
    UInt8 index_width = (base_width == 1) ? 3 : 4;
    *out = index_width;
    out += sizeof(UInt8);

    // dict_size
    unalignedStore<UInt16>(out, static_cast<UInt16>(dict_entries.size()));
    out += sizeof(UInt16);

    // dict entries
    for (const auto & entry : dict_entries)
    {
        out = writeVarUInt(static_cast<UInt64>(entry.size()), out);
        memcpy(out, entry.data(), entry.size());
        out += entry.size();
    }

    // num_rows
    unalignedStore<UInt32>(out, static_cast<UInt32>(ids.size()));
    out += sizeof(UInt32);

    // LZ4 compressed IDs: [lz4_compressed_size][lz4_data]
    unalignedStore<UInt32>(out, static_cast<UInt32>(lz4_size));
    out += sizeof(UInt32);
    memcpy(out, lz4_ids.data(), lz4_size);
    out += lz4_size;

    return static_cast<UInt32>(out - dest);
}

/// Helper: read dict entries and num_rows from a dictionary-encoded block.
/// Returns (dict_entries, num_rows, updated pos).
static std::tuple<std::vector<std::string>, UInt32, const char *> readDictHeader(
    const char * pos,
    const char * end)
{
    if (unlikely(pos + sizeof(UInt16) > end))
        throw Exception("CompressionCodecDictionary: truncated dict_size", ErrorCodes::CANNOT_DECOMPRESS);
    UInt16 dict_size = unalignedLoad<UInt16>(pos);
    pos += sizeof(UInt16);

    std::vector<std::string> dict_entries(dict_size);
    for (UInt16 i = 0; i < dict_size; ++i)
    {
        UInt64 entry_len = 0;
        pos = readVarUInt(entry_len, pos, end - pos);
        if (unlikely(pos + entry_len > end))
            throw Exception("CompressionCodecDictionary: truncated dict entry", ErrorCodes::CANNOT_DECOMPRESS);
        dict_entries[i].assign(pos, entry_len);
        pos += entry_len;
    }

    if (unlikely(pos + sizeof(UInt32) > end))
        throw Exception("CompressionCodecDictionary: truncated num_rows", ErrorCodes::CANNOT_DECOMPRESS);
    UInt32 num_rows = unalignedLoad<UInt32>(pos);
    pos += sizeof(UInt32);

    return {std::move(dict_entries), num_rows, pos};
}

/// Helper: decode raw (uncompressed) ID array into per-row UInt16 ids.
static void readRawIds(
    const char *& pos,
    const char * end,
    UInt8 index_width,
    UInt32 num_rows,
    std::vector<UInt16> & ids)
{
    ids.resize(num_rows);
    for (UInt32 i = 0; i < num_rows; ++i)
    {
        if (index_width == 1)
        {
            if (unlikely(pos + sizeof(UInt8) > end))
                throw Exception("CompressionCodecDictionary: truncated ids", ErrorCodes::CANNOT_DECOMPRESS);
            ids[i] = static_cast<UInt16>(*reinterpret_cast<const UInt8 *>(pos));
            pos += sizeof(UInt8);
        }
        else
        {
            if (unlikely(pos + sizeof(UInt16) > end))
                throw Exception("CompressionCodecDictionary: truncated ids", ErrorCodes::CANNOT_DECOMPRESS);
            ids[i] = unalignedLoad<UInt16>(pos);
            pos += sizeof(UInt16);
        }
    }
}

/// Helper: LZ4-decompress the ID array (v3 format).
static void readLZ4Ids(
    const char *& pos,
    const char * end,
    UInt8 base_width,
    UInt32 num_rows,
    std::vector<UInt16> & ids)
{
    if (unlikely(pos + sizeof(UInt32) > end))
        throw Exception("CompressionCodecDictionary: truncated lz4 size", ErrorCodes::CANNOT_DECOMPRESS);
    UInt32 lz4_size = unalignedLoad<UInt32>(pos);
    pos += sizeof(UInt32);

    if (unlikely(pos + lz4_size > end))
        throw Exception("CompressionCodecDictionary: truncated lz4 data", ErrorCodes::CANNOT_DECOMPRESS);

    size_t raw_ids_size = static_cast<size_t>(num_rows) * base_width;
    std::vector<char> raw_ids(raw_ids_size);
    if (unlikely(LZ4_decompress_safe(pos, raw_ids.data(), static_cast<int>(lz4_size), static_cast<int>(raw_ids_size)) < 0))
        throw Exception("CompressionCodecDictionary: LZ4 decompression failed", ErrorCodes::CANNOT_DECOMPRESS);
    pos += lz4_size;

    ids.resize(num_rows);
    if (base_width == 1)
    {
        for (UInt32 i = 0; i < num_rows; ++i)
            ids[i] = static_cast<UInt16>(static_cast<UInt8>(raw_ids[i]));
    }
    else
    {
        for (UInt32 i = 0; i < num_rows; ++i)
            ids[i] = unalignedLoad<UInt16>(&raw_ids[i * 2]);
    }
}

void CompressionCodecDictionary::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    const char * pos = source;
    const char * end = source + source_size;

    if (unlikely(pos + sizeof(UInt8) > end))
        throw Exception("CompressionCodecDictionary: truncated header", ErrorCodes::CANNOT_DECOMPRESS);
    UInt8 index_width = *pos;
    pos += sizeof(UInt8);

    if (index_width == 0)
    {
        UInt32 raw_size = source_size - sizeof(UInt8);
        if (unlikely(raw_size != uncompressed_size))
            throw Exception("CompressionCodecDictionary: raw fallback size mismatch", ErrorCodes::CANNOT_DECOMPRESS);
        memcpy(dest, pos, raw_size);
        return;
    }

    auto [dict_entries, num_rows, new_pos] = readDictHeader(pos, end);
    pos = new_pos;
    UInt16 dict_size = static_cast<UInt16>(dict_entries.size());

    std::vector<UInt16> ids;
    if (index_width == 3 || index_width == 4)
    {
        UInt8 base_width = (index_width == 3) ? 1 : 2;
        readLZ4Ids(pos, end, base_width, num_rows, ids);
    }
    else
    {
        readRawIds(pos, end, index_width, num_rows, ids);
    }

    char * out = dest;
    char * out_end = dest + uncompressed_size;
    for (UInt32 i = 0; i < num_rows; ++i)
    {
        if (unlikely(ids[i] >= dict_size))
            throw Exception("CompressionCodecDictionary: invalid dict ID", ErrorCodes::CANNOT_DECOMPRESS);
        const std::string & entry = dict_entries[ids[i]];
        out = writeVarUInt(static_cast<UInt64>(entry.size()), out);
        if (unlikely(out + entry.size() > out_end))
            throw Exception("CompressionCodecDictionary: output buffer overflow", ErrorCodes::CANNOT_DECOMPRESS);
        memcpy(out, entry.data(), entry.size());
        out += entry.size();
    }
}

ColumnPtr CompressionCodecDictionary::decompressAsColumnDictionary(
    const char * source,
    UInt32 source_size,
    UInt32 /*uncompressed_size*/,
    const DataTypePtr & value_type) const
{
    const char * pos = source;
    const char * end = source + source_size;

    if (unlikely(pos + sizeof(UInt8) > end))
        throw Exception("CompressionCodecDictionary: truncated header", ErrorCodes::CANNOT_DECOMPRESS);
    UInt8 index_width = *pos;
    pos += sizeof(UInt8);

    if (index_width == 0)
        return nullptr;

    auto [dict_entries_str, num_rows, new_pos] = readDictHeader(pos, end);
    pos = new_pos;

    std::vector<Field> dictionary(dict_entries_str.size());
    for (size_t i = 0; i < dict_entries_str.size(); ++i)
        dictionary[i] = std::move(dict_entries_str[i]);

    std::vector<UInt16> ids16;
    if (index_width == 3 || index_width == 4)
    {
        UInt8 base_width = (index_width == 3) ? 1 : 2;
        readLZ4Ids(pos, end, base_width, num_rows, ids16);
    }
    else
    {
        readRawIds(pos, end, index_width, num_rows, ids16);
    }

    PaddedPODArray<UInt32> ids;
    ids.reserve(num_rows);
    for (UInt16 id : ids16)
        ids.push_back(static_cast<UInt32>(id));

    return ColumnDictionary::createMutable(std::move(dictionary), std::move(ids), value_type);
}

UInt32 CompressionCodecDictionary::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // Raw fallback: 1 byte header + uncompressed data.
    // LZ4 bound on IDs could theoretically exceed input for incompressible data,
    // but we fall back to raw if dictionary is larger. Buffer for raw fallback.
    return uncompressed_size + sizeof(UInt8);
}

} // namespace DB
