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

UInt8 CompressionCodecDictionary::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Dictionary);
}

/**
 * Compressed format (v2):
 *   [index_width: UInt8]  (1=UInt8, 2=UInt16, 0=raw fallback)
 *   If index_width > 0:
 *     [dict_size: UInt16]
 *     For each dict entry: [len: VarUInt][data: bytes]
 *     [num_rows: UInt32]
 *     [ids: UInt8*num_rows or UInt16*num_rows]
 *   If index_width == 0:
 *     [raw SizePrefix data copied verbatim]
 *
 * Input (source) is in TiFlash SizePrefix format:
 *   For each row: [len: VarUInt][data: bytes]
 */
UInt32 CompressionCodecDictionary::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    // Parse source: VarUInt-prefixed strings.
    // Keys in dict_map are string_views into the source buffer (which is stable).
    std::unordered_map<std::string_view, UInt16> dict_map;
    std::vector<std::string_view> dict_entries; // views into source buffer
    std::vector<UInt16> ids;

    const char * pos = source;
    const char * end = source + source_size;

    while (pos < end)
    {
        UInt64 str_len = 0;
        pos = readVarUInt(str_len, pos, end - pos);
        if (unlikely(pos + str_len > end))
            throw Exception("CompressionCodecDictionary: input data truncated", ErrorCodes::CANNOT_COMPRESS);

        std::string_view sv(pos, str_len);
        auto it = dict_map.find(sv);
        UInt16 id;
        if (it == dict_map.end())
        {
            if (dict_entries.size() >= MAX_DICT_SIZE)
            {
                // Too many distinct values — write raw fallback
                char * out = dest;
                *out = 0; // index_width = 0 = raw fallback
                out += sizeof(UInt8);
                memcpy(out, source, source_size);
                out += source_size;
                return static_cast<UInt32>(out - dest);
            }
            id = static_cast<UInt16>(dict_entries.size());
            dict_entries.push_back(sv); // sv points into stable source buffer
            dict_map[sv] = id;
        }
        else
        {
            id = it->second;
        }
        ids.push_back(id);
        pos += str_len;
    }

    // Determine index width
    UInt8 index_width = (dict_entries.size() <= 256) ? 1 : 2;

    // Compute exact dictionary-encoded size to decide if it's worth it
    size_t dict_header_size = sizeof(UInt8) + sizeof(UInt16); // index_width + dict_size
    size_t dict_entries_size = 0;
    for (const auto & entry : dict_entries)
    {
        // VarUInt overhead: values < 128 need 1 byte, < 16384 need 2 bytes, etc.
        size_t varint_len = 1;
        UInt64 tmp = entry.size();
        while (tmp >= 0x80)
        {
            ++varint_len;
            tmp >>= 7;
        }
        dict_entries_size += varint_len + entry.size();
    }
    size_t dict_total = dict_header_size + dict_entries_size + sizeof(UInt32) + ids.size() * index_width;
    size_t raw_total = sizeof(UInt8) + source_size;

    if (dict_total >= raw_total)
    {
        // Dictionary encoding is not beneficial — use raw fallback
        char * out = dest;
        *out = 0;
        out += sizeof(UInt8);
        memcpy(out, source, source_size);
        out += source_size;
        return static_cast<UInt32>(out - dest);
    }

    // Write compressed format
    char * out = dest;

    // index_width
    *out = index_width;
    out += sizeof(UInt8);

    // dict_size
    unalignedStore<UInt16>(out, static_cast<UInt16>(dict_entries.size()));
    out += sizeof(UInt16);

    // dict entries: VarUInt length + bytes
    for (const auto & entry : dict_entries)
    {
        out = writeVarUInt(static_cast<UInt64>(entry.size()), out);
        memcpy(out, entry.data(), entry.size());
        out += entry.size();
    }

    // num_rows
    unalignedStore<UInt32>(out, static_cast<UInt32>(ids.size()));
    out += sizeof(UInt32);

    // ids array
    if (index_width == 1)
    {
        for (UInt16 id : ids)
        {
            *out = static_cast<UInt8>(id);
            out += sizeof(UInt8);
        }
    }
    else
    {
        for (UInt16 id : ids)
        {
            unalignedStore<UInt16>(out, id);
            out += sizeof(UInt16);
        }
    }

    return static_cast<UInt32>(out - dest);
}

/**
 * Decompress back to SizePrefix format: [VarUInt len][bytes] per row.
 */
void CompressionCodecDictionary::doDecompressData(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 uncompressed_size) const
{
    const char * pos = source;
    const char * end = source + source_size;

    // Read index_width
    if (unlikely(pos + sizeof(UInt8) > end))
        throw Exception("CompressionCodecDictionary: truncated header", ErrorCodes::CANNOT_DECOMPRESS);
    UInt8 index_width = *pos;
    pos += sizeof(UInt8);

    if (index_width == 0)
    {
        // Raw fallback — just copy
        UInt32 raw_size = source_size - sizeof(UInt8);
        if (unlikely(raw_size != uncompressed_size))
            throw Exception("CompressionCodecDictionary: raw fallback size mismatch", ErrorCodes::CANNOT_DECOMPRESS);
        memcpy(dest, pos, raw_size);
        return;
    }

    // Read dict_size
    if (unlikely(pos + sizeof(UInt16) > end))
        throw Exception("CompressionCodecDictionary: truncated dict_size", ErrorCodes::CANNOT_DECOMPRESS);
    UInt16 dict_size = unalignedLoad<UInt16>(pos);
    pos += sizeof(UInt16);

    // Read dictionary entries
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

    // Read num_rows
    if (unlikely(pos + sizeof(UInt32) > end))
        throw Exception("CompressionCodecDictionary: truncated num_rows", ErrorCodes::CANNOT_DECOMPRESS);
    UInt32 num_rows = unalignedLoad<UInt32>(pos);
    pos += sizeof(UInt32);

    // Read IDs and write SizePrefix format to dest
    char * out = dest;
    char * out_end = dest + uncompressed_size;

    for (UInt32 i = 0; i < num_rows; ++i)
    {
        UInt16 id;
        if (index_width == 1)
        {
            if (unlikely(pos + sizeof(UInt8) > end))
                throw Exception("CompressionCodecDictionary: truncated ids array", ErrorCodes::CANNOT_DECOMPRESS);
            id = static_cast<UInt16>(*reinterpret_cast<const UInt8 *>(pos));
            pos += sizeof(UInt8);
        }
        else
        {
            if (unlikely(pos + sizeof(UInt16) > end))
                throw Exception("CompressionCodecDictionary: truncated ids array", ErrorCodes::CANNOT_DECOMPRESS);
            id = unalignedLoad<UInt16>(pos);
            pos += sizeof(UInt16);
        }

        if (unlikely(id >= dict_size))
            throw Exception("CompressionCodecDictionary: invalid dict ID", ErrorCodes::CANNOT_DECOMPRESS);

        const std::string & entry = dict_entries[id];
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

    // Read index_width
    if (unlikely(pos + sizeof(UInt8) > end))
        throw Exception("CompressionCodecDictionary: truncated header", ErrorCodes::CANNOT_DECOMPRESS);
    UInt8 index_width = *pos;
    pos += sizeof(UInt8);

    if (index_width == 0)
        return nullptr; // raw fallback — caller must use standard decompress path

    // Read dict_size
    if (unlikely(pos + sizeof(UInt16) > end))
        throw Exception("CompressionCodecDictionary: truncated dict_size", ErrorCodes::CANNOT_DECOMPRESS);
    UInt16 dict_size = unalignedLoad<UInt16>(pos);
    pos += sizeof(UInt16);

    // Read dictionary entries → Field vector
    std::vector<Field> dictionary(dict_size);
    for (UInt16 i = 0; i < dict_size; ++i)
    {
        UInt64 entry_len = 0;
        pos = readVarUInt(entry_len, pos, end - pos);
        if (unlikely(pos + entry_len > end))
            throw Exception("CompressionCodecDictionary: truncated dict entry", ErrorCodes::CANNOT_DECOMPRESS);
        dictionary[i] = String(pos, entry_len);
        pos += entry_len;
    }

    // Read num_rows
    if (unlikely(pos + sizeof(UInt32) > end))
        throw Exception("CompressionCodecDictionary: truncated num_rows", ErrorCodes::CANNOT_DECOMPRESS);
    UInt32 num_rows = unalignedLoad<UInt32>(pos);
    pos += sizeof(UInt32);

    // Read IDs
    PaddedPODArray<UInt32> ids;
    ids.reserve(num_rows);
    for (UInt32 i = 0; i < num_rows; ++i)
    {
        UInt16 id;
        if (index_width == 1)
        {
            if (unlikely(pos + sizeof(UInt8) > end))
                throw Exception("CompressionCodecDictionary: truncated ids", ErrorCodes::CANNOT_DECOMPRESS);
            id = static_cast<UInt16>(*reinterpret_cast<const UInt8 *>(pos));
            pos += sizeof(UInt8);
        }
        else
        {
            if (unlikely(pos + sizeof(UInt16) > end))
                throw Exception("CompressionCodecDictionary: truncated ids", ErrorCodes::CANNOT_DECOMPRESS);
            id = unalignedLoad<UInt16>(pos);
            pos += sizeof(UInt16);
        }
        ids.push_back(static_cast<UInt32>(id));
    }

    return ColumnDictionary::createMutable(std::move(dictionary), std::move(ids), value_type);
}

UInt32 CompressionCodecDictionary::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // Worst case: dictionary format with all 1-byte rows (max row count = uncompressed_size),
    // each row's UInt16 id = 2 bytes → up to uncompressed_size * 2 for ids.
    // Plus dictionary entries (≤ uncompressed_size bytes) + 7 bytes overhead.
    // Raw fallback is only uncompressed_size + 1.
    // Since doCompressData falls back to raw when dictionary is larger,
    // the actual output never exceeds raw_total. But the buffer must be
    // large enough for the raw fallback path.
    return uncompressed_size + sizeof(UInt8);
}

} // namespace DB
