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
 * Compressed format:
 *   [dict_size: UInt32]
 *   For each dict entry: [len: VarUInt][data: bytes]
 *   [num_rows: UInt32]
 *   [ids: UInt32 * num_rows]
 *
 * Input (source) is in TiFlash SizePrefix format:
 *   For each row: [len: VarUInt][data: bytes]
 */
UInt32 CompressionCodecDictionary::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    // Parse source: VarUInt-prefixed strings
    std::unordered_map<std::string_view, UInt32> dict_map;
    std::vector<std::string> dict_entries;
    std::vector<UInt32> ids;

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
        UInt32 id;
        if (it == dict_map.end())
        {
            if (dict_entries.size() >= MAX_DICT_SIZE)
                throw Exception(
                    "CompressionCodecDictionary: too many distinct values for dictionary encoding",
                    ErrorCodes::CANNOT_COMPRESS);
            id = static_cast<UInt32>(dict_entries.size());
            dict_entries.emplace_back(sv);
            dict_map[std::string_view(dict_entries.back())] = id;
        }
        else
        {
            id = it->second;
        }
        ids.push_back(id);
        pos += str_len;
    }

    // Write compressed format
    char * out = dest;

    // dict_size
    unalignedStore<UInt32>(out, static_cast<UInt32>(dict_entries.size()));
    out += sizeof(UInt32);

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
    for (UInt32 id : ids)
    {
        unalignedStore<UInt32>(out, id);
        out += sizeof(UInt32);
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

    // Read dict_size
    if (unlikely(pos + sizeof(UInt32) > end))
        throw Exception("CompressionCodecDictionary: truncated header", ErrorCodes::CANNOT_DECOMPRESS);
    UInt32 dict_size = unalignedLoad<UInt32>(pos);
    pos += sizeof(UInt32);

    // Read dictionary entries
    std::vector<std::string> dict_entries(dict_size);
    for (UInt32 i = 0; i < dict_size; ++i)
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
        if (unlikely(pos + sizeof(UInt32) > end))
            throw Exception("CompressionCodecDictionary: truncated ids array", ErrorCodes::CANNOT_DECOMPRESS);
        UInt32 id = unalignedLoad<UInt32>(pos);
        pos += sizeof(UInt32);

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

UInt32 CompressionCodecDictionary::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // Worst case: every string is unique, each needs VarUInt(len) + data in dict,
    // plus UInt32 per row for IDs, plus headers.
    // Rough upper bound: uncompressed_size (dict) + uncompressed_size/4 * sizeof(UInt32) (ids) + headers
    return uncompressed_size * 2 + 1024;
}

} // namespace DB
