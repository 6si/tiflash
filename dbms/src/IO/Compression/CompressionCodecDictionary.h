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

namespace DB
{

/**
 * Dictionary compression codec for string columns with low cardinality.
 *
 * Compressed format (v3 — adaptive index width + LZ4 on IDs):
 *   [index_width: UInt8]
 *     0 = raw fallback (high NDV)
 *     1 = UInt8 IDs, no LZ4 (legacy v2)
 *     2 = UInt16 IDs, no LZ4 (legacy v2)
 *     3 = UInt8 IDs, LZ4-compressed (v3)
 *     4 = UInt16 IDs, LZ4-compressed (v3)
 *   If index_width > 0:
 *     [dict_size: UInt16]
 *     For each entry: [len: VarUInt][data: bytes]
 *     [num_rows: UInt32]
 *     If index_width == 3 or 4 (LZ4):
 *       [lz4_compressed_size: UInt32]
 *       [lz4_compressed_ids: bytes]
 *     Else (1 or 2, legacy):
 *       [ids: UInt8[num_rows] or UInt16[num_rows]]
 *   If index_width == 0:
 *     [raw SizePrefix data, unmodified]
 *
 * v3 applies LZ4 to the ID array, which for 100M rows × 5 NDV
 * compresses ~100MB → ~1MB (vs v2's uncompressed 100MB on disk).
 */
class CompressionCodecDictionary : public ICompressionCodec
{
public:
    static constexpr UInt32 MAX_DICT_SIZE = 65536;

    CompressionCodecDictionary() = default;

    UInt8 getMethodByte() const override;

    bool isCompression() const override { return true; }

    /// Decompress directly into a ColumnDictionary without materializing
    /// to intermediate ColumnString. Returns nullptr if the block is a
    /// raw fallback block (index_width == 0).
    ColumnPtr decompressAsColumnDictionary(
        const char * source,
        UInt32 source_size,
        UInt32 uncompressed_size,
        const DataTypePtr & value_type) const;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;
};

} // namespace DB
