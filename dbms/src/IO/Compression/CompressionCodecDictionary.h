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
 * Compressed format (v2 — adaptive index width):
 *   [index_width: UInt8]   - 1=UInt8 ids, 2=UInt16 ids, 0=raw fallback
 *   If index_width > 0 (dictionary encoded):
 *     [dict_size: UInt16]                               - number of dictionary entries
 *     [entry_0_len: VarUInt][entry_0_data: bytes]...    - dictionary entries (length-prefixed)
 *     [num_rows: UInt32]                                - number of rows
 *     [ids: UInt8[num_rows] or UInt16[num_rows]]        - per-row dictionary IDs
 *   If index_width == 0 (raw fallback for high NDV):
 *     [raw SizePrefix data, unmodified]
 *
 * The uncompressed format (for standard decompression) is:
 *   TiFlash SizePrefix format: [VarUInt length][string bytes] per row
 *
 * This codec also supports decompressAsColumnDictionary() which produces
 * a ColumnDictionary directly without materializing strings.
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
