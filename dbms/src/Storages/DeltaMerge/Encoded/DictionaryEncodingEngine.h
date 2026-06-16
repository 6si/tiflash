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
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/PODArray.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionCodecDictionary.h>

#include <unordered_map>
#include <vector>

namespace DB::DM
{

/// Configuration for dictionary encoding decisions
struct DictionaryEncodingConfig
{
    /// Maximum number of distinct values for dictionary encoding
    size_t max_cardinality = 4096;

    /// Whether dictionary encoding is enabled globally
    bool enabled = false;

    /// Minimum number of rows to consider dictionary encoding
    size_t min_rows_for_encoding = 64;
};

/**
 * DictionaryEncodingEngine manages the encoding/decoding pipeline.
 *
 * Responsibilities:
 * 1. Analyze a column to determine if dictionary encoding is beneficial
 * 2. Encode a raw column into a ColumnDictionary
 * 3. Decode a ColumnDictionary back to a raw column
 * 4. Provide helpers for the writer/reader paths
 */
class DictionaryEncodingEngine
{
public:
    struct EncodingResult
    {
        bool was_encoded = false;
        ColumnPtr column; // Either ColumnDictionary (if encoded) or original column
        size_t cardinality = 0;
    };

    /// Analyze a column and return whether it's suitable for dictionary encoding
    static bool isSuitableForDictionary(const IColumn & column, const DictionaryEncodingConfig & config);

    /// Compute the cardinality (number of distinct values) of a column
    static size_t computeCardinality(const IColumn & column, size_t max_check = 0);

    /// Encode a column into ColumnDictionary if cardinality is below threshold
    static EncodingResult tryEncode(const IColumn & column, const DataTypePtr & type, const DictionaryEncodingConfig & config);

    /// Encode a string column into ColumnDictionary
    static ColumnDictionary::MutablePtr encodeStringColumn(const ColumnString & column, const DataTypePtr & type);

    /// Encode an integer column into ColumnDictionary
    template <typename T>
    static ColumnDictionary::MutablePtr encodeIntegerColumn(const ColumnVector<T> & column, const DataTypePtr & type);

    /// Determine compression settings for a column based on analysis
    static bool shouldUseDictionaryCompression(const IColumn & column, const DictionaryEncodingConfig & config);
};

} // namespace DB::DM
