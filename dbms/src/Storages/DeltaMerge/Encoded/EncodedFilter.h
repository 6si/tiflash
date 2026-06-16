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
#include <Columns/IColumn.h>
#include <Core/Field.h>

#include <functional>
#include <optional>
#include <vector>

namespace DB::DM
{

/**
 * EncodedFilter evaluates filter predicates on dictionary-encoded data
 * without decoding. The key insight:
 *
 * 1. Pre-compute predicate result for each dictionary entry
 * 2. For each row, look up the pre-computed result using dictionary ID
 *
 * This avoids per-row string comparison for dictionary-encoded columns.
 *
 * Example:
 *   Dictionary: {0: "red", 1: "green", 2: "blue"}
 *   Filter: WHERE color = 'green'
 *   Pre-computed: {0: false, 1: true, 2: false}
 *   Row IDs: [0, 2, 1, 1, 0] → filter result: [false, false, true, true, false]
 */
class EncodedFilter
{
public:
    /// Predicate function type: takes a Field value, returns true if it passes the filter
    using Predicate = std::function<bool(const Field &)>;

    /// Result of an encoded filter execution
    struct FilterResult
    {
        /// Bitmask of rows passing the filter (1=pass, 0=reject)
        IColumn::Filter filter;
        /// Number of rows passing
        size_t count_passing = 0;
        /// Whether filter was applied in encoded mode (vs fallback)
        bool used_encoded_path = false;
    };

    /// Evaluate an equality predicate (col = value) on a dictionary-encoded column.
    /// Returns a filter bitmask without decoding the column.
    static FilterResult evaluateEquals(const ColumnDictionary & column, const Field & value);

    /// Evaluate an IN predicate (col IN (values...)) on a dictionary-encoded column.
    static FilterResult evaluateIn(const ColumnDictionary & column, const std::vector<Field> & values);

    /// Evaluate a general predicate on a dictionary-encoded column.
    /// Pre-computes predicate for each dictionary entry, then looks up per-row.
    static FilterResult evaluatePredicate(const ColumnDictionary & column, const Predicate & predicate);

    /// Evaluate a LIKE predicate (string pattern matching).
    static FilterResult evaluateLike(const ColumnDictionary & column, const String & pattern);

    /// Evaluate a NOT EQUAL predicate.
    static FilterResult evaluateNotEquals(const ColumnDictionary & column, const Field & value);

    /// Try to apply encoded filter on a column (detects if dictionary-encoded).
    /// Returns nullopt if column is not dictionary-encoded.
    static std::optional<FilterResult> tryApplyEquals(const IColumn & column, const Field & value);
    static std::optional<FilterResult> tryApplyIn(const IColumn & column, const std::vector<Field> & values);
    static std::optional<FilterResult> tryApplyPredicate(const IColumn & column, const Predicate & predicate);

private:
    /// Pre-compute filter results for all dictionary entries
    static std::vector<UInt8> precomputeDictionaryFilter(
        const std::vector<Field> & dictionary,
        const Predicate & predicate);

    /// Simple LIKE pattern matching (supports % and _ wildcards)
    static bool matchLike(const String & str, const String & pattern);
};

} // namespace DB::DM
