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

#include <Storages/DeltaMerge/Encoded/EncodedFilter.h>

namespace DB::DM
{

std::vector<UInt8> EncodedFilter::precomputeDictionaryFilter(
    const std::vector<Field> & dictionary,
    const Predicate & predicate)
{
    std::vector<UInt8> results(dictionary.size());
    for (size_t i = 0; i < dictionary.size(); ++i)
    {
        results[i] = predicate(dictionary[i]) ? 1 : 0;
    }
    return results;
}

EncodedFilter::FilterResult EncodedFilter::evaluateEquals(const ColumnDictionary & column, const Field & value)
{
    return evaluatePredicate(column, [&value](const Field & entry) { return entry == value; });
}

EncodedFilter::FilterResult EncodedFilter::evaluateNotEquals(const ColumnDictionary & column, const Field & value)
{
    return evaluatePredicate(column, [&value](const Field & entry) { return entry != value; });
}

EncodedFilter::FilterResult EncodedFilter::evaluateIn(
    const ColumnDictionary & column,
    const std::vector<Field> & values)
{
    return evaluatePredicate(column, [&values](const Field & entry) {
        for (const auto & v : values)
        {
            if (entry == v)
                return true;
        }
        return false;
    });
}

EncodedFilter::FilterResult EncodedFilter::evaluatePredicate(
    const ColumnDictionary & column,
    const Predicate & predicate)
{
    FilterResult result;
    result.used_encoded_path = true;

    const auto & dictionary = column.getDictionary();
    const auto & ids = column.getDictionaryIds();

    // Phase 1: Pre-compute predicate for each dictionary entry
    auto dict_results = precomputeDictionaryFilter(dictionary, predicate);

    // Phase 2: Apply to each row using dictionary ID lookup (no per-row decode)
    result.filter.resize(ids.size());
    result.count_passing = 0;

    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt8 pass = dict_results[ids[i]];
        result.filter[i] = pass;
        result.count_passing += pass;
    }

    return result;
}

bool EncodedFilter::matchLike(const String & str, const String & pattern)
{
    // Simple LIKE implementation supporting % (any sequence) and _ (single char)
    size_t si = 0, pi = 0;
    size_t star_pi = String::npos, star_si = 0;

    while (si < str.size())
    {
        if (pi < pattern.size() && (pattern[pi] == str[si] || pattern[pi] == '_'))
        {
            ++si;
            ++pi;
        }
        else if (pi < pattern.size() && pattern[pi] == '%')
        {
            star_pi = pi;
            star_si = si;
            ++pi;
        }
        else if (star_pi != String::npos)
        {
            pi = star_pi + 1;
            ++star_si;
            si = star_si;
        }
        else
        {
            return false;
        }
    }

    while (pi < pattern.size() && pattern[pi] == '%')
        ++pi;

    return pi == pattern.size();
}

EncodedFilter::FilterResult EncodedFilter::evaluateLike(const ColumnDictionary & column, const String & pattern)
{
    return evaluatePredicate(column, [&pattern](const Field & entry) {
        if (entry.getType() != Field::Types::String)
            return false;
        return matchLike(entry.get<String>(), pattern);
    });
}

std::optional<EncodedFilter::FilterResult> EncodedFilter::tryApplyEquals(const IColumn & column, const Field & value)
{
    if (const auto * dict_col = dynamic_cast<const ColumnDictionary *>(&column))
        return evaluateEquals(*dict_col, value);
    return std::nullopt;
}

std::optional<EncodedFilter::FilterResult> EncodedFilter::tryApplyIn(
    const IColumn & column,
    const std::vector<Field> & values)
{
    if (const auto * dict_col = dynamic_cast<const ColumnDictionary *>(&column))
        return evaluateIn(*dict_col, values);
    return std::nullopt;
}

std::optional<EncodedFilter::FilterResult> EncodedFilter::tryApplyPredicate(
    const IColumn & column,
    const Predicate & predicate)
{
    if (const auto * dict_col = dynamic_cast<const ColumnDictionary *>(&column))
        return evaluatePredicate(*dict_col, predicate);
    return std::nullopt;
}

} // namespace DB::DM
