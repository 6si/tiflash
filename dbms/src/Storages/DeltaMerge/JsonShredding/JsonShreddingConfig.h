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

#include <Core/Types.h>

#include <atomic>

namespace DB::DM
{

/// Global feature flag for JSON shredding.
///
/// Design:
///   - WRITE path: Always shreds (dual-write: blob + sub-columns) so data is available
///     for comparison regardless of flag state.
///   - READ path: Flag controls which representation is used for queries.
///     - ON (default): Read from shredded sub-columns (new fast path).
///     - OFF: Read from original blob column (current behavior, for comparison).
///
/// Controlled per-query via TiDB session variable @@tiflash_json_shredding which is
/// propagated through gRPC metadata. This allows instant A/B performance comparison
/// on the same data without re-ingestion.
class JsonShreddingFlag
{
public:
    static JsonShreddingFlag & instance()
    {
        static JsonShreddingFlag flag;
        return flag;
    }

    /// Whether to use shredded sub-columns for reading (query path).
    /// When false, queries use the original blob and parse with json_extract as before.
    bool useShredded() const { return use_shredded_.load(std::memory_order_relaxed); }

    void setUseShredded(bool val) { use_shredded_.store(val, std::memory_order_relaxed); }

    /// Whether to write shredded sub-columns (write path).
    /// Currently always true (dual-write for comparison), but can be made configurable later.
    bool writeShredded() const { return write_shredded_.load(std::memory_order_relaxed); }

    void setWriteShredded(bool val) { write_shredded_.store(val, std::memory_order_relaxed); }

private:
    JsonShreddingFlag()
        : use_shredded_(true) // Default ON; toggled per-query via @@tiflash_json_shredding
        , write_shredded_(true) // Always write shredded for now
    {}

    std::atomic<bool> use_shredded_;
    std::atomic<bool> write_shredded_;
};

} // namespace DB::DM
