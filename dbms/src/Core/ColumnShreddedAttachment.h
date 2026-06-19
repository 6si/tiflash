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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::DM
{

struct ShreddedJsonData;

/// Lightweight attachment that carries shredded JSON sub-column data with a column
/// through the pipeline. This survives thread handoffs (reader pool → MPP worker pool)
/// and column renames (PROJECT actions).
///
/// Lifecycle:
///   1. DMFileReader loads sidecar data and attaches to the JSON column
///   2. Column flows through pipeline (PROJECT renames, filter, etc.)
///   3. FunctionJsonExtract checks attachment for pre-computed sub-columns
///   4. Attachment is reference-counted (shared_ptr) — zero cost when null
struct ColumnShreddedAttachment
{
    /// Shared reference to the full sidecar data (cached, covers entire DMFile).
    /// Multiple packs from the same DMFile share this via shared_ptr.
    std::shared_ptr<const ShreddedJsonData> data;

    /// Row range within the sidecar that corresponds to this block/pack.
    size_t row_offset = 0;
    size_t row_count = 0;
};

using ColumnShreddedAttachmentPtr = std::shared_ptr<const ColumnShreddedAttachment>;

} // namespace DB::DM
