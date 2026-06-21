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

#include <Core/ColumnShreddedAttachment.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::DM
{

/// Thread-safe global cache for shredded JSON attachments.
/// Solves the cross-thread attachment loss problem in TiFlash's pipeline model:
///   - Read threads (DMFileReader) produce blocks with attachments
///   - Pipeline threads (filter/aggregation) consume blocks but attachments get lost
///     during block reconstruction in the pipeline operator chain
///
/// Usage:
///   1. DMFileReader calls registerAttachment() after creating an attachment
///   2. FunctionJsonShreddedFilter calls findAttachment() when the block's column
///      doesn't have an attachment (fallback path)
///   3. Query cleanup calls clearForDMFile() when a DMFile is no longer needed
///
/// The cache is keyed by (dmfile_path, col_name) → single attachment per DMFile+column.
/// Multiple packs from the same DMFile share the same manifest (only row_offset differs),
/// so we store one canonical attachment per DMFile and the filter adjusts row range.
class ShreddedAttachmentCache
{
public:
    static ShreddedAttachmentCache & instance()
    {
        static ShreddedAttachmentCache cache;
        return cache;
    }

    /// Register an attachment produced by DMFileReader.
    /// Called on the read thread after loading a sidecar manifest.
    void registerAttachment(const String & dmfile_path, const String & col_name, ColumnShreddedAttachmentPtr attachment)
    {
        std::unique_lock lock(mutex_);
        auto key = makeKey(dmfile_path, col_name);
        entries_[key] = std::move(attachment);
        // Also store by col_name alone for fallback lookup
        by_col_name_[col_name] = entries_[key];
    }

    /// Find an attachment by dmfile_path + col_name (preferred, exact match).
    ColumnShreddedAttachmentPtr findAttachment(const String & dmfile_path, const String & col_name) const
    {
        std::shared_lock lock(mutex_);
        auto key = makeKey(dmfile_path, col_name);
        auto it = entries_.find(key);
        if (it != entries_.end())
            return it->second;
        return nullptr;
    }

    /// Find an attachment by col_name only (fallback when dmfile_path is unknown).
    /// Returns the most recently registered attachment for this column name.
    ColumnShreddedAttachmentPtr findByColName(const String & col_name) const
    {
        std::shared_lock lock(mutex_);
        auto it = by_col_name_.find(col_name);
        if (it != by_col_name_.end())
            return it->second;
        return nullptr;
    }

    /// Remove all entries for a specific DMFile (called during cleanup/GC).
    void clearForDMFile(const String & dmfile_path)
    {
        std::unique_lock lock(mutex_);
        // Erase all entries with this dmfile_path prefix
        for (auto it = entries_.begin(); it != entries_.end();)
        {
            if (it->first.find(dmfile_path) == 0)
                it = entries_.erase(it);
            else
                ++it;
        }
    }

    /// Clear ALL entries (used in tests or shutdown).
    void clear()
    {
        std::unique_lock lock(mutex_);
        entries_.clear();
        by_col_name_.clear();
    }

private:
    ShreddedAttachmentCache() = default;

    static String makeKey(const String & dmfile_path, const String & col_name)
    {
        return dmfile_path + "|" + col_name;
    }

    mutable std::shared_mutex mutex_;
    std::unordered_map<String, ColumnShreddedAttachmentPtr> entries_;
    std::unordered_map<String, ColumnShreddedAttachmentPtr> by_col_name_;
};

} // namespace DB::DM
