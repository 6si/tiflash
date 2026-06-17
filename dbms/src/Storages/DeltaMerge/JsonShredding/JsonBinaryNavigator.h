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
#include <TiDB/Decode/JsonBinary.h>
#include <common/StringRef.h>

#include <cstring>
#include <optional>
#include <string>
#include <vector>

namespace DB::DM
{

/// Low-level navigator for MySQL 5.7 binary JSON format.
/// Provides read-only access to object keys and values without
/// relying on JsonBinary's private methods.
///
/// Binary JSON layout (after type byte):
///   Object: element_count(u32) + size(u32) + key_entries[N] + value_entries[N] + keys_data + values_data
///   Array:  element_count(u32) + size(u32) + value_entries[N] + values_data
///   key_entry = key_offset(u32) + key_length(u16) = 6 bytes
///   value_entry = type(u8) + offset_or_inline(u32) = 5 bytes
class JsonBinaryNavigator
{
public:
    static constexpr size_t HEADER_SIZE = 8;
    static constexpr size_t KEY_ENTRY_SIZE = 6;
    static constexpr size_t VALUE_ENTRY_SIZE = 5;

    /// A reference to a value within the binary JSON
    struct ValueRef
    {
        JsonBinary::JsonType type;
        StringRef data; // Points into the parent's binary data (after type byte)
        bool is_inline = false;
        UInt32 inline_value = 0;

        bool isNull() const { return type == JsonBinary::TYPE_CODE_LITERAL && !is_inline; }
        bool isObject() const { return type == JsonBinary::TYPE_CODE_OBJECT; }
        bool isArray() const { return type == JsonBinary::TYPE_CODE_ARRAY; }
        bool isScalar() const { return !isObject() && !isArray(); }
    };

    /// Parse an object: get element count
    static UInt32 getObjectElementCount(const StringRef & obj_data)
    {
        if (obj_data.size < 4)
            return 0;
        UInt32 count;
        memcpy(&count, obj_data.data, 4);
        return count;
    }

    /// Get the i-th key from an object
    static StringRef getObjectKey(const StringRef & obj_data, UInt32 index)
    {
        // key_entry at: HEADER_SIZE + index * KEY_ENTRY_SIZE
        size_t entry_offset = HEADER_SIZE + index * KEY_ENTRY_SIZE;
        if (entry_offset + 6 > obj_data.size)
            return StringRef();

        UInt32 key_offset;
        UInt16 key_length;
        memcpy(&key_offset, obj_data.data + entry_offset, 4);
        memcpy(&key_length, obj_data.data + entry_offset + 4, 2);

        if (key_offset + key_length > obj_data.size)
            return StringRef();

        return StringRef(obj_data.data + key_offset, key_length);
    }

    /// Get all keys from an object
    static std::vector<StringRef> getObjectKeys(const StringRef & obj_data)
    {
        UInt32 count = getObjectElementCount(obj_data);
        std::vector<StringRef> keys;
        keys.reserve(count);
        for (UInt32 i = 0; i < count; ++i)
            keys.push_back(getObjectKey(obj_data, i));
        return keys;
    }

    /// Get the i-th value from an object
    static ValueRef getObjectValue(const StringRef & obj_data, UInt32 index)
    {
        UInt32 elem_count = getObjectElementCount(obj_data);
        // value_entry at: HEADER_SIZE + elem_count * KEY_ENTRY_SIZE + index * VALUE_ENTRY_SIZE
        size_t entry_offset = HEADER_SIZE + elem_count * KEY_ENTRY_SIZE + index * VALUE_ENTRY_SIZE;
        return parseValueEntry(obj_data, entry_offset);
    }

    /// Get the i-th element from an array
    static ValueRef getArrayElement(const StringRef & arr_data, UInt32 index)
    {
        // value_entry at: HEADER_SIZE + index * VALUE_ENTRY_SIZE
        size_t entry_offset = HEADER_SIZE + index * VALUE_ENTRY_SIZE;
        return parseValueEntry(arr_data, entry_offset);
    }

    /// Navigate to a dot-separated path within a JSON value.
    /// Returns the ValueRef at the path, or nullopt if not found.
    static std::optional<ValueRef> navigatePath(
        JsonBinary::JsonType root_type,
        const StringRef & root_data,
        const String & dot_path)
    {
        if (dot_path.empty())
            return ValueRef{root_type, root_data};

        if (root_type != JsonBinary::TYPE_CODE_OBJECT)
            return std::nullopt;

        // Split by first dot
        size_t dot_pos = dot_path.find('.');
        String current_key = (dot_pos == String::npos) ? dot_path : dot_path.substr(0, dot_pos);
        String remaining = (dot_pos == String::npos) ? "" : dot_path.substr(dot_pos + 1);

        UInt32 elem_count = getObjectElementCount(root_data);
        for (UInt32 i = 0; i < elem_count; ++i)
        {
            StringRef key = getObjectKey(root_data, i);
            if (key.size == current_key.size()
                && memcmp(key.data, current_key.data(), key.size) == 0)
            {
                ValueRef val = getObjectValue(root_data, i);
                if (remaining.empty())
                    return val;
                // Recurse into nested object
                return navigatePath(val.type, val.data, remaining);
            }
        }

        return std::nullopt;
    }

    /// Extract Int64 from a value
    static Int64 getInt64(const ValueRef & val)
    {
        if (val.data.size < 8)
            return 0;
        Int64 result;
        memcpy(&result, val.data.data, 8);
        return result;
    }

    /// Extract UInt64 from a value
    static UInt64 getUInt64(const ValueRef & val)
    {
        if (val.data.size < 8)
            return 0;
        UInt64 result;
        memcpy(&result, val.data.data, 8);
        return result;
    }

    /// Extract Float64 from a value
    static Float64 getFloat64(const ValueRef & val)
    {
        if (val.data.size < 8)
            return 0.0;
        Float64 result;
        memcpy(&result, val.data.data, 8);
        return result;
    }

    /// Extract string from a value (decodes varint length prefix)
    static String getString(const ValueRef & val)
    {
        // String format: varint_length + utf8_data
        if (val.data.size == 0)
            return "";

        size_t offset = 0;
        UInt64 length = 0;
        UInt32 shift = 0;
        while (offset < val.data.size)
        {
            UInt8 byte = static_cast<UInt8>(val.data.data[offset]);
            offset++;
            length |= (static_cast<UInt64>(byte & 0x7F)) << shift;
            if ((byte & 0x80) == 0)
                break;
            shift += 7;
        }

        if (offset + length > val.data.size)
            return "";

        return String(val.data.data + offset, length);
    }

    /// Get the literal value (null/true/false)
    static UInt8 getLiteral(const ValueRef & val)
    {
        if (val.is_inline)
            return static_cast<UInt8>(val.inline_value);
        if (val.data.size > 0)
            return static_cast<UInt8>(val.data.data[0]);
        return JsonBinary::LITERAL_NIL;
    }

private:
    static ValueRef parseValueEntry(const StringRef & parent_data, size_t entry_offset)
    {
        ValueRef result;
        if (entry_offset + VALUE_ENTRY_SIZE > parent_data.size)
        {
            result.type = JsonBinary::TYPE_CODE_LITERAL;
            result.data = StringRef();
            return result;
        }

        result.type = static_cast<JsonBinary::JsonType>(parent_data.data[entry_offset]);
        UInt32 offset_or_inline;
        memcpy(&offset_or_inline, parent_data.data + entry_offset + 1, 4);

        // Check if value is inlined (small integers and literals)
        if (result.type == JsonBinary::TYPE_CODE_LITERAL)
        {
            result.is_inline = true;
            result.inline_value = offset_or_inline;
            result.data = StringRef();
        }
        else if (isInlineable(result.type) && offset_or_inline <= 0xFFFF)
        {
            // Small integers might be inlined — but in TiDB's implementation,
            // only literals are truly inlined. Other types use offsets.
            // For safety, always treat non-literal types as offset-based.
            result.data = StringRef(parent_data.data + offset_or_inline, parent_data.size - offset_or_inline);
        }
        else
        {
            result.data = StringRef(parent_data.data + offset_or_inline, parent_data.size - offset_or_inline);
        }

        return result;
    }

    static bool isInlineable(JsonBinary::JsonType type)
    {
        // In TiDB binary JSON, only literals are truly inlined
        return type == JsonBinary::TYPE_CODE_LITERAL;
    }
};

} // namespace DB::DM
