// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Functions/IFunction.cpp
//
// Copyright 2023 PingCAP, Inc.
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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <ext/collection_cast.h>
#include <ext/range.h>

#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
} // namespace ErrorCodes

namespace
{
/** Return ColumnNullable of src, with null map as OR-ed null maps of args columns in blocks.
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  */
ColumnPtr wrapInNullable(const ColumnPtr & src, Block & block, const ColumnNumbers & args, size_t result)
{
    ColumnPtr result_null_map_column;

    /// If result is already nullable.
    ColumnPtr src_not_nullable = src;

    if (src->onlyNull())
        return src;
    else if (src->isColumnNullable())
    {
        src_not_nullable = static_cast<const ColumnNullable &>(*src).getNestedColumnPtr();
        result_null_map_column = static_cast<const ColumnNullable &>(*src).getNullMapColumnPtr();
    }

    for (const auto & arg : args)
    {
        const ColumnWithTypeAndName & elem = block.getByPosition(arg);
        if (!elem.type->isNullable())
            continue;

        /// Const Nullable that are NULL.
        if (elem.column->onlyNull())
            return block.getByPosition(result).type->createColumnConst(block.rows(), Null());

        if (elem.column->isColumnConst())
            continue;

        if (elem.column->isColumnNullable())
        {
            const ColumnPtr & null_map_column = static_cast<const ColumnNullable &>(*elem.column).getNullMapColumnPtr();
            if (!result_null_map_column)
            {
                result_null_map_column = null_map_column;
            }
            else
            {
                MutableColumnPtr mutable_result_null_map_column = (*std::move(result_null_map_column)).mutate();

                NullMap & result_null_map = static_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
                const NullMap & src_null_map = static_cast<const ColumnUInt8 &>(*null_map_column).getData();

                for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                    if (src_null_map[i])
                        result_null_map[i] = 1;

                result_null_map_column = std::move(mutable_result_null_map_column);
            }
        }
    }

    if (!result_null_map_column)
        return makeNullable(src);

    if (src_not_nullable->isColumnConst())
        return ColumnNullable::create(src_not_nullable->convertToFullColumnIfConst(), result_null_map_column);
    else
        return ColumnNullable::create(src_not_nullable, result_null_map_column);
}

NullPresence getNullPresense(const ColumnsWithTypeAndName & args)
{
    NullPresence res;

    for (const auto & elem : args)
    {
        if (!res.has_nullable)
            res.has_nullable = elem.type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = elem.type->onlyNull();
    }

    return res;
}

bool allArgumentsAreConstants(const Block & block, const ColumnNumbers & args)
{
    for (auto arg : args)
        if (!block.getByPosition(arg).column->isColumnConst())
            return false;
    return true;
}
} // namespace

NullPresence getNullPresense(const Block & block, const ColumnNumbers & args)
{
    NullPresence res;

    for (const auto & arg : args)
    {
        const auto & elem = block.getByPosition(arg);

        if (!res.has_nullable)
            res.has_nullable = elem.type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = elem.type->onlyNull();
    }

    return res;
}

bool IExecutableFunction::defaultImplementationForConstantArguments(
    Block & block,
    const ColumnNumbers & args,
    size_t result) const
{
    ColumnNumbers arguments_to_remain_constants = getArgumentsThatAreAlwaysConstant();

    /// Check that these arguments are really constant.
    for (auto arg_num : arguments_to_remain_constants)
        if (arg_num < args.size() && !block.getByPosition(args[arg_num]).column->isColumnConst())
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Argument at index {} for function {}"
                " must be constant",
                arg_num,
                getName());

    if (args.empty() || !useDefaultImplementationForConstants() || !allArgumentsAreConstants(block, args))
        return false;

    Block temporary_block;
    bool have_converted_columns = false;

    size_t arguments_size = args.size();
    for (size_t arg_num = 0; arg_num < arguments_size; ++arg_num)
    {
        const ColumnWithTypeAndName & column = block.getByPosition(args[arg_num]);

        if (arguments_to_remain_constants.end()
            != std::find(arguments_to_remain_constants.begin(), arguments_to_remain_constants.end(), arg_num))
            temporary_block.insert(column);
        else
        {
            have_converted_columns = true;
            temporary_block.insert(
                {static_cast<const ColumnConst *>(column.column.get())->getDataColumnPtr(), column.type, column.name});
        }
    }

    /** When using default implementation for constants, the function requires at least one argument
      *  not in "arguments_to_remain_constants" set. Otherwise we get infinite recursion.
      */
    if (!have_converted_columns)
        throw Exception(
            "Number of arguments for function " + getName() + " doesn't match: the function requires more arguments",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    temporary_block.insert(block.getByPosition(result));

    ColumnNumbers temporary_argument_numbers(arguments_size);
    for (size_t i = 0; i < arguments_size; ++i)
        temporary_argument_numbers[i] = i;

    execute(temporary_block, temporary_argument_numbers, arguments_size);

    block.getByPosition(result).column
        = ColumnConst::create(temporary_block.getByPosition(arguments_size).column, block.rows());
    return true;
}


bool IExecutableFunction::defaultImplementationForNulls(Block & block, const ColumnNumbers & args, size_t result) const
{
    if (args.empty() || !useDefaultImplementationForNulls())
        return false;

    NullPresence null_presence = getNullPresense(block, args);

    if (null_presence.has_null_constant)
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
        return true;
    }

    if (null_presence.has_nullable)
    {
        Block temporary_block = createBlockWithNestedColumns(block, args, result);
        execute(temporary_block, args, result);
        block.getByPosition(result).column
            = wrapInNullable(temporary_block.getByPosition(result).column, block, args, result);
        return true;
    }

    return false;
}

bool IExecutableFunction::defaultImplementationForDictionaryColumns(
    Block & block,
    const ColumnNumbers & args,
    size_t result) const
{
    if (args.empty() || !useDefaultImplementationForDictionaryColumns())
        return false;

    // Find dictionary columns among arguments
    size_t dict_arg_idx = args.size(); // sentinel: not found
    size_t num_dict_cols = 0;
    for (size_t i = 0; i < args.size(); ++i)
    {
        const auto & col = block.getByPosition(args[i]).column;
        if (col && col->isDictionaryEncoded())
        {
            dict_arg_idx = i;
            ++num_dict_cols;
        }
    }

    // Auto-encode: If no dictionary columns found, check if there's a String column
    // alongside constants that could benefit from dictionary encoding.
    // Only try if block is large enough to justify the O(N) scan.
    static constexpr size_t MIN_ROWS_FOR_AUTO_ENCODE = 256;
    static constexpr UInt32 MAX_DICT_SIZE_AUTO = 65536;
    ColumnPtr original_string_col; // saved for restoration after auto-encode fast path
    size_t auto_encoded_arg_idx = args.size(); // sentinel
    if (num_dict_cols == 0 && args.size() >= 2)
    {
        size_t string_arg_idx = args.size();
        bool has_const_arg = false;
        for (size_t i = 0; i < args.size(); ++i)
        {
            const auto & col = block.getByPosition(args[i]).column;
            if (!col)
                continue;
            if (col->isColumnConst())
            {
                has_const_arg = true;
            }
            else if (string_arg_idx == args.size() && typeid_cast<const ColumnString *>(col.get()))
            {
                string_arg_idx = i;
            }
        }

        if (has_const_arg && string_arg_idx != args.size())
        {
            const auto & col = block.getByPosition(args[string_arg_idx]).column;
            size_t num_rows = col->size();
            if (num_rows >= MIN_ROWS_FOR_AUTO_ENCODE)
            {
                const auto * col_str = typeid_cast<const ColumnString *>(col.get());
                // StringRef keys point into the ColumnString's stable internal buffer.
                // dict_entries stores copies for ColumnDictionary but is NOT used as
                // hash map keys (avoids vector-reallocation dangling pointer bug).
                std::vector<Field> dict_entries;
                std::unordered_map<StringRef, UInt32> dict_map;
                PaddedPODArray<UInt32> ids;
                ids.reserve(num_rows);
                bool success = true;

                for (size_t i = 0; i < num_rows; ++i)
                {
                    StringRef ref = col_str->getDataAt(i);
                    auto it = dict_map.find(ref);
                    if (it != dict_map.end())
                    {
                        ids.push_back(it->second);
                    }
                    else
                    {
                        if (dict_entries.size() >= MAX_DICT_SIZE_AUTO)
                        {
                            success = false;
                            break;
                        }
                        UInt32 new_id = static_cast<UInt32>(dict_entries.size());
                        dict_map[ref] = new_id;
                        dict_entries.emplace_back(String(ref.data, ref.size));
                        ids.push_back(new_id);
                    }
                }

                if (success)
                {
                    auto dict_col_ptr = ColumnDictionary::createMutable(
                        std::move(dict_entries),
                        std::move(ids),
                        block.getByPosition(args[string_arg_idx]).type);
                    // Save original column so we can restore it after the fast path
                    // (the block may be used by subsequent operations that expect ColumnString)
                    original_string_col = block.getByPosition(args[string_arg_idx]).column;
                    auto_encoded_arg_idx = string_arg_idx;
                    block.getByPosition(args[string_arg_idx]).column = std::move(dict_col_ptr);
                    dict_arg_idx = string_arg_idx;
                    num_dict_cols = 1;
                }
            }
        }
    }

    if (num_dict_cols == 0)
        return false;

    const auto * dict_col = typeid_cast<const ColumnDictionary *>(
        block.getByPosition(args[dict_arg_idx]).column.get());
    if (!dict_col)
        return false;

    // Fast path: single dictionary column + all other args are const
    // Execute function on dictionary entries only (K values), then remap via IDs
    bool all_others_const = true;
    if (num_dict_cols == 1)
    {
        for (size_t i = 0; i < args.size(); ++i)
        {
            if (i == dict_arg_idx)
                continue;
            const auto & col = block.getByPosition(args[i]).column;
            if (col && !col->isColumnConst())
            {
                all_others_const = false;
                break;
            }
        }
    }
    else
    {
        all_others_const = false;
    }

    if (all_others_const && num_dict_cols == 1)
    {
        const auto & dictionary = dict_col->getDictionary();
        const auto & ids = dict_col->getDictionaryIds();
        size_t dict_size = dictionary.size();
        size_t num_rows = ids.size();

        // Build a temporary block with dictionary entries as a regular column
        Block dict_block;
        for (size_t i = 0; i < args.size(); ++i)
        {
            const auto & original = block.getByPosition(args[i]);
            if (i == dict_arg_idx)
            {
                // Replace dictionary column with its dictionary entries as ColumnString
                auto dict_string_col = dict_col->getValueType()->createColumn();
                for (size_t d = 0; d < dict_size; ++d)
                    dict_string_col->insert(dictionary[d]);
                dict_block.insert({std::move(dict_string_col), original.type, original.name});
            }
            else
            {
                // Resize const column to dict_size
                if (const auto * const_col = typeid_cast<const ColumnConst *>(original.column.get()))
                {
                    dict_block.insert(
                        {ColumnConst::create(const_col->getDataColumnPtr(), dict_size),
                         original.type,
                         original.name});
                }
                else
                {
                    dict_block.insert(original);
                }
            }
        }

        // Add result column placeholder
        dict_block.insert(block.getByPosition(result));

        // Build argument indices for the temporary block
        ColumnNumbers dict_args(args.size());
        for (size_t i = 0; i < args.size(); ++i)
            dict_args[i] = i;
        size_t dict_result = args.size();

        // Execute function on dictionary entries only
        executeImpl(dict_block, dict_args, dict_result);

        // Remap results: for each row, look up the pre-computed result using its dictionary ID
        const auto & dict_result_col = dict_block.getByPosition(dict_result).column;
        MutableColumnPtr remapped;

        // Fast remap for UInt8 result (common for comparison/filter functions)
        if (const auto * uint8_result = typeid_cast<const ColumnUInt8 *>(dict_result_col.get()))
        {
            auto uint8_remapped = ColumnUInt8::create();
            auto & out_data = uint8_remapped->getData();
            out_data.resize(num_rows);
            const auto & src_data = uint8_result->getData();
            for (size_t i = 0; i < num_rows; ++i)
                out_data[i] = src_data[ids[i]];
            remapped = std::move(uint8_remapped);
        }
        else
        {
            remapped = dict_result_col->cloneEmpty();
            remapped->reserve(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
                remapped->insertFrom(*dict_result_col, ids[i]);
        }

        block.getByPosition(result).column = std::move(remapped);
        // Restore original ColumnString if we auto-encoded it
        if (original_string_col && auto_encoded_arg_idx < args.size())
            block.getByPosition(args[auto_encoded_arg_idx]).column = original_string_col;
        return true;
    }

    // Slow path: multiple dictionary columns or mixed with non-const columns
    // Materialize all dictionary columns to regular columns
    Block materialized_block = block;
    for (size_t i = 0; i < args.size(); ++i)
    {
        auto & col_ref = materialized_block.getByPosition(args[i]);
        if (col_ref.column && col_ref.column->isDictionaryEncoded())
        {
            col_ref.column = col_ref.column->convertToFullColumnIfDictionary();
        }
    }

    executeImpl(materialized_block, args, result);
    block.getByPosition(result).column = materialized_block.getByPosition(result).column;
    // Restore original ColumnString if we auto-encoded it
    if (original_string_col && auto_encoded_arg_idx < args.size())
        block.getByPosition(args[auto_encoded_arg_idx]).column = original_string_col;
    return true;
}

void IExecutableFunction::execute(Block & block, const ColumnNumbers & args, size_t result) const
{
    if (defaultImplementationForConstantArguments(block, args, result))
        return;

    if (defaultImplementationForNulls(block, args, result))
        return;

    if (defaultImplementationForDictionaryColumns(block, args, result))
        return;

    executeImpl(block, args, result);
}

void IFunctionBuilder::checkNumberOfArguments(size_t number_of_arguments) const
{
    if (isVariadic())
        return;

    size_t expected_number_of_arguments = getNumberOfArguments();

    if (number_of_arguments != expected_number_of_arguments)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {} , should be {}",
            getName(),
            number_of_arguments,
            expected_number_of_arguments);
}

FunctionBasePtr IFunctionBuilder::build(
    const ColumnsWithTypeAndName & arguments,
    const TiDB::TiDBCollatorPtr & collator) const
{
    return buildImpl(arguments, getReturnType(arguments), collator);
}

DataTypePtr IFunctionBuilder::getReturnType(const ColumnsWithTypeAndName & arguments) const
{
    checkNumberOfArguments(arguments.size());

    if (!arguments.empty() && useDefaultImplementationForNulls())
    {
        NullPresence null_presense = getNullPresense(arguments);

        if (null_presense.has_null_constant)
        {
            return makeNullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presense.has_nullable)
        {
            Block nested_block = createBlockWithNestedColumns(
                Block(arguments),
                ext::collection_cast<ColumnNumbers>(ext::range(0, arguments.size())));
            auto return_type = getReturnTypeImpl(ColumnsWithTypeAndName(nested_block.begin(), nested_block.end()));
            return makeNullable(return_type);
        }
    }

    return getReturnTypeImpl(arguments);
}

void IFunctionBuilder::getLambdaArgumentTypes(DataTypes & arguments [[maybe_unused]]) const
{
    checkNumberOfArguments(arguments.size());
    return getLambdaArgumentTypesImpl(arguments);
}

} // namespace DB
