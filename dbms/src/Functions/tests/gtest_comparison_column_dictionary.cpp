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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsComparison.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class ComparisonColumnDictionaryTest : public ::testing::Test
{
protected:
    static MutableColumnPtr makeDictColumn(
        const std::vector<String> & dict_entries,
        const std::vector<UInt32> & row_ids)
    {
        std::vector<Field> dictionary;
        dictionary.reserve(dict_entries.size());
        for (const auto & s : dict_entries)
            dictionary.emplace_back(s);

        PaddedPODArray<UInt32> ids;
        ids.reserve(row_ids.size());
        for (auto id : row_ids)
            ids.push_back(id);
        return ColumnDictionary::createMutable(
            std::move(dictionary),
            std::move(ids),
            std::make_shared<DataTypeString>());
    }
};

// --- compareAt tests ---

TEST_F(ComparisonColumnDictionaryTest, CompareAtDictVsString)
{
    auto dict_col = makeDictColumn({"apple", "banana", "cherry"}, {0, 1, 2, 0, 1});
    auto str_col = ColumnString::create();
    str_col->insert(Field(String("banana")));
    str_col->insert(Field(String("apple")));

    ASSERT_LT(dict_col->compareAt(0, 0, *str_col, 0), 0); // apple < banana
    ASSERT_EQ(dict_col->compareAt(1, 0, *str_col, 0), 0);  // banana == banana
    ASSERT_GT(dict_col->compareAt(2, 0, *str_col, 0), 0);  // cherry > banana
    ASSERT_GT(dict_col->compareAt(1, 1, *str_col, 0), 0);  // banana > apple
}

TEST_F(ComparisonColumnDictionaryTest, CompareAtDictVsDict)
{
    auto dict_col1 = makeDictColumn({"apple", "banana"}, {0, 1, 0});
    auto dict_col2 = makeDictColumn({"cherry", "apple"}, {0, 1, 0});

    ASSERT_LT(dict_col1->compareAt(0, 0, *dict_col2, 0), 0); // apple < cherry
    ASSERT_EQ(dict_col1->compareAt(0, 1, *dict_col2, 0), 0);  // apple == apple
    ASSERT_GT(dict_col1->compareAt(1, 1, *dict_col2, 0), 0);  // banana > apple
}

TEST_F(ComparisonColumnDictionaryTest, CompareAtSameValues)
{
    auto dict_col = makeDictColumn({"abc"}, {0, 0, 0});
    auto str_col = ColumnString::create();
    str_col->insert(Field(String("abc")));

    ASSERT_EQ(dict_col->compareAt(0, 0, *str_col, 0), 0);
    ASSERT_EQ(dict_col->compareAt(1, 0, *str_col, 0), 0);
    ASSERT_EQ(dict_col->compareAt(2, 0, *str_col, 0), 0);
}

// --- insertRangeFrom mixed type tests ---

TEST_F(ComparisonColumnDictionaryTest, InsertRangeFromColumnString)
{
    auto dict_col = makeDictColumn({"a", "b"}, {0, 1});
    auto str_col = ColumnString::create();
    str_col->insert(Field(String("b")));
    str_col->insert(Field(String("c")));
    str_col->insert(Field(String("a")));

    dict_col->insertRangeFrom(*str_col, 0, 3);

    ASSERT_EQ(dict_col->size(), 5);
    ASSERT_EQ((*dict_col)[0].get<String>(), "a");
    ASSERT_EQ((*dict_col)[1].get<String>(), "b");
    ASSERT_EQ((*dict_col)[2].get<String>(), "b");
    ASSERT_EQ((*dict_col)[3].get<String>(), "c");
    ASSERT_EQ((*dict_col)[4].get<String>(), "a");
}

TEST_F(ComparisonColumnDictionaryTest, InsertManyFromColumnString)
{
    auto dict_col = makeDictColumn({"x", "y"}, {0});
    auto str_col = ColumnString::create();
    str_col->insert(Field(String("z")));

    dict_col->insertManyFrom(*str_col, 0, 3);

    ASSERT_EQ(dict_col->size(), 4); // 1 original + 3 inserted
    ASSERT_EQ((*dict_col)[0].get<String>(), "x");
    ASSERT_EQ((*dict_col)[1].get<String>(), "z");
    ASSERT_EQ((*dict_col)[2].get<String>(), "z");
    ASSERT_EQ((*dict_col)[3].get<String>(), "z");
}

TEST_F(ComparisonColumnDictionaryTest, InsertRangeFromSameDictionary)
{
    // When source and dest share the same dictionary, IDs copy directly
    auto dict_col = makeDictColumn({"a", "b", "c"}, {0, 1, 2});
    // Clone it - shares same dictionary object? No - it's a copy.
    // This tests the fallback path (different dictionaries)
    auto dict_col2 = makeDictColumn({"a", "b", "c"}, {2, 0, 1});

    dict_col->insertRangeFrom(*dict_col2, 0, 3);

    ASSERT_EQ(dict_col->size(), 6);
    ASSERT_EQ((*dict_col)[3].get<String>(), "c");
    ASSERT_EQ((*dict_col)[4].get<String>(), "a");
    ASSERT_EQ((*dict_col)[5].get<String>(), "b");
}

// --- FunctionComparison dictionary fast path tests ---
// These test that FunctionComparison::tryExecuteColumnDictionary works by
// calling executeImpl directly through the IFunction interface.

TEST_F(ComparisonColumnDictionaryTest, EqualsWithConstFastPath)
{
    // Dictionary: {"active"=0, "pending"=1, "closed"=2}
    // Rows: [active, pending, active, closed, pending]
    auto dict_col = makeDictColumn({"active", "pending", "closed"}, {0, 1, 0, 2, 1});

    auto str_const = ColumnString::create();
    str_const->insert(Field(String("active")));
    auto const_col = ColumnConst::create(std::move(str_const), 5);

    Block block;
    block.insert({ColumnPtr(std::move(dict_col)), std::make_shared<DataTypeString>(), "left"});
    block.insert({std::move(const_col), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    // Create and call FunctionComparison directly
    auto func = std::make_shared<FunctionComparison<EqualsOp, NameEquals>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 1); // active == active
    ASSERT_EQ(res_col->getUInt(1), 0); // pending == active
    ASSERT_EQ(res_col->getUInt(2), 1); // active == active
    ASSERT_EQ(res_col->getUInt(3), 0); // closed == active
    ASSERT_EQ(res_col->getUInt(4), 0); // pending == active
}

TEST_F(ComparisonColumnDictionaryTest, NotEqualsWithConstFastPath)
{
    auto dict_col = makeDictColumn({"active", "pending", "closed"}, {0, 1, 0, 2, 1});

    auto str_const = ColumnString::create();
    str_const->insert(Field(String("active")));
    auto const_col = ColumnConst::create(std::move(str_const), 5);

    Block block;
    block.insert({ColumnPtr(std::move(dict_col)), std::make_shared<DataTypeString>(), "left"});
    block.insert({std::move(const_col), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    auto func = std::make_shared<FunctionComparison<NotEqualsOp, NameNotEquals>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 0); // active != active -> false
    ASSERT_EQ(res_col->getUInt(1), 1); // pending != active -> true
    ASSERT_EQ(res_col->getUInt(2), 0);
    ASSERT_EQ(res_col->getUInt(3), 1);
    ASSERT_EQ(res_col->getUInt(4), 1);
}

TEST_F(ComparisonColumnDictionaryTest, LessWithConstFastPath)
{
    // Lexicographic: "active" < "closed" < "pending"
    auto dict_col = makeDictColumn({"active", "pending", "closed"}, {0, 1, 0, 2, 1});

    auto str_const = ColumnString::create();
    str_const->insert(Field(String("closed")));
    auto const_col = ColumnConst::create(std::move(str_const), 5);

    Block block;
    block.insert({ColumnPtr(std::move(dict_col)), std::make_shared<DataTypeString>(), "left"});
    block.insert({std::move(const_col), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    auto func = std::make_shared<FunctionComparison<LessOp, NameLess>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 1); // active < closed -> true
    ASSERT_EQ(res_col->getUInt(1), 0); // pending < closed -> false
    ASSERT_EQ(res_col->getUInt(2), 1); // active < closed -> true
    ASSERT_EQ(res_col->getUInt(3), 0); // closed < closed -> false
    ASSERT_EQ(res_col->getUInt(4), 0); // pending < closed -> false
}

TEST_F(ComparisonColumnDictionaryTest, ConstOnLeftSide)
{
    // Constant on LEFT, dictionary on RIGHT
    auto dict_col = makeDictColumn({"active", "pending", "closed"}, {0, 1, 0, 2, 1});

    auto str_const = ColumnString::create();
    str_const->insert(Field(String("active")));
    auto const_col = ColumnConst::create(std::move(str_const), 5);

    Block block;
    block.insert({std::move(const_col), std::make_shared<DataTypeString>(), "left"});
    block.insert({ColumnPtr(std::move(dict_col)), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    auto func = std::make_shared<FunctionComparison<EqualsOp, NameEquals>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 1); // active == active
    ASSERT_EQ(res_col->getUInt(1), 0); // active == pending
    ASSERT_EQ(res_col->getUInt(2), 1);
    ASSERT_EQ(res_col->getUInt(3), 0);
    ASSERT_EQ(res_col->getUInt(4), 0);
}

TEST_F(ComparisonColumnDictionaryTest, GreaterOrEqualsWithConstFastPath)
{
    auto dict_col = makeDictColumn({"active", "pending", "closed"}, {0, 1, 0, 2, 1});

    auto str_const = ColumnString::create();
    str_const->insert(Field(String("closed")));
    auto const_col = ColumnConst::create(std::move(str_const), 5);

    Block block;
    block.insert({ColumnPtr(std::move(dict_col)), std::make_shared<DataTypeString>(), "left"});
    block.insert({std::move(const_col), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    auto func = std::make_shared<FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 0); // active >= closed -> false
    ASSERT_EQ(res_col->getUInt(1), 1); // pending >= closed -> true
    ASSERT_EQ(res_col->getUInt(2), 0);
    ASSERT_EQ(res_col->getUInt(3), 1); // closed >= closed -> true
    ASSERT_EQ(res_col->getUInt(4), 1);
}

TEST_F(ComparisonColumnDictionaryTest, SingleDictEntry)
{
    auto dict_col = makeDictColumn({"only_value"}, {0, 0, 0});

    auto str_const = ColumnString::create();
    str_const->insert(Field(String("only_value")));
    auto const_col = ColumnConst::create(std::move(str_const), 3);

    Block block;
    block.insert({ColumnPtr(std::move(dict_col)), std::make_shared<DataTypeString>(), "left"});
    block.insert({std::move(const_col), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    auto func = std::make_shared<FunctionComparison<EqualsOp, NameEquals>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 1);
    ASSERT_EQ(res_col->getUInt(1), 1);
    ASSERT_EQ(res_col->getUInt(2), 1);
}

TEST_F(ComparisonColumnDictionaryTest, FallbackToGenericWhenBothDicts)
{
    // Two dict columns (not dict + const) should fall through to executeGeneric
    // which now works because compareAt is implemented
    auto dict_col1 = makeDictColumn({"a", "b"}, {0, 1, 0});
    auto dict_col2 = makeDictColumn({"a", "c"}, {0, 1, 0});

    Block block;
    block.insert({ColumnPtr(std::move(dict_col1)), std::make_shared<DataTypeString>(), "left"});
    block.insert({ColumnPtr(std::move(dict_col2)), std::make_shared<DataTypeString>(), "right"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});

    auto func = std::make_shared<FunctionComparison<EqualsOp, NameEquals>>();
    func->executeImpl(block, {0, 1}, 2);

    const auto & res_col = block.getByPosition(2).column;
    ASSERT_EQ(res_col->getUInt(0), 1); // a == a
    ASSERT_EQ(res_col->getUInt(1), 0); // b == c -> false
    ASSERT_EQ(res_col->getUInt(2), 1); // a == a
}

} // namespace DB::tests
