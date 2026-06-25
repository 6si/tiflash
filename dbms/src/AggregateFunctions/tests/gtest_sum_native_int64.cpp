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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumNativeInt64.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnVector.h>
#include <Common/Arena.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <numeric>
#include <random>
#include <vector>

namespace DB
{
namespace tests
{

class AggregateFunctionSumNativeInt64Test : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        try
        {
            registerAggregateFunctions();
        }
        catch (DB::Exception &)
        {
            // Already registered
        }
    }

    AggregateFunctionPtr getFunction(bool nullable = false)
    {
        DataTypePtr type = std::make_shared<DataTypeInt64>();
        if (nullable)
            type = std::make_shared<DataTypeNullable>(type);
        DataTypes arg_types = {type};
        return AggregateFunctionFactory::instance().get(
            *TiFlashTestEnv::getContext(),
            "sumNativeInt64",
            arg_types,
            {},
            0,
            false);
    }
};

/// Verify the function is registered and returns the correct type
TEST_F(AggregateFunctionSumNativeInt64Test, ReturnType)
try
{
    auto func = getFunction(false);
    ASSERT_NE(func, nullptr);
    ASSERT_EQ(func->getName(), "sumNativeInt64");

    auto ret_type = func->getReturnType();
    ASSERT_NE(ret_type, nullptr);

    // Should return Decimal128 type
    auto * dec_type = typeid_cast<const DataTypeDecimal<Decimal128> *>(ret_type.get());
    ASSERT_NE(dec_type, nullptr);
    ASSERT_EQ(dec_type->getPrec(), 38u);
    ASSERT_EQ(dec_type->getScale(), 0u);
}
CATCH

/// Basic correctness: sum a small set of values
TEST_F(AggregateFunctionSumNativeInt64Test, BasicSum)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    // Create a column with values [1, 2, 3, 4, 5]
    auto col = ColumnVector<Int64>::create();
    col->getData().assign({1, 2, 3, 4, 5});

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, 5, place, columns, &arena, -1);

    // Result should be 15
    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    ASSERT_EQ(result_col->getData().size(), 1u);
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(15));

    func->destroy(place);
}
CATCH

/// Sum with negative values
TEST_F(AggregateFunctionSumNativeInt64Test, NegativeValues)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    auto col = ColumnVector<Int64>::create();
    col->getData().assign({-100, 50, -200, 300, -50});

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, 5, place, columns, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    // Expected: -100 + 50 + (-200) + 300 + (-50) = 0
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(0));

    func->destroy(place);
}
CATCH

/// Int64 boundary values that would overflow a single Int64 accumulator
TEST_F(AggregateFunctionSumNativeInt64Test, OverflowInt64Boundary)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    // Two values at Int64 max: their sum overflows Int64 but not Int128
    Int64 max_val = std::numeric_limits<Int64>::max(); // 9223372036854775807
    auto col = ColumnVector<Int64>::create();
    col->getData().assign({max_val, max_val, max_val});

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, 3, place, columns, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    // Expected: 3 * 9223372036854775807 = 27670116110564327421
    Int128 expected = Int128(max_val) * 3;
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), expected);

    func->destroy(place);
}
CATCH

/// Int64 min values
TEST_F(AggregateFunctionSumNativeInt64Test, OverflowInt64Min)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    Int64 min_val = std::numeric_limits<Int64>::min(); // -9223372036854775808
    auto col = ColumnVector<Int64>::create();
    col->getData().assign({min_val, min_val});

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, 2, place, columns, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    // Expected: 2 * (-9223372036854775808) = -18446744073709551616
    Int128 expected = Int128(min_val) * 2;
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), expected);

    func->destroy(place);
}
CATCH

/// Large batch to exercise SIMD path (> 16 elements for AVX2 unrolled loop)
TEST_F(AggregateFunctionSumNativeInt64Test, LargeBatchSIMD)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    // Create 10000 values: alternating pattern to test SIMD correctness
    const size_t count = 10000;
    auto col = ColumnVector<Int64>::create();
    auto & data = col->getData();
    data.resize(count);

    Int128 expected_sum = 0;
    for (size_t i = 0; i < count; ++i)
    {
        Int64 val = static_cast<Int64>(i * 1000 - 5000000);
        data[i] = val;
        expected_sum += static_cast<Int128>(val);
    }

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, count, place, columns, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), expected_sum);

    func->destroy(place);
}
CATCH

/// Null-aware sum: test addBatchSinglePlaceNotNull directly on non-nullable function
/// (In production, the Null combinator wraps the function and handles null extraction)
TEST_F(AggregateFunctionSumNativeInt64Test, NullHandling)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    // Inner column values
    auto inner_col = ColumnVector<Int64>::create();
    inner_col->getData().assign({100, 200, 300, 400, 500});

    // null_map: 1 = null, 0 = not null
    // Mark index 1 and 3 as null
    UInt8 null_map[] = {0, 1, 0, 1, 0};

    const IColumn * columns[] = {inner_col.get()};
    func->addBatchSinglePlaceNotNull(0, 5, place, columns, null_map, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    // Expected: 100 + 300 + 500 = 900 (skip indices 1 and 3)
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(900));

    func->destroy(place);
}
CATCH

/// All-null input
TEST_F(AggregateFunctionSumNativeInt64Test, AllNull)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    auto inner_col = ColumnVector<Int64>::create();
    inner_col->getData().assign({100, 200, 300});

    UInt8 null_map[] = {1, 1, 1}; // all null

    const IColumn * columns[] = {inner_col.get()};
    func->addBatchSinglePlaceNotNull(0, 3, place, columns, null_map, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    // Expected: 0 (no values added)
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(0));

    func->destroy(place);
}
CATCH

/// Merge: simulates MPP partial result merging
TEST_F(AggregateFunctionSumNativeInt64Test, Merge)
try
{
    auto func = getFunction(false);
    Arena arena;

    // Create two partial states
    auto place1 = arena.alloc(func->sizeOfData());
    auto place2 = arena.alloc(func->sizeOfData());
    func->create(place1);
    func->create(place2);

    // State 1: sum of [1000, 2000, 3000] = 6000
    auto col1 = ColumnVector<Int64>::create();
    col1->getData().assign({1000, 2000, 3000});
    const IColumn * columns1[] = {col1.get()};
    func->addBatchSinglePlace(0, 3, place1, columns1, &arena, -1);

    // State 2: sum of [4000, 5000] = 9000
    auto col2 = ColumnVector<Int64>::create();
    col2->getData().assign({4000, 5000});
    const IColumn * columns2[] = {col2.get()};
    func->addBatchSinglePlace(0, 2, place2, columns2, &arena, -1);

    // Merge state2 into state1
    func->merge(place1, place2, &arena);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place1, *result_col, &arena);

    // Expected: 6000 + 9000 = 15000
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(15000));

    func->destroy(place1);
    func->destroy(place2);
}
CATCH

/// Serialize/Deserialize: state survives round-trip
TEST_F(AggregateFunctionSumNativeInt64Test, SerializeDeserialize)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    // Add values that overflow Int64
    Int64 max_val = std::numeric_limits<Int64>::max();
    auto col = ColumnVector<Int64>::create();
    col->getData().assign({max_val, max_val});
    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, 2, place, columns, &arena, -1);

    // Serialize
    String serialized;
    {
        WriteBufferFromString write_buf(serialized);
        func->serialize(place, write_buf);
    }

    // Deserialize into new state
    auto place2 = arena.alloc(func->sizeOfData());
    func->create(place2);

    ReadBufferFromString read_buf(serialized);
    func->deserialize(place2, read_buf, &arena);

    // Compare results
    auto result_col1 = ColumnDecimal<Decimal128>::create(0, 0);
    auto result_col2 = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col1, &arena);
    func->insertResultInto(place2, *result_col2, &arena);

    ASSERT_EQ(
        static_cast<Int128>(result_col1->getData()[0].value),
        static_cast<Int128>(result_col2->getData()[0].value));

    Int128 expected = Int128(max_val) * 2;
    ASSERT_EQ(static_cast<Int128>(result_col2->getData()[0].value), expected);

    func->destroy(place);
    func->destroy(place2);
}
CATCH

/// Benchmark-relevant test: simulate the actual workload (100K rows, values in [50, 5000])
TEST_F(AggregateFunctionSumNativeInt64Test, RealisticWorkload)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    const size_t count = 100000;
    auto col = ColumnVector<Int64>::create();
    auto & data = col->getData();
    data.resize(count);

    std::mt19937 rng(42);
    std::uniform_int_distribution<Int64> dist(50, 5000);

    Int128 expected_sum = 0;
    for (size_t i = 0; i < count; ++i)
    {
        data[i] = dist(rng);
        expected_sum += static_cast<Int128>(data[i]);
    }

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlace(0, count, place, columns, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), expected_sum);

    func->destroy(place);
}
CATCH

/// Single value per-row add path
TEST_F(AggregateFunctionSumNativeInt64Test, SingleRowAdd)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    auto col = ColumnVector<Int64>::create();
    col->getData().assign({7, -3, 10, 0, -14});

    const IColumn * columns[] = {col.get()};
    for (size_t i = 0; i < 5; ++i)
        func->add(place, columns, i, &arena);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    // Expected: 7 + (-3) + 10 + 0 + (-14) = 0
    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(0));

    func->destroy(place);
}
CATCH

/// Large batch with nulls to exercise SIMD null-aware path
TEST_F(AggregateFunctionSumNativeInt64Test, LargeBatchWithNulls)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    const size_t count = 8192; // typical TiFlash block size
    auto col = ColumnVector<Int64>::create();
    auto & data = col->getData();
    data.resize(count);

    std::vector<UInt8> null_map(count);

    Int128 expected_sum = 0;
    for (size_t i = 0; i < count; ++i)
    {
        data[i] = static_cast<Int64>(i * 7 - 28000);
        null_map[i] = (i % 3 == 0) ? 1 : 0; // every 3rd row is null
        if (!null_map[i])
            expected_sum += static_cast<Int128>(data[i]);
    }

    const IColumn * columns[] = {col.get()};
    func->addBatchSinglePlaceNotNull(0, count, place, columns, null_map.data(), &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), expected_sum);

    func->destroy(place);
}
CATCH

/// Verify that start_offset is respected
TEST_F(AggregateFunctionSumNativeInt64Test, BatchWithOffset)
try
{
    auto func = getFunction(false);
    Arena arena;

    auto place = arena.alloc(func->sizeOfData());
    func->create(place);

    auto col = ColumnVector<Int64>::create();
    col->getData().assign({10, 20, 30, 40, 50, 60, 70, 80, 90, 100});

    const IColumn * columns[] = {col.get()};
    // Sum only indices 3-7: 40+50+60+70+80 = 300
    func->addBatchSinglePlace(3, 5, place, columns, &arena, -1);

    auto result_col = ColumnDecimal<Decimal128>::create(0, 0);
    func->insertResultInto(place, *result_col, &arena);

    ASSERT_EQ(static_cast<Int128>(result_col->getData()[0].value), Int128(300));

    func->destroy(place);
}
CATCH

} // namespace tests
} // namespace DB
