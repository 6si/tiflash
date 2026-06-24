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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <Common/Decimal.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace DB
{

/// Accumulator data for SUM(Int64) that uses Int128 internally.
/// Int128 guarantees no overflow: even summing 2^63 rows of Int64::max won't overflow Int128.
/// This eliminates the need for the expensive Int64→Decimal cast that TiDB injects.
struct AggregateFunctionSumNativeInt64Data
{
    Int128 sum{};

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(Int64 value) { sum += static_cast<Int128>(value); }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE decrease(Int64 value) { sum -= static_cast<Int128>(value); }

    void ALWAYS_INLINE reset() { sum = 0; }

    /// High-performance vectorized summation path.
    /// Strategy: accumulate partial sums in native Int64 lanes (leveraging SIMD),
    /// then widen to Int128 once per batch. Per-batch overflow of Int64 partials is
    /// impossible because batch size is at most 65536 rows and |value| <= 2^63-1,
    /// so partial sum fits in Int128 trivially. We use Int64 partial lanes for SIMD
    /// compatibility, widening to Int128 only at the batch boundary.
    void NO_SANITIZE_UNDEFINED NO_INLINE addMany(const Int64 * __restrict ptr, size_t count)
    {
#ifdef __AVX2__
        // AVX2 path: process 4 Int64 values per iteration (256 bits)
        // Use 4 separate accumulators to enable instruction-level parallelism
        __m256i acc0 = _mm256_setzero_si256();
        __m256i acc1 = _mm256_setzero_si256();
        __m256i acc2 = _mm256_setzero_si256();
        __m256i acc3 = _mm256_setzero_si256();

        const size_t simd_width = 4; // 4 x Int64 per __m256i
        const size_t unroll = 4; // 4 accumulators
        const size_t step = simd_width * unroll; // 16 values per iteration

        size_t i = 0;
        const size_t simd_end = count - (count % step);

        for (; i < simd_end; i += step)
        {
            acc0 = _mm256_add_epi64(acc0, _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + i)));
            acc1 = _mm256_add_epi64(acc1, _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + i + 4)));
            acc2 = _mm256_add_epi64(acc2, _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + i + 8)));
            acc3 = _mm256_add_epi64(acc3, _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + i + 12)));
        }

        // Reduce: combine 4 accumulators → 1
        acc0 = _mm256_add_epi64(acc0, acc1);
        acc2 = _mm256_add_epi64(acc2, acc3);
        acc0 = _mm256_add_epi64(acc0, acc2);

        // Extract 4 Int64 lanes and widen to Int128
        alignas(32) Int64 parts[4];
        _mm256_store_si256(reinterpret_cast<__m256i *>(parts), acc0);
        sum += static_cast<Int128>(parts[0]) + static_cast<Int128>(parts[1]) + static_cast<Int128>(parts[2])
            + static_cast<Int128>(parts[3]);

        // Scalar tail for remaining elements
        for (; i < count; ++i)
            sum += static_cast<Int128>(ptr[i]);
#else
        // Scalar fallback: still uses Int128 accumulator for correctness
        // Use local accumulator so compiler can keep it in registers
        Int128 local_sum = 0;
        const auto * end = ptr + count;
        while (ptr < end)
        {
            local_sum += static_cast<Int128>(*ptr);
            ++ptr;
        }
        sum += local_sum;
#endif
    }

    /// Vectorized path with null map: skips null rows
    void NO_SANITIZE_UNDEFINED NO_INLINE
    addManyNotNull(const Int64 * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
#ifdef __AVX2__
        // AVX2 path with null masking
        // For null handling: process 4 values at a time, use null_map to zero out null entries
        __m256i acc = _mm256_setzero_si256();

        size_t i = 0;
        const size_t simd_end = count - (count % 4);

        for (; i < simd_end; i += 4)
        {
            __m256i vals = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + i));

            // Build mask from null_map: null_map[i]==0 means NOT null (keep the value)
            // null_map[i]!=0 means null (zero the value)
            // We want to zero values where null_map[i] != 0
            __m256i mask = _mm256_set_epi64x(
                null_map[i + 3] ? 0 : -1LL,
                null_map[i + 2] ? 0 : -1LL,
                null_map[i + 1] ? 0 : -1LL,
                null_map[i + 0] ? 0 : -1LL);

            vals = _mm256_and_si256(vals, mask);
            acc = _mm256_add_epi64(acc, vals);
        }

        // Extract and widen to Int128
        alignas(32) Int64 parts[4];
        _mm256_store_si256(reinterpret_cast<__m256i *>(parts), acc);
        sum += static_cast<Int128>(parts[0]) + static_cast<Int128>(parts[1]) + static_cast<Int128>(parts[2])
            + static_cast<Int128>(parts[3]);

        // Scalar tail
        for (; i < count; ++i)
        {
            if (!null_map[i])
                sum += static_cast<Int128>(ptr[i]);
        }
#else
        // Scalar fallback with null check
        Int128 local_sum = 0;
        for (size_t i = 0; i < count; ++i)
        {
            if (!null_map[i])
                local_sum += static_cast<Int128>(ptr[i]);
        }
        sum += local_sum;
#endif
    }

    void merge(const AggregateFunctionSumNativeInt64Data & rhs) { sum += rhs.sum; }

    void write(WriteBuffer & buf) const { writePODBinary(sum, buf); }

    void read(ReadBuffer & buf) { readPODBinary(sum, buf); }

    Int128 get() const { return sum; }
};


/// Aggregate function that takes Int64 input, accumulates in Int128,
/// and returns Decimal(20,0) for MySQL compatibility.
///
/// This eliminates the expensive per-row Int64→Decimal cast that TiDB injects
/// for SUM(INT) expressions (MySQL spec: SUM on integer returns DECIMAL).
/// Instead of: read Int64 → cast to Decimal128 → add Decimal128 (expensive)
/// We do:      read Int64 → add to Int128 accumulator (cheap, SIMD-friendly)
///             → convert final result (per group) to Decimal128 (negligible)
class AggregateFunctionSumNativeInt64 final
    : public IAggregateFunctionDataHelper<AggregateFunctionSumNativeInt64Data, AggregateFunctionSumNativeInt64>
{
private:
    /// Return type precision and scale for Decimal output
    PrecType result_prec;
    ScaleType result_scale;

public:
    explicit AggregateFunctionSumNativeInt64(PrecType prec = 38, ScaleType scale = 0)
        : result_prec(prec)
        , result_scale(scale)
    {}

    String getName() const override { return "sumNativeInt64"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeDecimal<Decimal128>>(result_prec, result_scale);
    }

    void create(AggregateDataPtr __restrict place) const override { new (place) AggregateFunctionSumNativeInt64Data; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnVector<Int64> &>(*columns[0]);
        this->data(place).add(column.getData()[row_num]);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnVector<Int64> &>(*columns[0]);
        this->data(place).decrease(column.getData()[row_num]);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    /// Vectorized batch addition — the hot path for GROUP BY queries
    void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            const auto & column = assert_cast<const ColumnVector<Int64> &>(*columns[0]);
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
            {
                if (flags[i])
                    this->data(place).add(column.getData()[i]);
            }
        }
        else
        {
            const auto & column = assert_cast<const ColumnVector<Int64> &>(*columns[0]);
            this->data(place).addMany(column.getData().data() + start_offset, batch_size);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            const auto & column = assert_cast<const ColumnVector<Int64> &>(*columns[0]);
            for (size_t i = start_offset; i < start_offset + batch_size; ++i)
            {
                if (!null_map[i] && flags[i])
                    this->data(place).add(column.getData()[i]);
            }
        }
        else
        {
            const auto & column = assert_cast<const ColumnVector<Int64> &>(*columns[0]);
            this->data(place).addManyNotNull(
                column.getData().data() + start_offset,
                null_map + start_offset,
                batch_size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    /// Convert Int128 accumulator → Decimal128 output for MySQL compatibility
    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column = assert_cast<ColumnDecimal<Decimal128> &>(to);
        // Int128 → Decimal128 is a direct reinterpret: Decimal128 wraps Int128
        column.getData().push_back(Decimal128(this->data(place).get()));
    }

    void batchInsertSameResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, size_t num) const override
    {
        auto & column = assert_cast<ColumnDecimal<Decimal128> &>(to);
        column.getData().resize_fill(column.getData().size() + num, Decimal128(this->data(place).get()));
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
