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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <gtest/gtest.h>

#include <future>
#include <random>
#include <thread>

namespace DB::DM::tests
{

class WritePathPerfTest : public ::testing::Test
{
protected:
    static Block buildMultiColumnBlock(size_t num_rows, size_t num_string_cols, size_t string_size)
    {
        Block block;
        std::mt19937 rng(42);

        // Add handle column (Int64)
        auto handle_col = ColumnInt64::create();
        for (size_t i = 0; i < num_rows; ++i)
            handle_col->insert(static_cast<Int64>(i));
        block.insert(ColumnWithTypeAndName{std::move(handle_col), std::make_shared<DataTypeInt64>(), "handle"});

        // Add version column (UInt64)
        auto version_col = ColumnUInt64::create();
        for (size_t i = 0; i < num_rows; ++i)
            version_col->insert(static_cast<UInt64>(1));
        block.insert(ColumnWithTypeAndName{std::move(version_col), std::make_shared<DataTypeUInt64>(), "version"});

        // Add string columns (simulating JSON-like payloads)
        for (size_t c = 0; c < num_string_cols; ++c)
        {
            auto str_col = ColumnString::create();
            for (size_t i = 0; i < num_rows; ++i)
            {
                String s(string_size, 'x');
                // Add some entropy for realistic compression
                for (size_t j = 0; j < string_size; j += 8)
                    s[j] = 'a' + (rng() % 26);
                str_col->insertData(s.data(), s.size());
            }
            block.insert(ColumnWithTypeAndName{
                std::move(str_col),
                std::make_shared<DataTypeString>(),
                fmt::format("col_{}", c)});
        }

        return block;
    }
};

/// Benchmark: Sequential column serialization (current approach)
TEST_F(WritePathPerfTest, SerializeColumnsSequential)
{
    const size_t num_rows = 8192;
    const size_t num_string_cols = 5;
    const size_t string_size = 512; // 512B per string value

    auto block = buildMultiColumnBlock(num_rows, num_string_cols, string_size);
    size_t total_bytes = block.bytes();

    // Warm up
    for (int warm = 0; warm < 2; ++warm)
    {
        MemoryWriteBuffer buf;
        for (const auto & col : block)
        {
            serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
        }
    }

    // Benchmark sequential serialization
    const int iterations = 10;
    Stopwatch sw;
    size_t total_compressed = 0;

    for (int iter = 0; iter < iterations; ++iter)
    {
        MemoryWriteBuffer buf;
        for (const auto & col : block)
        {
            serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
        }
        total_compressed += buf.count();
    }

    double elapsed_ms = sw.elapsedMilliseconds();
    double per_iter_ms = elapsed_ms / iterations;
    double throughput_mbs = (static_cast<double>(total_bytes) * iterations) / (elapsed_ms * 1000.0);

    fprintf(
        stderr,
        "\n[SerializeColumnsSequential] rows=%zu cols=%zu string_size=%zu\n"
        "  Total block bytes: %.1f MB\n"
        "  Per-iteration: %.2f ms\n"
        "  Throughput: %.1f MB/s\n"
        "  Avg compressed size: %.1f MB (ratio: %.2fx)\n",
        num_rows,
        num_string_cols + 2,
        string_size,
        total_bytes / 1e6,
        per_iter_ms,
        throughput_mbs,
        total_compressed / (iterations * 1e6),
        static_cast<double>(total_bytes * iterations) / total_compressed);
}

/// Benchmark: Parallel column serialization
TEST_F(WritePathPerfTest, SerializeColumnsParallel)
{
    const size_t num_rows = 8192;
    const size_t num_string_cols = 5;
    const size_t string_size = 512;

    auto block = buildMultiColumnBlock(num_rows, num_string_cols, string_size);
    size_t total_bytes = block.bytes();

    // Warm up
    for (int warm = 0; warm < 2; ++warm)
    {
        std::vector<std::future<String>> futures;
        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.getByPosition(i);
            futures.push_back(std::async(std::launch::async, [&col, num_rows]() {
                MemoryWriteBuffer buf;
                serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
                auto read_buf = buf.tryGetReadBuffer();
                String result(buf.count(), '\0');
                read_buf->readStrict(result.data(), result.size());
                return result;
            }));
        }
        for (auto & f : futures)
            f.get();
    }

    // Benchmark parallel serialization
    const int iterations = 10;
    Stopwatch sw;
    size_t total_compressed = 0;

    for (int iter = 0; iter < iterations; ++iter)
    {
        std::vector<std::future<String>> futures;
        futures.reserve(block.columns());

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.getByPosition(i);
            futures.push_back(std::async(std::launch::async, [&col, num_rows]() {
                MemoryWriteBuffer buf;
                serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
                auto read_buf = buf.tryGetReadBuffer();
                String result(buf.count(), '\0');
                read_buf->readStrict(result.data(), result.size());
                return result;
            }));
        }

        size_t compressed_this_iter = 0;
        for (auto & f : futures)
            compressed_this_iter += f.get().size();
        total_compressed += compressed_this_iter;
    }

    double elapsed_ms = sw.elapsedMilliseconds();
    double per_iter_ms = elapsed_ms / iterations;
    double throughput_mbs = (static_cast<double>(total_bytes) * iterations) / (elapsed_ms * 1000.0);

    fprintf(
        stderr,
        "\n[SerializeColumnsParallel] rows=%zu cols=%zu string_size=%zu\n"
        "  Total block bytes: %.1f MB\n"
        "  Per-iteration: %.2f ms\n"
        "  Throughput: %.1f MB/s\n"
        "  Avg compressed size: %.1f MB (ratio: %.2fx)\n",
        num_rows,
        num_string_cols + 2,
        string_size,
        total_bytes / 1e6,
        per_iter_ms,
        throughput_mbs,
        total_compressed / (iterations * 1e6),
        static_cast<double>(total_bytes * iterations) / total_compressed);
}

/// Benchmark: Large JSON-like columns (3KB per row, realistic ingest scenario)
TEST_F(WritePathPerfTest, SerializeLargeJsonSequentialVsParallel)
{
    const size_t num_rows = 8192;
    const size_t num_json_cols = 3;
    const size_t json_size = 3072; // 3KB per JSON blob

    auto block = buildMultiColumnBlock(num_rows, num_json_cols, json_size);
    size_t total_bytes = block.bytes();

    const int iterations = 5;

    // Sequential
    Stopwatch sw_seq;
    for (int iter = 0; iter < iterations; ++iter)
    {
        MemoryWriteBuffer buf;
        for (const auto & col : block)
        {
            serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
        }
    }
    double seq_ms = sw_seq.elapsedMilliseconds() / iterations;

    // Parallel
    Stopwatch sw_par;
    for (int iter = 0; iter < iterations; ++iter)
    {
        std::vector<std::future<String>> futures;
        futures.reserve(block.columns());

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.getByPosition(i);
            futures.push_back(std::async(std::launch::async, [&col, num_rows]() {
                MemoryWriteBuffer buf;
                serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
                auto read_buf = buf.tryGetReadBuffer();
                String result(buf.count(), '\0');
                read_buf->readStrict(result.data(), result.size());
                return result;
            }));
        }
        for (auto & f : futures)
            f.get();
    }
    double par_ms = sw_par.elapsedMilliseconds() / iterations;

    double speedup = seq_ms / par_ms;

    fprintf(
        stderr,
        "\n[SerializeLargeJson] rows=%zu json_cols=%zu json_size=%zu\n"
        "  Total block bytes: %.1f MB\n"
        "  Sequential: %.2f ms/iter\n"
        "  Parallel:   %.2f ms/iter\n"
        "  Speedup:    %.2fx\n",
        num_rows,
        num_json_cols,
        json_size,
        total_bytes / 1e6,
        seq_ms,
        par_ms,
        speedup);

    // Parallel should be faster for large blocks
    EXPECT_GT(speedup, 1.0) << "Parallel serialization should not be slower than sequential";
}

/// Benchmark: End-to-end writeColumnFileData simulation
/// Tests the full path: serialize all columns → write to buffer (simulates PageStorage write)
TEST_F(WritePathPerfTest, EndToEndWriteSequentialVsParallel)
{
    const size_t num_rows = 8192;
    const size_t num_string_cols = 8; // More columns = more parallelism benefit
    const size_t string_size = 1024;  // 1KB per string

    auto block = buildMultiColumnBlock(num_rows, num_string_cols, string_size);
    size_t total_bytes = block.bytes();

    const int iterations = 5;

    // Sequential (current implementation)
    Stopwatch sw_seq;
    for (int iter = 0; iter < iterations; ++iter)
    {
        MemoryWriteBuffer write_buf;
        PageFieldSizes col_data_sizes;
        for (const auto & col : block)
        {
            auto last_buf_size = write_buf.count();
            serializeColumn(write_buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
            col_data_sizes.push_back(write_buf.count() - last_buf_size);
        }
    }
    double seq_ms = sw_seq.elapsedMilliseconds() / iterations;

    // Parallel serialization + sequential assembly
    Stopwatch sw_par;
    for (int iter = 0; iter < iterations; ++iter)
    {
        // Phase 1: Parallel serialize each column into its own buffer
        std::vector<std::future<String>> futures;
        futures.reserve(block.columns());

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.getByPosition(i);
            futures.push_back(std::async(std::launch::async, [&col, num_rows]() {
                MemoryWriteBuffer buf;
                serializeColumn(buf, *col.column, col.type, 0, num_rows, CompressionMethod::LZ4, 1);
                auto read_buf = buf.tryGetReadBuffer();
                String result(buf.count(), '\0');
                read_buf->readStrict(result.data(), result.size());
                return result;
            }));
        }

        // Phase 2: Sequential assembly into final buffer
        MemoryWriteBuffer final_buf;
        PageFieldSizes col_data_sizes;
        for (auto & f : futures)
        {
            auto data = f.get();
            final_buf.write(data.data(), data.size());
            col_data_sizes.push_back(data.size());
        }
    }
    double par_ms = sw_par.elapsedMilliseconds() / iterations;

    double speedup = seq_ms / par_ms;

    fprintf(
        stderr,
        "\n[EndToEndWrite] rows=%zu cols=%zu string_size=%zu\n"
        "  Total block bytes: %.1f MB\n"
        "  Sequential: %.2f ms/iter\n"
        "  Parallel:   %.2f ms/iter\n"
        "  Speedup:    %.2fx\n",
        num_rows,
        num_string_cols + 2,
        string_size,
        total_bytes / 1e6,
        seq_ms,
        par_ms,
        speedup);

    EXPECT_GT(speedup, 1.0) << "Parallel write should not be slower than sequential";
}

} // namespace DB::DM::tests
