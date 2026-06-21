# SIMD Optimization Plan for Encoded Columnstore Operations

## Status: Deferred (Phase 5)
Prerequisite: Validate dictionary-aware filter delivers 3-5x on live cluster first.

## Current State (No Explicit SIMD)

The dictionary ID scan loop operates on `PaddedPODArray<UInt32>` (64-byte aligned).
Clang auto-vectorizes simple equality loops at `-O2`, but does not generate optimal
SIMD for masked scans or multi-predicate evaluation.

## Opportunities

### 1. Dictionary ID Scan (Highest Impact)

**Current code (EncodedFilter::evaluateEquals):**
```cpp
for (size_t i = 0; i < num_rows; ++i)
    filter[i] = (ids[i] == target_id) ? 1 : 0;
```

**SIMD version (AVX2 — 8 IDs per instruction):**
```cpp
#include <immintrin.h>

__m256i target = _mm256_set1_epi32(target_id);
for (size_t i = 0; i + 8 <= num_rows; i += 8)
{
    __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&ids[i]));
    __m256i cmp = _mm256_cmpeq_epi32(data, target);
    // Pack 32-bit mask → 8-bit results
    int mask = _mm256_movemask_epi8(cmp);
    // Convert to byte filter (every 4th bit → 1 byte)
    for (int j = 0; j < 8; ++j)
        filter[i + j] = (mask >> (j * 4)) & 1;
}
// Scalar tail for remaining rows
```

**AVX-512 version (16 IDs per instruction):**
```cpp
__m512i target = _mm512_set1_epi32(target_id);
for (size_t i = 0; i + 16 <= num_rows; i += 16)
{
    __m512i data = _mm512_loadu_si512(&ids[i]);
    __mmask16 mask = _mm512_cmpeq_epi32_mask(data, target);
    // Direct mask → filter array
    for (int j = 0; j < 16; ++j)
        filter[i + j] = (mask >> j) & 1;
}
```

**Expected speedup:** 4-8x on the ID scan loop itself.
**Impact on overall query:** Depends on ratio of scan time vs I/O. At 30M rows with
warm page cache, scan is ~30% of total → overall 1.5-2x additional.

### 2. Multi-Predicate Evaluation (AND/OR filters)

When a query has `WHERE event = 'purchase' AND source = 'web'`, each predicate
produces a UInt8 filter array. Combining them:

**Current:**
```cpp
for (size_t i = 0; i < n; ++i)
    result[i] = filter_a[i] & filter_b[i];
```

**SIMD (AVX2 — 32 bytes per instruction):**
```cpp
for (size_t i = 0; i + 32 <= n; i += 32)
{
    __m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&filter_a[i]));
    __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(&filter_b[i]));
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(&result[i]), _mm256_and_si256(a, b));
}
```

### 3. Null Map Application

Combine filter result with null map (nulls should not pass filter):

```cpp
// SIMD: filter[i] &= !null_map[i]  →  filter = filter AND_NOT null_map
__m256i f = _mm256_loadu_si256(&filter[i]);
__m256i n = _mm256_loadu_si256(&null_map[i]);
_mm256_storeu_si256(&filter[i], _mm256_andnot_si256(n, f));
```

### 4. Selectivity Counting (popcount)

After filter is computed, count passing rows for optimizer feedback:

```cpp
size_t count = 0;
for (size_t i = 0; i + 8 <= n; i += 8)
{
    uint64_t word = *reinterpret_cast<const uint64_t *>(&filter[i]);
    count += __builtin_popcountll(word & 0x0101010101010101ULL);
}
```

### 5. Dictionary LIKE with Short Patterns

For short fixed patterns (e.g., prefix match `'purchase%'`), can use SIMD string
comparison on dictionary entries. Not beneficial for the typical 5-50 entry
dictionary, but useful if dictionary grows to thousands of entries.

## Implementation Plan

1. **Gate on CPU features at runtime** — Use `__builtin_cpu_supports("avx2")` to
   select SIMD vs scalar path. TiFlash already has CPU feature detection.

2. **Add to EncodedFilter** — The `evaluateEquals`, `evaluateNotEquals`, `evaluateIn`,
   and `evaluateLike` methods should dispatch to SIMD kernels when available.

3. **Benchmark methodology** — Use `gtests_dbms` performance test
   (`DictionaryFilterPerformance`) at 1M and 10M rows, measure with and without
   SIMD, report cycles/row.

4. **Files to modify:**
   - `dbms/src/Storages/DeltaMerge/Encoded/EncodedFilter.cpp` — SIMD kernels
   - `dbms/src/Storages/DeltaMerge/Encoded/EncodedFilterSIMD.h` — new header with intrinsics
   - `CMakeLists.txt` — add `-mavx2` flag for the SIMD compilation unit

5. **Fallback** — Always provide scalar path. SIMD is an optimization, not a requirement.

## Expected Combined Impact

| Component | Current | With SIMD | Notes |
|-----------|---------|-----------|-------|
| Dictionary ID scan | ~14µs/100K rows | ~2-3µs/100K rows | 4-8x on loop |
| Filter combination | trivial at 1M | trivial | Only matters at 10M+ |
| Null map application | trivial | trivial | Only matters at 10M+ |
| Overall single-path filter query (30M rows, warm cache) | TBD after arch fix | +1.2-1.5x on top | Diminishing returns vs I/O |

## References

- SingleStore: "Encoded Processing" whitepaper (VLDB 2020)
- ClickHouse: `src/Columns/ColumnVector.cpp` uses SIMD for filter operations
- TiFlash existing SIMD: `dbms/src/Common/` has some SSE/AVX utilities
- Knowledge note: "Encoded Columnstore Plan for TiDB/TiFlash" — Phase 5
