#include "residual_i8_kernel.h"

#include <immintrin.h>
#include <stdint.h>
#include <string.h>

static inline float marmot_decode_float16(uint16_t h) {
  uint32_t sign = ((uint32_t)h & 0x8000u) << 16;
  uint32_t exp = ((uint32_t)h >> 10) & 0x1fu;
  uint32_t mant = (uint32_t)h & 0x03ffu;
  uint32_t bits;
  if (exp == 0) {
    if (mant == 0) {
      bits = sign;
    } else {
      exp = 127 - 15 + 1;
      while ((mant & 0x0400u) == 0) {
        mant <<= 1;
        exp--;
      }
      mant &= 0x03ffu;
      bits = sign | (exp << 23) | (mant << 13);
    }
  } else if (exp == 0x1fu) {
    bits = sign | 0x7f800000u | (mant << 13);
  } else {
    bits = sign | ((exp + (127 - 15)) << 23) | (mant << 13);
  }
  float out;
  memcpy(&out, &bits, sizeof(out));
  return out;
}

__attribute__((target("avx")))
static inline int32_t marmot_hsum256_epi32(__m256i v) {
  __m128i lo = _mm256_castsi256_si128(v);
  __m128i hi = _mm_castps_si128(_mm256_extractf128_ps(_mm256_castsi256_ps(v), 1));
  __m128i sum = _mm_add_epi32(lo, hi);
  __m128i shuf = _mm_shuffle_epi32(sum, _MM_SHUFFLE(2, 3, 0, 1));
  sum = _mm_add_epi32(sum, shuf);
  shuf = _mm_shuffle_epi32(sum, _MM_SHUFFLE(1, 0, 3, 2));
  sum = _mm_add_epi32(sum, shuf);
  return _mm_cvtsi128_si32(sum);
}

#if defined(__AVX2INTRIN_H)
__attribute__((target("avx2")))
static int32_t marmot_dot_i8_avx2_impl(const int8_t* a, const int8_t* b, int n) {
  __m256i acc = _mm256_setzero_si256();
  int i = 0;
  for (; i + 31 < n; i += 32) {
    __m128i a0 = _mm_loadu_si128((const __m128i*)(a + i));
    __m128i b0 = _mm_loadu_si128((const __m128i*)(b + i));
    __m128i a1 = _mm_loadu_si128((const __m128i*)(a + i + 16));
    __m128i b1 = _mm_loadu_si128((const __m128i*)(b + i + 16));
    __m256i a0_16 = _mm256_cvtepi8_epi16(a0);
    __m256i b0_16 = _mm256_cvtepi8_epi16(b0);
    __m256i a1_16 = _mm256_cvtepi8_epi16(a1);
    __m256i b1_16 = _mm256_cvtepi8_epi16(b1);
    acc = _mm256_add_epi32(acc, _mm256_madd_epi16(a0_16, b0_16));
    acc = _mm256_add_epi32(acc, _mm256_madd_epi16(a1_16, b1_16));
  }
  int32_t sum = marmot_hsum256_epi32(acc);
  for (; i < n; i++) {
    sum += (int32_t)a[i] * (int32_t)b[i];
  }
  return sum;
}
#endif

#if defined(__AVXVNNIINT8INTRIN_H)
__attribute__((target("avxvnniint8")))
static int32_t marmot_dot_i8_vnni_impl(const int8_t* a, const int8_t* b, int n) {
  __m256i acc = _mm256_setzero_si256();
  int i = 0;
  for (; i + 31 < n; i += 32) {
    __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
    __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));
    acc = _mm256_dpbssd_epi32(acc, va, vb);
  }
  int32_t sum = marmot_hsum256_epi32(acc);
  for (; i < n; i++) {
    sum += (int32_t)a[i] * (int32_t)b[i];
  }
  return sum;
}
#endif

static void marmot_score_span(
    int rank_metric,
    float query_norm2,
    float base_dot,
    int dim,
    int blocks,
    int block_size,
    const float* query_scales,
    const int8_t* query_codes,
    const uint8_t* rows,
    int entry_size,
    int count,
    float* out,
    int32_t (*dot_fn)(const int8_t*, const int8_t*, int)) {
  for (int row = 0; row < count; row++) {
    const uint8_t* blob = rows + ((size_t)row * (size_t)entry_size) + 8u;
    float norm2 = 0.0f;
    int off = 0;
    if (rank_metric == 0) {
      memcpy(&norm2, blob, sizeof(norm2));
      off = 4;
    }
    const uint8_t* scale_bytes = blob + off;
    const int8_t* codes = (const int8_t*)(blob + off + blocks * 2);
    float residual_dot = 0.0f;
    for (int block = 0; block < blocks; block++) {
      float residual_scale = marmot_decode_float16(
          (uint16_t)scale_bytes[block * 2] |
          ((uint16_t)scale_bytes[block * 2 + 1] << 8));
      float query_scale = query_scales[block];
      if (residual_scale == 0.0f || query_scale == 0.0f) {
        continue;
      }
      int start = block * block_size;
      int n = block_size;
      if (start + n > dim) {
        n = dim - start;
      }
      int32_t dot = dot_fn(query_codes + start, codes + start, n);
      residual_dot += query_scale * residual_scale * (float)dot;
    }
    float dot = base_dot + residual_dot;
    if (rank_metric == 2) {
      out[row] = 1.0f - dot;
    } else {
      out[row] = query_norm2 + norm2 - 2.0f * dot;
    }
  }
}

int marmot_residual_score_span_avx2(
    int rank_metric,
    float query_norm2,
    float base_dot,
    int dim,
    int blocks,
    int block_size,
    const float* query_scales,
    const int8_t* query_codes,
    const uint8_t* rows,
    int entry_size,
    int count,
    float* out) {
#if defined(__AVX2INTRIN_H)
  marmot_score_span(rank_metric, query_norm2, base_dot, dim, blocks, block_size,
                    query_scales, query_codes, rows, entry_size, count, out,
                    marmot_dot_i8_avx2_impl);
  return 1;
#else
  (void)rank_metric;
  (void)query_norm2;
  (void)base_dot;
  (void)dim;
  (void)blocks;
  (void)block_size;
  (void)query_scales;
  (void)query_codes;
  (void)rows;
  (void)entry_size;
  (void)count;
  (void)out;
  return 0;
#endif
}

int marmot_residual_score_span_vnni(
    int rank_metric,
    float query_norm2,
    float base_dot,
    int dim,
    int blocks,
    int block_size,
    const float* query_scales,
    const int8_t* query_codes,
    const uint8_t* rows,
    int entry_size,
    int count,
    float* out) {
#if defined(__AVXVNNIINT8INTRIN_H)
  marmot_score_span(rank_metric, query_norm2, base_dot, dim, blocks, block_size,
                    query_scales, query_codes, rows, entry_size, count, out,
                    marmot_dot_i8_vnni_impl);
  return 1;
#else
  (void)rank_metric;
  (void)query_norm2;
  (void)base_dot;
  (void)dim;
  (void)blocks;
  (void)block_size;
  (void)query_scales;
  (void)query_codes;
  (void)rows;
  (void)entry_size;
  (void)count;
  (void)out;
  return 0;
#endif
}

int marmot_residual_score_span_arm64(
    int rank_metric,
    float query_norm2,
    float base_dot,
    int dim,
    int blocks,
    int block_size,
    const float* query_scales,
    const int8_t* query_codes,
    const uint8_t* rows,
    int entry_size,
    int count,
    float* out) {
  (void)rank_metric;
  (void)query_norm2;
  (void)base_dot;
  (void)dim;
  (void)blocks;
  (void)block_size;
  (void)query_scales;
  (void)query_codes;
  (void)rows;
  (void)entry_size;
  (void)count;
  (void)out;
  return 0;
}
