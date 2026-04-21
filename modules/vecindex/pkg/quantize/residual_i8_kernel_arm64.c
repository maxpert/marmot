#include "residual_i8_kernel.h"

#include <arm_neon.h>
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

__attribute__((target("dotprod")))
static int32_t marmot_dot_i8_dotprod_impl(const int8_t* a, const int8_t* b, int n) {
  int32x4_t acc = vdupq_n_s32(0);
  int i = 0;
  for (; i + 15 < n; i += 16) {
    int8x16_t va = vld1q_s8(a + i);
    int8x16_t vb = vld1q_s8(b + i);
    acc = vdotq_s32(acc, va, vb);
  }
  int32_t sum = vaddvq_s32(acc);
  for (; i < n; i++) {
    sum += (int32_t)a[i] * (int32_t)b[i];
  }
  return sum;
}

__attribute__((target("dotprod")))
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
      int32_t dot = marmot_dot_i8_dotprod_impl(query_codes + start, codes + start, n);
      residual_dot += query_scale * residual_scale * (float)dot;
    }
    float dot = base_dot + residual_dot;
    if (rank_metric == 2) {
      out[row] = 1.0f - dot;
    } else {
      out[row] = query_norm2 + norm2 - 2.0f * dot;
    }
  }
  return 1;
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
