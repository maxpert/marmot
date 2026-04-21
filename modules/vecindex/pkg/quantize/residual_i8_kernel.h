#include <stdint.h>

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
    float* out);

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
    float* out);

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
    float* out);
