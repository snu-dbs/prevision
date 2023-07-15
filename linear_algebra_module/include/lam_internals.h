#ifndef __LAM_INTERNALS_H__
#define __LAM_INTERNALS_H__

#include <stddef.h>
#include <stdint.h>
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "array_struct.h"

/**********************************************
 *  Internal functions declaration
 *  used for implementation of the interfaces.
 **********************************************/

/***********************************************
 * Internal lam-utility functions
 ***********************************************/
void calculate_chunk_coord(uint32_t dim_len,
                           int chunk_number,
                           uint64_t *array_size,
                           uint64_t *coord);
void calculate_cell_coord(uint32_t dim_len,
                          int cell_number,
                          uint64_t *chunk_size,
                          uint64_t *coord);

bool check_tile_equality_unary(Chunk *A,
                               Chunk *B);
bool check_tile_equality_binary(Chunk *A,
                                Chunk *B,
                                Chunk *C);

/***********************************************
 * Internal functions for elemwise operations
 ***********************************************/

void _lam_dense_elemwise(
        PFpage *lhs, PFpage *rhs, PFpage *result, int lhs_attr_type, int rhs_attr_type, int op_type);
void _lam_sparse_elemwise(
        PFpage *lhs, PFpage *rhs, PFpage *result, int lhs_attr_type, int rhs_attr_type, int op_type);
void _lam_mixed_elemwise(
        PFpage *lhs, PFpage *rhs, PFpage *result, uint64_t *tile_extnets, uint32_t dim_len,
        int lhs_attr_type, int rhs_attr_type, int op_type, bool is_lhs_dense);
void _lam_dense_elemwise_with_constant(
        PFpage *opnd, PFpage *result, int opnd_attr_type, int constant_type, void *constant, bool constant_equation_side, int op_type);
void _lam_sparse_elemwise_with_constant(
        PFpage *opnd, PFpage *result, int opnd_attr_type, int constant_type, void *constant, bool constant_equation_side, int op_type);

void _lam_dense_map(
        PFpage *A, PFpage *B, int opnd_attr_type, Array *output_array);
void _lam_sparse_map(
        PFpage *A, PFpage *B, int opnd_attr_type, Array *output_array);
void _lam_dense_exp(
        PFpage *A, PFpage *B, int opnd_attr_type, Array *output_array);

void elemwise_mixed_add(
        PFpage *lhs,
        PFpage *rhs,
        PFpage *result,
        uint64_t *tile_extnets,
        uint32_t dim_len,
        int lhs_attr_type,
        int rhs_attr_type,
        bool is_lhs_dense);
                         
void elemwise_mixed_sub(
        PFpage *lhs,
        PFpage *rhs,
        PFpage *result,
        uint64_t *tile_extnets,
        uint32_t dim_len,
        int lhs_attr_type,
        int rhs_attr_type,
        bool is_lhs_dense);

/*************************************************
 * Internal functions for matrix multiplication
 *************************************************/
void _lam_dense_matmul(Chunk *lhs,
                  Chunk *rhs,
                  Chunk *result,
                  int lhs_attr_type,
                  int rhs_attr_type,
                  uint64_t iteration,
                  bool lhs_transposed,
                  bool rhs_transposed,
                  bool is_first);

// Both needs to be exposed
void _lam_dense_matmul_impl_mul(Chunk *A, Chunk *B, Chunk *C, int type1, int type2, bool lhs_transposed, bool rhs_transposed);
void _lam_dense_matmul_impl_mulandadd(Chunk *A, Chunk *B, Chunk *C, int type1, int type2, bool lhs_transposed, bool rhs_transposed);

void _lam_sparse_matmul(Chunk *lhs,
                   Chunk *rhs,
                   Chunk *result,
                   int lhs_attr_type,
                   int rhs_attr_type,
                   uint64_t *chunksize,
                   uint64_t iteration,
                   bool lhs_transposed,
                   bool rhs_transposed,
                   bool is_first);

void _lam_sparse_matmul_finalize(Chunk *result,
                            int lhs_attr_type,
                            int rhs_attr_type);


void _lam_mixed_matmul(
        Chunk *lhs,
        Chunk *rhs,
        Chunk *result,
        int lhs_attr_type,
        int rhs_attr_type,
        uint64_t *chunksize,
        uint64_t iteration,
        bool lhs_transposed,
        bool rhs_transposed,
        bool lhs_dense,
        bool is_first);


int ts_type_to_chunk_type(int in);

/**************************************************
 * Internal functions for group by dim aggregation
 **************************************************/
/* Aggregate functions */
void group_by_dim_dense_count_aggregate_opt(Chunk *opnd,
                                            Chunk *result,
                                            void **state,
                                            int opnd_attr_type,
                                            uint8_t *dim_selection_map,
                                            Descriptor array_desc,
                                            uint64_t *chunk_size,
                                            uint64_t iteration);
void group_by_dim_dense_count_aggregate_iter(Chunk *opnd,
                                             Chunk *result,
                                             void **state,
                                             int opnd_attr_type,
                                             uint8_t *dim_selection_map,
                                             Descriptor array_desc,
                                             uint64_t *chunk_size,
                                             uint64_t iteration);

void group_by_dim_dense_sum_aggregate_opt(Chunk *opnd,
                                          Chunk *result,
                                          void **state,
                                          int opnd_attr_type,
                                          uint8_t *dim_selection_map,
                                          Descriptor array_desc,
                                          uint64_t *chunk_size,
                                          uint64_t iteration);
void group_by_dim_dense_sum_aggregate_iter(Chunk *opnd,
                                           Chunk *result,
                                           void **state,
                                           int opnd_attr_type,
                                           uint8_t *dim_selection_map,
                                           Descriptor array_desc,
                                           uint64_t *chunk_size,
                                           uint64_t iteration);

void group_by_dim_dense_avg_aggregate_opt(Chunk *opnd,
                                          Chunk *result,
                                          void **state,
                                          int opnd_attr_type,
                                          uint8_t *dim_selection_map,
                                          Descriptor array_desc,
                                          uint64_t *chunk_size,
                                          uint64_t iteration);
void group_by_dim_dense_avg_aggregate_iter(Chunk *opnd,
                                           Chunk *result,
                                           void **state,
                                           int opnd_attr_type,
                                           uint8_t *dim_selection_map,
                                           Descriptor array_desc,
                                           uint64_t *chunk_size,
                                           uint64_t iteration);

void group_by_dim_dense_max_aggregate_opt(Chunk *opnd,
                                          Chunk *result,
                                          void **state,
                                          int opnd_attr_type,
                                          uint8_t *dim_selection_map,
                                          Descriptor array_desc,
                                          uint64_t *chunk_size,
                                          uint64_t iteration);
void group_by_dim_dense_max_aggregate_iter(Chunk *opnd,
                                           Chunk *result,
                                           void **state,
                                           int opnd_attr_type,
                                           uint8_t *dim_selection_map,
                                           Descriptor array_desc,
                                           uint64_t *chunk_size,
                                           uint64_t iteration);

void group_by_dim_dense_min_aggregate_opt(Chunk *opnd,
                                          Chunk *result,
                                          void **state,
                                          int opnd_attr_type,
                                          uint8_t *dim_selection_map,
                                          Descriptor array_desc,
                                          uint64_t *chunk_size,
                                          uint64_t iteration);
void group_by_dim_dense_min_aggregate_iter(Chunk *opnd,
                                           Chunk *result,
                                           void **state,
                                           int opnd_attr_type,
                                           uint8_t *dim_selection_map,
                                           Descriptor array_desc,
                                           uint64_t *chunk_size,
                                           uint64_t iteration);

void group_by_dim_dense_var_aggregate_opt(Chunk *opnd,
                                          Chunk *result,
                                          void **state,
                                          int opnd_attr_type,
                                          uint8_t *dim_selection_map,
                                          Descriptor array_desc,
                                          uint64_t *chunk_size,
                                          uint64_t iteration);
void group_by_dim_dense_var_aggregate_iter(Chunk *opnd,
                                           Chunk *result,
                                           void **state,
                                           int opnd_attr_type,
                                           uint8_t *dim_selection_map,
                                           Descriptor array_desc,
                                           uint64_t *chunk_size,
                                           uint64_t iteration);

void group_by_dim_dense_stdev_aggregate_opt(Chunk *opnd,
                                            Chunk *result,
                                            void **state,
                                            int opnd_attr_type,
                                            uint8_t *dim_selection_map,
                                            Descriptor array_desc,
                                            uint64_t *chunk_size,
                                            uint64_t iteration);
void group_by_dim_dense_stdev_aggregate_iter(Chunk *opnd,
                                             Chunk *result,
                                             void **state,
                                             int opnd_attr_type,
                                             uint8_t *dim_selection_map,
                                             Descriptor array_desc,
                                             uint64_t *chunk_size,
                                             uint64_t iteration);

/* Lower && Write functions */
void group_by_dim_dense_count_write_opt(Chunk *result,
                                        void **state,
                                        int opnd_attr_type);
void group_by_dim_dense_count_write_iter(Chunk *result,
                                         void **state,
                                         int opnd_attr_type);

void group_by_dim_dense_sum_write_opt(Chunk *result,
                                      void **state,
                                      int opnd_attr_type);
void group_by_dim_dense_sum_write_iter(Chunk *result,
                                       void **state,
                                       int opnd_attr_type);

void group_by_dim_dense_avg_write_opt(Chunk *result,
                                      void **state,
                                      int opnd_attr_type);
void group_by_dim_dense_avg_write_iter(Chunk *result,
                                       void **state,
                                       int opnd_attr_type);

void group_by_dim_dense_max_write_opt(Chunk *result,
                                      void **state,
                                      int opnd_attr_type);
void group_by_dim_dense_max_write_iter(Chunk *result,
                                       void **state,
                                       int opnd_attr_type);

void group_by_dim_dense_min_write_opt(Chunk *result,
                                      void **state,
                                      int opnd_attr_type);
void group_by_dim_dense_min_write_iter(Chunk *result,
                                       void **state,
                                       int opnd_attr_type);

void group_by_dim_dense_var_write_opt(Chunk *result,
                                      void **state,
                                      int opnd_attr_type);
void group_by_dim_dense_var_write_iter(Chunk *result,
                                       void **state,
                                       int opnd_attr_type);

void group_by_dim_dense_stdev_write_opt(Chunk *result,
                                        void **state,
                                        int opnd_attr_type);
void group_by_dim_dense_stdev_write_iter(Chunk *result,
                                         void **state,
                                         int opnd_attr_type);

// Hash function for sparse aggregation
// inline uint64_t _sparse_coord_hash(uint64_t *coord,
//                                    uint64_t *state,
//                                    uint32_t dim_len);

/**************************************************
 * Internal functions for reduce_as_scala aggregation
 **************************************************/
void reduce_as_scala_dense_sum_aggregate_opt(PFpage *opnd,
                                             void **state,
                                             int opnd_attr_type,
                                             uint64_t iteration);

void reduce_as_scala_dense_norm_aggregate_opt(PFpage *opnd,
                                              void **state,
                                              int opnd_attr_type,
                                              uint64_t iteration);

void reduce_as_scala_sparse_sum_aggregate_opt(PFpage *opnd,
                                               void **state,
                                               int opnd_attr_type,
                                               uint64_t iteration);

void reduce_as_scala_sparse_norm_aggregate_opt(PFpage *opnd,
                                               void **state,
                                               int opnd_attr_type,
                                               uint64_t iteration);

double reduce_as_scala_sum_finalize(void **state);
double reduce_as_scala_norm_finalize(void **state);

/***********************************************
 *  Internal functions for window aggregation
 ***********************************************/
void window_dense_count(Chunk *chunk,
                        int64_t *window_size,
                        Chunk *result_chunk);

void window_dense_sum(Chunk *chunk,
                      int64_t *window_size,
                      Chunk *result_chunk,
                      int opnd_attr_type);

void window_dense_avg(Chunk *chunk,
                      int64_t *window_size,
                      Chunk *result_chunk,
                      int opnd_attr_type);

void window_dense_max(Chunk *chunk,
                      int64_t *window_size,
                      Chunk *result_chunk,
                      int opnd_attr_type);

void window_dense_min(Chunk *chunk,
                      int64_t *window_size,
                      Chunk *result_chunk,
                      int opnd_attr_type);

void window_dense_var(Chunk *chunk,
                      int64_t *window_size,
                      Chunk *result_chunk,
                      int opnd_attr_type);

void window_dense_stdev(Chunk *chunk,
                        int64_t *window_size,
                        Chunk *result_chunk,
                        int opnd_attr_type);

#endif