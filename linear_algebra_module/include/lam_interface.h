#ifndef __LAM_INTERFACE_H__
#define __LAM_INTERFACE_H__

#include <stddef.h>
#include <stdint.h>
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "array_struct.h"

#define lam_dim_size 10
#define lam_extent_size 5

/************************************************
 *     Type definitions for optimizability      *
 ************************************************/
typedef enum _GROUP_BY_OPTIMIZABILITY
{
    GROUP_BY_NOT_OPTIMIZABLE = 0,
    GROUP_BY_OPTIMIZABLE = 1
} GROUP_BY_OPTIMIZABILITY;

typedef enum _MATMUL_OPTIMIZABILITY
{
    MATMUL_NOT_OPTIMIZABLE = 0,
    MATMUL_OPTIMIZABLE = 1
} MATMUL_OPTIMIZABILITY;

/************************************************
 *       Type definitions for aggregator        *
 ************************************************/
typedef enum _AGGREGATE_FUNC_TYPE
{
    AGGREGATE_COUNT = 0,
    AGGREGATE_SUM = 1,
    AGGREGATE_AVG = 2,
    AGGREGATE_MAX = 3,
    AGGREGATE_MIN = 4,
    AGGREGATE_VAR = 5,
    AGGREGATE_STDEV = 6,
    AGGREGATE_NORM = 7
} AGGREGATE_FUNC_TYPE;

/************************************************
 *          Elemwise Arithmetic Ops             *
 ************************************************/
Chunk *lam_chunk_add(Chunk *lhs, Chunk *rhs, int lhs_attrtype, int rhs_attrtype, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_sub(Chunk *lhs, Chunk *rhs, int lhs_attrtype, int rhs_attrtype, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_product(Chunk *lhs, Chunk *rhs, int lhs_attrtype, int rhs_attrtype, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_div(Chunk *lhs, Chunk *rhs, int lhs_attrtype, int rhs_attrtype, Array *output_array, uint64_t pos, uint64_t *chunksize);

Chunk *lam_chunk_add_constant(Chunk *opnd, int opnd_attr_type, int constant_type, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_sub_constant(Chunk *opnd, int opnd_attr_type, int constant_type, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_product_constant(Chunk *opnd, int opnd_attr_type, int constant_type, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_div_constant(Chunk *opnd, int opnd_attr_type, int constant_type, Array *output_array, uint64_t pos, uint64_t *chunksize);

Chunk *lam_chunk_map(Chunk *opnd, int opnd_attr_type, Array *output_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_chunk_exp(Chunk *opnd, int opnd_attr_type, Array *output_array, uint64_t pos, uint64_t *chunksize);

/* util functions */
void calculate_chunk_coord(uint32_t dim_len, int chunk_number, uint64_t *array_size, uint64_t *coord);
void calculate_cell_coord(uint32_t dim_len, int cell_number, uint64_t *chunk_size, uint64_t *coord);

/************************************************
 *           Matrix Multiplication              *
 ************************************************/
bool lam_chunk_matmul(
    Chunk *lhs, Chunk *rhs, Chunk *result, int lhs_attr_type, int rhs_attr_type,
    Descriptor array_desc, uint64_t *chunksize, uint64_t iteration,
    bool lhs_transposed, bool rhs_transposed, bool is_first);

Chunk *lam_chunk_matmul_finalize(Chunk *result,
                                 int lhs_attr_type,
                                 int rhs_attr_type,
                                 bool is_result_csr);

/*************************************
 *   Array transposition operation   *
 *************************************/
Chunk *lam_chunk_transpose(Chunk *input_chunk,
                           tilestore_datatype_t attrtype,
                           Array *output_array,
                           uint64_t output_pos,
                           uint64_t *output_chunksize);

/************************************************
 *         Group By Dim Aggregation             *
 ************************************************/
void lam_chunk_group_by_dim_aggregate(Chunk *opnd,
                                      Chunk *result,
                                      int opnd_attr_type,
                                      void **state,
                                      AGGREGATE_FUNC_TYPE func,
                                      uint8_t *dim_selection_map,
                                      Descriptor array_desc,
                                      uint64_t *chunk_size,
                                      uint64_t iteration);

/************************************************
 *             Group By Dim Write               *
 ************************************************/
Chunk *lam_chunk_group_by_dim_write(Chunk *result,
                                    void **state,
                                    int opnd_attr_type,
                                    AGGREGATE_FUNC_TYPE func);

/************************************************
 *         Scala Reduce Aggregation             *
 ************************************************/
void lam_reduce_as_scala_aggregate(Chunk *opnd,
                                   int opnd_attr_type,
                                   void **state,
                                   AGGREGATE_FUNC_TYPE func,
                                   uint64_t iteration);

/************************************************
 *           Scala Reduce Finalize              *
 ************************************************/
double lam_reduce_as_scala_finalize(void **state,
                                    AGGREGATE_FUNC_TYPE func);

/************************************************
 *             Window Aggregations              *
 ************************************************/
Chunk *lam_chunk_window(Chunk *opnd,
                        int opnd_attr_type,
                        int64_t *window_size,
                        AGGREGATE_FUNC_TYPE func,
                        Array *output_array,
                        uint64_t pos,
                        uint64_t *chunk_size);

/************************************************
 *                     SVD                      *
 ************************************************/




/************************************************
 *                     Utils                    *
 ************************************************/

Chunk *lam_chunk_retile(
        Chunk *input_chunk, tilestore_datatype_t attrtype, Array *output_array, uint64_t output_pos, uint64_t *output_chunksize);

Chunk* lam_newarray(Array *out, uint64_t pos);

/**************************************************
 *       LAM Error codes definitions              *
 **************************************************/
#define LAM_OK 0
#define LAM_ARRAY_SIZE_NOT_MATCH (-1)
#define LAM_DOMAIN_NOT_MATCH (-2)
#define LAM_ATTR_TYPE_NOT_MATCH (-3)
#define LAM_UNSUPPORTED_ARRAY_TYPE (-4)
#define LAM_UNSUPPORTED_ATTR_TYPE (-5)
#define LAM_DIVISION_BY_ZERO_ERR (-6)
#define LAM_INVALID_DIM_LEN (-7)
#define LAM_INVALID_DIM_ORDER (-8)

#endif
