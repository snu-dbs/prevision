#include <stdio.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"

#include "tilestore.h"

#include <sys/time.h>

// array-array opeartion
unsigned long long __lam_elemwise_cnt = 0;
unsigned long long __lam_elemwise_time = 0;

#define __LAM_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_elemwise_time += diff;                                                                       \
    __lam_elemwise_cnt++;

// constant-array operation
unsigned long long __lam_elemwise_c_cnt = 0;
unsigned long long __lam_elemwise_c_time = 0;

#define __LAM_STAT_C_START() \
    struct timeval start;    \
    gettimeofday(&start, NULL);

#define __LAM_STAT_C_END()                                                                             \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_elemwise_c_time += diff;                                                                     \
    __lam_elemwise_c_cnt++;


// map operation
unsigned long long __lam_elemwise_m_cnt = 0;
unsigned long long __lam_elemwise_m_time = 0;

#define __LAM_STAT_M_START() \
    struct timeval start;    \
    gettimeofday(&start, NULL);

#define __LAM_STAT_M_END()                                                                             \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_elemwise_m_time += diff;                                                                     \
    __lam_elemwise_m_cnt++;


Chunk* _lam_chunk_elemwise(
    Chunk *lhs, Chunk *rhs,
    int lhs_attr_type, int rhs_attr_type, int op_type,
    Array *output_array,
    uint64_t pos,
    uint64_t *chunksize);

Chunk *_lam_chunk_elemwise_with_constant(
    Chunk *opnd,
    int opnd_attr_type, int constant_type, int op_type,
    Array *output_array,
    uint64_t pos,
    uint64_t *chunksize);


/*
 * Basic cell by cell arithmetic operations
 *
 * 	Currently assumes the followings.
 *
 * 	1. All array has only one attribute.
 *  2. Arithmetic operations take place between arrays of same size, same domain, same order.
 *    -> Throws error if this condition is broken.
 *  3. Does not support string arithmetic.
 *    -> Undefined behavior when arrays with string attribute are passed.
 *  4. Only support dense array. (for now)
 */

Chunk *lam_chunk_add(
        Chunk *lhs, Chunk *rhs,
        int lhs_attr_type, int rhs_attr_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise(
        lhs, rhs, lhs_attr_type, rhs_attr_type, CHUNK_OP_ADD, output_array, pos, chunksize);
}

Chunk *lam_chunk_sub(
        Chunk *lhs, Chunk *rhs,
        int lhs_attr_type, int rhs_attr_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise(
        lhs, rhs, lhs_attr_type, rhs_attr_type, CHUNK_OP_SUB, output_array, pos, chunksize);
}

Chunk *lam_chunk_product(
        Chunk *lhs, Chunk *rhs,
        int lhs_attr_type, int rhs_attr_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise(
        lhs, rhs, lhs_attr_type, rhs_attr_type, CHUNK_OP_MUL, output_array, pos, chunksize);
}


Chunk *lam_chunk_div(
        Chunk *lhs, Chunk *rhs,
        int lhs_attr_type, int rhs_attr_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise(
        lhs, rhs, lhs_attr_type, rhs_attr_type, CHUNK_OP_DIV, output_array, pos, chunksize);
}


Chunk *lam_chunk_add_constant(
        Chunk *opnd,
        int opnd_attr_type, int constant_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {

    return _lam_chunk_elemwise_with_constant(
        opnd, opnd_attr_type, constant_type, CHUNK_OP_ADD, output_array, pos, chunksize);
}


Chunk *lam_chunk_sub_constant(
        Chunk *opnd,
        int opnd_attr_type, int constant_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise_with_constant(
        opnd, opnd_attr_type, constant_type, CHUNK_OP_SUB, output_array, pos, chunksize);
}

Chunk *lam_chunk_product_constant(
        Chunk *opnd,
        int opnd_attr_type, int constant_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise_with_constant(
        opnd, opnd_attr_type, constant_type, CHUNK_OP_MUL, output_array, pos, chunksize);
}

Chunk *lam_chunk_div_constant(
        Chunk *opnd,
        int opnd_attr_type, int constant_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {
            
    return _lam_chunk_elemwise_with_constant(
        opnd, opnd_attr_type, constant_type, CHUNK_OP_DIV, output_array, pos, chunksize);
}


Chunk* _lam_chunk_elemwise(
        Chunk *lhs, Chunk *rhs,
        int lhs_attr_type, int rhs_attr_type, int op_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize) {

    uint32_t dim_len = output_array->desc.dim_len;

    /* Compute number of chunks per each dimension. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int i = 0; i < dim_len; i++)
    {
        num_chunk_per_dim[i] = ((output_array->desc.dim_domains[i][1] - output_array->desc.dim_domains[i][0] + chunksize[i]) / chunksize[i]);
    }

    /* Compute result chunk coordinate. */
    uint64_t *result_chunk_coords;
    result_chunk_coords = malloc(sizeof(uint64_t) * dim_len);

    Chunk *result;
    calculate_chunk_coord(dim_len, pos, num_chunk_per_dim, result_chunk_coords);
    chunk_get_chunk(output_array->desc.object_name, chunksize, result_chunk_coords, &result);

    // Read tiles. (Empty tile for the result chunk)
    chunk_getbuf(lhs, BF_EMPTYTILE_NONE);
    chunk_getbuf(rhs, BF_EMPTYTILE_NONE);

    /* Branch according to the array type */
    if (lhs->curpage == NULL || rhs->curpage == NULL) {
        assert(false);      // TODO: Not implemented yet
    } else {
        if (lhs->curpage->type == DENSE_FIXED &&
            rhs->curpage->type == DENSE_FIXED)
        {
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);

            __LAM_STAT_START();

            _lam_dense_elemwise(
                lhs->curpage, rhs->curpage, result->curpage, lhs_attr_type, rhs_attr_type, op_type);

            __LAM_STAT_END();
        }
        else if (lhs->curpage->type == SPARSE_FIXED &&
                rhs->curpage->type == SPARSE_FIXED)
        {
            chunk_getbuf(result, BF_EMPTYTILE_SPARSE_CSR);

            __LAM_STAT_START();

            _lam_sparse_elemwise(
                lhs->curpage, rhs->curpage, result->curpage, lhs_attr_type, rhs_attr_type, op_type);

            __LAM_STAT_END();
        } else if (lhs->curpage->type != rhs->curpage->type) {
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);

            __LAM_STAT_START();

            _lam_mixed_elemwise(
                lhs->curpage, rhs->curpage, result->curpage, chunksize, dim_len,
                lhs_attr_type, rhs_attr_type, op_type, lhs->curpage->type == DENSE_FIXED);

            __LAM_STAT_END();
        } 
    }

    free(result_chunk_coords);
    free(num_chunk_per_dim);

    result->dirty = 1;

    return result;
}

Chunk *_lam_chunk_elemwise_with_constant(
        Chunk *opnd,
        int opnd_attr_type, int constant_type, int op_type,
        Array *output_array,
        uint64_t pos,
        uint64_t *chunksize
) {
        uint32_t dim_len = output_array->desc.dim_len;

    /* Compute number of chunks per each dimension. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int i = 0; i < dim_len; i++)
    {
        num_chunk_per_dim[i] = ((output_array->desc.dim_domains[i][1] - output_array->desc.dim_domains[i][0] + chunksize[i]) / chunksize[i]);
    }

    /* Compute result chunk coordinate. */
    uint64_t *result_chunk_coords;
    result_chunk_coords = malloc(sizeof(uint64_t) * dim_len);

    Chunk *result;
    calculate_chunk_coord(dim_len, pos, num_chunk_per_dim, result_chunk_coords);
    chunk_get_chunk(output_array->desc.object_name, chunksize, result_chunk_coords, &result);

    // Read tiles. (Empty tile for the result chunk)
    chunk_getbuf(opnd, BF_EMPTYTILE_NONE);

    void *constant = output_array->op_param.elemConst.constant;
    bool constant_equation_side = output_array->op_param.elemConst.equation_side;

    if (opnd->curpage == NULL) {
        assert(false);      // TODO: Not implemented yet
    } else {
        if (opnd->curpage->type == DENSE_FIXED) {
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);

            __LAM_STAT_C_START();

            _lam_dense_elemwise_with_constant(
                opnd->curpage, result->curpage, opnd_attr_type, constant_type, constant, constant_equation_side, op_type);

            __LAM_STAT_C_END();
        }
        else if (opnd->curpage->type == SPARSE_FIXED) {
            chunk_getbuf(result, BF_EMPTYTILE_SPARSE_CSR);

            __LAM_STAT_C_START();

            _lam_sparse_elemwise_with_constant(
                opnd->curpage, result->curpage, opnd_attr_type, constant_type, constant, constant_equation_side, op_type);

            __LAM_STAT_C_END();
        }
    }

    free(result_chunk_coords);
    free(num_chunk_per_dim);

    result->dirty = 1;
    return result;
}

Chunk *lam_chunk_map(Chunk *opnd,
                     int opnd_attr_type,
                     Array *output_array,
                     uint64_t pos,
                     uint64_t *chunksize) {


    uint32_t dim_len = output_array->desc.dim_len;

    /* Compute number of chunks per each dimension. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int i = 0; i < dim_len; i++)
    {
        num_chunk_per_dim[i] = ((output_array->desc.dim_domains[i][1] - output_array->desc.dim_domains[i][0] + chunksize[i]) / chunksize[i]);
    }

    /* Compute result chunk coordinate. */
    uint64_t *result_chunk_coords;
    result_chunk_coords = malloc(sizeof(uint64_t) * dim_len);

    Chunk *result;
    calculate_chunk_coord(dim_len, pos, num_chunk_per_dim, result_chunk_coords);
    chunk_get_chunk(output_array->desc.object_name, chunksize, result_chunk_coords, &result);

    PFpage *opnd_original = NULL;
    chunk_getbuf(opnd, BF_EMPTYTILE_NONE);

    if (opnd->curpage == NULL) {
        assert(false);      // TODO: Not implemented yet
    } else {
        if (opnd->curpage->type == DENSE_FIXED) {
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);
            
            __LAM_STAT_M_START();

            _lam_dense_map(opnd->curpage, result->curpage, opnd_attr_type, output_array);
            
            __LAM_STAT_M_END();

        } else if (opnd->curpage->type == SPARSE_FIXED) {
            // chunk_getbuf(result, BF_EMPTYTILE_SPARSE_CSR);
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);

            __LAM_STAT_M_START();

            _lam_sparse_map(opnd->curpage, result->curpage, opnd_attr_type, output_array);

            __LAM_STAT_M_END();
        }
    }

    free(result_chunk_coords);
    free(num_chunk_per_dim);

    result->dirty = 1;

    return result;
}

Chunk *lam_chunk_exp(Chunk *opnd,
                     int opnd_attr_type,
                     Array *output_array,
                     uint64_t pos,
                     uint64_t *chunksize)
{
    uint32_t dim_len = output_array->desc.dim_len;

    /* Compute number of chunks per each dimension. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int i = 0; i < dim_len; i++)
    {
        num_chunk_per_dim[i] = ((output_array->desc.dim_domains[i][1] - output_array->desc.dim_domains[i][0] + chunksize[i]) / chunksize[i]);
    }

    /* Compute result chunk coordinate. */
    uint64_t *result_chunk_coords;
    result_chunk_coords = malloc(sizeof(uint64_t) * dim_len);

    Chunk *result;
    calculate_chunk_coord(dim_len, pos, num_chunk_per_dim, result_chunk_coords);
    chunk_get_chunk(output_array->desc.object_name, chunksize, result_chunk_coords, &result);

    // Read tiles. (Empty tile for the result chunk)
    chunk_getbuf(opnd, BF_EMPTYTILE_NONE);

    if (opnd->curpage == NULL) {
        assert(false);      // TODO: Not implemented yet
    } else {
        if (opnd->curpage->type == DENSE_FIXED) {
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);
            
            __LAM_STAT_M_START();

            _lam_dense_exp(opnd->curpage, result->curpage, opnd_attr_type, output_array);

            __LAM_STAT_M_END();

        } else if (opnd->curpage->type == SPARSE_FIXED) {
            chunk_getbuf(result, BF_EMPTYTILE_SPARSE_CSR);
            
            assert(false);          // TODO: Not implemented yet
        }
    }
    
    free(result_chunk_coords);
    free(num_chunk_per_dim);

    result->dirty = 1;

    return result;
}
