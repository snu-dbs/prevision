#include <stdio.h>
#include <string.h>
#include <math.h>
#include <assert.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"

#include <sys/time.h>

unsigned long long __lam_matmul_cnt = 0;
unsigned long long __lam_matmul_time = 0;

#define __LAM_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_matmul_time += diff;                                                                         \
    __lam_matmul_cnt++;

bool lam_chunk_matmul(Chunk *lhs,
                      Chunk *rhs,
                      Chunk *result,
                      int lhs_attr_type,
                      int rhs_attr_type,
                      Descriptor array_desc,
                      uint64_t *chunksize,
                      uint64_t iteration,
                      bool lhs_transposed,
                      bool rhs_transposed,
                      bool is_first)
{
    /* Matmul Assertions */
    assert(lhs->dim_len == 2);
    assert(rhs->dim_len == 2);
    assert(array_desc.dim_len == 2);

    if (lhs_transposed && rhs_transposed)
    {
        assert(lhs->chunk_domains[1][1] - lhs->chunk_domains[1][0] + 1 == chunksize[0]);
        assert(rhs->chunk_domains[0][1] - rhs->chunk_domains[0][0] + 1 == chunksize[1]);
        assert(lhs->chunk_domains[0][1] - lhs->chunk_domains[0][0] == rhs->chunk_domains[1][1] - rhs->chunk_domains[1][0]);
    }
    else if (lhs_transposed)
    {
        assert(lhs->chunk_domains[1][1] - lhs->chunk_domains[1][0] + 1 == chunksize[0]);
        assert(rhs->chunk_domains[1][1] - rhs->chunk_domains[1][0] + 1 == chunksize[1]);
        assert(lhs->chunk_domains[0][1] - lhs->chunk_domains[0][0] == rhs->chunk_domains[0][1] - rhs->chunk_domains[0][0]);
    }
    else if (rhs_transposed)
    {
        assert(lhs->chunk_domains[0][1] - lhs->chunk_domains[0][0] + 1 == chunksize[0]);
        assert(rhs->chunk_domains[0][1] - rhs->chunk_domains[0][0] + 1 == chunksize[1]);
        assert(lhs->chunk_domains[1][1] - lhs->chunk_domains[1][0] == rhs->chunk_domains[1][1] - rhs->chunk_domains[1][0]);
    }
    else
    {
        assert(lhs->chunk_domains[0][1] - lhs->chunk_domains[0][0] + 1 == chunksize[0]);
        assert(rhs->chunk_domains[1][1] - rhs->chunk_domains[1][0] + 1 == chunksize[1]);
        assert(lhs->chunk_domains[1][1] - lhs->chunk_domains[1][0] == rhs->chunk_domains[0][1] - rhs->chunk_domains[0][0]);
    }

    bool is_skipped = false;

    // Read tiles. (Empty tile for the result chunk)
    PFpage *lhs_original = NULL, *rhs_original = NULL;

    chunk_getbuf(lhs, BF_EMPTYTILE_NONE);
    chunk_getbuf(rhs, BF_EMPTYTILE_NONE);

    /********************************
     *      Do the Matmul
     *******************************/
    /* Branch according to the array type */

    if (lhs->curpage == NULL || rhs->curpage == NULL) {
        // LHS or RHS are empty tile, then we can skip calculation.

        // Nevertheless, we need to getbuf due to future log.
        chunk_getbuf(result, BF_EMPTYTILE_NONE);
        fprintf(stderr, "[LAM] matmul - empty matmul\n");

        is_skipped = true;

    } else {
        if (lhs->curpage->type == DENSE_FIXED &&
            rhs->curpage->type == DENSE_FIXED)
        {
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);

            __LAM_STAT_START();

            _lam_dense_matmul(lhs, rhs, result,
                        lhs_attr_type, rhs_attr_type,
                        iteration,
                        lhs_transposed, rhs_transposed,
                        is_first);

            fprintf(stderr, "[LAM] matmul - dense-dense matmul\n");

            __LAM_STAT_END();
        }
        else if (lhs->curpage->type == SPARSE_FIXED &&
                rhs->curpage->type == SPARSE_FIXED)
        {
            chunk_getbuf(result, BF_EMPTYTILE_SPARSE_CSR);

            __LAM_STAT_START();

            _lam_sparse_matmul(lhs, rhs, result,
                        lhs_attr_type, rhs_attr_type, chunksize,
                        iteration,
                        lhs_transposed, rhs_transposed,
                        is_first);

            fprintf(stderr, "[LAM] matmul - sparse-sparse matmul\n");

            __LAM_STAT_END();
        } else {
            // DxS should return a dense tile anyway, so the second argument is false
            // We will convert the result to CSR if is_csr is true.
            chunk_getbuf(result, BF_EMPTYTILE_DENSE);

            __LAM_STAT_START();

            bool_t lhs_dense = lhs->curpage->type == DENSE_FIXED;
            _lam_mixed_matmul(
                lhs, rhs, result,
                lhs_attr_type, rhs_attr_type, chunksize,
                iteration,
                lhs_transposed, rhs_transposed, lhs_dense,
                is_first);
            
            fprintf(stderr, "[LAM] matmul - dense-sparse matmul\n");

            __LAM_STAT_END();
        }
    }

    return is_skipped;
}

Chunk *lam_chunk_matmul_finalize(
        Chunk *result,
        int lhs_attr_type,
        int rhs_attr_type,
        bool is_result_csr) {
            
    array_key result_key;
    result_key.arrayname = result->array_name;
    result_key.attrname = result->attr_name;
    result_key.dcoords = result->chunk_coords;
    result_key.dim_len = result->dim_len;

    if (result->curpage->type == SPARSE_FIXED) {
        /* For CSR x CSR operation,
            * convert the internal csr object into csr tile */
        _lam_sparse_matmul_finalize(result, lhs_attr_type, rhs_attr_type);
    }

    BF_TouchBuf(result_key);

    return result;
}
