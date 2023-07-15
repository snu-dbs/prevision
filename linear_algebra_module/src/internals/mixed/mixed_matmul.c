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

// #include <math.h>

#define min(a, b) ((a > b) ? b : a)
#define max(a, b) ((a < b) ? b : a)


void _matmul_dense_sparse_to_dense(
        Chunk *A,
        Chunk *B,
        Chunk *C,
        int type1,
        int type2,
        bool lhs_transposed,
        bool rhs_transposed) {
        
    // prepare variables
        
    double *lhs_buf = (double *)bf_util_get_pagebuf(A->curpage);
    double *res_buf = (double *)bf_util_get_pagebuf(C->curpage);

    uint64_t *rhs_coord_sizes = bf_util_pagebuf_get_coords_lens(B->curpage);
    uint64_t *rhs_indptr = bf_util_pagebuf_get_coords(B->curpage, 0);
    uint64_t *rhs_indices = bf_util_pagebuf_get_coords(B->curpage, 1);
    double *rhs_buf = (double *)bf_util_get_pagebuf(B->curpage);

    uint64_t lhs_col_size = A->tile_extents[1];

    // Iterate the lhs tile's rows
    for (uint64_t lhs_row = 0; lhs_row < A->tile_extents[0]; lhs_row++) {
        // Iterate cells of the row
        for (uint64_t lhs_col = 0; lhs_col < A->tile_extents[1]; lhs_col++) {
            // For the row of the rhs that has a row index of lhs_col 
            for (uint64_t rhs_idx = rhs_indptr[lhs_col]; 
                    rhs_idx < rhs_indptr[lhs_col + 1];
                    rhs_idx++) {
                uint64_t rhs_row = lhs_col;
                uint64_t rhs_col = rhs_indices[rhs_idx];
                double rhs_val = rhs_buf[rhs_idx];
                
                res_buf[lhs_row * lhs_col_size + rhs_col] += 
                    lhs_buf[lhs_row * lhs_col_size + lhs_col] * rhs_val;
            } 
        }
    }
}


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
        bool is_first) {
    
    // Currently only support Sparse matrix - vector multiplication
    assert(lhs_dense == false);
    assert(rhs->dim_len == 1 || (rhs->dim_len == 2 && rhs->tile_extents[1] == 1));

    // // type
    // int in_lhs_type = ts_type_to_chunk_type(lhs_attr_type);
    // int in_rhs_type = ts_type_to_chunk_type(rhs_attr_type);

    // prepare
    double *lhs_values = bf_util_get_pagebuf(lhs->curpage);
    uint64_t *lhs_indptr = bf_util_pagebuf_get_coords(lhs->curpage, 0);
    uint64_t *lhs_indices = bf_util_pagebuf_get_coords(lhs->curpage, 1);

    double *rhs_values = bf_util_get_pagebuf(rhs->curpage);
    double *result_values = bf_util_get_pagebuf(result->curpage);

    // if ((result->tile_coords[0] == 0 && result->tile_coords[1] == 0) || (result->tile_coords[0] == 9 && result->tile_coords[1] == 0))
    // {
    //     fprintf(stderr, "{%lu,%lu}\n", result->tile_coords[0], result->tile_coords[1]);
    //     fprintf(stderr, "before:");
    //     for (uint64_t i = 0; i < 3; i++)
    //     {
    //         fprintf(stderr, "%lf,", result_values[i]);
    //     }
    //     fprintf(stderr, "...,");
    //     for (uint64_t i = result->tile_extents[0] - 3; i < result->tile_extents[0]; i++)
    //     {
    //         fprintf(stderr, "%lf,", result_values[i]);
    //     }
    //     fprintf(stderr, "\n");
    // }

    // _print_sparse(lhs->curpage);

    if (is_first) {
        for (uint64_t irow = 0; irow < lhs->tile_extents[0]; irow++) 
            result_values[irow] = 0;
    }

    for (uint64_t irow = 0; irow < lhs->tile_extents[0]; irow++) {
        for (uint64_t i1d = lhs_indptr[irow]; i1d < lhs_indptr[irow + 1]; i1d++) {
            uint64_t icol = lhs_indices[i1d];
            result_values[irow] += lhs_values[i1d] * rhs_values[icol];
        }
    }

    // if ((result->tile_coords[0] == 0 && result->tile_coords[1] == 0) || (result->tile_coords[0] == 9 && result->tile_coords[1] == 0))
    // {
    //     fprintf(stderr, "{%lu,%lu}\n", result->tile_coords[0], result->tile_coords[1]);
    //     fprintf(stderr, "after:");
    //     for (uint64_t i = 0; i < 3; i++)
    //     {
    //         fprintf(stderr, "%lf,", result_values[i]);
    //     }
    //     fprintf(stderr, "...,");
    //     for (uint64_t i = result->tile_extents[0] - 3; i < result->tile_extents[0]; i++)
    //     {
    //         fprintf(stderr, "%lf,", result_values[i]);
    //     }
    //     fprintf(stderr, "\n");
    // }
}
