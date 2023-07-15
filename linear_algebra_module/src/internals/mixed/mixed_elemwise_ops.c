#include <stdio.h>
#include <string.h>
#include <math.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "chunk_iter.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"


void _lam_mixed_add(
        void *dense_pagebuf,
        void *sparse_pagebuf,
        uint64_t *sparse_indices,
        uint64_t *sparse_indptr,
        uint64_t *tile_extents,
        uint32_t dim_len,
        void *result_pagebuf,       // in-place update if NULL is provided
        int dense_attr_type, 
        int sparse_attr_type) {

    // FIXME: Only double type is supported
    assert(dense_attr_type == TILESTORE_FLOAT64 && sparse_attr_type == TILESTORE_FLOAT64);

    double *d_buf = (double *) dense_pagebuf;
    double *r_buf = (double *) result_pagebuf;
    double *s_buf = (double *) sparse_pagebuf;

    // 1. Make a dense result tile.
    if (r_buf == NULL) {
        // in-place 
        r_buf = d_buf;
    } else {
        // Memcpy the LHS dense tile to the result
        memcpy(r_buf, d_buf, tile_extents[0] * tile_extents[1] * sizeof(double));
    }

    // 2. Add the RHS values by iterating the sparse tile
    for (uint64_t i = 0; i < tile_extents[0]; i++) {
        uint64_t rhs_pos = sparse_indptr[i];
        uint64_t rhs_end = sparse_indptr[i + 1];

        for (uint64_t j = rhs_pos; j < rhs_end; j++) {
            uint64_t row_idx = i;
            uint64_t col_idx = sparse_indices[j];
            double value = s_buf[j];

            r_buf[row_idx * tile_extents[1] + col_idx] += value;
        }
    }

    // super easy
}


void _lam_mixed_sub(
        void *dense_pagebuf,
        void *sparse_pagebuf,
        uint64_t *sparse_indices,
        uint64_t *sparse_indptr,
        uint64_t *tile_extents,
        uint32_t dim_len,
        void *result_pagebuf,
        int dense_attr_type, 
        int sparse_attr_type,
        bool is_dense_lhs) {
    
    // FIXME: Only double type is supported
    assert(dense_attr_type == TILESTORE_FLOAT64 && sparse_attr_type == TILESTORE_FLOAT64);

    if (is_dense_lhs) {
        /* Dense - Sparse */

        double *d_buf = (double *) dense_pagebuf;
        double *r_buf = (double *) result_pagebuf;
        double *s_buf = (double *) sparse_pagebuf;

        // 1. Make a dense result tile.
        // Memcpy the LHS dense tile to the result
        memcpy(r_buf, d_buf, tile_extents[0] * tile_extents[1] * sizeof(double));

        // 2. Sub the RHS values by iterating the sparse tile
        for (uint64_t i = 0; i < tile_extents[0]; i++) {
            uint64_t pos = sparse_indptr[i];
            uint64_t end = sparse_indptr[i + 1];

            for (uint64_t j = pos; j < end; j++) {
                uint64_t row_idx = i;
                uint64_t col_idx = sparse_indices[j];
                double value = s_buf[j];

                r_buf[row_idx * tile_extents[1] + col_idx] -= value;
            }
        }

    } else {
        /* Sparse - Dense */

        double *d_buf = (double *) dense_pagebuf;
        double *r_buf = (double *) result_pagebuf;
        double *s_buf = (double *) sparse_pagebuf;

        // 1. Make a dense result tile.
        // Memcpy the RHS dense tile to the result
        memcpy(r_buf, d_buf, tile_extents[0] * tile_extents[1] * sizeof(double));

        // flip sign bits of the result tile
        for (uint64_t i = 0; i < tile_extents[0] * tile_extents[1]; i++) {
            r_buf[i] = -r_buf[i];
        }

        // 2. Add the LHS values by iterating the sparse tile
        for (uint64_t i = 0; i < tile_extents[0]; i++) {
            uint64_t pos = sparse_indptr[i];
            uint64_t end = sparse_indptr[i + 1];

            for (uint64_t j = pos; j < end; j++) {
                uint64_t row_idx = i;
                uint64_t col_idx = sparse_indices[j];
                double value = s_buf[j];

                r_buf[row_idx * tile_extents[1] + col_idx] += value;
            }
        }
    }
}

void _lam_mixed_elemwise(
        PFpage *lhs,
        PFpage *rhs,
        PFpage *result,
        uint64_t *tile_extnets,
        uint32_t dim_len,
        int lhs_attr_type,
        int rhs_attr_type,
        int op_type,
        bool is_lhs_dense) {

    PFpage *dense_tile, *sparse_tile;
    int dense_attr_type, sparse_attr_type;
    if (is_lhs_dense) {
        dense_tile = lhs;
        sparse_tile = rhs;
        dense_attr_type = lhs_attr_type;
        sparse_attr_type = rhs_attr_type;
    } else {
        dense_tile = rhs;
        sparse_tile = lhs;
        dense_attr_type = rhs_attr_type;
        sparse_attr_type = lhs_attr_type;
    }

    void *d_buf = bf_util_get_pagebuf(dense_tile);
    void *r_buf = bf_util_get_pagebuf(result);
    void *s_buf = bf_util_get_pagebuf(sparse_tile);

    uint64_t *s_coord_sizes = bf_util_pagebuf_get_coords_lens(sparse_tile);
    uint64_t *s_indptr = bf_util_pagebuf_get_coords(sparse_tile, 0);
    uint64_t *s_indices = bf_util_pagebuf_get_coords(sparse_tile, 1);

    // Do the operation.
    if (op_type == CHUNK_OP_ADD) {
        _lam_mixed_add(
            d_buf, s_buf, s_indices, s_indptr, tile_extnets, dim_len, 
            r_buf, dense_attr_type, sparse_attr_type);

    } else if (op_type == CHUNK_OP_SUB) {
        _lam_mixed_sub(
            d_buf, s_buf, s_indices, s_indptr, tile_extnets, dim_len, 
            r_buf, dense_attr_type, sparse_attr_type, is_lhs_dense);

    } else {
        assert(false);
    }
}