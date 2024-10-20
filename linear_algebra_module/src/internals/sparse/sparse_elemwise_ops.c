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

void elemwise_sparse_add(PFpage *lhs,
                         PFpage *rhs,
                         PFpage *result,
                         int lhs_attr_type,
                         int rhs_attr_type)
{
    uint64_t *lhs_coord_sizes = bf_util_pagebuf_get_coords_lens(lhs);
    uint64_t *lhs_indptr = bf_util_pagebuf_get_coords(lhs, 0);
    uint64_t *lhs_indices = bf_util_pagebuf_get_coords(lhs, 1);

    uint64_t *rhs_coord_sizes = bf_util_pagebuf_get_coords_lens(rhs);
    uint64_t *rhs_indptr = bf_util_pagebuf_get_coords(rhs, 0);
    uint64_t *rhs_indices = bf_util_pagebuf_get_coords(rhs, 1);

    uint64_t nnz = 0;
    uint64_t expected_nnz = (lhs_coord_sizes[1] + rhs_coord_sizes[1]) / sizeof(uint64_t);

    BF_ResizeBuf(result, expected_nnz);
    uint64_t *res_indptr = bf_util_pagebuf_get_coords(result, 0);
    uint64_t *res_indices = bf_util_pagebuf_get_coords(result, 1);
    res_indptr[0] = 0;

    // Do the operation.
    if (lhs_attr_type == TILESTORE_INT32)
    {
        int *lhs_buf = (int *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            int *res_buf = (int *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(int32_t));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT32)
    {
        float *lhs_buf = (float *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            float *res_buf = (float *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(float));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT64)
    {
        double *lhs_buf = (double *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];

                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
}

void elemwise_sparse_sub(PFpage *lhs,
                         PFpage *rhs,
                         PFpage *result,
                         int lhs_attr_type,
                         int rhs_attr_type)
{
    uint64_t *lhs_coord_sizes = bf_util_pagebuf_get_coords_lens(lhs);
    uint64_t *lhs_indptr = bf_util_pagebuf_get_coords(lhs, 0);
    uint64_t *lhs_indices = bf_util_pagebuf_get_coords(lhs, 1);

    uint64_t *rhs_coord_sizes = bf_util_pagebuf_get_coords_lens(rhs);
    uint64_t *rhs_indptr = bf_util_pagebuf_get_coords(rhs, 0);
    uint64_t *rhs_indices = bf_util_pagebuf_get_coords(rhs, 1);

    uint64_t nnz = 0;
    uint64_t expected_nnz = (lhs_coord_sizes[1] + rhs_coord_sizes[1]) / sizeof(uint64_t);

    BF_ResizeBuf(result, expected_nnz);
    uint64_t *res_indptr = bf_util_pagebuf_get_coords(result, 0);
    uint64_t *res_indices = bf_util_pagebuf_get_coords(result, 1);
    res_indptr[0] = 0;

    // Do the operation.
    if (lhs_attr_type == TILESTORE_INT32)
    {
        int *lhs_buf = (int *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            int *res_buf = (int *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] - rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(int32_t));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = (double)lhs_buf[lhs_pos] - (double)rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = (double)lhs_buf[lhs_pos] - rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT32)
    {
        float *lhs_buf = (float *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] - rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            float *res_buf = (float *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        float res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(float));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT64)
    {
        double *lhs_buf = (double *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] - (double)rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] - (double)rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] - rhs_buf[rhs_pos];

                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = -rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = -rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
}

void elemwise_sparse_product(PFpage *lhs,
                             PFpage *rhs,
                             PFpage *result,
                             int lhs_attr_type,
                             int rhs_attr_type)
{
    uint64_t *lhs_coord_sizes = bf_util_pagebuf_get_coords_lens(lhs);
    uint64_t *lhs_indptr = bf_util_pagebuf_get_coords(lhs, 0);
    uint64_t *lhs_indices = bf_util_pagebuf_get_coords(lhs, 1);

    uint64_t *rhs_coord_sizes = bf_util_pagebuf_get_coords_lens(rhs);
    uint64_t *rhs_indptr = bf_util_pagebuf_get_coords(rhs, 0);
    uint64_t *rhs_indices = bf_util_pagebuf_get_coords(rhs, 1);

    uint64_t nnz = 0;
    uint64_t expected_nnz = (lhs_coord_sizes[1] + rhs_coord_sizes[1]) / sizeof(uint64_t);

    BF_ResizeBuf(result, expected_nnz);
    uint64_t *res_indptr = bf_util_pagebuf_get_coords(result, 0);
    uint64_t *res_indices = bf_util_pagebuf_get_coords(result, 1);
    res_indptr[0] = 0;

    // Do the operation.
    if (lhs_attr_type == TILESTORE_INT32)
    {
        int *lhs_buf = (int *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            int *res_buf = (int *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(int32_t));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT32)
    {
        float *lhs_buf = (float *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            float *res_buf = (float *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(float));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT64)
    {
        double *lhs_buf = (double *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];

                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        // res_indices[nnz] = lhs_col;
                        // res_buf[nnz] = lhs_buf[lhs_pos];
                        // res_buf[nnz] = (double) 0.f;
                        // nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        // res_indices[nnz] = rhs_col;
                        // res_buf[nnz] = rhs_buf[rhs_pos];
                        // res_buf[nnz] = (double) 0.f;
                        // nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    // res_indices[nnz] = lhs_indices[lhs_pos];
                    // res_buf[nnz] = lhs_buf[lhs_pos];
                    // res_buf[nnz] = (double) 0.f;
                    // nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    // res_indices[nnz] = rhs_indices[rhs_pos];
                    // res_buf[nnz] = rhs_buf[rhs_pos];
                    // res_buf[nnz] = (double) 0.f;
                    // nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
}

void elemwise_sparse_div(PFpage *lhs,
                         PFpage *rhs,
                         PFpage *result,
                         int lhs_attr_type,
                         int rhs_attr_type)
{
    uint64_t *lhs_coord_sizes = bf_util_pagebuf_get_coords_lens(lhs);
    uint64_t *lhs_indptr = bf_util_pagebuf_get_coords(lhs, 0);
    uint64_t *lhs_indices = bf_util_pagebuf_get_coords(lhs, 1);

    uint64_t *rhs_coord_sizes = bf_util_pagebuf_get_coords_lens(rhs);
    uint64_t *rhs_indptr = bf_util_pagebuf_get_coords(rhs, 0);
    uint64_t *rhs_indices = bf_util_pagebuf_get_coords(rhs, 1);

    uint64_t nnz = 0;
    uint64_t expected_nnz = (lhs_coord_sizes[1] + rhs_coord_sizes[1]) / sizeof(uint64_t);

    BF_ResizeBuf(result, expected_nnz);
    uint64_t *res_indptr = bf_util_pagebuf_get_coords(result, 0);
    uint64_t *res_indices = bf_util_pagebuf_get_coords(result, 1);
    res_indptr[0] = 0;

    // Do the operation.
    if (lhs_attr_type == TILESTORE_INT32)
    {
        int *lhs_buf = (int *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            int *res_buf = (int *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] / rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(int32_t));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT32)
    {
        float *lhs_buf = (float *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            float *res_buf = (float *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(float));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
    else if (lhs_attr_type == TILESTORE_FLOAT64)
    {
        double *lhs_buf = (double *)bf_util_get_pagebuf(lhs);

        if (rhs_attr_type == TILESTORE_INT32)
        {
            int *rhs_buf = (int *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] * rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT32)
        {
            float *rhs_buf = (float *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        int res_value = lhs_buf[lhs_pos] + rhs_buf[rhs_pos];
                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        res_buf[nnz] = lhs_buf[lhs_pos];
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        res_indices[nnz] = rhs_col;
                        res_buf[nnz] = rhs_buf[rhs_pos];
                        nnz++;

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    res_buf[nnz] = lhs_buf[lhs_pos];
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    res_indices[nnz] = rhs_indices[rhs_pos];
                    res_buf[nnz] = rhs_buf[rhs_pos];
                    nnz++;

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
        else if (rhs_attr_type == TILESTORE_FLOAT64)
        {
            double *rhs_buf = (double *)bf_util_get_pagebuf(rhs);
            double *res_buf = (double *)bf_util_get_pagebuf(result);

            for (uint64_t i = 0;
                 i < lhs_coord_sizes[0] / sizeof(uint64_t) - 1; i++)
            {
                uint64_t lhs_pos = lhs_indptr[i];
                uint64_t rhs_pos = rhs_indptr[i];
                uint64_t lhs_end = lhs_indptr[i + 1];
                uint64_t rhs_end = rhs_indptr[i + 1];

                while (lhs_pos < lhs_end && rhs_pos < rhs_end)
                {
                    uint64_t lhs_col = lhs_indices[lhs_pos];
                    uint64_t rhs_col = rhs_indices[rhs_pos];

                    if (lhs_col == rhs_col)
                    {
                        double res_value = lhs_buf[lhs_pos] / rhs_buf[rhs_pos];

                        if (res_value != 0)
                        {
                            res_indices[nnz] = lhs_col;
                            res_buf[nnz] = res_value;
                            nnz++;
                        }
                        lhs_pos++;
                        rhs_pos++;
                    }
                    else if (lhs_col < rhs_col)
                    {
                        res_indices[nnz] = lhs_col;
                        // res_buf[nnz] = lhs_buf[lhs_pos];
                        if (lhs_buf[lhs_pos] == 0)
                            res_buf[nnz] = NAN;
                        else
                            res_buf[nnz] = INFINITY;
                        nnz++;

                        lhs_pos++;
                    }
                    else
                    {
                        // res_indices[nnz] = rhs_col;
                        // res_buf[nnz] = rhs_buf[rhs_pos];
                        if (rhs_buf[rhs_pos] == 0)
                        {
                            res_indices[nnz] = rhs_col;
                            res_buf[nnz] = NAN;
                            nnz++;
                        }

                        rhs_pos++;
                    }
                }

                while (lhs_pos < lhs_end)
                {
                    res_indices[nnz] = lhs_indices[lhs_pos];
                    // res_buf[nnz] = lhs_buf[lhs_pos];
                    if (lhs_buf[lhs_pos] == 0)
                        res_buf[nnz] = NAN;
                    else
                        res_buf[nnz] = INFINITY;
                    nnz++;

                    lhs_pos++;
                }
                while (rhs_pos < rhs_end)
                {
                    // res_indices[nnz] = rhs_indices[rhs_pos];
                    // res_buf[nnz] = rhs_buf[rhs_pos];
                    if (rhs_buf[rhs_pos] == 0)
                    {
                        res_indices[nnz] = rhs_indices[rhs_pos];
                        res_buf[nnz] = NAN;
                        nnz++;
                    }

                    rhs_pos++;
                }

                res_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(result, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(result, nnz * sizeof(double));
        }
    }
}


void _lam_sparse_map(
        PFpage *A,
        PFpage *B,
        int opnd_attr_type,
        Array *output_array) {

    int result_attr_type = output_array->op_param.lambda.return_type;
    void (*lambda_csr)(uint64_t *, uint64_t *, void *, uint64_t nrows, void *, uint64_t) = output_array->op_param.lambda.lambda_func_sparse;

    uint64_t *A_coord_sizes = bf_util_pagebuf_get_coords_lens(A);
    uint64_t *A_indptr = bf_util_pagebuf_get_coords(A, 0);
    uint64_t *A_indices = bf_util_pagebuf_get_coords(A, 1);

    // uint64_t numcells = A_coord_sizes[1] / sizeof(uint64_t);

    uint64_t nnz = 0;
    uint64_t nrows = A_coord_sizes[0] / sizeof(uint64_t) - 1;

    if (result_attr_type == TILESTORE_FLOAT64)
    {
        double *Bbuf = (double *)bf_util_get_pagebuf(B);
        uint64_t numcells = B->pagebuf_len / sizeof(double);

        if (opnd_attr_type == TILESTORE_INT32)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            lambda_csr(A_indptr, A_indices, (int *)Abuf, nrows, (double *)Bbuf, numcells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT32)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            lambda_csr(A_indptr, A_indices, (float *)Abuf, nrows, (double *)Bbuf, numcells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT64)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            lambda_csr(A_indptr, A_indices, (double *)Abuf, nrows, (double *)Bbuf, numcells);
        }
    }
}

void _lam_sparse_elemwise_with_constant(
        PFpage *A, PFpage *B, int opnd_attr_type, int constant_type, void *constant, bool constant_equation_side, int op_type) {

    assert(op_type == CHUNK_OP_MUL);

    uint64_t *A_coord_sizes = bf_util_pagebuf_get_coords_lens(A);
    uint64_t *A_indptr = bf_util_pagebuf_get_coords(A, 0);
    uint64_t *A_indices = bf_util_pagebuf_get_coords(A, 1);

    uint64_t numcells = A_coord_sizes[1] / sizeof(uint64_t);

    BF_ResizeBuf(B, numcells);
    uint64_t *B_indptr = bf_util_pagebuf_get_coords(B, 0);
    uint64_t *B_indices = bf_util_pagebuf_get_coords(B, 1);

    B_indptr[0] = 0;
    uint64_t nnz = 0;
    uint64_t nrows = A_coord_sizes[0] / sizeof(uint64_t) - 1;

    if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        double *Abuf = (double *)bf_util_get_pagebuf(A);
        if (constant_type == TILESTORE_FLOAT64)
        {
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            for (uint64_t i = 0; i < nrows; i++)
            {
                uint64_t opnd_pos = A_indptr[i];
                uint64_t opnd_end = A_indptr[i + 1];

                while (opnd_pos < opnd_end)
                {
                    uint64_t opnd_col = A_indices[opnd_pos];
                    double res_value;

                    if (constant_equation_side == 0) // LHS
                        res_value = (*((double *)constant)) * Abuf[opnd_pos];
                    else if (constant_equation_side == 1) // RHS
                        res_value = Abuf[opnd_pos] * (*((double *)constant));

                    if (res_value != 0)
                    {
                        B_indices[nnz] = opnd_col;
                        Bbuf[nnz] = res_value;
                        nnz++;
                    }
                    opnd_pos++;
                }
                B_indptr[i + 1] = nnz;
            }

            bf_util_pagebuf_set_unfilled_idx(B, nnz);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(B, nnz * sizeof(double));
        } else {
            // TODO:
            assert(0);
        }
    }
}

void _lam_sparse_elemwise(
        PFpage *lhs,
        PFpage *rhs,
        PFpage *result,
        int lhs_attr_type,
        int rhs_attr_type,
        int op_type) {
    
    if (op_type == CHUNK_OP_ADD) elemwise_sparse_add(lhs, rhs, result, lhs_attr_type, rhs_attr_type);
    else if (op_type == CHUNK_OP_SUB) elemwise_sparse_sub(lhs, rhs, result, lhs_attr_type, rhs_attr_type);
    else if (op_type == CHUNK_OP_MUL) elemwise_sparse_product(lhs, rhs, result, lhs_attr_type, rhs_attr_type);
    else if (op_type == CHUNK_OP_DIV) elemwise_sparse_div(lhs, rhs, result, lhs_attr_type, rhs_attr_type);

}