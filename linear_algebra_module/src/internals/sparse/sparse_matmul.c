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
#include "array_struct.h"


typedef struct ChunkSparseMMOpFloat64Float64Args {
    uint64_t *A_indptr;
    uint64_t *A_indices;
    double *A_buf;

    uint64_t *B_indptr;
    uint64_t *B_indices;
    double *B_buf;

    uint64_t *RES_indptr;
    uint64_t *RES_indices;
    double *RES_buf;

    uint64_t nstart;
    uint64_t nend;
    uint64_t nrows;
    uint64_t ncols;
} ChunkSparseMMOpFloat64Float64Args;

void* chunk_sparse_mm_op_float64_float64(void *raw) {
    ChunkSparseMMOpFloat64Float64Args *args = (ChunkSparseMMOpFloat64Float64Args*) raw;
    uint64_t *A_indptr = args->A_indptr;
    uint64_t *A_indices = args->A_indices;
    double *A_buf = args->A_buf;

    uint64_t *B_indptr = args->B_indptr;
    uint64_t *B_indices = args->B_indices;
    double *B_buf = args->B_buf;

    uint64_t *RES_indptr = args->RES_indptr;
    uint64_t *RES_indices = args->RES_indices;
    double *RES_buf = args->RES_buf;

    uint64_t nstart = args->nstart;
    uint64_t nend = args->nend;
    uint64_t nrows = args->nrows;
    uint64_t ncols = args->ncols;

    double *values = calloc(ncols, sizeof(double));
    int64_t *mask = malloc(sizeof(int64_t) * ncols);

    uint64_t nnz = RES_indptr[nstart];

    // Calculate matmul result
    for (size_t i = 0; i < ncols; i++)
    {
        mask[i] = (int64_t)-1;
    }

    for (size_t i = nstart; i < nend; i++)
    {
        for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
        {
            size_t j = A_indices[jj];
            double v = A_buf[jj];

            for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
            {
                size_t k = B_indices[kk];

                values[k] += v * B_buf[kk];

                if (mask[k] != (int64_t)i)
                {
                    mask[k] = i;
                }
            }
        }
        for (size_t jj = 0; jj < ncols; jj++)
        {
            if (mask[jj] == (int64_t)i)
            {
                RES_indices[nnz] = jj;
                RES_buf[nnz] = values[jj];
                nnz++;
            }
            values[jj] = 0;
        }
    }

    free(mask);
    free(values);
}

/* Private functions for sparse matmul */
void _sparse_matmul_csr_csr(PFpage *A_page, PFpage *B_page, CSR_MATRIX *res,
                                  int type1, int type2)
{
    int num_threads = (getenv("__PREVISION_NUM_THREADS") == NULL) ? 1 : atoi(getenv("__PREVISION_NUM_THREADS"));

    uint64_t *A_indptr = bf_util_pagebuf_get_coords(A_page, 0);
    uint64_t *A_indices = bf_util_pagebuf_get_coords(A_page, 1);
    uint64_t *B_indptr = bf_util_pagebuf_get_coords(B_page, 0);
    uint64_t *B_indices = bf_util_pagebuf_get_coords(B_page, 1);
    
    res->indptr = malloc(sizeof(size_t) * (res->n_row + 1));
    res->indptr[0] = 0;

    // Calculate nnz of matmul result
    int64_t *mask = malloc(sizeof(int64_t) * res->n_col);
    for (size_t i = 0; i < res->n_col; i++)
    {
        mask[i] = (int64_t)-1;
    }

    size_t nnz = 0;
    for (size_t i = 0; i < res->n_row; i++)
    {
        for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
        { 
            size_t j = A_indices[jj];
            for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
            { 
                size_t k = B_indices[kk]; 
                if (mask[k] != (int64_t)i)
                {
                    mask[k] = i;
                    nnz++;
                }
            }
        }

        res->indptr[i + 1] = nnz;
    }
    res->indices = malloc(sizeof(size_t) * nnz);
    res->data = malloc(sizeof(double) * nnz);
    res->nnz = nnz;

    // Calculate matmul result
    for (size_t i = 0; i < res->n_col; i++)
    {
        mask[i] = (int64_t)-1;
    }
    double *values = calloc(res->n_col, sizeof(double));

    nnz = 0;
    if (type1 == TILESTORE_INT32)
    {
        int *A_buf = (int *)bf_util_get_pagebuf(A_page);

        if (type2 == TILESTORE_INT32)
        {
            int *B_buf = (int *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_INT32;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT32)
        {
            float *B_buf = (float *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT64)
        {
            double *B_buf = (double *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
    }
    else if (type1 == TILESTORE_FLOAT32)
    {
        float *A_buf = (float *)bf_util_get_pagebuf(A_page);

        if (type2 == TILESTORE_INT32)
        {
            int *B_buf = (int *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT32)
        {
            float *B_buf = (float *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT32;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT64)
        {
            double *B_buf = (double *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
    }
    else if (type1 == TILESTORE_FLOAT64)
    {
        double *A_buf = (double *)bf_util_get_pagebuf(A_page);

        if (type2 == TILESTORE_INT32)
        {
            int *B_buf = (int *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT32)
        {
            float *B_buf = (float *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                {
                    size_t j = A_indices[jj];
                    double v = A_buf[jj];

                    for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                    {
                        size_t k = B_indices[kk];

                        values[k] += v * B_buf[kk];

                        if (mask[k] != (int64_t)i)
                        {
                            mask[k] = i;
                        }
                    }
                }
                for (size_t jj = 0; jj < res->n_col; jj++)
                {
                    if (mask[jj] == (int64_t)i)
                    {
                        res->indices[nnz] = jj;
                        res->data[nnz] = values[jj];
                        nnz++;
                    }
                    values[jj] = 0;
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT64)
        {
            double *B_buf = (double *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            if (num_threads > 1) {
                pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
                ChunkSparseMMOpFloat64Float64Args *args = malloc(sizeof(ChunkSparseMMOpFloat64Float64Args) * num_threads);
                uint64_t row_per_thread = res->n_row / num_threads;
                for (int i = 0; i < num_threads; i++) {
                    args[i].A_indptr = A_indptr;
                    args[i].A_indices = A_indices;
                    args[i].A_buf = A_buf;

                    args[i].B_indptr = B_indptr;
                    args[i].B_indices = B_indices;
                    args[i].B_buf = B_buf;

                    args[i].RES_indptr = res->indptr;
                    args[i].RES_indices = res->indices;
                    args[i].RES_buf = res->data;

                    args[i].nstart = row_per_thread * i;
                    args[i].nend = (i + 1 == num_threads) ? 
                        res->n_row : (row_per_thread * i) + row_per_thread;
                    args[i].nrows = res->n_row;
                    args[i].ncols = res->n_col;
                    
                    pthread_create(&threads[i], NULL, chunk_sparse_mm_op_float64_float64, (void *) &args[i]);
                }

                for (int i = 0; i < num_threads; i++) {
                    pthread_join(threads[i], NULL);
                }

                free(args);
                free(threads);
            } else {
                for (size_t i = 0; i < res->n_row; i++)
                {
                    for (size_t jj = A_indptr[i]; jj < A_indptr[i + 1]; jj++)
                    {
                        size_t j = A_indices[jj];
                        double v = A_buf[jj];

                        for (size_t kk = B_indptr[j]; kk < B_indptr[j + 1]; kk++)
                        {
                            size_t k = B_indices[kk];

                            values[k] += v * B_buf[kk];

                            if (mask[k] != (int64_t)i)
                            {
                                mask[k] = i;
                            }
                        }
                    }
                    for (size_t jj = 0; jj < res->n_col; jj++)
                    {
                        if (mask[jj] == (int64_t)i)
                        {
                            res->indices[nnz] = jj;
                            res->data[nnz] = values[jj];
                            nnz++;
                        }
                        values[jj] = 0;
                    }
                }
            }
        }
    }

    free(values);
    free(mask);
}

void _sparse_matmul_csr_csc(PFpage *A_page, PFpage *B_page, CSR_MATRIX *res,
                                  int type1, int type2)
{
    uint64_t *A_indptr = bf_util_pagebuf_get_coords(A_page, 0);
    uint64_t *A_indices = bf_util_pagebuf_get_coords(A_page, 1);

    uint64_t *B_indptr = bf_util_pagebuf_get_coords(B_page, 0);
    uint64_t *B_indices = bf_util_pagebuf_get_coords(B_page, 1);

    res->indptr = malloc(sizeof(size_t) * (res->n_row + 1));
    res->indptr[0] = 0;

    // Calculate nnz of matmul result in order to allocate memory for result tile.
    size_t nnz = 0;
    for (size_t i = 0; i < res->n_row; i++)
    {
        for (size_t j = 0; j < res->n_col; j++)
        {
            size_t A_cursor = A_indptr[i];
            size_t B_cursor = B_indptr[j];

            while (A_cursor < A_indptr[i + 1] &&
                   B_cursor < B_indptr[j + 1])
            {
                if (A_indices[A_cursor] == B_indices[B_cursor])
                {
                    nnz++;
                    break;
                }
                else if (A_indices[A_cursor] > B_indices[B_cursor])
                {
                    B_cursor++;
                }
                else if (A_indices[A_cursor] < B_indices[B_cursor])
                {
                    A_cursor++;
                }
            }
        }
        res->indptr[i + 1] = nnz;
    }
    res->indices = malloc(sizeof(size_t) * nnz);
    res->data = malloc(sizeof(double) * nnz);
    res->nnz = nnz;

    // Calculate matmul result
    nnz = 0;
    if (type1 == TILESTORE_INT32)
    {
        int *A_buf = (int *)bf_util_get_pagebuf(A_page);

        if (type2 == TILESTORE_INT32)
        {
            int *B_buf = (int *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_INT32;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT32)
        {
            float *B_buf = (float *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT64)
        {
            double *B_buf = (double *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
    }
    else if (type1 == TILESTORE_FLOAT32)
    {
        float *A_buf = (float *)bf_util_get_pagebuf(A_page);

        if (type2 == TILESTORE_INT32)
        {
            int *B_buf = (int *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT32)
        {
            float *B_buf = (float *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT32;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT64)
        {
            double *B_buf = (double *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
    }
    else if (type1 == TILESTORE_FLOAT64)
    {
        double *A_buf = (double *)bf_util_get_pagebuf(A_page);

        if (type2 == TILESTORE_INT32)
        {
            int *B_buf = (int *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT32)
        {
            float *B_buf = (float *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
        else if (type2 == TILESTORE_FLOAT64)
        {
            double *B_buf = (double *)bf_util_get_pagebuf(B_page);
            res->data_type = TILESTORE_FLOAT64;

            for (size_t i = 0; i < res->n_row; i++)
            {
                for (size_t j = 0; j < res->n_col; j++)
                {
                    size_t A_cursor = A_indptr[i];
                    size_t B_cursor = B_indptr[j];
                    bool is_nonzero = false;
                    double value = 0;

                    while (A_cursor < A_indptr[i + 1] &&
                           B_cursor < B_indptr[j + 1])
                    {
                        if (A_indices[A_cursor] == B_indices[B_cursor])
                        {
                            is_nonzero = true;
                            value += (A_buf[A_cursor] * B_buf[B_cursor]);
                            A_cursor++;
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] > B_indices[B_cursor])
                        {
                            B_cursor++;
                        }
                        else if (A_indices[A_cursor] < B_indices[B_cursor])
                        {
                            A_cursor++;
                        }
                    }

                    if (is_nonzero)
                    {
                        res->indices[nnz] = j;
                        res->data[nnz] = value;
                        nnz++;
                    }
                }
            }
        }
    }
}

void _sparse_matmul_add_res(CSR_MATRIX *lhs, CSR_MATRIX *rhs, CSR_MATRIX *res)
{
    assert(lhs->n_row == rhs->n_row && rhs->n_row == res->n_row);
    assert(lhs->n_col == rhs->n_col && rhs->n_col == res->n_col);

    // Allocate result matrix conservatively. (nnz not known a priori)
    res->indptr = malloc(sizeof(size_t) * (res->n_row + 1));
    res->indices = malloc((sizeof(size_t) * (lhs->nnz + rhs->nnz)));
    res->data = malloc((sizeof(double) * (lhs->nnz + rhs->nnz)));

    res->indptr[0] = 0;
    size_t nnz = 0;

    for (size_t i = 0; i < res->n_row; i++)
    {
        size_t lhs_pos = lhs->indptr[i];
        size_t rhs_pos = rhs->indptr[i];
        size_t lhs_end = lhs->indptr[i + 1];
        size_t rhs_end = rhs->indptr[i + 1];

        // while not finished with either row
        while (lhs_pos < lhs_end && rhs_pos < rhs_end)
        {
            size_t lhs_j = lhs->indices[lhs_pos];
            size_t rhs_j = rhs->indices[rhs_pos];

            if (lhs_j == rhs_j)
            {
                double res_value = lhs->data[lhs_pos] + rhs->data[rhs_pos];
                if (res_value != 0)
                {
                    res->indices[nnz] = lhs_j;
                    res->data[nnz] = res_value;
                    nnz++;
                }
                lhs_pos++;
                rhs_pos++;
            }
            else if (lhs_j < rhs_j)
            {
                double res_value = lhs->data[lhs_pos];
                if (res_value != 0)
                {
                    res->indices[nnz] = lhs_j;
                    res->data[nnz] = res_value;
                    nnz++;
                }
                lhs_pos++;
            }
            else
            {
                double res_value = rhs->data[rhs_pos];
                if (res_value != 0)
                {
                    res->indices[nnz] = rhs_j;
                    res->data[nnz] = res_value;
                    nnz++;
                }
                rhs_pos++;
            }
        }

        // tail
        while (lhs_pos < lhs_end)
        {
            double res_value = lhs->data[lhs_pos];
            if (res_value != 0)
            {
                res->indices[nnz] = lhs->indices[lhs_pos];
                res->data[nnz] = res_value;
                nnz++;
            }
            lhs_pos++;
        }
        while (rhs_pos < rhs_end)
        {
            double res_value = rhs->data[rhs_pos];
            if (res_value != 0)
            {
                res->indices[nnz] = rhs->indices[rhs_pos];
                res->data[nnz] = res_value;
                nnz++;
            }
            rhs_pos++;
        }

        res->indptr[i + 1] = nnz;
    }

    res->nnz = nnz;
}

/* Currently assumes that each coordinate is of type TILESTORE_INT32 */
void sparse_matmul(Chunk *A,
                         Chunk *B,
                         Chunk *C,
                         int type1,
                         int type2,
                         bool lhs_transposed,
                         bool rhs_transposed)
{
    // Store temp result csr into result's chunk structure.
    C->temp_csr = malloc(sizeof(CSR_MATRIX));
    C->temp_csr->n_row = C->tile_extents[0];
    C->temp_csr->n_col = C->tile_extents[1];

    // Matmul-Transpose Merge
    if (!lhs_transposed && !rhs_transposed)
    {               
        _sparse_matmul_csr_csr(A->curpage, B->curpage, C->temp_csr, type1, type2);
    }

    else if (!lhs_transposed && rhs_transposed)
    {
        _sparse_matmul_csr_csc(A->curpage, B->curpage, C->temp_csr, type1, type2);
    }
}

void sparse_matmul_and_add(Chunk *A,
                                 Chunk *B,
                                 Chunk *C,
                                 int type1,
                                 int type2,
                                 bool lhs_transposed,
                                 bool rhs_transposed)
{
    CSR_MATRIX *matmul_res = malloc(sizeof(CSR_MATRIX));
    matmul_res->n_row = C->tile_extents[0];
    matmul_res->n_col = C->tile_extents[1];

    CSR_MATRIX *add_res = malloc(sizeof(CSR_MATRIX));
    add_res->n_row = C->tile_extents[0];
    add_res->n_col = C->tile_extents[1];

    if (!lhs_transposed && !rhs_transposed)
    {
        _sparse_matmul_csr_csr(A->curpage, B->curpage, matmul_res, type1, type2);
    }
    else if (!lhs_transposed && rhs_transposed)
    {
        _sparse_matmul_csr_csc(A->curpage, B->curpage, matmul_res, type1, type2);
    } else {
        // FIXME:
        assert(0);
    }

    // If C is sparse so that Sparse + Sparse is required.
    _sparse_matmul_add_res(matmul_res, C->temp_csr, add_res);

    // Replace the contents of C->temp_csr with add_res
    free(C->temp_csr->indptr);
    free(C->temp_csr->indices);
    free(C->temp_csr->data);
    C->temp_csr->nnz = add_res->nnz;
    C->temp_csr->indptr = add_res->indptr;
    C->temp_csr->indices = add_res->indices;
    C->temp_csr->data = add_res->data;

    free(matmul_res->indptr);
    free(matmul_res->indices);
    free(matmul_res->data);
    free(matmul_res);
    free(add_res);
}

/* Sparse matmul currently supports only optimized version */
void _lam_sparse_matmul(Chunk *lhs,
                   Chunk *rhs,
                   Chunk *result,
                   int lhs_attr_type,
                   int rhs_attr_type,
                   uint64_t *chunksize,
                   uint64_t iteration,
                   bool lhs_transposed,
                   bool rhs_transposed,
                   bool is_first)
{
    if (is_first) // First iteration : Just multiply
    {
        sparse_matmul(lhs, rhs, result,
                            lhs_attr_type, rhs_attr_type,
                            lhs_transposed, rhs_transposed);
    }
    else // Other iterations : Multiply and add
    {
        sparse_matmul_and_add(lhs, rhs, result,
                                    lhs_attr_type, rhs_attr_type,
                                    lhs_transposed, rhs_transposed);
    }
}

void _lam_sparse_matmul_finalize(Chunk *result,
                            int lhs_attr_type,
                            int rhs_attr_type)
{
    BF_ResizeBuf(result->curpage, result->temp_csr->nnz);

    uint64_t *res_indptr = bf_util_pagebuf_get_coords(result->curpage, 0);
    uint64_t *res_indices = bf_util_pagebuf_get_coords(result->curpage, 1);

    for (size_t i = 0; i <= result->temp_csr->n_row; i++)
    {
        res_indptr[i] = result->temp_csr->indptr[i];
    }

    // Branch according to the operands type
    if (result->temp_csr->data_type == TILESTORE_INT32)
    {
        int *res_buf = (int *)bf_util_get_pagebuf(result->curpage);

        for (size_t i = 0; i < result->temp_csr->nnz; i++)
        {
            res_indices[i] = result->temp_csr->indices[i];
            res_buf[i] = (int)(result->temp_csr->data[i]);
        }

        bf_util_pagebuf_set_unfilled_idx(result->curpage, result->temp_csr->nnz);
        bf_util_pagebuf_set_unfilled_pagebuf_offset(result->curpage, result->temp_csr->nnz * sizeof(int));
    }
    else if (result->temp_csr->data_type == TILESTORE_FLOAT32)
    {
        float *res_buf = (float *)bf_util_get_pagebuf(result->curpage);

        for (size_t i = 0; i < result->temp_csr->nnz; i++)
        {
            res_indices[i] = result->temp_csr->indices[i];
            res_buf[i] = (float)(result->temp_csr->data[i]);
        }

        bf_util_pagebuf_set_unfilled_idx(result->curpage, result->temp_csr->nnz);
        bf_util_pagebuf_set_unfilled_pagebuf_offset(result->curpage, result->temp_csr->nnz * sizeof(float));
    }
    else if (result->temp_csr->data_type == TILESTORE_FLOAT64)
    {
        double *res_buf = (double *)bf_util_get_pagebuf(result->curpage);

        for (size_t i = 0; i < result->temp_csr->nnz; i++)
        {
            res_indices[i] = result->temp_csr->indices[i];
            res_buf[i] = (double)(result->temp_csr->data[i]);
        }

        bf_util_pagebuf_set_unfilled_idx(result->curpage, result->temp_csr->nnz);
        bf_util_pagebuf_set_unfilled_pagebuf_offset(result->curpage, result->temp_csr->nnz * sizeof(double));
    }

    free(result->temp_csr->indptr);
    free(result->temp_csr->indices);
    free(result->temp_csr->data);
    free(result->temp_csr);
}