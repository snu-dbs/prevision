#include <stdio.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <cblas.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "array_struct.h"

int ts_type_to_chunk_type(int in) {
    switch (in) {
        case TILESTORE_INT32: return ATTR_TYPE_INT;
        case TILESTORE_FLOAT32: return ATTR_TYPE_FLOAT;
        case TILESTORE_FLOAT64: return ATTR_TYPE_DOUBLE;
        default: return -1;
    }
    return -1;
}


void _lam_dense_matmul_impl_mul(Chunk *A, Chunk *B, Chunk *C, int type1, int type2, bool lhs_transposed, bool rhs_transposed)
{
    uint32_t ndim = A->dim_len;

    assert(type1 >= 0 && type1 <= 2);
    assert(type2 >= 0 && type2 <= 2);

    uint64_t ncells_A = A->tile_extents[0] * A->tile_extents[1];
    uint64_t ncells_B = B->tile_extents[0] * B->tile_extents[1];
    uint64_t ncells_C = C->tile_extents[0] * C->tile_extents[1];

    // decide parameters
    int m, n, k, lda, ldb, ldc = 0;
    if (lhs_transposed)
    {
        m = A->tile_extents[1];
        k = A->tile_extents[0];
    }
    else
    {
        m = A->tile_extents[0];
        k = A->tile_extents[1];
    }

    if (rhs_transposed)
    {
        n = B->tile_extents[0];
    }
    else
    {
        n = B->tile_extents[1];
    }

    lda = A->tile_extents[1];
    ldb = B->tile_extents[1];
    if (rhs_transposed)
    {
        ldc = B->tile_extents[0];
    }
    else
    {
        ldc = B->tile_extents[1];
    }

    // transpose
    CBLAS_TRANSPOSE lhs_trans = lhs_transposed ? CblasTrans : CblasNoTrans;
    CBLAS_TRANSPOSE rhs_trans = rhs_transposed ? CblasTrans : CblasNoTrans;

    // matrix-vector optimization
    bool_t is_gemv = (B->tile_extents[0] == 1 || B->tile_extents[1] == 1);

    if (type1 == ATTR_TYPE_INT)
    { // INT
        if (type2 == ATTR_TYPE_INT)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A->curpage);
            int *Bbuf = (int *)bf_util_get_pagebuf(B->curpage);
            int *Cbuf = (int *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Btemp = malloc(sizeof(double) * ncells_B);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }
            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Btemp, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Btemp, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] = (int)Ctemp[i];
            }

            free(Atemp);
            free(Btemp);
            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A->curpage);
            float *Bbuf = (float *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Btemp = malloc(sizeof(double) * ncells_B);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }
            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Btemp, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Btemp, ldb,
                            0, Cbuf, ldc);
            }

            free(Atemp);
            free(Btemp);
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A->curpage);
            double *Bbuf = (double *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Bbuf, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Bbuf, ldb,
                            0, Cbuf, ldc);
            }

            free(Atemp);
        }
    }
    else if (type1 == ATTR_TYPE_FLOAT)
    { // FLOAT
        if (type2 == ATTR_TYPE_INT)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A->curpage);
            int *Bbuf = (int *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Btemp = malloc(sizeof(double) * ncells_B);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }
            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Btemp, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Btemp, ldb,
                            0, Cbuf, ldc);
            }

            free(Atemp);
            free(Btemp);
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A->curpage);
            float *Bbuf = (float *)bf_util_get_pagebuf(B->curpage);
            float *Cbuf = (float *)bf_util_get_pagebuf(C->curpage);

            if (is_gemv)
            {
                cblas_sgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Bbuf, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_sgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, k, Bbuf, n,
                            0, Cbuf, n);
            }
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A->curpage);
            double *Bbuf = (double *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Bbuf, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Bbuf, ldb,
                            0, Cbuf, ldc);
            }

            free(Atemp);
        }
    }
    else if (type1 == ATTR_TYPE_DOUBLE)
    { // DOUBLE
        if (type2 == ATTR_TYPE_INT)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A->curpage);
            int *Bbuf = (int *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Btemp = malloc(sizeof(double) * ncells_B);

            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Btemp, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Btemp, ldb,
                            0, Cbuf, ldc);
            }

            free(Btemp);
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A->curpage);
            float *Bbuf = (float *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Btemp = malloc(sizeof(double) * ncells_B);

            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Btemp, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Btemp, ldb,
                            0, Cbuf, ldc);
            }

            free(Btemp);
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A->curpage);
            double *Bbuf = (double *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Bbuf, 1, 0, Cbuf, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Bbuf, ldb,
                            0, Cbuf, ldc);
            }
        }
    }

    C->dirty = 1;
}

void _lam_dense_matmul_impl_mulandadd(Chunk *A, Chunk *B, Chunk *C, int type1, int type2, bool lhs_transposed, bool rhs_transposed)
{
    uint32_t ndim = A->dim_len;

    assert(type1 >= 0 && type1 <= 2);
    assert(type2 >= 0 && type2 <= 2);

    // decide parameters
    int m, n, k, lda, ldb, ldc = 0;
    if (lhs_transposed)
    {
        m = A->tile_extents[1];
        k = A->tile_extents[0];
    }
    else
    {
        m = A->tile_extents[0];
        k = A->tile_extents[1];
    }

    if (rhs_transposed)
    {
        n = B->tile_extents[0];
    }
    else
    {
        n = B->tile_extents[1];
    }

    lda = A->tile_extents[1];
    ldb = B->tile_extents[1];
    if (rhs_transposed)
    {
        ldc = B->tile_extents[0];
    }
    else
    {
        ldc = B->tile_extents[1];
    }

    uint64_t ncells_A = A->tile_extents[0] * A->tile_extents[1];
    uint64_t ncells_B = B->tile_extents[0] * B->tile_extents[1];
    uint64_t ncells_C = C->tile_extents[0] * C->tile_extents[1];

    // transpose
    CBLAS_TRANSPOSE lhs_trans = lhs_transposed ? CblasTrans : CblasNoTrans;
    CBLAS_TRANSPOSE rhs_trans = rhs_transposed ? CblasTrans : CblasNoTrans;

    // matrix-vector optimization
    bool_t is_gemv = (B->tile_extents[0] == 1 || B->tile_extents[1] == 1);

    // different logic is required to handle dense mode
    if (C->curpage->type == SPARSE_FIXED) {
        assert(type1 == ATTR_TYPE_DOUBLE && type2 == ATTR_TYPE_DOUBLE);
        BF_ConvertToDense(C->curpage, false);
    } 
    
    if (type1 == ATTR_TYPE_INT)
    { // INT
        if (type2 == ATTR_TYPE_INT)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A->curpage);
            int *Bbuf = (int *)bf_util_get_pagebuf(B->curpage);
            int *Cbuf = (int *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Btemp = malloc(sizeof(double) * ncells_B);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }
            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Btemp, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Btemp, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += (int)Ctemp[i];
            }

            free(Atemp);
            free(Btemp);
            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A->curpage);
            float *Bbuf = (float *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Btemp = malloc(sizeof(double) * ncells_B);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }
            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Btemp, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Btemp, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Atemp);
            free(Btemp);
            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A->curpage);
            double *Bbuf = (double *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Bbuf, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Bbuf, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Atemp);
            free(Ctemp);
        }
    }
    else if (type1 == ATTR_TYPE_FLOAT)
    { // FLOAT
        if (type2 == ATTR_TYPE_INT)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A->curpage);
            int *Bbuf = (int *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Btemp = malloc(sizeof(double) * ncells_B);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }
            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Btemp, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Btemp, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Atemp);
            free(Btemp);
            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A->curpage);
            float *Bbuf = (float *)bf_util_get_pagebuf(B->curpage);
            float *Cbuf = (float *)bf_util_get_pagebuf(C->curpage);

            float *Ctemp = malloc(sizeof(float) * ncells_C);

            if (is_gemv)
            {
                cblas_sgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Bbuf, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_sgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Bbuf, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A->curpage);
            double *Bbuf = (double *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Atemp = malloc(sizeof(double) * ncells_A);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_A; i++)
            {
                Atemp[i] = (double)Abuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Atemp, A->tile_extents[1], Bbuf, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Atemp, lda, Bbuf, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Atemp);
            free(Ctemp);
        }
    }
    else if (type1 == ATTR_TYPE_DOUBLE)
    { // DOUBLE
        if (type2 == ATTR_TYPE_INT)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A->curpage);
            int *Bbuf = (int *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Btemp = malloc(sizeof(double) * ncells_B);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Btemp, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Btemp, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Btemp);
            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A->curpage);
            float *Bbuf = (float *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            double *Btemp = malloc(sizeof(double) * ncells_B);
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            for (uint64_t i = 0; i < ncells_B; i++)
            {
                Btemp[i] = (double)Bbuf[i];
            }

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Btemp, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Btemp, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Btemp);
            free(Ctemp);
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A->curpage);
            double *Bbuf = (double *)bf_util_get_pagebuf(B->curpage);
            double *Cbuf = (double *)bf_util_get_pagebuf(C->curpage);

            // TODO: Should we malloc ctemp again?
            double *Ctemp = malloc(sizeof(double) * ncells_C);

            if (is_gemv)
            {
                cblas_dgemv(CblasRowMajor, lhs_trans,
                            A->tile_extents[0], A->tile_extents[1],
                            1, Abuf, A->tile_extents[1], Bbuf, 1, 0, Ctemp, 1);
            }
            else
            {
                cblas_dgemm(CblasRowMajor, lhs_trans, rhs_trans,
                            m, n, k,
                            1, Abuf, lda, Bbuf, ldb,
                            0, Ctemp, ldc);
            }

            for (uint64_t i = 0; i < ncells_C; i++)
            {
                Cbuf[i] += Ctemp[i];
            }

            free(Ctemp);
        }
    }
}


void _lam_dense_matmul(Chunk *lhs,
                  Chunk *rhs,
                  Chunk *result,
                  int lhs_attr_type,
                  int rhs_attr_type,
                  uint64_t iteration,
                  bool lhs_transposed,
                  bool rhs_transposed,
                  bool is_first)
{
    int in_lhs_type = ts_type_to_chunk_type(lhs_attr_type);
    int in_rhs_type = ts_type_to_chunk_type(rhs_attr_type);

    if (is_first) // First iteration : Just multiply 
        _lam_dense_matmul_impl_mul(lhs, rhs, result, in_lhs_type, in_rhs_type, lhs_transposed, rhs_transposed);
    else // Other iterations : Multiply and add
        _lam_dense_matmul_impl_mulandadd(lhs, rhs, result, in_lhs_type, in_rhs_type, lhs_transposed, rhs_transposed);
}