#include <stdio.h>
#include <string.h>
#include <math.h>

#include <sys/time.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"


/**
 *
 * @param A : ChunkA
 * @param B : ChunkB
 * @param C : ChunkC
 * @param type1 0: Int 1: Float 2:Double
 * @param type2 0: Int 1: Float 2:Double
 * @param op 0: ADD   1: SUB  2: MUL  3: DIV
 */

typedef struct ChunkArithmeticOpFloat64Float64Args {
    double *a;
    double *b;
    double *c;
    uint64_t ncells;
    int op;
} ChunkArithmeticOpFloat64Float64Args;

void* chunk_arithmetic_op_float64_float64(void *raw) {
    ChunkArithmeticOpFloat64Float64Args *args = (ChunkArithmeticOpFloat64Float64Args*) raw;
    double *a = args->a;
    double *b = args->b;
    double *c = args->c;
    uint64_t ncells = args->ncells;
    int op = args->op;
    switch (op) {
    case CHUNK_OP_ADD:
        for (uint64_t i = 0; i < ncells; i++)
        {
            c[i] = a[i] + b[i];
        }
        break;
    case CHUNK_OP_SUB:
        for (uint64_t i = 0; i < ncells; i++)
        {
            c[i] = a[i] - b[i];
        }
        break;

    case CHUNK_OP_MUL:
        for (uint64_t i = 0; i < ncells; i++)
        {
            c[i] = a[i] * b[i];
        }
        break;
    case CHUNK_OP_DIV:
        for (uint64_t i = 0; i < ncells; i++)
        {
            c[i] = a[i] / b[i];
        }
        break;
    }
}

void chunk_arithmetic_op(PFpage *A, PFpage *B, PFpage *C, int type1, int type2, int op)
{
    int num_threads = (getenv("__PREVISION_NUM_THREADS") == NULL) ? 1 : atoi(getenv("__PREVISION_NUM_THREADS"));

    if (type1 == ATTR_TYPE_INT)
    { // INT
        if (type2 == ATTR_TYPE_INT)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(int);

            int *Abuf = (int *)bf_util_get_pagebuf(A);
            int *Bbuf = (int *)bf_util_get_pagebuf(B);
            int *Cbuf = (int *)bf_util_get_pagebuf(C);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] + Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] - Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] * Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] / Bbuf[i];
                }
                break;
            }
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(int);

            int *Abuf = (int *)bf_util_get_pagebuf(A);
            float *Bbuf = (float *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] + (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] - (double)Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] * (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] / (double)Bbuf[i];
                }
                break;
            }
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(int);

            int *Abuf = (int *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] + Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] - Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] * Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] / Bbuf[i];
                }
                break;
            }
        }
    }
    else if (type1 == ATTR_TYPE_FLOAT)
    { // FLOAT
        if (type2 == ATTR_TYPE_INT)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(float);
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            int *Bbuf = (int *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);
            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] + (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] - (double)Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] * (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] / (double)Bbuf[i];
                }
                break;
            }
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(float);
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            float *Bbuf = (float *)bf_util_get_pagebuf(B);
            float *Cbuf = (float *)bf_util_get_pagebuf(C);
            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] + Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] - Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] * Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] / Bbuf[i];
                }
                break;
            }
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(float);
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);
            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] + Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] - Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] * Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = (double)Abuf[i] / Bbuf[i];
                }
                break;
            }
        }
    }
    else if (type1 == ATTR_TYPE_DOUBLE)
    { // DOUBLE
        if (type2 == ATTR_TYPE_INT)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(double);
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            int *Bbuf = (int *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);
            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] + (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] - (double)Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] * (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] / (double)Bbuf[i];
                }
                break;
            }
        }
        else if (type2 == ATTR_TYPE_FLOAT)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(double);
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            float *Bbuf = (float *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);
            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] + (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_SUB:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] - (double)Bbuf[i];
                }
                break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] * (double)Bbuf[i];
                }
                break;
            case CHUNK_OP_DIV:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Cbuf[i] = Abuf[i] / (double)Bbuf[i];
                }
                break;
            }
        }
        else if (type2 == ATTR_TYPE_DOUBLE)
        {
            uint64_t ncells = A->pagebuf_len / sizeof(double);
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);
            double *Cbuf = (double *)bf_util_get_pagebuf(C);

            if (num_threads > 1) {
                pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
                ChunkArithmeticOpFloat64Float64Args *args = malloc(sizeof(ChunkArithmeticOpFloat64Float64Args) * num_threads);
                uint64_t cell_per_thread = ncells / num_threads;
                for (int i = 0; i < num_threads; i++) {
                    args[i].a = Abuf + (cell_per_thread * i);
                    args[i].b = Bbuf + (cell_per_thread * i);
                    args[i].c = Cbuf + (cell_per_thread * i);
                    args[i].ncells = (i + 1 == num_threads) ? ncells - (cell_per_thread * (num_threads - 1)) : cell_per_thread;
                    args[i].op = op;
                    
                    pthread_create(&threads[i], NULL, chunk_arithmetic_op_float64_float64, (void *) &args[i]);
                }

                for (int i = 0; i < num_threads; i++) {
                    pthread_join(threads[i], NULL);
                }

                free(args);
                free(threads);
            } else {
                ChunkArithmeticOpFloat64Float64Args args;
                args.a = Abuf;
                args.b = Bbuf;
                args.c = Cbuf;
                args.op = op;
                args.ncells = ncells;
                chunk_arithmetic_op_float64_float64(&args);
            }
        }
    }
}


void chunk_arithmetic_op_constant(PFpage *A, void *param_constant, PFpage *B, int chunk_type, int constant_type, bool constant_side, int op)
{
    // uint32_t ndim = A->dim_len;
    // assert(A->dim_len == B->dim_len);
    // assert(op >= 0 && op <= 3);
    // assert(chunk_type >= 0 && chunk_type <= 2);
    // assert(constant_type >= 0 && constant_type <= 2);

    if (chunk_type == ATTR_TYPE_INT)
    { // INT
        uint64_t ncells = A->pagebuf_len / sizeof(int);

        if (constant_type == CONST_TYPE_INT)
        {
            int val = *((int *)param_constant);
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            int *Bbuf = (int *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] + val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val - Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] - val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] * val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val / Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] / val;
                }
            }
            break;
            }
        }
        else if (constant_type == CONST_TYPE_FLOAT)
        {
            float val = *((float *)param_constant);
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] + (double)val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val - (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] - (double)val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] * (double)val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val / (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] / (double)val;
                }
            }
            break;
            }
        }
        else if (constant_type == CONST_TYPE_DOUBLE)
        {
            double val = *((double *)param_constant);
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] + val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val - (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] - val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] * val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val / (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] / val;
                }
            }
            break;
            }
        }
    }
    else if (chunk_type == ATTR_TYPE_FLOAT)
    { // FLOAT
        uint64_t ncells = A->pagebuf_len / sizeof(float);

        if (constant_type == CONST_TYPE_INT)
        {
            int val = *((int *)param_constant);
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] + (double)val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val - (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] - (double)val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] * (double)val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val / (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] / (double)val;
                }
            }
            break;
            }
        }
        else if (constant_type == CONST_TYPE_FLOAT)
        {
            float val = *((float *)param_constant);
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            float *Bbuf = (float *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] + val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val - Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] - val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] * val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val / Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] / val;
                }
            }
            break;
            }
        }
        else if (constant_type == CONST_TYPE_DOUBLE)
        {
            double val = *((double *)param_constant);
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] + val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val - (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] - val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = (double)Abuf[i] * val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val / (double)Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)Abuf[i] / val;
                }
            }
            break;
            }
        }
    }
    else if (chunk_type == ATTR_TYPE_DOUBLE)
    { // DOUBLE
        uint64_t ncells = A->pagebuf_len / sizeof(double);

        if (constant_type == CONST_TYPE_INT)
        {
            int val = *((int *)param_constant);
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] + (double)val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val - Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] - (double)val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] * ((double)val);
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val / Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] / (double)val;
                }
            }
            break;
            }
        }
        else if (constant_type == CONST_TYPE_FLOAT)
        {
            float val = *((float *)param_constant);
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] + (double)val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val - Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] - (double)val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] * ((double)val);
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = (double)val / Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] / (double)val;
                }
            }
            break;
            }
        }
        else if (constant_type == CONST_TYPE_DOUBLE)
        {
            double val = *((double *)param_constant);
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            double *Bbuf = (double *)bf_util_get_pagebuf(B);

            switch (op)
            {
            case CHUNK_OP_ADD:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] + val;
                }
                break;
            case CHUNK_OP_SUB:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val - Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] - val;
                }
            }
            break;

            case CHUNK_OP_MUL:
                for (uint64_t i = 0; i < ncells; i++)
                {
                    Bbuf[i] = Abuf[i] * val;
                }
                break;
            case CHUNK_OP_DIV:
            {
                if (constant_side == 0) // LHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = val / Abuf[i];
                }
                else if (constant_side == 1) // RHS
                {
                    for (uint64_t i = 0; i < ncells; i++)
                        Bbuf[i] = Abuf[i] / val;
                }
            }
            break;
            }
        }
    }
}


typedef struct ChunkMapOpFloat64Float64Args {
    double *a;
    double *b;
    uint64_t ncells;
    void (*lambda)(void *, void *, uint64_t);
} ChunkMapOpFloat64Float64Args;

void* chunk_map_op_float64_float64(void *raw) {
    ChunkMapOpFloat64Float64Args *args = (ChunkMapOpFloat64Float64Args*) raw;
    double *a = args->a;
    double *b = args->b;
    uint64_t ncells = args->ncells;
    void (*lambda)(void *, void *, uint64_t) = args->lambda;
    lambda((double *) a, b, ncells);
}

void _lam_dense_map(
        PFpage *A,
        PFpage *B,
        int opnd_attr_type,
        Array *output_array) {

    int num_threads = (getenv("__PREVISION_NUM_THREADS") == NULL) ? 1 : atoi(getenv("__PREVISION_NUM_THREADS"));
    int result_attr_type = output_array->op_param.lambda.return_type;
    void (*lambda)(void *, void *, uint64_t) = output_array->op_param.lambda.lambda_func;

    uint64_t ncells = 1;
    for (int d = 0; d < output_array->desc.dim_len; d++)
        ncells *= output_array->desc.tile_extents[d];

    if (result_attr_type == TILESTORE_INT32)
    {
        int *Bbuf = (int *)bf_util_get_pagebuf(B);

        if (opnd_attr_type == TILESTORE_INT32)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            lambda((int *)Abuf, (int *)Bbuf, ncells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT32)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            lambda((float *)Abuf, (int *)Bbuf, ncells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT64)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            lambda((double *)Abuf, (int *)Bbuf, ncells);
        }
    }
    if (result_attr_type == TILESTORE_FLOAT32)
    {
        float *Bbuf = (float *)bf_util_get_pagebuf(B);

        if (opnd_attr_type == TILESTORE_INT32)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            lambda((int *)Abuf, (float *)Bbuf, ncells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT32)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            lambda((float *)Abuf, (float *)Bbuf, ncells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT64)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            lambda((double *)Abuf, (float *)Bbuf, ncells);
        }
    }
    if (result_attr_type == TILESTORE_FLOAT64)
    {   

        double *Bbuf = (double *)bf_util_get_pagebuf(B);

        if (opnd_attr_type == TILESTORE_INT32)
        {
            int *Abuf = (int *)bf_util_get_pagebuf(A);
            lambda((int *)Abuf, Bbuf, ncells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT32)
        {
            float *Abuf = (float *)bf_util_get_pagebuf(A);
            lambda((float *)Abuf, Bbuf, ncells);
        }
        if (opnd_attr_type == TILESTORE_FLOAT64)
        {
            double *Abuf = (double *)bf_util_get_pagebuf(A);
            if (num_threads > 1) {
                pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
                ChunkMapOpFloat64Float64Args *args = malloc(sizeof(ChunkMapOpFloat64Float64Args) * num_threads);
                uint64_t cell_per_thread = ncells / num_threads;
                for (int i = 0; i < num_threads; i++) {
                    args[i].a = Abuf + (cell_per_thread * i);
                    args[i].b = Bbuf + (cell_per_thread * i);
                    args[i].lambda = lambda;
                    args[i].ncells = (i + 1 == num_threads) ? ncells - (cell_per_thread * (num_threads - 1)) : cell_per_thread;
                    
                    pthread_create(&threads[i], NULL, chunk_map_op_float64_float64, (void *) &args[i]);
                }

                for (int i = 0; i < num_threads; i++) {
                    pthread_join(threads[i], NULL);
                }

                free(args);
                free(threads);
            } else {
                lambda((double *)Abuf, Bbuf, ncells);
            }
        }
    }
}

void elemwise_dense_add(PFpage *lhs,
                        PFpage *rhs,
                        PFpage *result,
                        int lhs_attr_type,
                        int rhs_attr_type)
{
    // If optimizable (Add using C for-loop)
    if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_INT, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_FLOAT, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_DOUBLE, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_INT, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_FLOAT, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_DOUBLE, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_FLOAT, CHUNK_OP_ADD);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_DOUBLE, CHUNK_OP_ADD);
}

void elemwise_dense_sub(PFpage *lhs,
                        PFpage *rhs,
                        PFpage *result,
                        int lhs_attr_type,
                        int rhs_attr_type)
{
    // If optimizable (Add using C for-loop)
    if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_INT, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_FLOAT, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_DOUBLE, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_INT, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_FLOAT, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_DOUBLE, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_FLOAT, CHUNK_OP_SUB);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_DOUBLE, CHUNK_OP_SUB);
}

void elemwise_dense_product(PFpage *lhs,
                            PFpage *rhs,
                            PFpage *result,
                            int lhs_attr_type,
                            int rhs_attr_type)
{
    if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_INT, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_FLOAT, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_DOUBLE, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_INT, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_FLOAT, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_DOUBLE, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_FLOAT, CHUNK_OP_MUL);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_DOUBLE, CHUNK_OP_MUL);
}

void elemwise_dense_div(PFpage *lhs,
                        PFpage *rhs,
                        PFpage *result,
                        int lhs_attr_type,
                        int rhs_attr_type)
{
    if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_INT, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_FLOAT, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_INT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_INT, ATTR_TYPE_DOUBLE, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_INT, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_FLOAT, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_FLOAT32 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_FLOAT, ATTR_TYPE_DOUBLE, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_INT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_INT, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT32)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_FLOAT, CHUNK_OP_DIV);
    else if (lhs_attr_type == TILESTORE_FLOAT64 && rhs_attr_type == TILESTORE_FLOAT64)
        chunk_arithmetic_op(lhs, rhs, result, ATTR_TYPE_DOUBLE, ATTR_TYPE_DOUBLE, CHUNK_OP_DIV);
}

int type_conv(tilestore_datatype_t in) {
    switch (in) {
        case TILESTORE_INT32: return ATTR_TYPE_INT;
        case TILESTORE_FLOAT32: return ATTR_TYPE_FLOAT;
        case TILESTORE_FLOAT64: return ATTR_TYPE_DOUBLE;
    }
    return -1;
}

void _lam_dense_elemwise(
        PFpage *lhs, 
        PFpage *rhs, 
        PFpage *result, 
        int lhs_attr_type, 
        int rhs_attr_type, 
        int op_type) {
            
    chunk_arithmetic_op(lhs, rhs, result, type_conv(lhs_attr_type), type_conv(rhs_attr_type), op_type);
}

void _lam_dense_elemwise_with_constant(
        PFpage *opnd,
        PFpage *result,
        int opnd_attr_type,
        int constant_type,
        void *constant,
        bool constant_equation_side,
        int operation_type) {

    chunk_arithmetic_op_constant(
        opnd, constant, result, type_conv(opnd_attr_type), type_conv(constant_type), constant_equation_side, operation_type);
}

void _lam_dense_exp(
        PFpage *A,
        PFpage *B,
        int opnd_attr_type,
        Array *output_array) {
            
    // Assuming optimized
    if (opnd_attr_type == TILESTORE_INT32)
    {
        uint64_t ncells = B->pagebuf_len / sizeof(int);
        int *Abuf = (int *)bf_util_get_pagebuf(A);
        double *Bbuf = (double *)bf_util_get_pagebuf(B);

        for (uint64_t i = 0; i < ncells; i++)
        {
            Bbuf[i] = exp((double)Abuf[i]);
        }
    }
    if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        uint64_t ncells = B->pagebuf_len / sizeof(float);
        float *Abuf = (float *)bf_util_get_pagebuf(A);
        double *Bbuf = (double *)bf_util_get_pagebuf(B);

        for (uint64_t i = 0; i < ncells; i++)
        {
            Bbuf[i] = exp((double)Abuf[i]);
        }
    }
    if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        uint64_t ncells = B->pagebuf_len / sizeof(double);
        double *Abuf = (double *)bf_util_get_pagebuf(A);
        double *Bbuf = (double *)bf_util_get_pagebuf(B);

        for (uint64_t i = 0; i < ncells; i++)
        {
            Bbuf[i] = exp(Abuf[i]);
        }
    }
}