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
#include "lam_internals.h"

#include <sys/time.h>

// GetBuf() is in aggr() so that we need to subtract bftime from the wall clock time.
extern unsigned long long bftime;

unsigned long long __lam_aggr_cnt = 0;
unsigned long long __lam_aggr_time = 0;

#define __LAM_STAT_START()                  \
    struct timeval start;                   \
    unsigned long long tempbftime = bftime; \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_aggr_time += (diff - (bftime - tempbftime));                                                 \
    __lam_aggr_cnt++;

void lam_chunk_group_by_dim_aggregate(Chunk *opnd,
                                      Chunk *result,
                                      int opnd_attr_type,
                                      void **state,
                                      AGGREGATE_FUNC_TYPE func,
                                      uint8_t *dim_selection_map,
                                      Descriptor array_desc,
                                      uint64_t *chunk_size,
                                      uint64_t iteration)
{
    /* Check for the optimizability */
    GROUP_BY_OPTIMIZABILITY optimizability = GROUP_BY_OPTIMIZABLE;
    uint32_t selected = 0;
    for (uint32_t d = 0; d < opnd->dim_len; d++)
    {
        if (opnd->chunk_domains[d][1] - opnd->chunk_domains[d][0] + 1 !=
            opnd->tile_extents[d])
        {
            optimizability = GROUP_BY_NOT_OPTIMIZABLE;
            break;
        }

        if (dim_selection_map[d] == 1)
        {
            if (opnd->tile_extents[d] != result->tile_extents[selected])
            {
                optimizability = GROUP_BY_NOT_OPTIMIZABLE;
                break;
            }

            if (result->chunk_domains[selected][1] - result->chunk_domains[selected][0] + 1 !=
                result->tile_extents[selected])
            {
                optimizability = GROUP_BY_NOT_OPTIMIZABLE;
                break;
            }
            selected++;
        }
    }

    __LAM_STAT_START();

    /* Do the aggregation */
    /* Optimized */
    if (optimizability == GROUP_BY_OPTIMIZABLE)
    {
        /* Branch according to the array type */
        /* For now,
            - if Opnd = Dense -> then Result = Dense
            - if Opnd = Sparse -> then Result = Sparse */
        if (opnd->array_type == TILESTORE_DENSE &&
            result->array_type == TILESTORE_DENSE)
        {
            switch (func)
            {
            case AGGREGATE_COUNT:
                group_by_dim_dense_count_aggregate_opt(opnd,
                                                       result,
                                                       state,
                                                       opnd_attr_type,
                                                       dim_selection_map,
                                                       array_desc,
                                                       chunk_size,
                                                       iteration);
                break;
            case AGGREGATE_SUM:
                group_by_dim_dense_sum_aggregate_opt(opnd,
                                                     result,
                                                     state,
                                                     opnd_attr_type,
                                                     dim_selection_map,
                                                     array_desc,
                                                     chunk_size,
                                                     iteration);
                break;
            case AGGREGATE_AVG:
                group_by_dim_dense_avg_aggregate_opt(opnd,
                                                     result,
                                                     state,
                                                     opnd_attr_type,
                                                     dim_selection_map,
                                                     array_desc,
                                                     chunk_size,
                                                     iteration);
                break;
            case AGGREGATE_MAX:
                group_by_dim_dense_max_aggregate_opt(opnd,
                                                     result,
                                                     state,
                                                     opnd_attr_type,
                                                     dim_selection_map,
                                                     array_desc,
                                                     chunk_size,
                                                     iteration);
                break;
            case AGGREGATE_MIN:
                group_by_dim_dense_min_aggregate_opt(opnd,
                                                     result,
                                                     state,
                                                     opnd_attr_type,
                                                     dim_selection_map,
                                                     array_desc,
                                                     chunk_size,
                                                     iteration);
                break;
            case AGGREGATE_VAR:
                group_by_dim_dense_var_aggregate_opt(opnd,
                                                     result,
                                                     state,
                                                     opnd_attr_type,
                                                     dim_selection_map,
                                                     array_desc,
                                                     chunk_size,
                                                     iteration);
                break;
            case AGGREGATE_STDEV:
                group_by_dim_dense_stdev_aggregate_opt(opnd,
                                                       result,
                                                       state,
                                                       opnd_attr_type,
                                                       dim_selection_map,
                                                       array_desc,
                                                       chunk_size,
                                                       iteration);
                break;

            default:
                break;
            }
        }

        else if (opnd->array_type == TILESTORE_SPARSE_CSR &&
                 result->array_type == TILESTORE_SPARSE_CSR)
        {
        }
    }

    /* Not optimized (iter) */
    else if (optimizability == GROUP_BY_NOT_OPTIMIZABLE)
    {
        switch (func)
        {
        case AGGREGATE_COUNT:
            group_by_dim_dense_count_aggregate_iter(opnd,
                                                    result,
                                                    state,
                                                    opnd_attr_type,
                                                    dim_selection_map,
                                                    array_desc,
                                                    chunk_size,
                                                    iteration);
            break;
        case AGGREGATE_SUM:
            group_by_dim_dense_sum_aggregate_iter(opnd,
                                                  result,
                                                  state,
                                                  opnd_attr_type,
                                                  dim_selection_map,
                                                  array_desc,
                                                  chunk_size,
                                                  iteration);
            break;
        case AGGREGATE_AVG:
            group_by_dim_dense_avg_aggregate_iter(opnd,
                                                  result,
                                                  state,
                                                  opnd_attr_type,
                                                  dim_selection_map,
                                                  array_desc,
                                                  chunk_size,
                                                  iteration);
            break;
        case AGGREGATE_MAX:
            group_by_dim_dense_max_aggregate_iter(opnd,
                                                  result,
                                                  state,
                                                  opnd_attr_type,
                                                  dim_selection_map,
                                                  array_desc,
                                                  chunk_size,
                                                  iteration);
            break;
        case AGGREGATE_MIN:
            group_by_dim_dense_min_aggregate_iter(opnd,
                                                  result,
                                                  state,
                                                  opnd_attr_type,
                                                  dim_selection_map,
                                                  array_desc,
                                                  chunk_size,
                                                  iteration);
            break;
        case AGGREGATE_VAR:
            group_by_dim_dense_var_aggregate_iter(opnd,
                                                  result,
                                                  state,
                                                  opnd_attr_type,
                                                  dim_selection_map,
                                                  array_desc,
                                                  chunk_size,
                                                  iteration);
            break;
        case AGGREGATE_STDEV:
            group_by_dim_dense_stdev_aggregate_iter(opnd,
                                                    result,
                                                    state,
                                                    opnd_attr_type,
                                                    dim_selection_map,
                                                    array_desc,
                                                    chunk_size,
                                                    iteration);
            break;
        }
    }

    __LAM_STAT_END();
}

Chunk *lam_chunk_group_by_dim_write(Chunk *result,
                                    void **state,
                                    int opnd_attr_type,
                                    AGGREGATE_FUNC_TYPE func)
{
    /* Check for the optimizability */
    GROUP_BY_OPTIMIZABILITY optimizability = GROUP_BY_OPTIMIZABLE;
    for (uint32_t d = 0; d < result->dim_len; d++)
    {
        if (result->chunk_domains[d][1] - result->chunk_domains[d][0] + 1 !=
            result->tile_extents[d])
        {
            optimizability = GROUP_BY_NOT_OPTIMIZABLE;
            break;
        }
    }

    /* Lower the aggregation and write its result to the destination chunk */
    if (optimizability == GROUP_BY_OPTIMIZABLE)
    {
        switch (func)
        {
        case AGGREGATE_COUNT:
            group_by_dim_dense_count_write_opt(result, state, opnd_attr_type);
            break;
        case AGGREGATE_SUM:
            group_by_dim_dense_sum_write_opt(result, state, opnd_attr_type);
            break;
        case AGGREGATE_AVG:
            group_by_dim_dense_avg_write_opt(result, state, opnd_attr_type);
            break;
        case AGGREGATE_MAX:
            group_by_dim_dense_max_write_opt(result, state, opnd_attr_type);
            break;
        case AGGREGATE_MIN:
            group_by_dim_dense_min_write_opt(result, state, opnd_attr_type);
            break;
        case AGGREGATE_VAR:
            group_by_dim_dense_var_write_opt(result, state, opnd_attr_type);
            break;
        case AGGREGATE_STDEV:
            group_by_dim_dense_stdev_write_opt(result, state, opnd_attr_type);
            break;

        default:
            break;
        }
    }

    else if (optimizability == GROUP_BY_NOT_OPTIMIZABLE)
    {
        switch (func)
        {
        case AGGREGATE_COUNT:
            group_by_dim_dense_count_write_iter(result, state, opnd_attr_type);
            break;
        case AGGREGATE_SUM:
            group_by_dim_dense_sum_write_iter(result, state, opnd_attr_type);
            break;
        case AGGREGATE_AVG:
            group_by_dim_dense_avg_write_iter(result, state, opnd_attr_type);
            break;
        case AGGREGATE_MAX:
            group_by_dim_dense_max_write_iter(result, state, opnd_attr_type);
            break;
        case AGGREGATE_MIN:
            group_by_dim_dense_min_write_iter(result, state, opnd_attr_type);
            break;
        case AGGREGATE_VAR:
            group_by_dim_dense_var_write_iter(result, state, opnd_attr_type);
            break;
        case AGGREGATE_STDEV:
            group_by_dim_dense_stdev_write_iter(result, state, opnd_attr_type);
            break;
        }
    }

    return result;
}

void lam_reduce_as_scala_aggregate(Chunk *opnd,
                                   int opnd_attr_type,
                                   void **state,
                                   AGGREGATE_FUNC_TYPE func,
                                   uint64_t iteration)
{
    /* Check for the optimizability */
    GROUP_BY_OPTIMIZABILITY optimizability = GROUP_BY_OPTIMIZABLE;
    uint32_t selected = 0;
    for (uint32_t d = 0; d < opnd->dim_len; d++)
    {
        if (opnd->chunk_domains[d][1] - opnd->chunk_domains[d][0] + 1 !=
            opnd->tile_extents[d])
        {
            optimizability = GROUP_BY_NOT_OPTIMIZABLE;
            break;
        }
    }

    /* Do the aggregation */
    /* Optimized */
    if (optimizability == GROUP_BY_OPTIMIZABLE)
    {
        chunk_getbuf(opnd, BF_EMPTYTILE_NONE);

        if (opnd->curpage == NULL) {
            // TODO: Not implemented yet
            assert(false);
        } else {
                /* Branch according to the array type */
            if (opnd->curpage->type == DENSE_FIXED)
            {
                // TODO: Not implemented yet
                assert(func == AGGREGATE_SUM || func == AGGREGATE_NORM);
                switch (func)
                {
                case AGGREGATE_COUNT:
                    break;
                case AGGREGATE_SUM:
                    reduce_as_scala_dense_sum_aggregate_opt(opnd->curpage,
                                                            state,
                                                            opnd_attr_type,
                                                            iteration);
                    break;
                case AGGREGATE_AVG:
                    break;
                case AGGREGATE_MAX:
                    break;
                case AGGREGATE_MIN:
                    break;
                case AGGREGATE_VAR:
                    break;
                case AGGREGATE_STDEV:
                    break;
                case AGGREGATE_NORM:
                    reduce_as_scala_dense_norm_aggregate_opt(opnd->curpage,
                                                            state,
                                                            opnd_attr_type,
                                                            iteration);
                    break;

                default:
                    break;
                }
            }

            else if (opnd->curpage->type == SPARSE_FIXED)
            {
                // TODO: Not implemented yet
                assert(func == AGGREGATE_SUM || func == AGGREGATE_NORM);
                switch (func)
                {
                case AGGREGATE_COUNT:
                    break;
                case AGGREGATE_SUM:
                    reduce_as_scala_sparse_sum_aggregate_opt(
                        opnd->curpage, state, opnd_attr_type, iteration);
                    break;
                case AGGREGATE_AVG:
                    break;
                case AGGREGATE_MAX:
                    break;
                case AGGREGATE_MIN:
                    break;
                case AGGREGATE_VAR:
                    break;
                case AGGREGATE_STDEV:
                    break;
                case AGGREGATE_NORM:
                    reduce_as_scala_sparse_norm_aggregate_opt(opnd->curpage,
                                                            state,
                                                            opnd_attr_type,
                                                            iteration);
                    break;

                default:
                    break;
                }
            }
        }
    }

    
    /* Not optimized (iter) */
    else if (optimizability == GROUP_BY_NOT_OPTIMIZABLE)
    {
        assert(false);          // TODO: Not implemented yet
        // switch (func)
        // {
        // case AGGREGATE_COUNT:
        //     break;
        // case AGGREGATE_SUM:
        //     break;
        // case AGGREGATE_AVG:
        //     break;
        // case AGGREGATE_MAX:
        //     break;
        // case AGGREGATE_MIN:
        //     break;
        // case AGGREGATE_VAR:
        //     break;
        // case AGGREGATE_STDEV:
        //     break;
        // }
    }
}

double lam_reduce_as_scala_finalize(void **state,
                                    AGGREGATE_FUNC_TYPE func)
{
    double result;

    /* Finalize the aggregation and return its result as 'double' type */
    switch (func)
    {
    case AGGREGATE_COUNT:
        break;
    case AGGREGATE_SUM:
        result = reduce_as_scala_sum_finalize(state);
        break;
    case AGGREGATE_AVG:
        break;
    case AGGREGATE_MAX:
        break;
    case AGGREGATE_MIN:
        break;
    case AGGREGATE_VAR:
        break;
    case AGGREGATE_STDEV:
        break;
    case AGGREGATE_NORM:
        result = reduce_as_scala_norm_finalize(state);
        break;

    default:
        break;
    }

    return result;
}