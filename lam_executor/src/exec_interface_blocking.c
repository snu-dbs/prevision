#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <lapacke.h>

#include <sys/time.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "exec_interface.h"
#include "node_interface.h"
#include "planner.h"
#include "simulate.h"


unsigned long long ex_min_fl_creation_time;

Array *execute(Array *root)
{
    BF_QueryStart();

    // planning
    uint8_t order = gen_plan(root);

    // for write prevention
    bool write_prevention_on = true;
    if (write_prevention_on) {
        hint_to_bf(root, ++order);      // all array's visited_for_consumer_cnt become false.
        
        // stats
        struct timeval fl_start;
        gettimeofday(&fl_start, NULL);

        fill_future(root);
        
        // stats
        struct timeval fl_end;
        gettimeofday(&fl_end, NULL);

        unsigned long long fl_diff = ((fl_end.tv_sec - fl_start.tv_sec) * 1000000) + (fl_end.tv_usec - fl_start.tv_usec); 
        ex_min_fl_creation_time += fl_diff;
    } else {
        fprintf(stderr, "[EXECUTOR] write prevention off\n");
    }
    
    Descriptor result_desc = root->desc;
    int ndim = (int)result_desc.dim_len;
    uint64_t result_chunksize[ndim];

    /* 1. Determine the result chunk size. */
    for (int d = 0; d < ndim; d++)
    {
        // no DEFAULT_TILE_EXTENT needed.
        result_chunksize[d] = result_desc.tile_extents[d];
    }

    /* 2. Compute EOCHUNK. */
    uint64_t EOCHUNK = 1;
    for (int d = 0; d < ndim; d++)
        EOCHUNK *= ceil((result_desc.dim_domains[d][1] - result_desc.dim_domains[d][0] + 1) / result_chunksize[d]);

    /* 3. Compute the result. */
    if (root->scalar_flag)
    {
        assert(ndim == 0);
        double result_value = get_scalar(root, NULL); // get scalar return value
    }
    else
    {
        assert(ndim > 0);
        for (uint64_t pos = 0; pos < EOCHUNK; pos++) // begin pipelining
        {
            Chunk *result_chunk = get_pos(root, pos, result_chunksize);

            if (result_chunk != NULL)
            {
                chunk_destroy(result_chunk);
            }
        }
    }

    BF_QueryEnd();

    root->executed = true;
    return root;
}

Chunk *get_pos(Array *res_array, uint64_t res_pos, uint64_t *res_chunksize)
{
    /* TODO: Validate the array object. */

    /* Prepare necessary variables. */
    uint64_t *chunksize_parent = res_chunksize;
    uint64_t **chunksize_child = NULL;
    Chunk **chunk_list = NULL;
    Chunk *return_chunk = NULL;

    uint64_t RES_EOCHUNK = get_num_of_chunks(res_array);

    /***************************************
     * Get target chunk(s) recursively.
     * Materialize the array if necessary.
     ***************************************/
    if (res_array->executed || res_array->computed_list[res_pos] == true)
        return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);
    else
    {
        Descriptor res_desc = res_array->desc;

        if (res_array->array_num == 0)
        {
            assert(res_array->arraylist == NULL);
            return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);
        }

        if (res_array->array_num == 1)
        {
            chunksize_child = malloc(sizeof(uint64_t *) * 1);

            switch (res_array->op_type)
            {
            case OP_SUBARRAY:
            case OP_SUBARRAYSCAN:
            {
                chunk_list = NULL;
                chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
                determine_child_chunksize(chunksize_parent, res_array, OP_SUBARRAYSCAN, chunksize_child);
                return_chunk = lam_operation(chunk_list, res_array->arraylist, res_array, res_pos, chunksize_child[0]);

                break;
            }

            case OP_ADD_CONSTANT:
            case OP_SUB_CONSTANT:
            case OP_PRODUCT_CONSTANT:
            case OP_DIV_CONSTANT:
            case OP_EXPONENTIAL:
            {
                chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
                chunk_list = malloc(sizeof(Chunk *));

                determine_child_chunksize(chunksize_parent, res_array, res_array->op_type, chunksize_child);

                for (uint64_t p = 0; p < RES_EOCHUNK; p++) {
                    chunk_list[0] = get_pos(res_array->arraylist[0], p, chunksize_child[0]);
                    return_chunk = lam_operation(chunk_list, res_array->arraylist, res_array, p, chunksize_parent);
                    chunk_destroy(chunk_list[0]);
                    chunk_destroy(return_chunk);
                }

                mark_all_computed(res_array);
                res_array->blocking_flag = true;

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                free(chunk_list);

                break;
            }

            case OP_TRANSPOSE:
            {
                // variables
                uint32_t dim_len = res_desc.dim_len;
                uint64_t *parent_chunk_lens = NULL;
                uint64_t *child_chunk_lens = NULL;
                uint64_t *parent_coords = NULL;
                uint64_t *child_coords = NULL;

                // malloc
                chunksize_child[0] = malloc(sizeof(uint64_t) * dim_len);
                chunk_list = malloc(sizeof(Chunk *));
                parent_chunk_lens = malloc(sizeof(uint64_t) * dim_len);
                child_chunk_lens = malloc(sizeof(uint64_t) * dim_len);
                parent_coords = malloc(sizeof(uint64_t) * dim_len);
                child_coords = malloc(sizeof(uint64_t) * dim_len);

                for (uint64_t p = 0; p < RES_EOCHUNK; p++) {
                    // calculate parent_chunk_lens
                    for (uint32_t d = 0; d < dim_len; d++)
                    {
                        uint64_t dim_side_len = res_desc.dim_domains[d][1] - res_desc.dim_domains[d][0] + 1;
                        parent_chunk_lens[d] = ceil(((double)dim_side_len / chunksize_parent[d]));
                    }

                    // calculate current chunk ND coordinates.
                    bf_util_calculate_nd_from_1d_row_major(p, parent_chunk_lens, dim_len, &parent_coords);

                    // calculate child chunk size and coords from parents' ones
                    uint32_t *dim_order = res_array->op_param.trans.transpose_func;
                    for (uint32_t d = 0; d < dim_len; d++)
                    {
                        // calculate child_chunk_lens
                        child_chunk_lens[d] = parent_chunk_lens[dim_order[d]];

                        // calculate child chunk size
                        chunksize_child[0][d] = chunksize_parent[dim_order[d]];

                        // calculate the original chunk's ND coordinates.
                        child_coords[d] = parent_coords[dim_order[d]];
                    }

                    // calculate the original chunk 1D coordinate.
                    uint64_t child_pos;
                    bf_util_calculate_1d_from_nd_row_major(child_coords, child_chunk_lens, dim_len, &child_pos);

                    // get child chunk and transpose it.
                    chunk_list[0] = get_pos(res_array->arraylist[0], child_pos, chunksize_child[0]);

                    return_chunk = lam_operation(chunk_list, res_array->arraylist, res_array, p, chunksize_parent);
                    chunk_destroy(chunk_list[0]);
                    chunk_destroy(return_chunk);
                }

                mark_all_computed(res_array);
                res_array->blocking_flag = true;

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                // free
                free(child_coords);
                free(parent_coords);
                free(child_chunk_lens);
                free(parent_chunk_lens);
                free(chunk_list);
                break;
            }

            case OP_AGGR:
            {
                // variables
                Array *aggr_target_array = res_array->arraylist[0];
                uint32_t aggr_ndim = aggr_target_array->desc.dim_len;
                GroupBy aggr_info = res_array->op_param.groupBy;

                /* Determine chunksize for target array (bigger array) */
                chunksize_child[0] = malloc(sizeof(uint64_t) * aggr_ndim);
                for (int d = 0; d < aggr_ndim; d++)
                    chunksize_child[0][d] = 0;
                determine_child_chunksize(chunksize_parent, res_array, OP_AGGR, chunksize_child);

                for (uint64_t p = 0; p < RES_EOCHUNK; p++) {
                    /* Pre-calculate dimension_selection_map for re-use. */
                    uint8_t *dim_selection_map;
                    bf_util_get_dim_selection_map(res_desc.object_name,
                                                aggr_info.groupby,
                                                aggr_ndim,
                                                aggr_info.agg_groupbylen,
                                                &dim_selection_map);

                    uint64_t partition_loop = 1;
                    for (int i = 0; i < aggr_ndim; i++)
                    {
                        if (!dim_selection_map[i])
                            partition_loop *= ceil((int)(aggr_target_array->desc.dim_domains[i][1] - aggr_target_array->desc.dim_domains[i][0] + 1) / (int)chunksize_child[0][i]);
                    }
                    return_chunk = lam_operation_aggr(res_array, p, chunksize_parent, chunksize_child[0], dim_selection_map, partition_loop);

                    storage_util_free_selected(&dim_selection_map);
                }

                mark_all_computed(res_array);
                res_array->blocking_flag = true;

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);
                break;
            }

            case OP_WINDOW:
            {
                chunk_list = NULL;
                chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
                determine_child_chunksize(chunksize_parent, res_array, OP_WINDOW, chunksize_child);

                uint64_t EOCHUNK = 1;
                for (int d = 0; d < res_desc.dim_len; d++)
                {
                    EOCHUNK *= ceil((res_desc.dim_domains[d][1] - res_desc.dim_domains[d][0] + 1) / chunksize_parent[d]);
                }

                for (int materialize_pos = 0; materialize_pos < EOCHUNK; materialize_pos++)
                {
                    Chunk *materialize_res_chunk = lam_operation(chunk_list, res_array->arraylist, res_array, materialize_pos, chunksize_parent);
                    chunk_destroy(materialize_res_chunk);
                }

                assert(res_pos == 0);
                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                res_array->blocking_flag = true;
                mark_all_computed(res_array);
                break;
            }

            case OP_MAP:
            {
                chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
                determine_child_chunksize(chunksize_parent, res_array, OP_MAP, chunksize_child);
                chunk_list = malloc(sizeof(Chunk *));

                for (uint64_t p = 0; p < RES_EOCHUNK; p++) {
                    chunk_list[0] = get_pos(res_array->arraylist[0], p, chunksize_child[0]);
                    return_chunk = lam_operation(chunk_list, res_array->arraylist, res_array, p, chunksize_parent);

                    chunk_destroy(chunk_list[0]);
                    chunk_destroy(return_chunk);
                }

                free(chunk_list);

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                mark_all_computed(res_array);
                res_array->blocking_flag = true;
                break;
            }
            
            case OP_RETILE:
            {
                // blocking operation; calculate all tiles
                // only getpos() for the first position allowed
                assert(res_pos == 0);

                // the chunksize of a child is same with the tilesize of the child
                Descriptor in_desc = res_array->arraylist[0]->desc;
                chunksize_child[0] = in_desc.tile_extents;
                
                // pre-compute all chunks of input array
                uint64_t C_EOCHUNK = 1;
                for (int d = 0; d < in_desc.dim_len; d++)
                {
                    C_EOCHUNK *= ceil((in_desc.dim_domains[d][1] - in_desc.dim_domains[d][0] + 1) / chunksize_child[0][d]);
                }
                for (uint64_t pos = 0; pos < C_EOCHUNK; pos++)
                {   
                    Chunk *res_chunk = get_pos(res_array->arraylist[0], pos, chunksize_child[0]);
                    chunk_destroy(res_chunk);
                }

                // calculate EOCHUNK
                uint64_t EOCHUNK = 1;
                for (int d = 0; d < res_desc.dim_len; d++)
                {
                    EOCHUNK *= ceil((res_desc.dim_domains[d][1] - res_desc.dim_domains[d][0] + 1) / chunksize_parent[d]);
                } 

                // invoke lam function for all chunks
                for (uint64_t pos = 0; pos < EOCHUNK; pos++)
                {   
                    Chunk *res_chunk = lam_operation_retile(
                        res_array->arraylist[0], res_array, pos, 
                        chunksize_child[0], chunksize_parent);
                    chunk_destroy(res_chunk);
                }

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                res_array->blocking_flag = true;
                mark_all_computed(res_array);

                break;
            }
            case OP_SVD:
            {
                chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
                chunksize_child[0][0] = res_array->arraylist[0]->desc.tile_extents[0];
                chunksize_child[0][1] = chunksize_parent[1];

                uint64_t EOCHUNK =
                    ceil((res_array->arraylist[0]->desc.dim_domains[0][1] -
                          res_array->arraylist[0]->desc.dim_domains[0][0] + 1) /
                         res_array->arraylist[0]->desc.tile_extents[0]);

                for (uint64_t opnd_pos = 0; opnd_pos < EOCHUNK; opnd_pos++)
                {
                    Chunk *opnd_chunk = get_pos(res_array->arraylist[0], opnd_pos, chunksize_child[0]);
                    Chunk *temp_res_u_chunk = scan_without_range(res_array->op_param.svd.svd_u, opnd_pos,
                                                                 chunksize_child[0]);

                    // Step 1
                }

                // Step 2

                // Step 3

                for (uint64_t opnd_pos = 0; opnd_pos < EOCHUNK; opnd_pos++)
                {
                    // Step 4
                }

                // Return
                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                res_array->blocking_flag = true;
                mark_all_computed(res_array);

                break;
            }

            case OP_INV:
            {
                chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
                chunksize_child[0][0] = chunksize_parent[0];
                chunksize_child[0][1] = chunksize_parent[1];

                // Tiled LU-Decomposition
                uint64_t num_chunk_row = (res_desc.dim_domains[0][1] - res_desc.dim_domains[0][0] + 1) / chunksize_parent[0];
                uint64_t num_chunk_col = (res_desc.dim_domains[1][1] - res_desc.dim_domains[1][0] + 1) / chunksize_parent[1];
                uint64_t EOCHUNK = num_chunk_row > num_chunk_col ? num_chunk_col : num_chunk_row;
                int *permutation = malloc(chunksize_parent[0] * sizeof(int)); // chunksize_parent[0], [1] => Doesn't matter. We just need the minimum of two.
                int info;

                uint64_t pos = 0;
                for (uint64_t k = 0; k < EOCHUNK; k++)
                {
                    Chunk *opnd_chunk = get_pos(res_array->arraylist[0], pos, chunksize_child[0]);
                    Chunk *decomp_chunk = scan_without_range(res_array, pos,
                                                             chunksize_child[0]);
                    chunk_getbuf(opnd_chunk, false);
                    chunk_getbuf(decomp_chunk, false);

                    // Step 1 : DGETRF
                    double *opnd_buf = (double *)BF_SHM_DATA_PTR(opnd_chunk->curpage->pagebuf_o);
                    double *decomp_buf = (double *)BF_SHM_DATA_PTR(decomp_chunk->curpage->pagebuf_o);
                    memcpy(decomp_buf, opnd_buf, chunksize_parent[0] * chunksize_parent[1] * sizeof(double)); // lapack_dgetrf overwrites the input.
                    LAPACK_dgetrf((int*) chunksize_parent[0], (int*) chunksize_parent[1], decomp_buf,
                                  (int*) chunksize_parent[0], permutation, &info);

                    // Step 2 : DGESSM

                    chunk_destroy(opnd_chunk);
                    chunk_destroy(decomp_chunk);
                }

                res_array->blocking_flag = true;
                mark_all_computed(res_array);
                break;
            }
            }

            free(chunksize_child[0]);
            free(chunksize_child);
        }

        if (res_array->array_num == 2)
        {
            chunk_list = malloc(sizeof(Chunk *) * 2);
            chunksize_child = malloc(sizeof(uint64_t *) * 2);
            chunksize_child[0] = malloc(sizeof(uint64_t) * res_desc.dim_len);
            chunksize_child[1] = malloc(sizeof(uint64_t) * res_desc.dim_len);

            switch (res_array->op_type)
            {
            case OP_ADD:
            case OP_SUB:
            case OP_PRODUCT:
            case OP_DIV:
            {
                determine_child_chunksize(chunksize_parent, res_array, res_array->op_type, chunksize_child);

                for (uint64_t p = 0; p < RES_EOCHUNK; p++) {
                    chunk_list[0] = get_pos(res_array->arraylist[0], p, chunksize_child[0]);
                    chunk_list[1] = get_pos(res_array->arraylist[1], p, chunksize_child[1]);
                    return_chunk = lam_operation(chunk_list, res_array->arraylist, res_array, p, chunksize_parent);

                    chunk_destroy(chunk_list[0]);
                    chunk_destroy(chunk_list[1]);
                    chunk_destroy(return_chunk);
                }

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                mark_all_computed(res_array);
                res_array->blocking_flag = true;
                
                break;
            }

            case OP_MATMUL:
            {
                determine_child_chunksize(chunksize_parent, res_array, res_array->op_type, chunksize_child);

                assert(res_pos == 0);

                uint64_t EOCHUNK = 1;
                for (int d = 0; d < res_desc.dim_len; d++)
                {
                    EOCHUNK *= ceil((res_desc.dim_domains[d][1] - res_desc.dim_domains[d][0] + 1) / chunksize_parent[d]);
                }

                // EOCHUNK = 1;
                for (int materialize_pos = 0; materialize_pos < EOCHUNK; materialize_pos++)
                {
                    // fprintf(stderr, "mat pos %ld ", materialize_pos);
                    Chunk *materialize_res_chunk = lam_operation_matmul(res_array->arraylist, res_array, materialize_pos, chunksize_parent, chunksize_child);

                    // fprintf(stderr, "done\n ");
                    chunk_destroy(materialize_res_chunk);
                }

                return_chunk = scan_without_range(res_array, res_pos, chunksize_parent);

                res_array->blocking_flag = true;
                mark_all_computed(res_array);

                break;
            }
            }

            if (chunk_list != NULL)
                free(chunk_list);
            free(chunksize_child[0]);
            free(chunksize_child[1]);
            free(chunksize_child);
        }

        res_array->computed_list[res_pos] = true;
    }

    // BF_shm_print_stats(_mspace_idata);
    return return_chunk;
}

double get_scalar(Array *res_array, uint64_t *res_chunksize)
{
    /* Validate the array object. */
    assert(res_array->scalar_flag == true);

    /* Prepare necessary variables. */
    uint64_t *chunksize_parent = res_chunksize;
    uint64_t **chunksize_child = NULL;
    double return_val;

    /************************************************
     * Compute the scalar result value and return it.
     ************************************************/
    if (res_array->executed)
        return_val = res_array->scalar_val_double;
    else
    {
        Descriptor res_desc = res_array->desc;

        switch (res_array->op_type)
        {
        case OP_AGGR:
        {
            assert(res_array->array_num == 1);

            // Prepare variables
            Array *aggr_target_array = res_array->arraylist[0];
            uint32_t aggr_ndim = aggr_target_array->desc.dim_len;
            GroupBy aggr_info = res_array->op_param.groupBy;

            // Determine chunksize for target array (bigger array)
            chunksize_child = malloc(sizeof(uint64_t *) * 1);
            chunksize_child[0] = malloc(sizeof(uint64_t) * aggr_ndim);
            for (int d = 0; d < aggr_ndim; d++)
                chunksize_child[0][d] = 0;
            determine_child_chunksize(chunksize_parent, res_array, OP_AGGR, chunksize_child);

            // Calculate the # of loops for aggregating partitions
            uint64_t partition_loop = 1;
            for (int i = 0; i < aggr_ndim; i++)
            {
                partition_loop *= ceil((int)(aggr_target_array->desc.dim_domains[i][1] - aggr_target_array->desc.dim_domains[i][0] + 1) / (int)chunksize_child[0][i]);
            }

            // Do the aggregation
            int aggr_func = res_array->op_param.groupBy.agg_func;
            for (int i = 0; i < partition_loop; i++)
            {
                Chunk *aggr_chunk = get_pos(aggr_target_array, i, chunksize_child[0]);

                lam_reduce_as_scala_aggregate(aggr_chunk,
                                              res_array->arraylist[0]->desc.attr_type,
                                              res_array->op_param.groupBy.state,
                                              aggr_func,
                                              i);
                chunk_destroy(aggr_chunk);
            }
            return_val = lam_reduce_as_scala_finalize(res_array->op_param.groupBy.state, aggr_func);
            res_array->scalar_val_double = return_val;
            res_array->blocking_flag = true;
            if (!res_array->scalar_flag)
            {
                mark_all_computed(res_array);
            }

            free(chunksize_child[0]);

            break;
        }
        }

        free(chunksize_child);
    }

    return return_val;
}

Chunk *lam_operation(Chunk **chunklist, Array **arraylist, Array *result_array, uint64_t pos, uint64_t *chunksize)
{
    Chunk *chunk_ptr;

    switch (result_array->op_type)
    {

    case OP_SUBARRAYSCAN:
    {
        int attrtype = arraylist[0]->desc.attr_type;
        Range scan_range = result_array->op_param.range;
        chunk_ptr = scan_with_range(arraylist[0], scan_range, attrtype, result_array->desc, pos, chunksize);
        break;
    }

    case OP_ADD:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = arraylist[1]->desc.attr_type;

        chunk_ptr = lam_chunk_add(chunklist[0], chunklist[1], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_ADD_CONSTANT:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = result_array->op_param.elemConst.constant_type;
        chunk_ptr = lam_chunk_add_constant(chunklist[0], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_SUB:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = arraylist[1]->desc.attr_type;
        chunk_ptr = lam_chunk_sub(chunklist[0], chunklist[1], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_SUB_CONSTANT:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = result_array->op_param.elemConst.constant_type;
        chunk_ptr = lam_chunk_sub_constant(chunklist[0], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_PRODUCT:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = arraylist[1]->desc.attr_type;
        chunk_ptr = lam_chunk_product(chunklist[0], chunklist[1], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_PRODUCT_CONSTANT:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = result_array->op_param.elemConst.constant_type;
        chunk_ptr = lam_chunk_product_constant(chunklist[0], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_DIV:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = arraylist[1]->desc.attr_type;
        chunk_ptr = lam_chunk_div(chunklist[0], chunklist[1], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_DIV_CONSTANT:
    {
        int attrtype[2];
        attrtype[0] = arraylist[0]->desc.attr_type;
        attrtype[1] = result_array->op_param.elemConst.constant_type;
        chunk_ptr = lam_chunk_div_constant(chunklist[0], attrtype[0], attrtype[1], result_array, pos, chunksize);
        break;
    }

    case OP_TRANSPOSE:
    {
        int attrtype = arraylist[0]->desc.attr_type;
        chunk_ptr = lam_chunk_transpose(chunklist[0], attrtype, result_array, pos, chunksize);
        break;
    }

    case OP_WINDOW:
    {
        int attrtype = arraylist[0]->desc.attr_type; // TODO: should it be double?
        Chunk *opnd_chunk = get_pos(arraylist[0], pos, chunksize);

        ParamInfo window_param = result_array->op_param;
        chunk_ptr = lam_chunk_window(opnd_chunk,
                                     arraylist[0]->desc.attr_type,
                                     window_param.window.window_size,
                                     window_param.window.window_func,
                                     result_array,
                                     pos,
                                     chunksize);
        break;
    }

    case OP_MAP:
    {
        int attrtype = arraylist[0]->desc.attr_type;
        chunk_ptr = lam_chunk_map(chunklist[0], attrtype, result_array, pos, chunksize);
        break;
    }

    case OP_EXPONENTIAL:
    {
        int attrtype = arraylist[0]->desc.attr_type;
        chunk_ptr = lam_chunk_exp(chunklist[0], attrtype, result_array, pos, chunksize);
        break;
    }
    }

    return chunk_ptr;
}

Chunk *lam_operation_matmul(Array **arraylist, Array *result_array, uint64_t pos, uint64_t *chunksize_parent, uint64_t **chunksize_child)
{
    int attrtype[2];
    attrtype[0] = arraylist[0]->desc.attr_type;
    attrtype[1] = arraylist[1]->desc.attr_type;

    uint64_t num_chunk_per_dim[2], result_chunk_coord[2];
    for (int i = 0; i < result_array->desc.dim_len; i++)
    {
        num_chunk_per_dim[i] = ((result_array->desc.dim_domains[i][1] - result_array->desc.dim_domains[i][0] + chunksize_parent[i]) / chunksize_parent[i]);
    }

    calculate_chunk_coord(result_array->desc.dim_len, pos, num_chunk_per_dim, result_chunk_coord);

    Chunk *result_chunk;
    chunk_get_chunk(result_array->desc.object_name, chunksize_parent, result_chunk_coord, &result_chunk);

    uint64_t lhs_coord[2] = {result_chunk_coord[0], 0};
    uint64_t rhs_coord[2] = {0, result_chunk_coord[1]};

    // calculate array size in terms of chunk
    uint64_t lhs_chunk_size[2] = {
        0,
    };
    uint64_t rhs_chunk_size[2] = {
        0,
    };
    for (uint32_t d = 0; d < 2; d++)
    {
        lhs_chunk_size[d] = (uint64_t)ceil((double)(arraylist[0]->desc.dim_domains[d][1] - arraylist[0]->desc.dim_domains[d][0] + 1) / chunksize_child[0][d]);
        rhs_chunk_size[d] = (uint64_t)ceil((double)(arraylist[1]->desc.dim_domains[d][1] - arraylist[1]->desc.dim_domains[d][0] + 1) / chunksize_child[1][d]);
    }

    uint64_t loop = 0;
    if (result_array->op_param.matmul.lhs_transposed) {
        // to calculate "loop" correctly when matmul-transpose merged
        loop = (arraylist[0]->desc.dim_domains[0][1] - arraylist[0]->desc.dim_domains[0][0] + 1) / chunksize_child[0][0];
    } else {
        loop = (arraylist[0]->desc.dim_domains[1][1] - arraylist[0]->desc.dim_domains[1][0] + 1) / chunksize_child[0][1];
    }

    for (int i = 0; i < loop; i++)
    {
        Chunk *lhs_chunk, *rhs_chunk;
        uint64_t lhs_pos, rhs_pos;

        if (result_array->op_param.matmul.lhs_transposed)
        {
            uint64_t lhs_coord_trans[2] = {lhs_coord[1], lhs_coord[0]};
            bf_util_calculate_1d_from_nd_row_major(lhs_coord_trans, lhs_chunk_size, 2, &lhs_pos);
        }
        else
        {
            bf_util_calculate_1d_from_nd_row_major(lhs_coord, lhs_chunk_size, 2, &lhs_pos);
        }
        lhs_chunk = get_pos(arraylist[0], lhs_pos, chunksize_child[0]);

        if (result_array->op_param.matmul.rhs_transposed)
        {
            uint64_t rhs_coord_trans[2] = {rhs_coord[1], rhs_coord[0]};
            bf_util_calculate_1d_from_nd_row_major(rhs_coord_trans, rhs_chunk_size, 2, &rhs_pos);
        }
        else
        {
            bf_util_calculate_1d_from_nd_row_major(rhs_coord, rhs_chunk_size, 2, &rhs_pos);
        }
        rhs_chunk = get_pos(arraylist[1], rhs_pos, chunksize_child[1]);

        lam_chunk_matmul(
            lhs_chunk, rhs_chunk, result_chunk,
            attrtype[0], attrtype[1], result_array->desc,
            chunksize_parent, i,
            result_array->op_param.matmul.lhs_transposed, 
            result_array->op_param.matmul.rhs_transposed);

        chunk_destroy(lhs_chunk);
        chunk_destroy(rhs_chunk);

        lhs_coord[1]++;
        rhs_coord[0]++;
    }

    // At this time, the type of the result tile is:
    //      1. sparse type, if only SxS operations are performed
    //      2. dense type, otherwise
    result_chunk = lam_chunk_matmul_finalize(
        result_chunk, 
        attrtype[0], 
        attrtype[1],
        false);
    
    return result_chunk;
}

Chunk *lam_operation_retile(
        Array *in,              // input array
        Array *out,             // output array
        uint64_t pos,           // position of output chunk
        uint64_t *in_chunksize, // the chunk size of input array
        uint64_t *out_chunksize // the chunk size of output array 
) {
    // input_chunk will be used in lam_retile()
    //      For dense tile, a chunk iterator over input_chunk will be used.
    //      For sparse tile, input_chunk is used only for some information.
    Chunk *input_chunk;

    uint64_t *input_chunk_coords = NULL;
    uint64_t *input_chunk_coord_lens = NULL;
    uint64_t **dim_domains = in->desc.dim_domains;
    uint32_t dim_len = in->desc.dim_len;

    storage_util_get_dcoord_lens(dim_domains, out_chunksize, dim_len, &input_chunk_coord_lens);
    bf_util_calculate_nd_from_1d_row_major(pos, input_chunk_coord_lens, dim_len, &input_chunk_coords);

    chunk_get_chunk(
        in->desc.object_name, 
        out_chunksize, 
        input_chunk_coords, 
        &input_chunk);

    free(input_chunk_coord_lens);
    free(input_chunk_coords);

    int attrtype = in->desc.attr_type;
    Chunk *r = lam_chunk_retile(input_chunk, attrtype, out, pos, out_chunksize);
    chunk_destroy(input_chunk);
    return r;
}

Chunk *lam_operation_aggr(Array *result_array, uint64_t pos, uint64_t *chunksize_parent, uint64_t *chunksize_child, uint8_t *dim_selection_map, uint64_t partition_loop)
{
    /* variables */
    Array *aggr_array = result_array->arraylist[0];
    int attrtype = aggr_array->desc.attr_type;
    GroupBy aggr_param = result_array->op_param.groupBy;

    /* get result tile coordinates */
    uint32_t result_ndim = result_array->desc.dim_len;
    uint64_t *result_chunk_coord;                                               // output tile coords
    uint64_t *result_pos_nd_lens = malloc(sizeof(uint64_t) * result_ndim);      // how many tiles in the dimension?

    for (int d = 0; d < result_ndim; d++)
    {
        result_pos_nd_lens[d] = (result_array->desc.dim_domains[d][1] - result_array->desc.dim_domains[d][0] + 1) / chunksize_parent[d];
    }
    bf_util_calculate_nd_from_1d_row_major(pos, result_pos_nd_lens, result_ndim, &result_chunk_coord);

    /* get input tile's coordinates */
    uint64_t *aggr_pos_nd_lens = malloc(sizeof(uint64_t) * aggr_array->desc.dim_len);       // how many tiles in the dimension?
    for (int d = 0; d < aggr_array->desc.dim_len; d++)
    {
        aggr_pos_nd_lens[d] = (aggr_array->desc.dim_domains[d][1] - aggr_array->desc.dim_domains[d][0] + 1) / chunksize_child[d];
    }

    uint64_t *aggr_chunk_coord = malloc(sizeof(uint64_t) * aggr_array->desc.dim_len);       // same with output tile except for aggregated dimenion's coordisnates
    uint64_t aggr_pos;
    int k = 0;
    for (int d = 0; d < aggr_array->desc.dim_len; d++)
    {
        if (dim_selection_map[d])
        {
            aggr_chunk_coord[d] = result_chunk_coord[k];
            k++;
        }
        else
            aggr_chunk_coord[d] = 0;
    }
    
    bf_util_calculate_1d_from_nd_row_major(aggr_chunk_coord, aggr_pos_nd_lens, aggr_array->desc.dim_len, &aggr_pos);        

    /* aggregate operations */
    // FIXME: Why pos is used instaed of aggr_pos?
    Chunk *result_chunk = scan_without_range(result_array, pos, chunksize_parent);

    int aggr_func = result_array->op_param.groupBy.agg_func;
    for (int i = 0; i < partition_loop; i++)
    {
        Chunk *aggr_chunk = get_pos(aggr_array, pos, chunksize_child);

        lam_chunk_group_by_dim_aggregate(aggr_chunk,
                                         result_chunk,
                                         attrtype,
                                         result_array->op_param.groupBy.state,
                                         aggr_func,
                                         dim_selection_map, result_array->desc, chunksize_parent, i);

        chunk_destroy(aggr_chunk);
    }
    lam_chunk_group_by_dim_write(result_chunk, result_array->op_param.groupBy.state, attrtype, aggr_func);

    free(result_chunk_coord);
    free(result_pos_nd_lens);
    free(aggr_pos_nd_lens);
    free(aggr_chunk_coord);

    return result_chunk;
}

Chunk *scan_without_range(Array *array, uint64_t pos, uint64_t *chunksize)
{
    Descriptor desc = array->desc;
    uint32_t dim_len = desc.dim_len;
    uint64_t **dim_domains = desc.dim_domains;

    /* Compute the coordinates of the chunk to scan. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int d = 0; d < dim_len; d++)
        num_chunk_per_dim[d] = ceil((dim_domains[d][1] - dim_domains[d][0] + 1) / chunksize[d]);
    uint64_t *coords;
    bf_util_calculate_nd_from_1d_row_major(pos, num_chunk_per_dim, dim_len, &coords);
    /* Fetch the target chunk. */
    Chunk *result_chunk;
    chunk_get_chunk(desc.object_name, chunksize, coords, &result_chunk);
    // result_chunk->curpage->type = SPARSE_FIXED;

    free(num_chunk_per_dim);
    free(coords);
    return result_chunk;
}

Chunk *scan_with_range(Array *array_to_scan, Range scan_range, int attr_type, Descriptor array_desc, uint64_t pos, uint64_t *chunksize)
{

    uint32_t dim_len = scan_range.ndim;
    uint64_t **dim_domains = array_desc.dim_domains;

    /* Compute number of chunks per each dimension. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int i = 0; i < dim_len; i++)
    {
        num_chunk_per_dim[i] = ((dim_domains[i][1] - dim_domains[i][0] + chunksize[i]) / chunksize[i]);
    }

    uint64_t *result_chunk_coord = malloc(sizeof(uint64_t) * dim_len);
    uint64_t *starting_cell_coords = malloc(sizeof(uint64_t) * dim_len);

    Chunk *scan_chunk, *result_chunk;

    /* First, fetch the result chunk to write. */
    calculate_chunk_coord(dim_len, pos, num_chunk_per_dim, result_chunk_coord);
    chunk_get_chunk(array_desc.object_name, chunksize, result_chunk_coord, &result_chunk);

    /* Next, fetch the scan chunk to copy into result chunk. */
    for (int i = 0; i < dim_len; i++)
    {
        starting_cell_coords[i] = result_chunk_coord[i] * chunksize[i] + scan_range.lb[i];
    }
    chunk_get_chunk_custom_pos(array_to_scan->desc.object_name, starting_cell_coords, chunksize, &scan_chunk);

    /* Write the cell values from scan chunk to result chunk.*/
    // TODO: if optimizable

    // if not optimizable
    uint64_t num_of_cells = 1;
    for (int i = 0; i < dim_len; i++)
        num_of_cells *= chunksize[i];

    uint64_t *cell_coords = malloc(sizeof(uint64_t) * dim_len);
    if (attr_type == TILESTORE_INT32)
    {
        for (int i = 0; i < num_of_cells; i++)
        {
            calculate_cell_coord(dim_len, i, chunksize, cell_coords);
            int val;
            if (chunk_get_cell_int(scan_chunk, cell_coords, &val) == 0)
                chunk_write_cell_int(result_chunk, cell_coords, &val);
        }
    }
    else if (attr_type == TILESTORE_FLOAT32)
    {
        for (int i = 0; i < num_of_cells; i++)
        {
            calculate_cell_coord(dim_len, i, chunksize, cell_coords);
            float val;
            if (chunk_get_cell_float(scan_chunk, cell_coords, &val) == 0)
                chunk_write_cell_float(result_chunk, cell_coords, &val);
        }
    }
    else if (attr_type == TILESTORE_FLOAT64)
    {
        for (int i = 0; i < num_of_cells; i++)
        {
            calculate_cell_coord(dim_len, i, chunksize, cell_coords);
            double val;
            if (chunk_get_cell_double(scan_chunk, cell_coords, &val) == 0)
                chunk_write_cell_double(result_chunk, cell_coords, &val);
        }
    }

    free(num_chunk_per_dim);
    free(result_chunk_coord);
    free(starting_cell_coords);
    free(cell_coords);
    free(scan_chunk);

    return result_chunk;
}
