#include "simulate.h"

void hint_to_bf(Array *array, uint8_t order) {
    if (array->visited_flag == order) return;
    array->visited_flag++;
    
    if (array->executed) return;

    bool target = array->scalar_flag == false && array->desc.attr_name[0] != 'i' && array->persist == false;
    if (target) {
        // num tiles
        uint64_t num_tiles = 1;
        for (uint32_t d = 0; d < array->desc.dim_len; d++) {
            double side = (double) (array->desc.dim_domains[d][1] - array->desc.dim_domains[d][0] + 1);
            uint64_t num_of_chunks_in_dim = (uint64_t) ceil(side / array->desc.tile_extents[d]);
            num_tiles *= num_of_chunks_in_dim;
        }

        BF_HintDCnt(array->desc.object_name, num_tiles);
    }

    // dfs
    for (int i = 0; i < array->array_num; i++) {
        Array *child = array->arraylist[i];
        if (child == NULL) continue;
        hint_to_bf(child, order);
    }
}

void insert_or_update_future_hash(Array *array, uint64_t idx, uint64_t *ts) {
    uint64_t *dcoord_lens = NULL;
    uint64_t *dcoords = NULL;
    storage_util_get_dcoord_lens(
        array->desc.dim_domains, array->desc.tile_extents, 2, &dcoord_lens);
    bf_util_calculate_nd_from_1d_row_major(idx, dcoord_lens, 2, &dcoords);

    array_key key;
    key.arrayname = array->desc.object_name;
    key.attrname = array->desc.attr_name;
    key.dcoords = dcoords;
    key.dim_len = 2;
    key.emptytile_template = BF_EMPTYTILE_DENSE;

    // fprintf(stderr, "[SIMULATOR] reference log - %lu,{%s,%lu,%lu},\n", *ts, key.arrayname, key.dcoords[0], key.dcoords[1]);
    BF_AddFuture(key, (*ts)++);

    free(dcoord_lens);
    free(dcoords);

    return;
}

void _fill_future(Array* array, uint64_t idx, uint64_t *ts) {
    if (array->simulated_materialized_list[idx])
        return;
    
    if (array->array_num == 0) {
        if (array->op_type == OP_NEW_ARRAY) {
            insert_or_update_future_hash(array, idx, ts);
            array->simulated_materialized_list[idx] = true;
        }
    } else if (array->array_num == 1) {
        switch (array->op_type) {
            case OP_ADD_CONSTANT:
            case OP_SUB_CONSTANT:
            case OP_PRODUCT_CONSTANT:
            case OP_DIV_CONSTANT:
            case OP_EXPONENTIAL: 
            case OP_MAP: {
                _fill_future(array->arraylist[0], idx, ts);
                insert_or_update_future_hash(array->arraylist[0], idx, ts);
                insert_or_update_future_hash(array, idx, ts);
                break;
            }
            case OP_TRANSPOSE: {
                // variables
                uint32_t dim_len = array->desc.dim_len;
                uint64_t *parent_chunk_lens = NULL;
                uint64_t *child_chunk_lens = NULL;
                uint64_t *parent_coords = NULL;
                uint64_t *child_coords = NULL;

                // malloc
                parent_chunk_lens = malloc(sizeof(uint64_t) * dim_len);
                child_chunk_lens = malloc(sizeof(uint64_t) * dim_len);
                parent_coords = malloc(sizeof(uint64_t) * dim_len);
                child_coords = malloc(sizeof(uint64_t) * dim_len);

                // calculate parent_chunk_lens
                for (uint32_t d = 0; d < dim_len; d++)
                {
                    uint64_t dim_side_len = array->desc.dim_domains[d][1] - array->desc.dim_domains[d][0] + 1;
                    parent_chunk_lens[d] = ceil(((double)dim_side_len / array->desc.tile_extents[d]));
                }

                // calculate current chunk ND coordinates.
                bf_util_calculate_nd_from_1d_row_major(idx, parent_chunk_lens, dim_len, &parent_coords);

                // calculate child chunk size and coords from parents' ones
                uint32_t *dim_order = array->op_param.trans.transpose_func;
                for (uint32_t d = 0; d < dim_len; d++)
                {
                    // calculate child_chunk_lens
                    child_chunk_lens[d] = parent_chunk_lens[dim_order[d]];

                    // calculate the original chunk's ND coordinates.
                    child_coords[d] = parent_coords[dim_order[d]];
                }

                // calculate the original chunk 1D coordinate.
                uint64_t child_pos;
                bf_util_calculate_1d_from_nd_row_major(child_coords, child_chunk_lens, dim_len, &child_pos);

                free(parent_chunk_lens);
                free(child_chunk_lens);
                free(parent_coords);
                free(child_coords);

                _fill_future(array->arraylist[0], child_pos, ts);
                insert_or_update_future_hash(array->arraylist[0], child_pos, ts);
                insert_or_update_future_hash(array, idx, ts);
                break;
            }
            default: assert(0);
        }
    } else if (array->array_num == 2) {
        switch (array->op_type) {
            case OP_ADD:
            case OP_SUB:
            case OP_PRODUCT:
            case OP_DIV: {
                _fill_future(array->arraylist[0], idx, ts);
                _fill_future(array->arraylist[1], idx, ts);
                insert_or_update_future_hash(array->arraylist[0], idx, ts);
                insert_or_update_future_hash(array->arraylist[1], idx, ts);
                insert_or_update_future_hash(array, idx, ts);
                break;
            }
            case OP_MATMUL: {
                uint64_t **chunksize_child = malloc(sizeof(uint64_t *) * 2);
                chunksize_child[0] = malloc(sizeof(uint64_t) * 2);
                chunksize_child[1] = malloc(sizeof(uint64_t) * 2);

                determine_child_chunksize(array->desc.tile_extents, array, array->op_type, chunksize_child);

                uint64_t num_chunk_per_dim[2], result_chunk_coord[2];
                for (int i = 0; i < array->desc.dim_len; i++)
                    num_chunk_per_dim[i] = ((array->desc.dim_domains[i][1] - array->desc.dim_domains[i][0] + array->desc.tile_extents[i]) / array->desc.tile_extents[i]);

                calculate_chunk_coord(array->desc.dim_len, idx, num_chunk_per_dim, result_chunk_coord);

                uint64_t lhs_coord[2] = {result_chunk_coord[0], 0};
                uint64_t rhs_coord[2] = {0, result_chunk_coord[1]};

                // calculate array size in terms of chunk
                uint64_t lhs_chunk_size[2] = {0, };
                uint64_t rhs_chunk_size[2] = {0, };
                for (uint32_t d = 0; d < 2; d++)
                {
                    lhs_chunk_size[d] = (uint64_t)ceil((double)(array->arraylist[0]->desc.dim_domains[d][1] - array->arraylist[0]->desc.dim_domains[d][0] + 1) / chunksize_child[0][d]);
                    rhs_chunk_size[d] = (uint64_t)ceil((double)(array->arraylist[1]->desc.dim_domains[d][1] - array->arraylist[1]->desc.dim_domains[d][0] + 1) / chunksize_child[1][d]);
                }

                uint64_t loop = 0;
                if (array->op_param.matmul.lhs_transposed) {
                    // to calculate "loop" correctly when matmul-transpose merged
                    loop = (array->arraylist[0]->desc.dim_domains[0][1] - array->arraylist[0]->desc.dim_domains[0][0] + 1) / chunksize_child[0][0];
                } else {
                    loop = (array->arraylist[0]->desc.dim_domains[1][1] - array->arraylist[0]->desc.dim_domains[1][0] + 1) / chunksize_child[0][1];
                }

                for (int i = 0; i < loop; i++)
                {
                    Chunk *lhs_chunk, *rhs_chunk;
                    uint64_t lhs_pos, rhs_pos;

                    if (array->op_param.matmul.lhs_transposed) {
                        uint64_t lhs_coord_trans[2] = {lhs_coord[1], lhs_coord[0]};
                        bf_util_calculate_1d_from_nd_row_major(lhs_coord_trans, lhs_chunk_size, 2, &lhs_pos);
                    } else {
                        bf_util_calculate_1d_from_nd_row_major(lhs_coord, lhs_chunk_size, 2, &lhs_pos);
                    }
                    _fill_future(array->arraylist[0], lhs_pos, ts);

                    if (array->op_param.matmul.rhs_transposed) {
                        uint64_t rhs_coord_trans[2] = {rhs_coord[1], rhs_coord[0]};
                        bf_util_calculate_1d_from_nd_row_major(rhs_coord_trans, rhs_chunk_size, 2, &rhs_pos);
                    } else {
                        bf_util_calculate_1d_from_nd_row_major(rhs_coord, rhs_chunk_size, 2, &rhs_pos);
                    }
                    _fill_future(array->arraylist[1], rhs_pos, ts);

                    insert_or_update_future_hash(array->arraylist[0], lhs_pos, ts);
                    insert_or_update_future_hash(array->arraylist[1], rhs_pos, ts);
                    insert_or_update_future_hash(array, idx, ts);

                    lhs_coord[1]++;
                    rhs_coord[0]++;
                }

                free(chunksize_child[0]);
                free(chunksize_child[1]);
                free(chunksize_child);
                break;
            }
            default: assert(0);
        }
    }

    array->simulated_materialized_list[idx] = true;
}

void _fill_future_scala(Array *array, uint64_t *ts) {
    switch (array->op_type) {
        case OP_AGGR: {
            // Prepare variables
            Array *aggr_target_array = array->arraylist[0];
            uint32_t aggr_ndim = aggr_target_array->desc.dim_len;
            GroupBy aggr_info = array->op_param.groupBy;

            // Calculate the # of loops for aggregating partitions
            uint64_t *child_tilesize = array->arraylist[0]->desc.tile_extents;
            uint64_t partition_loop = 1;
            for (int i = 0; i < aggr_ndim; i++)
                partition_loop *= ceil((int)(aggr_target_array->desc.dim_domains[i][1] - aggr_target_array->desc.dim_domains[i][0] + 1) / (int)child_tilesize[i]);

            // Do the aggregation
            int aggr_func = array->op_param.groupBy.agg_func;
            for (uint64_t i = 0; i < partition_loop; i++) {
                _fill_future(array->arraylist[0], i, ts);
                insert_or_update_future_hash(array->arraylist[0], i, ts);
            }
        }
    }

    // array->simulated_materialized_list[0] = true;
}

void fill_future(Array* array) {
    uint64_t ts = 0;

    // scala operations
    if (array->scalar_flag) {
        _fill_future_scala(array, &ts);
        BF_future_printall();       // debug
        return;
    }

    // matrix operations
    uint64_t EOCHUNK = 1;
    for (int d = 0; d < array->desc.dim_len; d++)
        EOCHUNK *= ceil((double) (array->desc.dim_domains[d][1] - array->desc.dim_domains[d][0] + 1) / array->desc.tile_extents[d]);
    
    for (uint64_t idx = 0; idx < EOCHUNK; idx++) {
        _fill_future(array, idx, &ts);
    }

    BF_future_printall();       // debug
}