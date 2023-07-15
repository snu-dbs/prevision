#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "bf.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "array_struct.h"
#include "exec_interface.h"
#include "node_interface.h"
#include "planner.h"

void test_sparse_csr_add_lhs_larger()
{
    char *lhs_name = "__test_sparse_csr_add_lhs_larger_lhs";
    char *rhs_name = "__test_sparse_csr_add_lhs_larger_rhs";

    printf("test_sparse_csr_add_lhs_larger started!\n");

    /***********************************************
     * Prepare tilestore arrays using BF-layer
     ***********************************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 9999, 0, 9999};
    int tile_extents[] = {1000, 1000};

    // Array Create
    storage_util_create_array(lhs_name, TILESTORE_SPARSE_CSR,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);
    storage_util_create_array(rhs_name, TILESTORE_SPARSE_CSR,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    // Array Write
    array_key lhs_key, rhs_key;
    uint64_t tile_coord[2];
    PFpage *lhs_page, *rhs_page;
    uint64_t *lhs_indptr, *rhs_indptr, *lhs_indices, *rhs_indices;

    lhs_key.arrayname = lhs_name;
    lhs_key.attrname = "i"; // Default attr name..?
    lhs_key.dim_len = 2;
    lhs_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;
    rhs_key.arrayname = rhs_name;
    rhs_key.attrname = "i";
    rhs_key.dim_len = 2;
    rhs_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            lhs_key.dcoords = tile_coord;
            rhs_key.dcoords = tile_coord;
            BF_GetBuf(lhs_key, &lhs_page);
            BF_GetBuf(rhs_key, &rhs_page);

            // LHS : 2% - density (20,000 elements), RHS: 1% - density (10,000 elements)
            // Allocate enough space for them.
            // FIXME: I updated it without testing
            BF_ResizeBuf(lhs_page, 20000);
            // while (bf_util_pagebuf_get_coords_lens(lhs_page)[1] / sizeof(uint64_t) <
            //        20000)
            // {
            //     bf_util_double_pagebuf(lhs_page);
            //     bf_util_double_coords(lhs_page);
            // }
            // FIXME: I updated it without testing
            BF_ResizeBuf(rhs_page, 10000);
            // while (bf_util_pagebuf_get_coords_lens(rhs_page)[1] / sizeof(uint64_t) <
            //        10000)
            // {
            //     bf_util_double_pagebuf(rhs_page);
            //     bf_util_double_coords(rhs_page);
            // }

            lhs_indptr = bf_util_pagebuf_get_coords(lhs_page, 0);
            rhs_indptr = bf_util_pagebuf_get_coords(rhs_page, 0);
            lhs_indices = bf_util_pagebuf_get_coords(lhs_page, 1);
            rhs_indices = bf_util_pagebuf_get_coords(rhs_page, 1);

            // Fill out values. (3.0)
            // LHS : (:, mod50 == 0),  RHS : (:, mod100 == 0)
            lhs_indptr[0] = 0;
            rhs_indptr[0] = 0;
            for (uint64_t i = 0; i < 1000; i++)
            {
                lhs_indptr[i + 1] = 20 + lhs_indptr[i];
                rhs_indptr[i + 1] = 10 + rhs_indptr[i];

                for (uint64_t j = 0; j < 20; j++)
                {
                    if (j % 2 == 0)
                    {
                        rhs_indices[10 * i + j / 2] = 100 * (j / 2);
                        bf_util_pagebuf_set_double(rhs_page, 10 * i + j / 2, 3.0);
                    }
                    lhs_indices[20 * i + j] = 50 * j;
                    bf_util_pagebuf_set_double(lhs_page, 20 * i + j, 3.0);
                }
            }
            bf_util_pagebuf_set_unfilled_idx(lhs_page, 20000); // only for column indices
            bf_util_pagebuf_set_unfilled_pagebuf_offset(lhs_page, 20000 * sizeof(double));
            bf_util_pagebuf_set_unfilled_idx(rhs_page, 10000); // only for column indices
            bf_util_pagebuf_set_unfilled_pagebuf_offset(rhs_page, 10000 * sizeof(double));

            BF_TouchBuf(lhs_key);
            BF_TouchBuf(rhs_key);
            BF_UnpinBuf(lhs_key);
            BF_UnpinBuf(rhs_key);
        }
    }

    BF_Detach();
    BF_Free();

    /*****************
     * Do addition
     *****************/
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array *M = open_array(lhs_name);
    Array *N = open_array(rhs_name);
    Array *RES = elemwise_op(M, N, OP_ADD);

    // calculate_consumer_cnt(RES);
    execute(RES);

    BF_Detach();
    BF_Free();

    /*******************
     *  Check result
     *******************/
    BF_Init();
    BF_Attach();

    array_key res_key;
    PFpage *res_page;
    uint64_t *res_indptr, *res_indices;
    double *res_buf;

    res_key.arrayname = RES->desc.object_name;
    res_key.attrname = "i";
    res_key.dim_len = 2;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            res_key.dcoords = tile_coord;
            BF_GetBuf(res_key, &res_page);

            res_indptr = bf_util_pagebuf_get_coords(res_page, 0);
            res_indices = bf_util_pagebuf_get_coords(res_page, 1);
            res_buf = (double *)bf_util_get_pagebuf(res_page);

            assert(res_indptr[0] == 0);
            for (uint64_t i = 0; i < 1000; i++)
            {
                assert(res_indptr[i + 1] == 20 * (i + 1));

                for (uint64_t j = 0; j < 20; j++)
                {
                    assert(res_indices[20 * i + j] == 50 * j);
                    if (j % 2 == 0)
                    {
                        assert(res_buf[20 * i + j] == 6.0);
                    }
                    else
                    {
                        assert(res_buf[20 * i + j] == 3.0);
                    }
                }
            }
        }
    }

    printf("test_sparse_csr_add_lhs_larger passed!\n");

    BF_Detach();
    BF_Free();

    // delete
    free_node(M);
    free_node(N);
    free_node(RES);
}

void test_dense_norm()
{
    char *opnd_name = "__test_dense_norm_opnd";

    printf("test_dense_norm started!\n");

    /***********************************************
     * Prepare tilestore arrays using BF-layer
     ***********************************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 9999, 0, 9999};
    int tile_extents[] = {1000, 1000};

    // Array Create
    storage_util_create_array(opnd_name, TILESTORE_DENSE,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    // Array Write
    array_key opnd_key;
    uint64_t tile_coord[2];
    PFpage *opnd_page;

    opnd_key.arrayname = opnd_name;
    opnd_key.attrname = "i"; // Default attr name..?
    opnd_key.dim_len = 2;
    opnd_key.emptytile_template = BF_EMPTYTILE_DENSE;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            opnd_key.dcoords = tile_coord;
            BF_GetBuf(opnd_key, &opnd_page);

            for (int i = 0; i < 1000 * 1000; i++)
            {
                double cell_value = i % 2 == 0 ? 1 : 2;
                bf_util_pagebuf_set_double(opnd_page, i, cell_value);
            }

            BF_TouchBuf(opnd_key);
            BF_UnpinBuf(opnd_key);
        }
    }
    BF_Detach();
    BF_Free();

    /*****************
     * Do addition
     *****************/
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array *M = open_array(opnd_name);
    Array *RES = aggr(M, AGGREGATE_NORM, NULL, 0);

    // calculate_consumer_cnt(RES);
    execute(RES);

    BF_Detach();
    BF_Free();

    /*******************
     *  Check result
     *******************/
    assert(RES->scalar_val_double == sqrt(2500 * 1000 * 100));
    printf("test_dense_norm passed!\n");

    // delete
    free_node(M);
    free_node(RES);
}

void test_sparse_norm()
{
    char *opnd_name = "__test_sparse_norm_opnd";

    printf("test_sparse_norm started!\n");

    /***********************************************
     * Prepare tilestore arrays using BF-layer
     ***********************************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 9999, 0, 9999};
    int tile_extents[] = {1000, 1000};

    // Array Create
    storage_util_create_array(opnd_name, TILESTORE_SPARSE_CSR,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    // Array Write
    array_key opnd_key;
    uint64_t tile_coord[2];
    PFpage *opnd_page;
    uint64_t *opnd_indptr, *opnd_indices;

    opnd_key.arrayname = opnd_name;
    opnd_key.attrname = "i"; // Default attr name..?
    opnd_key.dim_len = 2;
    opnd_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            opnd_key.dcoords = tile_coord;
            BF_GetBuf(opnd_key, &opnd_page);
            // FIXME: I updated it without testing
            BF_ResizeBuf(opnd_page, 10000);
            // while (bf_util_pagebuf_get_coords_lens(opnd_page)[1] / sizeof(uint64_t) <
            //        10000)
            // {
            //     bf_util_double_pagebuf(opnd_page);
            //     bf_util_double_coords(opnd_page);
            // }

            opnd_indptr = bf_util_pagebuf_get_coords(opnd_page, 0);
            opnd_indices = bf_util_pagebuf_get_coords(opnd_page, 1);

            opnd_indptr[0] = 0;
            for (int i = 0; i < 1000; i++)
            {
                opnd_indptr[i + 1] = 10 + opnd_indptr[i];
                for (int j = 0; j < 10; j++)
                {
                    double cell_value = j % 2 == 0 ? 1 : 2;
                    opnd_indices[10 * i + j] = 100 * j;
                    bf_util_pagebuf_set_double(opnd_page, 10 * i + j, cell_value);
                }
            }

            bf_util_pagebuf_set_unfilled_idx(opnd_page, 10000); // only for column indices
            bf_util_pagebuf_set_unfilled_pagebuf_offset(opnd_page, 10000 * sizeof(double));
            BF_TouchBuf(opnd_key);
            BF_UnpinBuf(opnd_key);
        }
    }
    BF_Detach();
    BF_Free();

    /*****************
     * Do addition
     *****************/
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array *M = open_array(opnd_name);
    Array *RES = aggr(M, AGGREGATE_NORM, NULL, 0);

    // calculate_consumer_cnt(RES);
    execute(RES);

    BF_Detach();
    BF_Free();

    /*******************
     *  Check result
     *******************/
    assert(RES->scalar_val_double == sqrt(25 * 1000 * 100));
    printf("test_sparse_norm passed!\n");

    // delete
    free_node(M);
    free_node(RES);
}

void test_dense_matmul()
{
    char *lhs_name = "__test_dense_matmul_lhs";
    char *rhs_name = "__test_dense_matmul_rhs";

    printf("test_dense_matmul started!\n");

    /***********************************************
     * Prepare tilestore arrays using BF-layer
     ***********************************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 9999, 0, 9999};
    int tile_extents[] = {1000, 1000};

    // Array Create
    storage_util_create_array(lhs_name, TILESTORE_DENSE,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);
    storage_util_create_array(rhs_name, TILESTORE_DENSE,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    // Array Write
    array_key lhs_key, rhs_key;
    uint64_t tile_coord[2];
    PFpage *lhs_page, *rhs_page;

    lhs_key.arrayname = lhs_name;
    lhs_key.attrname = "i"; // Default attr name..?
    lhs_key.dim_len = 2;
    lhs_key.emptytile_template = BF_EMPTYTILE_DENSE;

    rhs_key.arrayname = rhs_name;
    rhs_key.attrname = "i"; // Default attr name..?
    rhs_key.dim_len = 2;
    rhs_key.emptytile_template = BF_EMPTYTILE_DENSE;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            lhs_key.dcoords = tile_coord;
            rhs_key.dcoords = tile_coord;
            BF_GetBuf(lhs_key, &lhs_page);
            BF_GetBuf(rhs_key, &rhs_page);

            for (int i = 0; i < 1000 * 1000; i++)
            {
                bf_util_pagebuf_set_double(lhs_page, i, i % 1000);
                bf_util_pagebuf_set_double(rhs_page, i, i % 1000);
            }

            BF_TouchBuf(lhs_key);
            BF_TouchBuf(rhs_key);
            BF_UnpinBuf(lhs_key);
            BF_UnpinBuf(rhs_key);
        }
    }
    BF_Detach();
    BF_Free();

    /*****************
     * Do addition
     *****************/
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array *A = open_array(lhs_name);
    Array *B = open_array(rhs_name);
    Array *RES_AB = matmul(A, B);

    uint32_t transpose_order[2] = {1, 0};
    Array *BT = transpose(B, transpose_order);
    Array *RES_ABT = matmul(A, BT);

    // calculate_consumer_cnt(RES_AB);
    // calculate_consumer_cnt(RES_ABT);
    execute(RES_AB);
    execute(RES_ABT);

    BF_Detach();
    BF_Free();

    /*******************
     *  Check result
     *******************/
    BF_Init();
    BF_Attach();

    array_key resAB_key;
    PFpage *resAB_page;
    double *resAB_buf;

    array_key resABT_key;
    PFpage *resABT_page;
    double *resABT_buf;

    resAB_key.arrayname = RES_AB->desc.object_name;
    resAB_key.attrname = "i";
    resAB_key.dim_len = 2;

    resABT_key.arrayname = RES_ABT->desc.object_name;
    resABT_key.attrname = "i";
    resABT_key.dim_len = 2;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            resAB_key.dcoords = tile_coord;
            resABT_key.dcoords = tile_coord;
            BF_GetBuf(resAB_key, &resAB_page);
            BF_GetBuf(resABT_key, &resABT_page);

            resAB_buf = (double *)bf_util_get_pagebuf(resAB_page);
            resABT_buf = (double *)bf_util_get_pagebuf(resABT_page);

            for (int i = 0; i < 1000; i++)
            {
                for (int j = 0; j < 1000; j++)
                {
                    assert(resAB_buf[1000 * i + j] == (double)4995000 * j);
                    assert(resABT_buf[1000 * i + j] == (double)3328335000);
                }
            }

            BF_UnpinBuf(resAB_key);
            BF_UnpinBuf(resABT_key);
        }
    }
    printf("test_dense_matmul passed!\n");

    BF_Detach();
    BF_Free();

    // delete
    free_node(A);
    free_node(B);
    free_node(RES_AB);
    free_node(RES_ABT);
}

void test_sparse_matmul()
{
    char *lhs_name = "__test_sparse_matmul_lhs";
    char *rhs_name = "__test_sparse_matmul_rhs";

    printf("test_sparse_matmul started!\n");

    /***********************************************
     * Prepare tilestore arrays using BF-layer
     ***********************************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 9999, 0, 9999};
    int tile_extents[] = {1000, 1000};

    // Array Create
    storage_util_create_array(lhs_name, TILESTORE_SPARSE_CSR,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);
    storage_util_create_array(rhs_name, TILESTORE_SPARSE_CSR,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    // Array Write
    array_key lhs_key, rhs_key;
    uint64_t tile_coord[2];
    PFpage *lhs_page, *rhs_page;
    uint64_t *lhs_indptr, *rhs_indptr, *lhs_indices, *rhs_indices;

    lhs_key.arrayname = lhs_name;
    lhs_key.attrname = "i"; // Default attr name..?
    lhs_key.dim_len = 2;
    lhs_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;
    rhs_key.arrayname = rhs_name;
    rhs_key.attrname = "i";
    rhs_key.dim_len = 2;
    rhs_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            lhs_key.dcoords = tile_coord;
            rhs_key.dcoords = tile_coord;
            BF_GetBuf(lhs_key, &lhs_page);
            BF_GetBuf(rhs_key, &rhs_page);

            // LHS : 2% - density (20,000 elements), RHS: 1% - density (10,000 elements)
            // Allocate enough space for them.
            // FIXME: I updated it without testing
            BF_ResizeBuf(lhs_page, 20000);
            // while (bf_util_pagebuf_get_coords_lens(lhs_page)[1] / sizeof(uint64_t) <
            //        20000)
            // {
            //     bf_util_double_pagebuf(lhs_page);
            //     bf_util_double_coords(lhs_page);
            // }
            // FIXME: I updated it without testing
            BF_ResizeBuf(rhs_page, 10000);
            // while (bf_util_pagebuf_get_coords_lens(rhs_page)[1] / sizeof(uint64_t) <
            //        10000)
            // {
            //     bf_util_double_pagebuf(rhs_page);
            //     bf_util_double_coords(rhs_page);
            // }

            lhs_indptr = bf_util_pagebuf_get_coords(lhs_page, 0);
            rhs_indptr = bf_util_pagebuf_get_coords(rhs_page, 0);
            lhs_indices = bf_util_pagebuf_get_coords(lhs_page, 1);
            rhs_indices = bf_util_pagebuf_get_coords(rhs_page, 1);

            // Fill out values.
            // LHS : (:, mod50 == 0),  RHS : (:, mod100 == 0)
            lhs_indptr[0] = 0;
            rhs_indptr[0] = 0;
            for (uint64_t i = 0; i < 1000; i++)
            {
                lhs_indptr[i + 1] = 20 + lhs_indptr[i];
                rhs_indptr[i + 1] = 10 + rhs_indptr[i];

                for (uint64_t j = 0; j < 20; j++)
                {
                    if (j % 2 == 0)
                    {
                        rhs_indices[10 * i + j / 2] = 100 * (j / 2);
                        bf_util_pagebuf_set_double(rhs_page, 10 * i + j / 2, (double)(j + 1));
                    }
                    lhs_indices[20 * i + j] = 50 * j;
                    bf_util_pagebuf_set_double(lhs_page, 20 * i + j, (double)(j + 1));
                }
            }
            bf_util_pagebuf_set_unfilled_idx(lhs_page, 20000); // only for column indices
            bf_util_pagebuf_set_unfilled_pagebuf_offset(lhs_page, 20000 * sizeof(double));
            bf_util_pagebuf_set_unfilled_idx(rhs_page, 10000); // only for column indices
            bf_util_pagebuf_set_unfilled_pagebuf_offset(rhs_page, 10000 * sizeof(double));

            BF_TouchBuf(lhs_key);
            BF_TouchBuf(rhs_key);
            BF_UnpinBuf(lhs_key);
            BF_UnpinBuf(rhs_key);
        }
    }

    BF_Detach();
    BF_Free();

    /*****************
     * Do Matmul
     *****************/
    struct timeval start, end;
    double wall_time;
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array *A = open_array(lhs_name);
    Array *B = open_array(rhs_name);
    Array *RES_AB = matmul(A, B);

    uint32_t transpose_order[2] = {1, 0};
    Array *BT = transpose(B, transpose_order);
    Array *RES_ABT = matmul(A, BT);

    // calculate_consumer_cnt(RES_AB);
    // calculate_consumer_cnt(RES_ABT);
    // execute(RES_AB);
    gettimeofday(&start, NULL);
    execute(RES_ABT);
    gettimeofday(&end, NULL);
    wall_time = (double)(end.tv_usec - start.tv_usec) / 1000000 + (double)(end.tv_sec - start.tv_sec);

    printf("Wall time: %.2f (ms)\n", wall_time * 1000);

    BF_Detach();
    BF_Free();

    /*******************
     *  Check result
     *******************/
    BF_Init();
    BF_Attach();

    array_key resAB_key;
    PFpage *resAB_page;
    double *resAB_buf;

    array_key resABT_key;
    PFpage *resABT_page;
    double *resABT_buf;

    resAB_key.arrayname = RES_AB->desc.object_name;
    resAB_key.attrname = "i";
    resAB_key.dim_len = 2;

    resABT_key.arrayname = RES_ABT->desc.object_name;
    resABT_key.attrname = "i";
    resABT_key.dim_len = 2;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++)
        {
            tile_coord[1] = col;

            resAB_key.dcoords = tile_coord;
            resABT_key.dcoords = tile_coord;
            // BF_GetBuf(resAB_key, &resAB_page);
            BF_GetBuf(resABT_key, &resABT_page);

            resABT_buf = (double *)bf_util_get_pagebuf(resABT_page);

            // uint64_t *resAB_indptr = bf_util_pagebuf_get_coords(resAB_page, 0);
            // uint64_t *resAB_indices = bf_util_pagebuf_get_coords(resAB_page, 1);
            uint64_t *resABT_indptr = bf_util_pagebuf_get_coords(resABT_page, 0);
            uint64_t *resABT_indices = bf_util_pagebuf_get_coords(resABT_page, 1);

            // assert(resAB_indptr[0] == 0);
            assert(resABT_indptr[0] == 0);
            for (int i = 0; i < 1000; i++)
            {
                // assert(resAB_indptr[i + 1] == 10 * (i + 1));
                assert(resABT_indptr[i + 1] == 1000 * (i + 1));
                for (int j = 0; j < 10; j++)
                {
                    // assert(resAB_indices[10 * i + j] == 100 * j);
                    // assert(resAB_buf[10 * i + j] == (double)(2100 * (2 * j + 1)));
                }

                for (int j = 0; j < 1000; j++)
                {
                    assert(resABT_indices[1000 * i + j] == j);
                    assert(resABT_buf[1000 * i + j] == (double)(13300));
                }
            }

            // BF_UnpinBuf(resAB_key);
            BF_UnpinBuf(resABT_key);
        }
    }
    printf("test_sparse_matmul passed!\n");

    BF_Detach();
    BF_Free();

    // delete
    free_node(A);
    free_node(B);
    free_node(RES_AB);
    free_node(RES_ABT);
}

void test_dense_svd()
{
    char *opnd_name = "__test_dense_svd_opnd";

    printf("test_dense_svd started!\n");

    /***********************************************
     * Prepare tilestore arrays using BF-layer
     ***********************************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 99999, 0, 99};
    int tile_extents[] = {1000, 100};

    // Array Create
    storage_util_create_array(opnd_name, TILESTORE_DENSE,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    // Array Write
    array_key opnd_key;
    uint64_t tile_coord[2];
    PFpage *opnd_page;

    opnd_key.arrayname = opnd_name;
    opnd_key.attrname = "i"; // Default attr name..?
    opnd_key.dim_len = 2;
    opnd_key.emptytile_template = BF_EMPTYTILE_DENSE;

    tile_coord[1] = 0;
    for (int row = 0; row < 100; row++)
    {
        tile_coord[0] = row;

        opnd_key.dcoords = tile_coord;
        BF_GetBuf(opnd_key, &opnd_page);

        for (int i = 0; i < 1000 * 100; i++)
        {
            bf_util_pagebuf_set_double(opnd_page, i, i % 1000);
        }

        BF_TouchBuf(opnd_key);
        BF_UnpinBuf(opnd_key);
    }
    BF_Detach();
    BF_Free();

    /*****************
     * Do SVD
     *****************/
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array *A = open_array(opnd_name);
    Array **SVD_res = svd(A);

    // execute(SVD_res[0]);

    BF_Detach();
    BF_Free();

    /*****************
     * Check Results
     *****************/
    assert(SVD_res[0]->op_type == OP_SVD);
    assert(SVD_res[1]->op_type == OP_SVD);
    assert(SVD_res[2]->op_type == OP_SVD);
    assert(SVD_res[0]->desc.dim_domains[0][1] -
               SVD_res[0]->desc.dim_domains[0][0] ==
           99999);
    assert(SVD_res[0]->desc.dim_domains[1][1] -
               SVD_res[0]->desc.dim_domains[1][0] ==
           99);
    assert(SVD_res[1]->desc.dim_domains[0][1] -
               SVD_res[1]->desc.dim_domains[0][0] ==
           99);
    assert(SVD_res[1]->desc.dim_domains[1][1] -
               SVD_res[1]->desc.dim_domains[1][0] ==
           99);
    assert(SVD_res[2]->desc.dim_domains[0][1] -
               SVD_res[2]->desc.dim_domains[0][0] ==
           99);
    assert(SVD_res[2]->desc.dim_domains[1][1] -
               SVD_res[2]->desc.dim_domains[1][0] ==
           99);
    assert(SVD_res[0]->desc.tile_extents[0] == 1000);
    assert(SVD_res[0]->desc.tile_extents[1] == 100);
    assert(SVD_res[1]->desc.tile_extents[0] == 100);
    assert(SVD_res[1]->desc.tile_extents[1] == 100);
    assert(SVD_res[2]->desc.tile_extents[0] == 100);
    assert(SVD_res[2]->desc.tile_extents[1] == 100);

    BF_Init();
    BF_Attach();

    tile_coord[0] = 0;
    tile_coord[1] = 0;
    array_key key;
    PFpage *page;
    key.arrayname = SVD_res[0]->desc.object_name;
    key.attrname = "i";
    key.dim_len = 2;
    key.dcoords = tile_coord;

    BF_GetBuf(key, &page);
    assert(page->pagebuf_len == 1000 * 100 * sizeof(double));
    BF_UnpinBuf(key);

    key.arrayname = SVD_res[1]->desc.object_name;
    BF_GetBuf(key, &page);
    assert(page->pagebuf_len == 100 * 100 * sizeof(double));
    BF_UnpinBuf(key);

    key.arrayname = SVD_res[2]->desc.object_name;
    BF_GetBuf(key, &page);
    assert(page->pagebuf_len == 100 * 100 * sizeof(double));
    BF_UnpinBuf(key);

    free_node(SVD_res[0]);
    free_node(SVD_res[1]);
    free_node(SVD_res[2]);
    free_node(A);
    free(SVD_res);

    BF_Detach();
    BF_Free();
    printf("test_dense_svd passed!\n");
}

int main(void)
{
    // test_sparse_csr_add_lhs_larger();
    // test_dense_norm();
    // test_sparse_norm();
    // test_dense_matmul();
    // test_sparse_matmul();
    test_dense_svd();

    return 0;
}