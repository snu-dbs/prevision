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

#include "tilestore.h"
#include "hashtable.h"


void sigmoid(void *_opnd_bufptr, void *_result_bufptr, uint64_t numCells) {
    double *opnd_bufptr = _opnd_bufptr;
    double *result_bufptr = _result_bufptr;

    for(int i=0; i<numCells; i++){
        result_bufptr[i] = 1/ (1 + exp(-((double) opnd_bufptr[i])));
    }
}

void sigmoid_sparse(
        uint64_t* opnd_idxptr, uint64_t* opnd_indices, void *_opnd_bufptr, uint64_t nrows, 
        void *_res_bufptr, uint64_t numCells) {   
    double *opnd_bufptr = _opnd_bufptr;
    double *res_bufptr = _res_bufptr;

    uint64_t ncols = numCells / nrows;

    // zero value
    double _default = 1.0 / (double)(1.0 + exp(0));
    memset(res_bufptr, _default, numCells * sizeof(double));

    for (uint64_t i = 0; i < nrows; i++) // for each row in one tile
    {
        uint64_t opnd_pos = opnd_idxptr[i];
        uint64_t opnd_end = opnd_idxptr[i + 1];

        while (opnd_pos < opnd_end) {
            uint64_t opnd_col = opnd_indices[opnd_pos];

            double res_value = 1.0 / (double)(1.0 + exp((double)-opnd_bufptr[opnd_pos]));
            uint64_t row_idx = i;
            uint64_t col_idx = opnd_indices[opnd_pos];
            res_bufptr[row_idx * ncols + col_idx] = res_value;

            opnd_pos++;
        }
    }
}

void LR_dense_test()
{

    printf("%s started\n", __func__);

    char *arrname = "__dense_10000x10000_X",
         *vec__y = "__dense_10000x1_vec_y",
         *vec__w = "__dense_10000x1_vec_w";

    /**************
     CREATE ARRAYS
    ***************/
    uint32_t num_of_dim = 2;

    uint64_t vec_size[] = {10000, 1};
    uint64_t vec_tile_extents[] = {500, 1};

    uint64_t array_size[] = {10000, 10000};
    uint64_t tile_extents[] = {500, 500};

    int res1, res2, res3;
    res1 = tilestore_create_array(arrname, array_size, tile_extents, num_of_dim, TILESTORE_FLOAT64, TILESTORE_DENSE);
    res2 = tilestore_create_array(vec__y, vec_size, vec_tile_extents, num_of_dim, TILESTORE_FLOAT64, TILESTORE_DENSE);
    res3 = tilestore_create_array(vec__w, vec_size, vec_tile_extents, num_of_dim, TILESTORE_FLOAT64, TILESTORE_DENSE);
    printf("create: %d %d %d \n", res1, res2, res3);

    /**************
     WRITE ARRAYS
    ***************/

    // for n*n array
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);
    size_t data_size_unaligned = sizeof(double) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    double *data_unaligned = malloc(data_size + 511);
    double *data = aligned_512bytes_ptr(data_unaligned);

    for (size_t i = 0; i < num_of_elems; i++)
        data[i] = (double)0.135081;

    for (uint32_t i = 0; i < 20; i++)
    {
        for (uint32_t j = 0; j < 20; j++)
        {
            uint64_t tile_coords[] = {i, j};

            res1 = tilestore_write_dense(arrname,
                                         tile_coords, num_of_dim, data, data_size_unaligned, TILESTORE_DIRECT);
        }
    }
    printf("write: %d\n", res1);

    // for vectors
    size_t vec_num_of_elems = (vec_tile_extents[0] * vec_tile_extents[1]);
    size_t vec_data_size_unaligned = sizeof(double) * vec_num_of_elems;
    size_t vec_data_size = ceil_to_512bytes(vec_data_size_unaligned);
    double *vec_data_unaligned = malloc(vec_data_size + 511);
    double *vec_data = aligned_512bytes_ptr(vec_data_unaligned);

    for (size_t i = 0; i < vec_num_of_elems; i++)
        vec_data[i] = (double)0.330168;

    for (uint32_t i = 0; i < 20; i++)
    {
        for (uint32_t j = 0; j < 1; j++)
        {
            uint64_t tile_coords[] = {i, j};

            res2 = tilestore_write_dense(vec__y,
                                         tile_coords, num_of_dim, vec_data, vec_data_size_unaligned, TILESTORE_DIRECT);
        }
    }
    printf("write: %d\n", res2);

    for (size_t i = 0; i < vec_num_of_elems; i++)
        vec_data[i] = (double)0.271229;

    for (uint32_t i = 0; i < 20; i++)
    {
        for (uint32_t j = 0; j < 1; j++)
        {
            uint64_t tile_coords[] = {i, j};

            res3 = tilestore_write_dense(vec__w,
                                         tile_coords, num_of_dim, vec_data, vec_data_size_unaligned, TILESTORE_DIRECT);
        }
    }
    printf("write: %d\n", res3);

    /***********
     COMPUTE LR
    ************/
    BF_Init();
    BF_Attach();

    Array *X = open_array(arrname); // n*n
    Array *w = open_array(vec__w);  // n*1
    Array *y = open_array(vec__y);  // n*1

    uint32_t trans_order[] = {1, 0};
    int iteration = 3;
    double alpha = 0.000001;
    Array *Xw, *sigmoid_temp1, *temp2, *Xt, *aXt, *temp3;

    for (int i = 0; i < iteration; i++)
    {

        Xw = matmul(X, w);                                   // n*1
        sigmoid_temp1 = map(Xw, sigmoid, sigmoid_sparse, TILESTORE_FLOAT64); // n*1
        temp2 = elemwise_op(sigmoid_temp1, y, OP_SUB);       // n*1

        Xt = transpose(X, trans_order); // n*n
        aXt = elemwise_op_constant(Xt, &alpha, TILESTORE_FLOAT64, LHS, OP_PRODUCT_CONSTANT);

        temp3 = matmul(aXt, temp2); // n*1
        w = elemwise_op(w, temp3, OP_SUB);
    }
    execute(w);

    BF_Detach();
    BF_Free();

    /************
     READ RESULT
    *************/
    // check the final result vector w
    for (uint32_t i = 0; i < 20; i++)
    {
        for (uint32_t j = 0; j < 1; j++)
        {
            uint64_t tile_coords[] = {i, j};

            // first, read metadata of tile
            tilestore_tile_metadata_t metadata;
            res1 = tilestore_read_metadata_of_tile(w->desc.object_name,
                                                   tile_coords, num_of_dim, &metadata);

            res2 = tilestore_read_dense(w->desc.object_name,
                                        metadata, vec_data, vec_data_size_unaligned, TILESTORE_DIRECT);
            // for(int i=0; i<5; i++) {
            //     // assert(vec_data[i] == (double) 0.268515); // WHY assert NOT working..?
            //     printf("%f ", vec_data[i]);
            // }
            // printf("\n");
        }
    }
    // printf("read: metadata %d, data %d\n", res1, res2);

    // free resources
    free_node(X);
    free_node(w);
    free_node(y);

    free_node(Xw);
    free_node(sigmoid_temp1);
    free_node(temp2);
    free_node(temp3);
    free_node(Xt);
    free_node(aXt);

    free(data_unaligned);
    free(vec_data_unaligned);

    printf("%s finished\n", __func__);
}

void LR_sparse_test() {
    
    printf("%s started\n", __func__);

    char *arrname = "__sparse_10000x10000_X_1pct", // N*N matrix, 1% density, fill with value 0.25
        *vec__y = "__sparse_10000x1_vec_y",        // N*1 vector, 1% density, fill with value 0.33
            *vec__w = "__sparse_10000x1_vec_w";    // N*1 vector, 1% density, fill with value 0.27

    /**************
     CREATE ARRAYS
    ***************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {0, 9999, 0, 9999};
    int tile_extents[] = {1000, 1000};

    int vec_dim_domain[] = {0, 9999, 0, 0};
    int vec_tile_extents[] = {1000, 1};

    storage_util_create_array(arrname, TILESTORE_SPARSE_CSR,
                              dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, false);
    storage_util_create_array(vec__y, TILESTORE_SPARSE_CSR,
                              vec_dim_domain, vec_tile_extents, 2, 1, TILESTORE_FLOAT64, false);
    storage_util_create_array(vec__w, TILESTORE_SPARSE_CSR,
                              vec_dim_domain, vec_tile_extents, 2, 1, TILESTORE_FLOAT64, false);

    /**************
     WRITE ARRAYS
    ***************/
    // Array Write
    array_key arr_key;
    uint64_t tile_coord[2];
    PFpage *arr_page;
    uint64_t *arr_indptr, *arr_indices;

    arr_key.arrayname = arrname;
    arr_key.attrname = "i";
    arr_key.dim_len = 2;
    arr_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 10; col++) // 1) for each tile  (total 10x10 tiles)
        {
            tile_coord[1] = col;

            arr_key.dcoords = tile_coord;
            BF_GetBuf(arr_key, &arr_page); // 2) get the pagebuffer with coordinates

            // 1% density
            // FIXME: I update it without testing
            BF_ResizeBuf(arr_page, 10000);
            // while (bf_util_pagebuf_get_coords_lens(arr_page)[1] / sizeof(uint64_t) < 10000)
            // {
            //     bf_util_double_pagebuf(arr_page);
            //     bf_util_double_coords(arr_page);
            // }

            arr_indptr = bf_util_pagebuf_get_coords(arr_page, 0);
            arr_indices = bf_util_pagebuf_get_coords(arr_page, 1);

            // WRITE
            arr_indptr[0] = 0;
            for (uint64_t i = 0; i < 1000; i++)
            {
                arr_indptr[i + 1] = 10 + arr_indptr[i];

                for (uint64_t j = 0; j < 20; j++)
                {
                    if (j % 2 == 0)
                    {
                        arr_indices[10 * i + j / 2] = 100 * (j / 2);
                        bf_util_pagebuf_set_double(arr_page, 10 * i + j / 2, 0.25); // cell value 0.25)
                    }
                }
            }
            bf_util_pagebuf_set_unfilled_idx(arr_page, 10000);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(arr_page, 10000 * sizeof(double));

            BF_TouchBuf(arr_key);
            BF_UnpinBuf(arr_key);
        }
    }

    // Vector Write
    array_key vec_key;
    // uint64_t tile_coord[2];
    PFpage *vec_page;
    uint64_t *vec_indptr, *vec_indices;

    // WRITE vec__y
    vec_key.arrayname = vec__y;
    vec_key.attrname = "i";
    vec_key.dim_len = 2;
    vec_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 1; col++) // 1) for each tile
        {
            tile_coord[1] = col;

            vec_key.dcoords = tile_coord;
            BF_GetBuf(vec_key, &vec_page); // 2) get the pagebuffer with coordinates

            // 1% density
            // FIXME: I updated it without testing
            BF_ResizeBuf(vec_page, 100);
            // while (bf_util_pagebuf_get_coords_lens(vec_page)[1] / sizeof(uint64_t) < 100)
            // {
            //     bf_util_double_pagebuf(vec_page);
            //     bf_util_double_coords(vec_page);
            // }

            // set the CSR ptrs
            vec_indptr = bf_util_pagebuf_get_coords(vec_page, 0);
            vec_indices = bf_util_pagebuf_get_coords(vec_page, 1);

            // WRITE
            vec_indptr[0] = 0;
            for (uint64_t i = 0; i < 1000; i++) // for each row per tile
            {
                if (i % 100 == 0)
                {
                    vec_indptr[i + 1] = 1 + vec_indptr[i];
                    vec_indices[i / 100] = 0;
                    bf_util_pagebuf_set_double(vec_page, i / 100, 0.33);
                }
                else
                {
                    vec_indptr[i + 1] = vec_indptr[i];
                }
            }
            bf_util_pagebuf_set_unfilled_idx(vec_page, 100);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(vec_page, 100 * sizeof(double));

            BF_TouchBuf(vec_key);
            BF_UnpinBuf(vec_key);
        }
    }

    // WRITE vec__w
    vec_key.arrayname = vec__w;
    vec_key.attrname = "i";
    vec_key.dim_len = 2;
    vec_key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 1; col++) // 1) for each tile
        {
            tile_coord[1] = col;

            vec_key.dcoords = tile_coord;
            BF_GetBuf(vec_key, &vec_page); // 2) get the pagebuffer with coordinates

            // 1% density
            // FIXME: I updated it without testing
            BF_ResizeBuf(vec_page, 100);
            // while (bf_util_pagebuf_get_coords_lens(vec_page)[1] / sizeof(uint64_t) < 100)
            // {
            //     bf_util_double_pagebuf(vec_page);
            //     bf_util_double_coords(vec_page);
            // }

            // set the CSR ptrs
            vec_indptr = bf_util_pagebuf_get_coords(vec_page, 0);
            vec_indices = bf_util_pagebuf_get_coords(vec_page, 1);

            // WRITE
            vec_indptr[0] = 0;
            for (uint64_t i = 0; i < 1000; i++) // for each row per tile
            {
                if (i % 100 == 0)
                {
                    vec_indptr[i + 1] = 1 + vec_indptr[i];
                    vec_indices[i / 100] = 0;
                    bf_util_pagebuf_set_double(vec_page, i / 100, 0.27); // cell value 0.27
                }
                else
                {
                    vec_indptr[i + 1] = vec_indptr[i];
                }
            }
            bf_util_pagebuf_set_unfilled_idx(vec_page, 100);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(vec_page, 100 * sizeof(double));

            BF_TouchBuf(vec_key);
            BF_UnpinBuf(vec_key);
        }
    }

    BF_Detach();
    BF_Free();

    /***********
     COMPUTE LR
    ************/
    BF_Init();
    BF_Attach();

    Array *X = open_array(arrname); // n*n
    Array *w = open_array(vec__w);  // n*1
    Array *y = open_array(vec__y);  // n*1
    X->desc.array_type = TILESTORE_SPARSE_CSR;
    w->desc.array_type = TILESTORE_SPARSE_CSR;
    y->desc.array_type = TILESTORE_SPARSE_CSR;

    uint32_t trans_order[] = {1, 0};
    int noi = 3;
    double alpha = 0.000001;
    Array *Xw, *sigmoid_res, *yDiff, *Xt, *rhs, *XtDiff;
    for (int i = 0; i < noi; i++)
    {
        Xw = matmul(X, w);                                               // n*1
        sigmoid_res = map(Xw, sigmoid, sigmoid_sparse, TILESTORE_FLOAT64); // n*1
        yDiff = elemwise_op(sigmoid_res, y, OP_SUB);                     // n*1

        Xt = transpose(X, trans_order); // n*n
        XtDiff = matmul(Xt, yDiff);     // n*1

        rhs = elemwise_op_constant(XtDiff, &alpha, TILESTORE_FLOAT64, LHS, OP_PRODUCT_CONSTANT);
        w = elemwise_op(w, rhs, OP_SUB);

        continue;
        // w = execute(w);

        // Array *RMSE_diff = elemwise_op(map(matmul(X, w), sigmoid, TILESTORE_FLOAT64), y, OP_SUB);
        // Array *RMSE_hat = aggr(elemwise_op(RMSE_diff, RMSE_diff, OP_PRODUCT), 1, NULL, 0);
        // RMSE_hat = execute(RMSE_hat);
        // double rmse = sqrt(RMSE_hat->scalar_val_double / (10000000 * 1));
        // fprintf(stderr, "iter=%d, rmse=%lf\n", i, rmse);
    }
    w = execute(w);

    BF_Detach();
    BF_Free();

    /************
     READ RESULT
    *************/
    BF_Init();
    BF_Attach();

    array_key res_key;
    PFpage *res_page;
    // uint64_t tile_coord[2];
    uint64_t *res_indptr, *res_indices;
    double *res_buf;
    res_key.attrname = "i";
    res_key.dim_len = 2;
    res_key.arrayname = w->desc.object_name;

    for (int row = 0; row < 10; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 1; col++)
        {
            tile_coord[1] = col;

            res_key.dcoords = tile_coord;
            BF_GetBuf(res_key, &res_page);

            uint64_t *res_coord_sizes = bf_util_pagebuf_get_coords_lens(res_page);
            printf("%ld, %ld\n", res_coord_sizes[0] / sizeof(uint64_t) - 1, res_coord_sizes[1] / sizeof(uint64_t));
            res_indptr = bf_util_pagebuf_get_coords(res_page, 0);
            res_indices = bf_util_pagebuf_get_coords(res_page, 1);
            res_buf = (double *) bf_util_get_pagebuf(res_page);

            assert(res_indptr[0] == 0);
            for (uint64_t i = 0; i < 10; i++)
            {
                printf("%f ", res_buf[i]);
                // //     // assert(res_indptr);
                // //     if(i%100==0)
                // //     {
                // //         assert(res_indices[ i/100] == 0);
                // //         printf("%f ", res_buf[i/100]);
                // //         // assert(res_buf[ i/100] == 0.27);
                // //     }
            }
        }
    }

    BF_Detach();
    BF_Free();

    free_node(X);
    free_node(w);
    free_node(y);
    free_node(Xw);
    // free_node(sigmoid_temp1);
    // free_node(temp2);
    // free_node(temp3);
    // free_node(Xt);
    // free_node(aXt);

    printf("%s finished\n", __func__);
}


void LR_sparse_test_slab() {
    
    printf("%s started\n", __func__);

    char *arrname = "400000000x100_sparse_0.0125", // N*N matrix, 1% density, fill with value 0.25
        *vec__y = "400000000x1_sparse_0.0125",     // N*1 vector, 1% density, fill with value 0.33
            *vec__w = "100x1_sparse_0.0125";       // N*1 vector, 1% density, fill with value 0.27

    /***********
     COMPUTE LR
    ************/
    BF_Init();
    BF_Attach();

    Array *X = open_array(arrname); // n*n
    Array *w = open_array(vec__w);  // n*1
    Array *y = open_array(vec__y);  // n*1
    X->desc.array_type = TILESTORE_SPARSE_CSR;
    w->desc.array_type = TILESTORE_SPARSE_CSR;
    y->desc.array_type = TILESTORE_SPARSE_CSR;

    uint32_t trans_order[] = {1, 0};
    int noi = 1;
    double alpha = 0.0000001;
    Array *Xw, *sigmoid_res, *yDiff, *Xt, *rhs, *XtDiff;
    for (int i = 0; i < noi; i++)
    {
        Xw = matmul(X, w);                                               // n*1
        sigmoid_res = map(Xw, sigmoid, sigmoid_sparse, TILESTORE_FLOAT64); // n*1
        yDiff = elemwise_op(sigmoid_res, y, OP_SUB);                     // n*1

        Xt = transpose(X, trans_order); // n*n
        XtDiff = matmul(Xt, yDiff);     // n*1

        rhs = elemwise_op_constant(XtDiff, &alpha, TILESTORE_FLOAT64, LHS, OP_PRODUCT_CONSTANT);
        w = elemwise_op(w, rhs, OP_SUB);

        continue;
        // w = execute(w);
    }

    // w = execute(w);
    Array *RMSE_diff = elemwise_op(map(matmul(X, w), sigmoid, sigmoid_sparse, TILESTORE_FLOAT64), y, OP_SUB);
    Array *RMSE_hat = aggr(elemwise_op(RMSE_diff, RMSE_diff, OP_PRODUCT), 1, NULL, 0);
    RMSE_hat = execute(RMSE_hat);
    // double rmse = sqrt(RMSE_hat->scalar_val_double / (10000000 * 1));
    // fprintf(stderr, "iter=%d, rmse=%lf\n", -1, rmse);

    BF_Detach();
    BF_Free();

    /************
     READ RESULT
    *************/
    BF_Init();
    BF_Attach();

    array_key res_key;
    PFpage *res_page;
    uint64_t tile_coord[2];
    uint64_t *res_indptr, *res_indices;
    double *res_buf;
    res_key.attrname = "i";
    res_key.dim_len = 2;
    res_key.arrayname = w->desc.object_name;

    for (int row = 0; row < 1; row++)
    {
        tile_coord[0] = row;

        for (int col = 0; col < 1; col++)
        {
            tile_coord[1] = col;

            res_key.dcoords = tile_coord;
            BF_GetBuf(res_key, &res_page);

            uint64_t *res_coord_sizes = bf_util_pagebuf_get_coords_lens(res_page);
            // printf("%ld, %ld\n", res_coord_sizes[0] / sizeof(uint64_t) -1, res_coord_sizes[1] / sizeof(uint64_t));
            res_indptr = bf_util_pagebuf_get_coords(res_page, 0);
            res_indices = bf_util_pagebuf_get_coords(res_page, 1);
            res_buf = (double *) bf_util_get_pagebuf(res_page);

            assert(res_indptr[0] == 0);
            for (uint64_t i = 0; i < 1; i++)
            {
                printf("%f ", res_buf[i]);
                // //     // assert(res_indptr);
                // //     if(i%100==0)
                // //     {
                // //         assert(res_indices[ i/100] == 0);
                // //         printf("%f ", res_buf[i/100]);
                // //         // assert(res_buf[ i/100] == 0.27);
                // //     }
            }
        }
    }

    BF_Detach();
    BF_Free();

    free_node(X);
    free_node(w);
    free_node(y);
    free_node(Xw);
    free_node(sigmoid_res);
    free_node(yDiff);
    free_node(Xt);
    free_node(XtDiff);
    free_node(rhs);

    printf("%s finished\n", __func__);
}

void eval_NMF()
{

    struct timeval start, end;
    gettimeofday(&start, NULL);

    uint32_t general_tranpose_order[] = {1, 0};

    BF_Init();
    BF_Attach();

    // Construct DAG
    char *array_name = "400000000x100_sparse_0.0125";
    char *w_name = "400000000x10_sparse_0.0125";
    char *h_name = "10x100_sparse_0.0125";

    Array *X = open_array(array_name);
    Array *W = open_array(w_name);
    Array *H = open_array(h_name);

    X->desc.array_type = TILESTORE_SPARSE_CSR;
    W->desc.array_type = TILESTORE_SPARSE_CSR;
    H->desc.array_type = TILESTORE_SPARSE_CSR;

    for (int i = 0; i < 1; i++)
    {
        /* W part */
        // X @ H^T
        Array *Ht = transpose(H, general_tranpose_order);
        Array *XHt = matmul(X, Ht);

        // W @ H @ H^T
        // Array *WHHt = matmul(matmul(W, H), Ht);

        // RHS
        // Array *W_RHS = elemwise_op(XHt, WHHt, OP_DIV);
        // W = elemwise_op(W, W_RHS, OP_PRODUCT);
        // W = execute(W);

        /* H part */
        // W^T @ X
        // Array *Wt = transpose(W, general_tranpose_order);
        // Array *WtX = matmul(Wt, X);

        // // W^T @ W @ H
        // Array *WtWH = matmul(matmul(Wt, W), H);

        // // RHS
        // Array *H_RHS = elemwise_op(WtX, WtWH, OP_DIV);
        // H = elemwise_op(H, H_RHS, OP_PRODUCT);
        // H = execute(H);

        execute(XHt);
        continue;
    }
    // W = execute(W);
    // H = execute(H);

    BF_Detach();
    BF_Free();
}


void retile_test() {
    char *arrayname = "noexist";
    uint64_t dim_domains[2][2] = {{0, 99}, {0, 99}};
    uint64_t tilesize[2] = {10, 10};

    BF_Init();
    BF_Attach();

    create_temparray_with_tilesize(
        arrayname, (uint64_t**) dim_domains, 2, TILESTORE_DENSE, TILESTORE_FLOAT64, tilesize);
    
    Array *arr = open_array(arrayname);

    uint64_t tilesize1[] = {10, 10};
    Array *res = retile(arr, tilesize1);
    
    execute(res);

    BF_Detach();
    BF_Free();

    // free_node(res);
    // free_node(arr);
    return;
}

void _test_trans_dm_create_array() {
    // make a temp array
    int array_domain[] = {0, 10000 - 1, 0, 10000 - 1};
    int tile_size[] = {100, 100};
    storage_util_create_array(
        "test_trans_dm", 
        TILESTORE_SPARSE_CSR,        // obsolete
        array_domain,
        tile_size,
        2,
        1,
        TILESTORE_FLOAT64,
        false);

    // array_key and coords
    array_key key;
    key.arrayname = "test_trans_dm";
    key.attrname = "i";
    key.dim_len = 2;
    key.emptytile_template = BF_EMPTYTILE_SPARSE_CSR;
    uint64_t coords[] = {0, 0};
    key.dcoords = coords;

    double inc = 1;

    for (uint64_t i = 0; i < 100; i++) {
        for (uint64_t j = 0; j < 100; j++) {
            coords[0] = i, coords[1] = j;

            PFpage *tile;
            assert(BF_GetBuf(key, &tile) == BFE_OK);
            assert(BF_ResizeBuf(tile, 10 * 100) == BFE_OK);

            uint64_t *indptr = bf_util_pagebuf_get_coords(tile, 0);
            uint64_t *indices = bf_util_pagebuf_get_coords(tile, 1);
            double *pagebuf = bf_util_get_pagebuf(tile);
            uint64_t idx = 0;
            for (uint64_t ii = 0; ii < 100; ii++) {
                for (uint64_t jj = 0; jj < 100; jj++) {
                    uint64_t p = jj % 10;
                    bool write = p == 0;
                    // uint64_t p = j % 3;
                    // bool write = p == 0 || p == 1;
                    if (!write) continue;

                    pagebuf[idx] = inc++;
                    indices[idx] = jj;
                    indptr[ii + 1]++;
                    idx++;
                    
                    // if ((int) inc % 123 == 0) {
                    //     printf("%lu,%lu,%lf\n", i * 100 + ii, j * 100 + jj, inc - 1);
                    //     fprintf(stderr, "%lu,%lu,%lf\n", j * 100 + jj, i * 100 + ii, inc - 1);
                    // }
                }
                if (ii < 99) indptr[ii + 2] = indptr[ii + 1];
            }

            // printf("%lu\n", idx);

            bf_util_pagebuf_set_unfilled_idx(tile, 1000);
            bf_util_pagebuf_set_unfilled_pagebuf_offset(tile, 1000 * sizeof(double));

            assert(BF_TouchBuf(key) == BFE_OK);
            assert(BF_UnpinBuf(key) == BFE_OK);
        }
    }

}

void _test_trans_dm_delete_array() {
    storage_util_delete_array("test_trans_dm");
}


void test_trans_dm() {
    BF_Init();
    BF_Attach();
    
    _test_trans_dm_create_array();

    Array *arr = open_array("test_trans_dm");
    uint32_t o[2] = {1, 0};
    Array *trans = transpose(arr, o);
    execute(trans);

    free_node(arr);
    free_node(trans);

    BF_Detach();
    BF_Free();

    // _test_trans_dm_delete_array();
    return;
}

int main(){
    retile_test();
    return 0;

    test_trans_dm();
    return 0;
}
