#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>

// #include <tiledb/tiledb.h>
#include "bf.h"
#include "chunk_interface.h"
#include "array_struct.h"
#include "exec_interface.h"
#include "node_interface.h"
#include "planner.h"

extern unsigned long long bftime, bf_this_query;

extern unsigned long long bf_tmpbuf_cnt;
extern unsigned long long malloc_time;
extern unsigned long long bf_read_io_time, bf_iread_io_time, bf_read_io_size, bf_iread_io_size;
extern unsigned long long bf_write_io_time, bf_write_io_size;
extern unsigned long long bf_getbuf_cnt_hit, bf_getbuf_cnt_total;
extern unsigned long long bf_getbuf_io_hit, bf_getbuf_io_total;
extern unsigned long long ex_min_fl_creation_time, bf_min_sl_update_time, bf_min_fl_retrival_time;

extern unsigned long long __lam_matmul_time, __lam_matmul_cnt;
extern unsigned long long __lam_trans_time, __lam_trans_cnt;
extern unsigned long long __lam_elemwise_time, __lam_elemwise_cnt;
extern unsigned long long __lam_elemwise_c_time, __lam_elemwise_c_cnt;
extern unsigned long long __lam_elemwise_m_time, __lam_elemwise_m_cnt;
extern unsigned long long __lam_newarr_cnt, __lam_newarr_time;

size_t data_size, idata_size, key_size, bf_size;


void eval_printstats() {
    printf("total\tbf\tio_r\tio_ir\tio_w\tphit\tpreq\tflgen\tflget\tsl\n");
    printf("%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n", 
        bf_this_query, bftime, bf_read_io_time, bf_iread_io_time, bf_write_io_time, bf_getbuf_cnt_hit, bf_getbuf_cnt_total, ex_min_fl_creation_time, bf_min_fl_retrival_time, bf_min_sl_update_time);
    printf("%d\t%d\t%lld\t%lld\t%lld\t%lld\t%lld\t%d\t%d\t%d\n", 
        0, 0, bf_read_io_size, bf_iread_io_size, bf_write_io_size, bf_getbuf_io_hit, bf_getbuf_io_total, 0, 0, 0);

    printf("matmul\ttrans\telem\telem_c\telem_m\tnewarr\n");
    printf("%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n", 
        __lam_matmul_time, __lam_trans_time, __lam_elemwise_time, __lam_elemwise_c_time, __lam_elemwise_m_time, __lam_newarr_time);
    printf("%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n", 
        __lam_matmul_cnt, __lam_trans_cnt, __lam_elemwise_cnt, __lam_elemwise_c_cnt, __lam_elemwise_m_cnt, __lam_newarr_cnt);

}

uint64_t get_num_of_chunk(Array *arr, uint64_t *chunk_size, uint32_t dim_len) {
    uint64_t res = 1;
    for (int d = 0; d < dim_len; d++) {
        uint64_t len = arr->desc.dim_domains[d][1] - arr->desc.dim_domains[d][0] + 1;
        uint64_t num_of_chunks_in_dim = len / chunk_size[d];
        res *= num_of_chunks_in_dim;
    }

    return res;
}

void eval_TRANS(char *array_name) {
    // prepare
    BF_Init();
    BF_Attach();

    uint64_t *tile_extents;
    uint32_t dim_len;

    storage_util_get_tile_extents(array_name, &tile_extents, &dim_len);

    // Construct DAG
    Array* X = open_array(array_name);
    uint32_t dim_order[2] = {1, 0};
    Array* Xt = transpose(X, dim_order);

    uint64_t chunk_size[2] = {tile_extents[1], tile_extents[0]};

    Xt = execute(Xt);
    // get_pos()
    // uint64_t EOCHUNK = get_num_of_chunk(Xt, chunk_size, dim_len);
    // for (uint64_t pos = 0; pos < EOCHUNK; pos++) {
    //     Chunk* chk = get_pos(Xt, pos, chunk_size);
    //     chunk_destroy(chk);
    // }

    // delete
    // free_node(X);
    // free_node(Xt);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void eval_NORM(char *array_name) {
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array* X = open_array(array_name);
    Array* RES = aggr(X, /* AGGREGATE_NORM */ 7, NULL, 0);

    // Array* X = open_array(array_name);
    // Array* XX = elemwise_op(X, X, OP_PRODUCT); 
    // Array* RES = aggr(XX, /* AGGREGATE_NORM */ 1, NULL, 0);
    
    RES = execute(RES);

    // printf("%lf\n", RES->scalar_val);
    // printf("%lf\n", sqrt(RES->scalar_val));

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void eval_GRM(char *array_name) {
    BF_Init();
    BF_Attach();

    uint64_t *tile_extents;
    uint32_t dim_len;

    storage_util_get_tile_extents(array_name, &tile_extents, &dim_len);

    // Construct DAG
    Array* X = open_array(array_name);
    uint32_t dim_order[2] = {1, 0};
    Array* Xt = transpose(X, dim_order);
    Array* RES = matmul(Xt, X);

    // get_pos()
    uint64_t chunk_size[2] = {tile_extents[1], tile_extents[1]};
    uint64_t EOCHUNK = get_num_of_chunk(RES, chunk_size, dim_len);
    for (uint64_t pos = 0; pos < EOCHUNK; pos++) {
        Chunk* chk = get_pos(RES, pos, chunk_size);
        chunk_destroy(chk);
    }

    // delete
    free_node(X);
    free_node(Xt);
    free_node(RES);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void eval_MVM(char *lhs_array_name, char *rhs_array_name) {
    BF_Init();
    BF_Attach();

    uint64_t *lhs_tile_extents;
    uint64_t *rhs_tile_extents;
    uint32_t dim_len;

    storage_util_get_tile_extents(lhs_array_name, &lhs_tile_extents, &dim_len);
    storage_util_get_tile_extents(rhs_array_name, &rhs_tile_extents, &dim_len);

    // Construct DAG
    Array* X = open_array(lhs_array_name);
    Array* w = open_array(rhs_array_name);
    Array* RES = matmul(X, w);

    // get_pos()
    uint64_t chunk_size[2] = {lhs_tile_extents[0], rhs_tile_extents[1]};
    uint64_t EOCHUNK = get_num_of_chunk(RES, chunk_size, dim_len);
    for (uint64_t pos = 0; pos < EOCHUNK; pos++) {
        Chunk* chk = get_pos(RES, pos, chunk_size);
        chunk_destroy(chk);
    }

    // delete
    free_node(X);
    free_node(w);
    free_node(RES);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void eval_ADD(char *lhs_array_name, char *rhs_array_name) {
    struct timeval start, end;
    gettimeofday(&start, NULL);

    BF_Init();
    BF_Attach();

    // Construct DAG
    Array* M = open_array(lhs_array_name);
    Array* N = open_array(rhs_array_name);
    Array* RES = elemwise_op(M, N, OP_ADD);

    // computation
    execute(RES);

    // delete
    free_node(M);
    free_node(N);
    free_node(RES);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void eval_GMM(char *lhs_array_name, char *rhs_array_name) {
    BF_Init();
    BF_Attach();

    // Construct DAG
    Array* M = open_array(lhs_array_name);
    Array* N = open_array(rhs_array_name);
    Array* RES = matmul(M, N);

    // computation
    execute(RES);

    // delete
    free_node(M);
    free_node(N);
    free_node(RES);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void eval_MANYADD(char *m_name, char *n_name, char *o_name, char *p_name, char *q_name) {
    BF_Init();
    BF_Attach();

    uint64_t *tile_extents;
    uint32_t dim_len;

    storage_util_get_tile_extents(m_name, &tile_extents, &dim_len);

    // Construct DAG
    Array* M = open_array(m_name);
    Array* N = open_array(n_name);
    Array* O = open_array(o_name);
    Array* P = open_array(p_name);
    Array* Q = open_array(q_name);

    Array* MN = elemwise_op(M, N, OP_ADD);
    Array* OP = elemwise_op(O, P, OP_ADD);
    Array* MNOP = elemwise_op(MN, OP, OP_ADD);
    Array* RES = elemwise_op(MNOP, Q, OP_ADD);

    // get_pos()
    uint64_t EOCHUNK = get_num_of_chunk(RES, tile_extents, dim_len);
    for (uint64_t pos = 0; pos < EOCHUNK; pos++) {
        Chunk* chk = get_pos(RES, pos, tile_extents);
        chunk_destroy(chk);
    }

    // delete
    free_node(M);
    free_node(N);
    free_node(O);
    free_node(P);
    free_node(Q);

    free_node(MN);
    free_node(OP);
    free_node(MNOP);
    free_node(RES);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}


void eval_NMF(
        char *array_name,
        int noi,        /* number of iterations */
        int rank,
	char *w_name,
	char *h_name) {

    struct timeval start, end;
    gettimeofday(&start, NULL);

    uint32_t general_tranpose_order[] = {1, 0};

    BF_Init();
    BF_Attach();

    // Construct DAG
    Array* X = open_array(array_name);
    // Array* W, H;        /* How to set random values? */
    // TODO: expose creating temp array function to user, map() function 
    // using rank
    Array* W = open_array(w_name);
    Array* H = open_array(h_name);

    for (int i = 0; i < noi; i++) {
        /* W part */
        // X @ H^T
        Array *Ht = transpose(H, general_tranpose_order);   // 100 x 10
        Array *XHt = matmul(X, Ht);                         // 10M x 10

        // W @ H @ H^T
        Array *WHHt = matmul(matmul(W, H), Ht);             // 10M x 100, 10M x 10

        // RHS
        Array *W_RHS = elemwise_op(XHt, WHHt, OP_DIV);      // 10M x 10
        W = elemwise_op(W, W_RHS, OP_PRODUCT);              // 10M x 10
        // W = execute(W);                         

        /* H part */
        // W^T @ X
        Array *Wt = transpose(W, general_tranpose_order);   // 10 x 10M
        Array *WtX = matmul(Wt, X);                         // 10 x 100

        // W^T @ W @ H
        Array *WtWH = matmul(matmul(Wt, W), H);             // 10 x 10, 10 x 100

        // RHS
        Array *H_RHS = elemwise_op(WtX, WtWH, OP_DIV);      // 10 x 100
        H = elemwise_op(H, H_RHS, OP_PRODUCT);              // 10 x 100
        // H = execute(H);

	    continue;   

        Array *RMSE_diff = elemwise_op(matmul(W, H), X, OP_SUB);
        Array *RMSE_hat = aggr(elemwise_op(RMSE_diff, RMSE_diff, OP_PRODUCT), 1, NULL, 0);
        RMSE_hat = execute(RMSE_hat);
        double rmse = sqrt(RMSE_hat->scalar_val_double / (10000000 * 100));
        fprintf(stderr, "iter=%d, rmse=%lf\n", i, rmse);
    }

    // W->persist = H->persist = true;
    // H = execute(H);

    
    Array *RMSE_diff = elemwise_op(matmul(W, H), X, OP_SUB);
    Array *RMSE_hat = aggr(elemwise_op(RMSE_diff, RMSE_diff, OP_PRODUCT), 1, NULL, 0);
    RMSE_hat = execute(RMSE_hat);
    double rmse = sqrt(RMSE_hat->scalar_val_double / (10000000 * 100));
    fprintf(stderr, "final rmse=%lf\n", rmse);
    

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); BF_Free();
}

void sigmoid(void *_opnd_bufptr, void *_result_bufptr, uint64_t num_cells) {
    double *opnd_bufptr = _opnd_bufptr;
    double *result_bufptr = _result_bufptr;

    for(int i=0; i<num_cells; i++){
        result_bufptr[i] = 1/ (1 + exp(-((double) opnd_bufptr[i])));
    }
}

void sigmoid_sparse(uint64_t* opnd_idxptr, uint64_t* opnd_indices, void *_opnd_bufptr,
                    uint64_t* res_idxptr, uint64_t* res_indices, void *_res_bufptr,
                    uint64_t nrows, uint64_t* nnz)
{   
    double *opnd_bufptr = _opnd_bufptr;
    double *res_bufptr = _res_bufptr;

    for (uint64_t i = 0; i < nrows; i++) // for each row in one tile
    {
        uint64_t opnd_pos = opnd_idxptr[i];
        uint64_t opnd_end = opnd_idxptr[i + 1];

        while (opnd_pos < opnd_end)
        {
            uint64_t opnd_col = opnd_indices[opnd_pos];

            double res_value = 1.0 / (double)(1.0 + exp((double)-opnd_bufptr[opnd_pos]));
            if (res_value != 0)
            {
                res_indices[(*nnz)] = opnd_col;
                res_bufptr[(*nnz)] = res_value;
                (*nnz)++;
            }
            opnd_pos++;
        }
        res_idxptr[i + 1] = *nnz;
    }
}

void eval_LR(
        char *array_name,
        char *y_name,
        char *w_name,
        int noi,        /* number of iterations */
        double alpha
    ) {

    uint32_t trans_order[] = {1,0};

    BF_Init();
    BF_Attach();

    Array* X = open_array(array_name); // n*n
    Array* w = open_array(w_name);  // n*1 
    Array* y = open_array(y_name);  // n*1

    Array *Xw, *sigmoid_res, *yDiff, *Xt, *rhs, *XtDiff;
    for (int i = 0; i < noi; i++) {
        Xw = matmul(X, w);  // n*1
        sigmoid_res = map(Xw, sigmoid, sigmoid_sparse, TILESTORE_FLOAT64); // n*1
        yDiff = elemwise_op(sigmoid_res, y, OP_SUB); // n*1
        Xt = transpose(X, trans_order);  // n*n
        XtDiff = matmul(Xt, yDiff); // n*1
        rhs = elemwise_op_constant(XtDiff, &alpha, TILESTORE_FLOAT64, LHS, OP_PRODUCT_CONSTANT);
        w = elemwise_op(w, rhs, OP_SUB);
    }

    // w->persist = true;
    // w = execute(w);

    Array *RMSE_diff = elemwise_op(map(matmul(X, w), sigmoid, sigmoid_sparse, TILESTORE_FLOAT64), y, OP_SUB);
    Array *RMSE_hat = aggr(elemwise_op(RMSE_diff, RMSE_diff, OP_PRODUCT), 1, NULL, 0);
    RMSE_hat = execute(RMSE_hat);
    double rmse = sqrt(RMSE_hat->scalar_val_double / (10000000 * 1));
    fprintf(stderr, "iter=%d, rmse=%lf\n", -1, rmse);

    // fprintf(stderr, "Done!\n");

    eval_printstats();          // this is called here since a query flushes all buffers at the end

    BF_Detach();
    BF_Free();

    // free_node(w);
    // free_node(X); free_node(y);
    // free_node(Xw);
    // free_node(sigmoid_temp1);
    // free_node(Xt);
    // free_node(aXt);
    // free_node(temp2);
    // free_node(temp3);
}

void eval_PageRank(
        char *array_name,
        char *v_name,
        int noi,        /* number of iterations */
        double d) {

    BF_Init();
    BF_Attach();

    #define OPC_LHS false
    #define OPC_RHS true

    Array *X = open_array(array_name); // n*n
    Array *v = open_array(v_name); // n
    double N = X->desc.real_dim_domains[0][1] - X->desc.real_dim_domains[0][0] + 1;

    uint64_t ov_arrsize[] = {N, 1};
    uint64_t ov_tilesize[] = {(N + 9) / 10, 1}; 

    Array *right = full(ov_arrsize, ov_tilesize, 2, (1 - d) / N, TILESTORE_DENSE);

    for (int i = 0; i < noi; i++) {
        Array *MR = matmul(X, v);
        Array *dMR = elemwise_op_constant(MR, &d, TILESTORE_FLOAT64, OPC_LHS, OP_PRODUCT_CONSTANT);

        v = elemwise_op(dMR, right, OP_ADD);
    }

    v->persist = true;
    v = execute(v);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); 
    BF_Free();
}

void randgen(
        uint64_t array_width, 
        uint64_t array_height, 
        uint64_t tile_width, 
        uint64_t tile_height, 
        double value) {
    // prepare
    BF_Init(); BF_Attach();

    // Array Gen
    uint64_t arrsize[] = {array_width, array_height};
    uint64_t tilesize[] = {tile_width, tile_height};

    Array *out = full(arrsize, tilesize, 2, value, TILESTORE_DENSE);

    /*
    random(arrsize, tilesize, 2, TILESTORE_SPARSE_CSR);
    full(arrsize, tilesize, 2, value, TILESTORE_DENSE);
    */

    out->persist = true;
    execute(out);

    eval_printstats();          // this is called here since a query flushes all buffers at the end
    BF_Detach(); 
    BF_Free();
}

void conversion(
        uint64_t array_width, 
        uint64_t array_height, 
        uint64_t tile_width, 
        uint64_t tile_height, 
        char *path) {
    // prepare
    BF_Init(); BF_Attach();

    // CSV -> Array
    uint64_t arrsize[] = {array_width, array_height};
    uint64_t tilesize[] = {tile_width, tile_height};

    Array *out = _csv_to_sparse_with_metadata(path, arrsize, tilesize, 2);
    out->persist = true;
    execute(out);

    BF_Detach(); BF_Free();
}

int main(int argc, char *argv[]) {
    char* op = argv[1];
    if (strcmp(op, "TRANS") == 0) {
        eval_TRANS(argv[2]);
    } else if (strcmp(op, "NORM") == 0) {
        eval_NORM(argv[2]);
    } else if (strcmp(op, "GRM") == 0) {
        eval_GRM(argv[2]);
    } else if (strcmp(op, "MVM") == 0) {
        eval_MVM(argv[2], argv[3]);
    } else if (strcmp(op, "ADD") == 0) {
        eval_ADD(argv[2], argv[3]);
    } else if (strcmp(op, "GMM") == 0) {
        eval_GMM(argv[2], argv[3]);
    } else if (strcmp(op, "MANYADD") == 0) {
        eval_MANYADD(argv[2], argv[3], argv[4], argv[5], argv[6]);
    } else if (strcmp(op, "NMF") == 0) {
        int iter = atoi(argv[5]);
        eval_NMF(argv[2], iter, 10, argv[3], argv[4]);
    } else if (strcmp(op, "LR") == 0) {
        int iter = atoi(argv[5]);
        eval_LR(argv[2], argv[3], argv[4], iter, 0.0000001);
    } else if (strcmp(op, "PAGERANK") == 0) {
        int iter = atoi(argv[4]);
        eval_PageRank(argv[2], argv[3], iter, 0.85);
    } else if (strcmp(op, "CONV") == 0) {
        uint64_t array_width = (uint64_t) atol(argv[2]);
        uint64_t array_height = (uint64_t) atol(argv[3]);
        uint64_t tile_width = (uint64_t) atol(argv[4]);
        uint64_t tile_height = (uint64_t) atol(argv[5]);
        char* path = argv[6];

        conversion(array_width, array_height, tile_width, tile_height, path);
    } else if (strcmp(op, "RAND") == 0) {
        uint64_t array_width = (uint64_t) atol(argv[2]);
        uint64_t array_height = (uint64_t) atol(argv[3]);
        uint64_t tile_width = (uint64_t) atol(argv[4]);
        uint64_t tile_height = (uint64_t) atol(argv[5]);
        double value = (double) atof(argv[6]);

        randgen(array_width, array_height, tile_width, tile_height, value);
    }
}
