//
// Created by mxmdb on 22. 1. 27..
//

#ifndef LAM_EXECUTOR_NODE_INTERFACE_H
#define LAM_EXECUTOR_NODE_INTERFACE_H

#include "array_struct.h"
#include "chunk_struct.h"

/** Array Processing Engine (APE) **/

/***
 *  Function for Users
 */

/* Array Operation */
Array *open_array(char *arrayname);
Array *elemwise_op(Array *A, Array *B, int op_type);
Array *elemwise_op_constant(Array *A, void *constant, tilestore_datatype_t constant_type, int eq_side, int op_type);
Array *matmul(Array *A, Array *B);
Array *subarray(Array *A, Range range);
Array *transpose(Array *A, uint32_t *order);
Array *aggr(Array *A, int agg_func, uint32_t *groupby_dim, uint32_t groupby_len);
Array *window(Array *A, int64_t *window_size, int window_func);
Array *map(
    Array *A, 
    void (*fp_d)(void *, void *, uint64_t),
    void (*fp_s)(uint64_t *, uint64_t *, void *, uint64_t, void *, uint64_t),
    int return_type);
Array *map_dense(Array *A, void (*fp)(void *, void *, uint64_t), int return_type);
Array *map_sparse(Array *A, void (*fp)(uint64_t *, uint64_t *, void *, uint64_t *, uint64_t *, void *, uint64_t, uint64_t *), int return_type);
Array *exponential(Array *A);
Array **svd(Array *A);
Array *inv(Array *A);
Array* retile(Array* A, uint64_t *tilesize);
Array* _csv_to_sparse_with_metadata(char *csvpath, uint64_t *arrsize, uint64_t *tilesize, uint32_t dimlen);
Array* full(uint64_t *arrsize, uint64_t *tilesize, uint32_t dimlen, double value, tilestore_format_t format);

void free_node(Array *A);
void print_array(Array *A, int limit);

/* Build Range */
Range set_range(uint64_t *lower_bound, uint64_t *upper_bound, int ndim);

/* DEBUG */
void print_dag(Array *A, uint8_t order);

/* utility */
int InferTypeBinaryOp(int type1, int type2);
char *rand_string(char *str, size_t size);
void create_temparray(char *arrayname, uint64_t **dim_domain, int ndim, int attrtype);
void create_temparray_with_tilesize(char *arrayname, uint64_t **dim_domain, int ndim, int arraytype, int attrtype, uint64_t *tilesize);
void PrintArrayInfo(Array *array);
Array *scanForTest(char *arrayname, int ndim, uint64_t **dim_domains);
void PrintArrayInfoDFS(Array *A);
Array *expanded_copy_plan(Array *src);
void determine_child_chunksize(uint64_t *chunksize_parent, struct Array *array, int op_type, uint64_t **chunksize_child);
uint64_t get_num_of_chunks(Array* arr);
#endif // LAM_EXECUTOR_NODE_INTERFACE_H
