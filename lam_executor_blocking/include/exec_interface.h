#ifndef __EXEC_INTERFACE_H__
#define __EXEC_INTERFACE_H__

#include "array_struct.h"
#include "chunk_struct.h"

#define LHS 0
#define RHS 1

/********************************
 *   Executor layer interface   *
 ********************************/
void calculate_consumer_cnt(Array *array);
Array *execute(Array *root);
Chunk *get_pos(Array *res_array, uint64_t res_pos, uint64_t *res_chunksize);
double get_scalar(Array *res_array, uint64_t *res_chunksize);

Chunk *lam_operation(Chunk **chunklist, Array **arraylist, Array *result_array, uint64_t pos, uint64_t *chunksize);
Chunk *lam_operation_matmul(Array **arraylist, Array *result_array, uint64_t pos, uint64_t *chunksize_parent, uint64_t **chunksize_child);
Chunk *lam_operation_aggr(Array *result_array, uint64_t pos, uint64_t *chunksize_parent, uint64_t *chunksize_child, uint8_t *dim_selection_map, uint64_t partition_loop);
Chunk* lam_operation_retile(Array *in, Array *out, uint64_t pos, uint64_t *in_chunksize, uint64_t *out_chunksize);
Chunk *lam_operation_newarray(Array *out, uint64_t pos);

Chunk *scan_without_range(Array *array, uint64_t pos, uint64_t *chunksize);
Chunk *scan_with_range(Array *array_to_scan, Range scan_range, int attr_type, Descriptor array_desc, uint64_t pos, uint64_t *chunksize);

// temp func for debugging
void read_chunk(Chunk *chunk, uint32_t dimlen, int attrtype, uint64_t *chunksize);

/******************************************
 *      LAM Op Type Code Definition       *
 ******************************************/
#define OP_SCAN                           0
#define OP_ADD                            1
#define OP_SUB                            2
#define OP_PRODUCT                        3
#define OP_DIV                            4
#define OP_ADD_CONSTANT                   5
#define OP_SUB_CONSTANT                   6
#define OP_PRODUCT_CONSTANT               7
#define OP_DIV_CONSTANT                   8
#define OP_TRANSPOSE                      9
#define OP_SUBARRAY                       10
#define OP_JOIN                           11
#define OP_WINDOW                         12
#define OP_AGGR                           13
#define OP_MATMUL                         14
#define OP_SUBARRAYSCAN                   15
#define OP_MAP                            16
#define OP_EXPONENTIAL                    17
#define OP_SVD                            18
#define OP_INV                            19
#define OP_RETILE                         20
#define OP_NEW_ARRAY                      21

#endif
