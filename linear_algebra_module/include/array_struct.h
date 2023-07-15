#ifndef __ARRAY_STRUCT_H__
#define __ARRAY_STRUCT_H__

#include <stddef.h>
#include <stdint.h>
#include "chunk_struct.h"

#include "tilestore.h"

#define DEFAULT_EXTENT_SIZE 10000000

/**
 * Struct definitions for ParamInfo
 */
typedef struct Range
{
    uint64_t *lb;  // lower bound
    uint64_t *ub;  // upper bound
    uint32_t ndim; // number of dimensions
} Range;

typedef struct MatMul {
    bool lhs_transposed;
    bool rhs_transposed;
} MatMul;

typedef struct Transposition
{
    uint32_t *transpose_func; // M
    uint32_t ndim;            // number of dimensions
} Transposition;

typedef struct GroupyBy
{
    int agg_func;
    uint32_t agg_groupbylen;
    uint32_t *groupby;
    void **state;
} GroupBy;

typedef struct Window
{
    int window_func;
    int64_t *window_size;
} Window;

typedef struct Lambda
{
    void (*lambda_func)(void *, void *, uint64_t);
    void (*lambda_func_sparse)(uint64_t *, uint64_t *, void *, uint64_t *, uint64_t *, void *, uint64_t, uint64_t *);
    int return_type;
} Lambda;

typedef struct ElemwiseConstant
{
    void *constant;
    int constant_type;
    bool equation_side;
} ElemwiseConstant;

typedef struct SVD
{
    struct Array *svd_u;
} SVD;

typedef struct NewArray
{
    bool is_sparse;
    double density;
    double default_value;
} NewArray;

/**
 *  Param Info
 */
typedef struct ParamInfo
{
    Range range;
    MatMul matmul;
    Transposition trans;
    GroupBy groupBy;
    Window window;
    Lambda lambda;
    ElemwiseConstant elemConst;
    SVD svd;
    NewArray newarr;
} ParamInfo;

/**
 *  Descriptor for storing array schema
 */
typedef struct Descriptor
{
    char *object_name; // name of this array structure itself

    tilestore_format_t array_type;
    tilestore_datatype_t attr_type;  // assume single attr
    char *attr_name;
    uint32_t attr_len;

    uint64_t **dim_domains;
    uint64_t *tile_extents;
    uint32_t dim_len;

    uint64_t **real_dim_domains;
} Descriptor;

typedef struct Array
{
    int array_num; // the number of operand arrays
    int op_type;   // expresses the type of LAM operation
    struct Array **arraylist;

    ParamInfo op_param;
    Descriptor desc;

    bool blocking_flag;
    bool *computed_list; // is a chunk computed? this is an array and a boolean can be accessed with 1D idx.
    
    bool *simulated_materialized_list; // is a chunk computed? this is an array and a boolean can be accessed with 1D idx.

    bool executed;       // is an array executed?
    bool is_expanded;       // is expanded array?

    uint64_t num_children; // the number of children
    uint8_t visited_flag;

    bool scalar_flag;
    int scalar_val_int;
    double scalar_val_double;

    bool persist;           // if true, it is stored in the disk

    struct Array *copied_array;
} Array;

typedef Array *ArrayPtr;

Range initRange();
Transposition initTrans();
GroupBy initGroupBy();
ParamInfo initParamInfo();
ParamInfo copy_param(ParamInfo src);
Descriptor copy_desc(Descriptor src);
Array *copy_node(Array *src);

void init_computed_list(Array *arr);
void init_simulated_materialized_list(Array *arr);
void mark_all_computed(Array *arr);

void free_param(ParamInfo info);
void free_desc(Descriptor desc);
void free_node(Array *node);

#endif