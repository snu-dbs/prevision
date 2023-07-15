//
// Created by mxmdb on 22. 1. 27..
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <chunk_interface.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <stdlib.h>
#include <math.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "node_interface.h"
#include "exec_interface.h"
#include "helper_ops.h"

double totaltime = 0;

// the caller must free Array*
Array* _init_array(
        int array_num,
        int op_type,
        Array **array_list,
        bool blocking_flag,
        bool scalar_flag,
        uint64_t num_childern,
        bool executed,
        bool persist,
        bool is_expanded) {

    Array *newArray = malloc(sizeof(Array));

    newArray->array_num = array_num;
    newArray->op_type = op_type;
    newArray->arraylist = array_list;
    newArray->blocking_flag = blocking_flag;
    newArray->scalar_flag = scalar_flag;
    newArray->num_children = num_childern;
    newArray->executed = executed;
    newArray->persist = persist;
    newArray->is_expanded = is_expanded;
    newArray->visited_flag = 0;

    return newArray;
}

int InferArrayType(int type1, int type2)
{
    if (type1 == TILESTORE_DENSE && type2 == TILESTORE_DENSE)
        return TILESTORE_DENSE;
    if (type1 == TILESTORE_DENSE && type2 == TILESTORE_SPARSE_CSR)
        return TILESTORE_DENSE;
    if (type1 == TILESTORE_SPARSE_CSR && type2 == TILESTORE_DENSE)
        return TILESTORE_DENSE;
    if (type1 == TILESTORE_SPARSE_CSR && type2 == TILESTORE_SPARSE_CSR)
        return TILESTORE_SPARSE_CSR;
}

int InferTypeBinaryOp(int type1, int type2)
{
    if (type1 == TILESTORE_INT32 && type2 == TILESTORE_INT32)
        return TILESTORE_INT32;
    if (type1 == TILESTORE_INT32 && type2 == TILESTORE_FLOAT32)
        return TILESTORE_FLOAT64;
    if (type1 == TILESTORE_INT32 && type2 == TILESTORE_FLOAT64)
        return TILESTORE_FLOAT64;
    if (type1 == TILESTORE_FLOAT32 && type2 == TILESTORE_INT32)
        return TILESTORE_FLOAT64;
    if (type1 == TILESTORE_FLOAT32 && type2 == TILESTORE_FLOAT32)
        return TILESTORE_FLOAT32;
    if (type1 == TILESTORE_FLOAT32 && type2 == TILESTORE_FLOAT64)
        return TILESTORE_FLOAT64;
    if (type1 == TILESTORE_FLOAT64 && type2 == TILESTORE_INT32)
        return TILESTORE_FLOAT64;
    if (type1 == TILESTORE_FLOAT64 && type2 == TILESTORE_FLOAT32)
        return TILESTORE_FLOAT64;
    if (type1 == TILESTORE_FLOAT64 && type2 == TILESTORE_FLOAT64)
        return TILESTORE_FLOAT64;
}


char *genArrayName(int size)
{
    static int cnt = 0;
    char cnt_str[128];
    sprintf(cnt_str, "%d%c", cnt, '\0');

    char *name = malloc(sizeof(char) * (strlen(TMPARR_NAME_PREFIX) + strlen(cnt_str) + 1));
    strcpy(name, TMPARR_NAME_PREFIX);
    strcpy((name + strlen(TMPARR_NAME_PREFIX)), cnt_str);

    cnt++;
    return name;
}

void attrname_for_type(char type, char **dst) {
    *dst = malloc(sizeof(char) * 2);

    // 'i' : input array, 'a' : normal (including intermediate), 't' : temp tile
    if (!(type == 'i' || type == 'a' || type == 't')) assert(0);

    (*dst)[0] = type; 
    (*dst)[1] = '\0';
}

void create_temparray_with_tilesize(char *arrayname, uint64_t **dim_domain, int ndim, int arraytype, int attrtype, uint64_t *tilesize)
{
    // FIXME: attrtype

    int *_dim_domain = malloc(sizeof(int) * ndim * 2);
    int *_tilesize = malloc(sizeof(int) * ndim);

    for (uint32_t d = 0; d < ndim; d++)
    {
        _dim_domain[(d * 2)] = dim_domain[d][0];
        _dim_domain[(d * 2) + 1] = dim_domain[d][1];

        _tilesize[d] = (int)tilesize[d];
    }

    if (arraytype == TILESTORE_DENSE)
    {
        // printf("create DENSE temp\n");
        storage_util_create_array(arrayname, TILESTORE_DENSE, _dim_domain, _tilesize, ndim, 1, TILESTORE_FLOAT64, 0);
    }
    else if (arraytype == TILESTORE_SPARSE_CSR)
    {
        storage_util_create_array(arrayname, TILESTORE_SPARSE_CSR, _dim_domain, _tilesize, ndim, 1, TILESTORE_FLOAT64, 0);
    }
    free(_dim_domain);
    free(_tilesize);
}

void create_temparray(char *arrayname, uint64_t **dim_domain, int ndim, int attrtype)
{
    // FIXME: attrtype
    int *tilesize = malloc(sizeof(int) * ndim);
    int *_dim_domain = malloc(sizeof(int) * ndim * 2);
    for (uint32_t d = 0; d < ndim; d++)
    {
        _dim_domain[(d * 2)] = dim_domain[d][0];
        _dim_domain[(d * 2) + 1] = dim_domain[d][1];

        tilesize[d] = DEFAULT_EXTENT_SIZE;
    }
    storage_util_create_array(arrayname, TILESTORE_DENSE, _dim_domain, tilesize, ndim, 1, TILESTORE_FLOAT64, 0);
    free(tilesize);
    free(_dim_domain);
}

Array* open_array(char* arrayname){
    Array *newArray = _init_array(
        0, OP_SCAN, NULL, false, false, 0, false, true, false);

    ParamInfo paramInfo = initParamInfo();
    newArray->op_param = paramInfo;

    Descriptor desc;
    desc.object_name = arrayname;
    storage_util_get_array_type(arrayname, &desc.array_type);
    storage_util_get_dim_domains(arrayname, &desc.dim_domains, &desc.dim_len);
    storage_util_get_unpadded_dim_domains(arrayname, &desc.real_dim_domains, &desc.dim_len);
    storage_util_get_tile_extents(arrayname, &desc.tile_extents, &desc.dim_len);
    storage_util_get_attribute_type(arrayname, NULL, &desc.attr_type);
    attrname_for_type('i', &desc.attr_name);

    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    return newArray;
}

Array* retile(Array *A, uint64_t *tilesize) {
    A->num_children++;
    
    Array* newArray = malloc(sizeof(Array));

    // No parameter needed
    ParamInfo paramInfo = initParamInfo();
    newArray->op_param = paramInfo;

    // Initialize Array structure
    newArray->op_type = OP_RETILE;
    newArray->arraylist = malloc(sizeof(Array*));
    newArray->arraylist[0] = A;
    newArray->array_num = 1;
    newArray->blocking_flag = true;
    newArray->scalar_flag = false;
    newArray->num_children = 0;
    newArray->executed = false;
    newArray->persist = false;
    newArray->is_expanded = false;

    // description
    Descriptor desc;
    char* name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;
    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    for(int d = 0; d < desc.dim_len; d++)
    {
        desc.tile_extents[d] = tilesize[d];
    }

    desc.real_dim_domains = NULL;
    desc.dim_domains = malloc(sizeof(uint64_t*) * A->desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++){
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }
    
    desc.array_type = A->desc.array_type;
    desc.attr_type = A->desc.attr_type;
    attrname_for_type('a', &desc.attr_name);
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name, 
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array* elemwise_op(Array* A, Array* B, int op_type){
    assert(A->desc.array_type == B->desc.array_type);
    assert(A->desc.dim_len == B->desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        assert(A->desc.tile_extents[d] == B->desc.tile_extents[d]);
    }

    A->num_children++;
    B->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 2);
    arraylist[0] = A;
    arraylist[1] = B;

    Array *newArray = _init_array(
        2, op_type, arraylist, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    for (int d = 0; d < desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];
    }

    desc.dim_domains = malloc(sizeof(uint64_t *) * A->desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }

    desc.real_dim_domains = NULL;
    desc.array_type = InferArrayType(A->desc.array_type, B->desc.array_type);
    desc.attr_type = InferTypeBinaryOp(A->desc.attr_type, B->desc.attr_type);
    attrname_for_type('a', &desc.attr_name);
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array *elemwise_op_constant(Array *A, void *constant, tilestore_datatype_t constant_type, int eq_side, int op_type)
{
    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *));
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, op_type, arraylist, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    paramInfo.elemConst.constant = constant;
    paramInfo.elemConst.constant_type = constant_type;
    paramInfo.elemConst.equation_side = eq_side;
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    for (int d = 0; d < desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];
    }

    desc.dim_domains = malloc(sizeof(uint64_t *) * A->desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }

    desc.real_dim_domains = NULL;
    desc.array_type = A->desc.array_type;
    desc.attr_type = InferTypeBinaryOp(A->desc.attr_type, constant_type);
    attrname_for_type('a', &desc.attr_name);
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array *matmul(Array *A, Array *B)
{
    assert((A->desc.dim_len == B->desc.dim_len) && (B->desc.dim_len == 2));
    assert((A->desc.dim_domains[1][1] - A->desc.dim_domains[1][0]) == (B->desc.dim_domains[0][1] - B->desc.dim_domains[0][0]));

    A->num_children++;
    B->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 2);
    arraylist[0] = A;
    arraylist[1] = B;

    Array *newArray = _init_array(
        2, OP_MATMUL, arraylist, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    newArray->op_param = paramInfo;

    // FIXME: What is means?
    // if ((A->desc.dim_domains[0][1] - A->desc.dim_domains[0][0]) == 1 &&
    //     (B->desc.dim_domains[1][1] - B->desc.dim_domains[1][0]) == 1)
    // {
    //     newArray->scalar_flag = true;
    // }
    // else
    //     newArray->scalar_flag = false;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;

    desc.array_type = A->desc.array_type;
    desc.dim_len = 2;

    desc.tile_extents = malloc(sizeof(uint64_t) * 2);
    desc.tile_extents[0] = A->desc.tile_extents[0];
    desc.tile_extents[1] = B->desc.tile_extents[1];

    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    desc.dim_domains[0] = malloc(sizeof(uint64_t) * 2);
    desc.dim_domains[1] = malloc(sizeof(uint64_t) * 2);
    desc.dim_domains[0][0] = A->desc.dim_domains[0][0];
    desc.dim_domains[0][1] = A->desc.dim_domains[0][1];
    desc.dim_domains[1][0] = B->desc.dim_domains[1][0];
    desc.dim_domains[1][1] = B->desc.dim_domains[1][1];
    
    desc.real_dim_domains = NULL;

    attrname_for_type('a', &desc.attr_name);
    desc.array_type = InferArrayType(A->desc.array_type, B->desc.array_type);
    desc.attr_type = InferTypeBinaryOp(A->desc.attr_type, B->desc.attr_type);

    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);
    return newArray;
}

Array *subarray(Array *A, Range range)
{
    assert(range.ndim = A->desc.dim_len);

    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 1);
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, OP_SUBARRAYSCAN, arraylist, false, false, 0, false, false, false);

    /* copy range into param */
    ParamInfo paramInfo = initParamInfo();
    paramInfo.range.ndim = range.ndim;
    paramInfo.range.lb = malloc(range.ndim * sizeof(uint64_t));
    paramInfo.range.ub = malloc(range.ndim * sizeof(uint64_t));
    memcpy(paramInfo.range.lb, range.lb, range.ndim * sizeof(uint64_t));
    memcpy(paramInfo.range.ub, range.ub, range.ndim * sizeof(uint64_t));
    newArray->op_param = paramInfo;

    Descriptor desc;

    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t *) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];

        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        desc.dim_domains[d][0] = 0;
        uint64_t ub = range.ub[d] < A->desc.dim_domains[d][1] ? range.ub[d] : A->desc.dim_domains[d][1];
        uint64_t lb = range.lb[d] > A->desc.dim_domains[d][0] ? range.lb[d] : A->desc.dim_domains[d][0];
        desc.dim_domains[d][1] = ub - lb;
    }
        
    desc.real_dim_domains = NULL;

    // TODO: how to determine subarray's array type
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = A->desc.attr_type;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray(newArray->desc.object_name, newArray->desc.dim_domains, newArray->desc.dim_len, newArray->desc.attr_type);

    return newArray;
}

Array *transpose(Array *A, uint32_t *dim_order)
{
    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 1);
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, OP_TRANSPOSE, arraylist, false, false, 0, false, false, false);

    /* copy dim order into param */
    ParamInfo paramInfo = initParamInfo();
    paramInfo.trans.ndim = A->desc.dim_len;
    paramInfo.trans.transpose_func = dim_order;
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;

    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t *) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);

    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[dim_order[d]];

        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        desc.dim_domains[d][0] = A->desc.dim_domains[dim_order[d]][0];
        desc.dim_domains[d][1] = A->desc.dim_domains[dim_order[d]][1];
    }
    
    desc.real_dim_domains = NULL;
    desc.array_type = A->desc.array_type;
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = A->desc.attr_type;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array *aggr(Array *A, int agg_func, uint32_t *groupby, uint32_t groupby_len)
{
    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 1);
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, OP_AGGR, arraylist, false, false, 0, false, false, false);

    if (!groupby_len)
        newArray->scalar_flag = true;

    /* copy groupBy info into param */
    ParamInfo paramInfo = initParamInfo();
    paramInfo.groupBy.agg_func = agg_func;
    paramInfo.groupBy.agg_groupbylen = groupby_len;
    paramInfo.groupBy.groupby = malloc(sizeof(uint32_t) * groupby_len);
    memcpy(paramInfo.groupBy.groupby, groupby, sizeof(uint32_t) * groupby_len);
    paramInfo.groupBy.state = malloc(sizeof(double *) * 4);
    paramInfo.groupBy.state[3] = NULL;

    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = groupby_len;

    if (newArray->scalar_flag)
    {
        newArray->desc = desc;
        newArray->computed_list = NULL;
        return newArray;
    }

    desc.tile_extents = malloc(sizeof(uint64_t *) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[groupby[d]];

        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        desc.dim_domains[d][0] = A->desc.dim_domains[groupby[d]][0];
        desc.dim_domains[d][1] = A->desc.dim_domains[groupby[d]][1];
    }
    
    desc.real_dim_domains = NULL;
    desc.array_type = A->desc.array_type;
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = A->desc.attr_type;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array *window(Array *A, int64_t *window_size, int window_func)
{
    assert(A->desc.dim_len == 2);

    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 1);
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, OP_WINDOW, arraylist, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    paramInfo.window.window_func = window_func;
    paramInfo.window.window_size = malloc(sizeof(int64_t) * A->desc.dim_len);
    memcpy(paramInfo.window.window_size, window_size, sizeof(int64_t) * A->desc.dim_len);
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = 2;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];

        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }    
    desc.real_dim_domains = NULL;
    desc.array_type = A->desc.array_type;
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = A->desc.attr_type;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array *map(
    Array *A, 
    void (*fp_d)(void *, void *, uint64_t),
    void (*fp_s)(uint64_t *, uint64_t *, void *, uint64_t *, uint64_t *, void *, uint64_t, uint64_t *),
    int return_type)
{
    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 1);
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, OP_MAP, arraylist, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    paramInfo.lambda.lambda_func = fp_d;
    paramInfo.lambda.lambda_func_sparse = fp_s;
    paramInfo.lambda.return_type = return_type;
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];

        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }    
    desc.real_dim_domains = NULL;
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = return_type;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array *exponential(Array *A)
{
    A->num_children++;

    Array **arraylist = malloc(sizeof(Array *) * 1);
    arraylist[0] = A;

    Array *newArray = _init_array(
        1, OP_EXPONENTIAL, arraylist, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }    
    desc.real_dim_domains = NULL;
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = TILESTORE_FLOAT64;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array **svd(Array *A)
{
    A->num_children += 3;

    // List of 3 array pointers
    Array **result_arrays = malloc(sizeof(Array *) * 3);
    result_arrays[0] = malloc(sizeof(Array)); // U
    result_arrays[1] = malloc(sizeof(Array)); // Sigma
    result_arrays[2] = malloc(sizeof(Array)); // VT

    for (int i = 0; i < 3; i++)
    {
        // Set up basic infos
        result_arrays[i]->op_param = initParamInfo();
        result_arrays[i]->array_num = 1;
        result_arrays[i]->op_type = OP_SVD;
        result_arrays[i]->arraylist = malloc(sizeof(Array *));
        result_arrays[i]->arraylist[0] = A;
        result_arrays[i]->blocking_flag = false;
        result_arrays[i]->scalar_flag = false;
        result_arrays[i]->executed = false;
        result_arrays[i]->persist = false;
        result_arrays[i]->is_expanded = false;

        // Set up ParamInfo
        result_arrays[i]->op_param.svd.svd_u = result_arrays[0];

        // Set up descriptor
        result_arrays[i]->desc.object_name = genArrayName(128);
        result_arrays[i]->desc.array_type = A->desc.array_type;
        result_arrays[i]->desc.dim_len = 2;

        // Tile-Extent : Assuming tall-skinny matrix only.
        // U : Same tiling as A,
        // Sigma, VT : Single tile (A[1] * A[1])
        result_arrays[i]->desc.tile_extents = malloc(sizeof(uint64_t) * 2);
        result_arrays[i]->desc.tile_extents[0] =
            i == 0
                ? A->desc.tile_extents[0]
                : A->desc.tile_extents[1];
        result_arrays[i]->desc.tile_extents[1] = A->desc.tile_extents[1];

        // Dim-Domains : Assuming dim_domain always starts with 0.
        result_arrays[i]->desc.dim_domains = malloc(sizeof(uint64_t *) * 2);
        result_arrays[i]->desc.dim_domains[0] = malloc(sizeof(uint64_t) * 2);
        result_arrays[i]->desc.dim_domains[1] = malloc(sizeof(uint64_t) * 2);
        result_arrays[i]->desc.dim_domains[0][0] = A->desc.dim_domains[0][0];
        result_arrays[i]->desc.dim_domains[0][1] =
            i == 0
                ? A->desc.dim_domains[0][1]
                : A->desc.dim_domains[1][1];
        result_arrays[i]->desc.dim_domains[1][0] = A->desc.dim_domains[1][0];
        result_arrays[i]->desc.dim_domains[1][1] = A->desc.dim_domains[1][1];
    
        result_arrays[i]->desc.real_dim_domains = NULL;

        attrname_for_type('a', &result_arrays[i]->desc.attr_name);

        result_arrays[i]->desc.attr_type = TILESTORE_FLOAT64; // Result arrays are always double-type for now.

        init_computed_list(result_arrays[i]);
        init_simulated_materialized_list(result_arrays[i]);

        create_temparray_with_tilesize(
            result_arrays[i]->desc.object_name,
            result_arrays[i]->desc.dim_domains,
            result_arrays[i]->desc.dim_len,
            result_arrays[i]->desc.array_type,
            result_arrays[i]->desc.attr_type,
            result_arrays[i]->desc.tile_extents);
    }

    return result_arrays;
}

Array *inv(Array *A)
{
    assert(false);      // TODO: Not implemented yet

    Array *newArray = malloc(sizeof(Array));

    newArray->array_num = 1;
    newArray->op_type = OP_INV;
    newArray->arraylist = malloc(sizeof(Array *) * 1);
    newArray->arraylist[0] = A;
    newArray->blocking_flag = false;
    newArray->scalar_flag = false;
    newArray->num_children = 0;
    newArray->executed = false;
    newArray->persist = false;
    newArray->is_expanded = false;

    ParamInfo paramInfo = initParamInfo();
    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = A->desc.array_type;
    desc.dim_len = A->desc.dim_len;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < A->desc.dim_len; d++)
    {
        desc.tile_extents[d] = A->desc.tile_extents[d];
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], A->desc.dim_domains[d], sizeof(uint64_t) * 2);
    }    
    desc.real_dim_domains = NULL;
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = TILESTORE_FLOAT64;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

Array* _csv_to_sparse_with_metadata(char *csvpath, uint64_t *arrsize, uint64_t *tilesize, uint32_t dimlen) {
    // create array
    char *name = genArrayName(128);
    int res = tilestore_create_array(name, arrsize, tilesize, dimlen, TILESTORE_FLOAT64, TILESTORE_SPARSE_CSR);
    assert(res == TILESTORE_OK);

    // open array
    Array *out = open_array(name);

    // read
    csv_to_sparse_rowmajor(csvpath, '\t', false, out);
    return out;
}

Array* full(
        uint64_t *arrsize, 
        uint64_t *tilesize, 
        uint32_t dimlen, 
        double value,
        tilestore_format_t format) {

    Array *newArray = _init_array(
        0, OP_NEW_ARRAY, NULL, false, false, 0, false, false, false);

    ParamInfo paramInfo = initParamInfo();
    paramInfo.newarr.default_value = value;
    if (format == TILESTORE_SPARSE_CSR) {
        paramInfo.newarr.density = 1;
        paramInfo.newarr.is_sparse = true;
    } else if (format == TILESTORE_DENSE) {
        paramInfo.newarr.is_sparse = false;
    } else {
        assert(false);
    }

    newArray->op_param = paramInfo;

    Descriptor desc;
    char *name = genArrayName(128);
    desc.object_name = name;
    desc.array_type = format;
    desc.dim_len = dimlen;

    desc.tile_extents = malloc(sizeof(uint64_t) * desc.dim_len);
    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < dimlen; d++)
    {
        desc.tile_extents[d] = tilesize[d];
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        desc.dim_domains[d][0] = 0;
        desc.dim_domains[d][1] = arrsize[d] - 1;

        if (arrsize[d] % tilesize[d] != 0) {
            newArray->is_expanded = true;
        }
    }
    if (newArray->is_expanded) {
        desc.real_dim_domains = desc.dim_domains;
        desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
        for (int d = 0; d < dimlen; d++)
        {
            uint64_t expanded_size = arrsize[d];
            if (arrsize[d] % tilesize[d] != 0) {
                expanded_size += (tilesize[d] - arrsize[d] % tilesize[d]);
            }
            
            desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
            desc.dim_domains[d][0] = 0;
            desc.dim_domains[d][1] = expanded_size - 1;
        }
    }
    attrname_for_type('a', &desc.attr_name);
    desc.attr_type = TILESTORE_FLOAT64;
    newArray->desc = desc;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    create_temparray_with_tilesize(
        newArray->desc.object_name,
        newArray->desc.dim_domains,
        newArray->desc.dim_len,
        newArray->desc.array_type,
        newArray->desc.attr_type,
        desc.tile_extents);

    return newArray;
}

void free_node(Array *array)
{
    int op_type = array->op_type;

    if (array->array_num == 0)
    {
        storage_util_free_dim_domains(&array->desc.dim_domains, array->desc.dim_len);
        storage_util_free_tile_extents(&array->desc.tile_extents, array->desc.dim_len);
    }

    if (array->array_num > 0)
    {
        free(array->arraylist);
        free(array->desc.object_name);

        switch (op_type)
        {
        case OP_SUBARRAY:
        {
            free(array->op_param.range.lb);
            free(array->op_param.range.ub);
        }

        case OP_AGGR:
        {
            free(array->op_param.groupBy.groupby);
            if (array->op_param.groupBy.state[3] != NULL)
                free(array->op_param.groupBy.state[3]);
            free(array->op_param.groupBy.state);
        }

        case OP_WINDOW:
        {
            free(array->op_param.window.window_size);
        }

        case OP_TRANSPOSE:
        {
        }
        }

        if (!array->scalar_flag)
        {
            for (int d = 0; d < array->desc.dim_len; d++)
                free(array->desc.dim_domains[d]);

            free(array->desc.dim_domains);
            free(array->desc.tile_extents);
        }
    }

    free(array->desc.attr_name);
    free(array->computed_list);
    free(array);
}

Array *scanForTest(char *arrayname, int ndim, uint64_t **dim_domains)
{

    Array *newArray = malloc(sizeof(Array));

    ParamInfo paramInfo = initParamInfo();
    newArray->op_type = OP_SCAN;
    newArray->arraylist = NULL;
    newArray->array_num = 0;
    newArray->blocking_flag = false;
    newArray->executed = false;
    newArray->persist = false;
    newArray->is_expanded = false;

    Descriptor desc;
    /* TODO
     * Initialize descriptor
     */

    desc.dim_len = ndim;
    desc.object_name = malloc(strlen(arrayname) * sizeof(char) + 1);
    memcpy(desc.object_name, arrayname, strlen(arrayname) * sizeof(char) + 1);

    desc.dim_domains = malloc(sizeof(uint64_t *) * desc.dim_len);
    for (int d = 0; d < desc.dim_len; d++)
    {
        desc.dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        memcpy(desc.dim_domains[d], dim_domains[d], sizeof(uint64_t) * 2);
    }

    desc.attr_type = TILESTORE_INT32;
    
    desc.real_dim_domains = NULL;
    newArray->desc = desc;
    newArray->op_param = paramInfo;

    init_computed_list(newArray);
    init_simulated_materialized_list(newArray);

    return newArray;
}

void print_array(Array *A, int limit)
{
    //    Chunk* chunk = get_next(&A);
    //
    //    int ncells = 0;
    //    int ndim = 0;
    //    int counter = 0;
    //
    //    while(chunk != NULL){
    //        for( int i = 0; i < ncells; i++){
    //            uint64_t *cell_coords_chunk;
    //            double cell_value;
    //            chunk_get_cell_double(chunk, cell_coords_chunk, &cell_value);
    //            counter++;
    //            if( counter > limit ) break;
    //        }
    //        if( counter > limit ) break;
    //    }
}

char *rand_string(char *str, size_t size)
{
    const char charset[] = "ABCDEFGHIJK";
    if (size)
    {
        for (size_t n = 0; n < size; n++)
        {
            int key = rand() % (int)(sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

void print_rand_string(int size)
{
    char name[size + 1];
    rand_string(name, size);
    fprintf(stderr, "%s", name);
}

int print_op_type(int op_type)
{
    switch (op_type)
    {
    case OP_SCAN:
        fprintf(stderr, "[OP_SCAN]");
        return strlen("[OP_SCAN]");
    case OP_ADD:
        fprintf(stderr, "[OP_ADD]");
        return strlen("[OP_ADD]");
    case OP_SUB:
        fprintf(stderr, "[OP_SUB]");
        return strlen("[OP_SUB]");
    case OP_PRODUCT:
        fprintf(stderr, "[OP_PRODUCT]");
        return strlen("[OP_PRODUCT]");
    case OP_DIV:
        fprintf(stderr, "[OP_DIV]");
        return strlen("[OP_DIV]");
    case OP_TRANSPOSE:
        fprintf(stderr, "[OP_TRANSPOSE]");
        return strlen("[OP_TRANSPOSE]");
    case OP_SUBARRAY:
        fprintf(stderr, "[OP_SUBARRAY]");
        return strlen("[OP_SUBARRAY]");
    case OP_JOIN:
        fprintf(stderr, "[OP_JOIN]");
        return strlen("[OP_JOIN]");
    case OP_WINDOW:
        fprintf(stderr, "[OP_WINDOW]");
        return strlen("[OP_WINDOW]");
    case OP_AGGR:
        fprintf(stderr, "[OP_AGGR]");
        return strlen("[OP_AGGR]");
    case OP_MATMUL:
        fprintf(stderr, "[OP_MATMUL]");
        return strlen("[OP_MATMUL]");
    case OP_SUBARRAYSCAN:
        fprintf(stderr, "[OP_SUBARRAYSCAN]");
        return strlen("[OP_SUBARRAYSCAN]");
    case OP_RETILE:
        fprintf(stderr, "[OP_RETILE]");
        return strlen("[OP_RETILE]");
    case OP_MAP:
        fprintf(stderr, "[OP_MAP]");
        return strlen("[OP_MAP]");
    case OP_ADD_CONSTANT:
        fprintf(stderr, "[OP_ADD_CONSTANT]");
        return strlen("[OP_ADD_CONSTANT]");
    case OP_SUB_CONSTANT:
        fprintf(stderr, "[OP_SUB_CONSTANT]");
        return strlen("[OP_SUB_CONSTANT]");
    case OP_PRODUCT_CONSTANT:
        fprintf(stderr, "[OP_PRODUCT_CONSTANT]");
        return strlen("[OP_PRODUCT_CONSTANT]");
    case OP_DIV_CONSTANT:
        fprintf(stderr, "[OP_DIV_CONSTANT]");
        return strlen("[OP_DIV_CONSTANT]");
    }
}

int print_arrayname(Array *array)
{
    if (array->desc.object_name != NULL)
    {
        fprintf(stderr, "Array(");
        fprintf(stderr, "%s", array->desc.object_name);
        fprintf(stderr, ")");
        // fprintf(stderr, " %d", array->visited_flag);
        return strlen(array->desc.object_name) + 7;
    }
    else
    {
        fprintf(stderr, "Array(");
        int size = 3;
        print_rand_string(size);
        fprintf(stderr, ")");
        // fprintf(stderr, " %d", array->visited_flag);
        return size + 7;
    }
}

void print_dag_DFS(Array *A, int offset, uint8_t order)
{
    int size1 = print_arrayname(A);
    fprintf(stderr, " - ");
    int size2 = print_op_type(A->op_type);
    
    if (A->blocking_flag) fprintf(stderr, " - blocking");
    if (A->executed) {
        fprintf(stderr, " - executed\n");
        return;
    }

    fprintf(stderr, "\n");

    if (A->visited_flag == order) return;
    A->visited_flag++;

    int n = 2 + offset;
    for (int i = 0; i < A->array_num; i++)
    {
        fprintf(stderr, "%*s", n, "");
        fprintf(stderr, " L ");
        print_dag_DFS(A->arraylist[i], n + 2, order);
    }
}
void print_dag(Array *A, uint8_t order)
{
    print_dag_DFS(A, 2, order);
    fprintf(stderr, "\n");
}

/* Build Range */
Range set_range(uint64_t *lower_bound, uint64_t *upper_bound, int ndim)
{
    Range range;
    range.lb = lower_bound;
    range.ub = upper_bound;
    range.ndim = ndim;
    return range;
}

void PrintArrayInfo(Array *array)
{
    fprintf(stderr, "\tArrayName: %s\n", array->desc.object_name);
    fprintf(stderr, "\tArrayDim#: %d\n", array->desc.dim_len);

    fprintf(stderr, "\tShape::\n");
    for (int d = 0; d < array->desc.dim_len; d++)
        fprintf(stderr, "\t\tDim[%d][0]: %ld\tDim[%d][1]: %ld\n", d, array->desc.dim_domains[d][0], d, array->desc.dim_domains[d][1]);

    fprintf(stderr, "\tTile Size::\n");
    for (int d = 0; d < array->desc.dim_len; d++)
        fprintf(stderr, "\t\tDim[%d]: %ld\n", d, array->desc.tile_extents[d]);

    if (array->op_param.range.ndim != 0)
    {
        fprintf(stderr, "\tParam::\n");
        for (int d = 0; d < array->op_param.range.ndim; d++)
            fprintf(stderr, "\t\tSubRange[%d] %ld\t%ld\n", d, array->op_param.range.lb[d], array->op_param.range.ub[d]);
    }

    if (array->op_param.trans.ndim != 0)
    {
        fprintf(stderr, "\tParam::\n");
        for (int d = 0; d < array->op_param.trans.ndim; d++)
            fprintf(stderr, "\t\tTrans[%d] %d\n", d, array->op_param.trans.transpose_func[d]);
    }
    fprintf(stderr, "\t======================================\n\n");
}

void PrintArrayInfoDFS(Array *A)
{
    PrintArrayInfo(A);
    for (int i = 0; i < A->array_num; i++)
    {
        PrintArrayInfoDFS(A->arraylist[i]);
    }
}

void determine_child_chunksize(uint64_t *chunksize_parent, struct Array *array, int op_type, uint64_t **chunksize_child)
{
    uint32_t ndim = array->desc.dim_len;

    switch (op_type)
    {
    case OP_TRANSPOSE:
    case OP_SUBARRAY:
    case OP_SUBARRAYSCAN:
    case OP_AGGR:
    {
        for (int d = 0; d < array->desc.dim_len; d++)
        {
            uint32_t dim = array->op_param.groupBy.groupby[d];
            chunksize_child[0][dim] = chunksize_parent[d];
        }

        for (int d = 0; d < array->arraylist[0]->desc.dim_len; d++)
        {
            if (!chunksize_child[0][d])
                chunksize_child[0][d] = array->arraylist[0]->desc.tile_extents[d];
        }
        break;
    }

    case OP_ADD_CONSTANT:
    case OP_SUB_CONSTANT:
    case OP_PRODUCT_CONSTANT:
    case OP_DIV_CONSTANT:
    case OP_EXPONENTIAL:
    case OP_WINDOW:
    case OP_MAP:
        for (int d = 0; d < ndim; d++)
        {
            chunksize_child[0][d] = chunksize_parent[d];
        }
        break;

    case OP_ADD:
    case OP_SUB:
    case OP_PRODUCT:
    case OP_DIV:
    case OP_JOIN:
        for (int d = 0; d < ndim; d++)
        {
            chunksize_child[0][d] = chunksize_parent[d];
            chunksize_child[1][d] = chunksize_parent[d];
        }
        break;

    case OP_MATMUL:
    {
        chunksize_child[0][0] = array->arraylist[0]->desc.tile_extents[0];
        chunksize_child[0][1] = array->arraylist[0]->desc.tile_extents[1];

        chunksize_child[1][0] = array->arraylist[1]->desc.tile_extents[0];
        chunksize_child[1][1] = array->arraylist[1]->desc.tile_extents[1];

        break;
    }
    }
}
