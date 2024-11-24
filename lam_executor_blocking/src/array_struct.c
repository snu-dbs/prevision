//
// Created by mxmdb on 22. 1. 28..
//

#include <string.h>
#include <malloc.h>
#include <math.h>
#include <planner.h>
#include "array_struct.h"

Range initRange(){
    Range range;
    range.ndim = 0;
    range.ub = NULL;
    range.lb = NULL;
    return range;
}

MatMul initMatMul() {
    MatMul matmul;
    matmul.lhs_transposed = false;
    matmul.rhs_transposed = false;
    return matmul;
}

Transposition  initTrans(){
    Transposition trans;
    trans.transpose_func = NULL;
    trans.ndim = 0 ;
    return trans;
}

GroupBy initGroupBy(){
    GroupBy groupBy;
    groupBy.agg_func = -1;
    groupBy.agg_groupbylen = 0;
    groupBy.groupby = NULL;
    return groupBy;
}
Window initWindow(){
    Window window;
    window.window_func = -1;
    window.window_size = NULL;
    return window;
}

Lambda initLambda(){
    Lambda lambda;
    lambda.lambda_func = NULL;
    lambda.lambda_func_sparse = NULL;
    lambda.return_type = -1;
    return lambda;
}

ElemwiseConstant initElemConst(){
    ElemwiseConstant elemConst;
    elemConst.constant = NULL;
    elemConst.constant_type = -1;
    elemConst.equation_side = 1; // default: RHS
    return elemConst;
}

NewArray initNewArray() {
    NewArray newarr;
    newarr.default_value = 0;
    newarr.density = 0;
    newarr.is_sparse = false;
    return newarr;
}

ParamInfo initParamInfo(){
    ParamInfo info;
    info.range = initRange();
    info.matmul = initMatMul();
    info.trans = initTrans();
    info.groupBy = initGroupBy();
    info.window = initWindow();
    info.lambda = initLambda();
    info.elemConst = initElemConst();
    info.newarr = initNewArray();
    return info;
}

ParamInfo copy_param(ParamInfo src){
    ParamInfo dest = initParamInfo();
    if (src.groupBy.agg_func != -1){
        dest.groupBy = src.groupBy;
        dest.groupBy.groupby = malloc(sizeof(uint32_t)*dest.groupBy.agg_groupbylen);
        memcpy(dest.groupBy.groupby, src.groupBy.groupby, sizeof(uint32_t)*dest.groupBy.agg_groupbylen);
    }

    if (src.range.ndim != 0){
        dest.range = src.range;
        dest.range.ub = malloc( sizeof(int64_t)*dest.range.ndim);
        memcpy(dest.range.ub, src.range.ub, sizeof(int64_t)*dest.range.ndim);
        dest.range.lb = malloc( sizeof(int64_t)*dest.range.ndim);
        memcpy(dest.range.lb, src.range.lb, sizeof(int64_t)*dest.range.ndim);
    }

    if (src.trans.ndim != 0 ){
        dest.trans = src.trans;
        dest.trans.transpose_func = malloc(sizeof(uint32_t)*dest.trans.ndim);
        memcpy(dest.trans.transpose_func, src.trans.transpose_func, sizeof(uint32_t)*dest.trans.ndim);
    }
    return dest;
}

void free_param(ParamInfo info){
    if (info.groupBy.agg_groupbylen != 0){

        free(info.groupBy.groupby);
    }

    if (info.range.ndim != 0){
        free(info.range.ub);
        free(info.range.lb);
    }

    if (info.trans.ndim != 0 ){
        free(info.trans.transpose_func);
    }
}

Descriptor copy_desc(Descriptor src){
    Descriptor dest = src;
    dest.dim_domains = malloc(src.dim_len*sizeof(uint64_t*));
    for( int d = 0; d < dest.dim_len ; d++){
        dest.dim_domains[d] = malloc(sizeof(uint64_t)*2);
        dest.dim_domains[d][0] = src.dim_domains[d][0];
        dest.dim_domains[d][1] = src.dim_domains[d][1];
    }
    if (dest.real_dim_domains != NULL) {
        dest.real_dim_domains = malloc(src.dim_len*sizeof(uint64_t*));
        for( int d = 0; d < dest.dim_len ; d++){
            dest.real_dim_domains[d] = malloc(sizeof(uint64_t)*2);
            dest.real_dim_domains[d][0] = src.real_dim_domains[d][0];
            dest.real_dim_domains[d][1] = src.real_dim_domains[d][1];
        }
    } 
    dest.object_name = malloc(strlen(src.object_name)*sizeof (char)+1);
    memcpy(dest.object_name, src.object_name,strlen(src.object_name)*sizeof (char)+1);
    dest.dim_len = src.dim_len;
    dest.tile_extents = malloc(sizeof(uint64_t) * src.dim_len);
    memcpy(dest.tile_extents, src.tile_extents, sizeof(uint64_t) * src.dim_len);
    return dest;
}

void free_desc(Descriptor desc){
    for( int d = 0; d < desc.dim_len ; d++){
        free( desc.dim_domains[d] );
    }
    free( desc.dim_domains );
    if (desc.real_dim_domains != NULL) {
        for( int d = 0; d < desc.dim_len ; d++){
            free( desc.real_dim_domains[d] );
        }
        free( desc.real_dim_domains );
    }
    free ( desc.object_name);
}

Array* copy_node(Array* src)
{
    Array* dest = malloc(sizeof(Array));
    (*dest) = (*src);
    dest->arraylist = malloc(sizeof(Array*)*dest->array_num);
    if (dest->array_num == 0) {
        dest->arraylist = NULL;
    } else {
        for( int i = 0;  i < dest->array_num ; i++ ){
            dest->arraylist[i] = src->arraylist[i];
        }
    }
    dest->op_param = copy_param(src->op_param);
    dest->desc = copy_desc(src->desc);

    init_computed_list(dest);
    init_simulated_materialized_list(dest);

    return dest;
}

uint64_t get_num_of_chunks(Array* arr) {
    uint64_t ret = 1;
    for (uint32_t d = 0; d < arr->desc.dim_len; d++) {
        double side = (double) (arr->desc.dim_domains[d][1] - arr->desc.dim_domains[d][0] + 1);
        uint64_t num_of_chunks_in_dim = (uint64_t) ceil(side / arr->desc.tile_extents[d]);
        ret *= num_of_chunks_in_dim;
    }
    
    return ret;
}

void init_computed_list(Array *arr) {
    uint64_t num_of_chunks = get_num_of_chunks(arr);
    arr->computed_list = calloc(num_of_chunks, sizeof(bool));
}

void init_simulated_materialized_list(Array *arr) {
    uint64_t num_of_chunks = get_num_of_chunks(arr);
    arr->simulated_materialized_list = calloc(num_of_chunks, sizeof(bool));
}

void mark_all_computed(Array *arr) {
    uint64_t num_of_chunks = get_num_of_chunks(arr);
    for (uint64_t i = 0; i < num_of_chunks; i++) {
        arr->computed_list[i] = true;
    }
}
