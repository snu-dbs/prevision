#ifndef __ARRAY_KEY__H__
#define __ARRAY_KEY__H__

#include <stddef.h>
#include <stdint.h>

#include "tilestore.h"
#include "bf_struct.h"


typedef enum emptytile_template_type_t {
    BF_EMPTYTILE_DENSE,
    BF_EMPTYTILE_SPARSE_CSR,
    BF_EMPTYTILE_SPARSE_COO,
    BF_EMPTYTILE_NONE,
} emptytile_template_type_t;

// array information what user want to get
typedef struct array_key {
    char                        *arrayname;         // (char*) 
    char                        *attrname;          // (char*)
	uint64_t                    *dcoords;           // (uint64_t*)
    uint32_t                    dim_len;
    emptytile_template_type_t   emptytile_template; // the return type of buffered tile when requested tile is empty tile (i.e., not in the storage engine) 
} array_key;

// internal structures
// this should be considered seperately from array_key because it must be freed.
typedef struct _array_key {
    BFshm_offset_t              arrayname_o;     // (char*) 
    BFshm_offset_t              attrname_o;      // (char*)
	BFshm_offset_t              dcoords_o;       // (uint64_t*)
    uint32_t                    dim_len;
    emptytile_template_type_t   emptytile_template;
} _array_key;

int init_internal_array_key(array_key key, _array_key **res);
int free_internal_array_key(_array_key *res);
int compare_array_key(_array_key *a, _array_key *b);

#endif