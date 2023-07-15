#include <stdlib.h>
#include <string.h>

#include "bf_struct.h"
#include "arraykey.h"
// #include "malloc.h"
#include "bf_malloc.h"

#include <stdio.h>

extern BFmspace *_mspace_key;

int init_internal_array_key(array_key key, _array_key **res) {
    (*res) = BF_shm_malloc(_mspace_key, sizeof(_array_key));
    (*res)->dim_len = key.dim_len;

    char *arrayname = BF_shm_malloc(_mspace_key, strlen(key.arrayname) + 1);
    char *attrname = BF_shm_malloc(_mspace_key, strlen(key.attrname) + 1);
    uint32_t *dcoords = BF_shm_malloc(_mspace_key, sizeof(size_t) * key.dim_len);

    strcpy(arrayname, key.arrayname);
    strcpy(attrname, key.attrname);
    memcpy(dcoords, key.dcoords, sizeof(size_t) * key.dim_len);

    (*res)->arrayname_o = BF_SHM_KEY_OFFSET(arrayname);
    (*res)->attrname_o = BF_SHM_KEY_OFFSET(attrname);
    (*res)->dcoords_o = BF_SHM_KEY_OFFSET(dcoords);

    if (key.emptytile_template != BF_EMPTYTILE_DENSE &&
            key.emptytile_template != BF_EMPTYTILE_SPARSE_CSR &&
            key.emptytile_template != BF_EMPTYTILE_NONE) {
        // set default value for backward compatibility
        (*res)->emptytile_template = BF_EMPTYTILE_DENSE;    
    } else {
        (*res)->emptytile_template = key.emptytile_template;
    }

    return 0;
}

int free_internal_array_key(_array_key *res) {
    if (res == NULL) return 0;

    if (res->arrayname_o != BF_SHM_NULL) 
        BF_shm_free(_mspace_key, BF_SHM_KEY_PTR_CHAR(res->arrayname_o));

    if (res->attrname_o != BF_SHM_NULL) 
        BF_shm_free(_mspace_key, BF_SHM_KEY_PTR_CHAR(res->attrname_o));

    if (res->dcoords_o != BF_SHM_NULL) 
        BF_shm_free(_mspace_key, BF_SHM_KEY_PTR_UINT64T(res->dcoords_o));

    res->arrayname_o = BF_SHM_NULL;
    res->attrname_o = BF_SHM_NULL;
    res->dcoords_o = BF_SHM_NULL;

    BF_shm_free(_mspace_key, res);
    res = BF_SHM_NULL;

    return 0;
}

int compare_array_key(_array_key *a, _array_key *b) {
    if (a->dim_len != b->dim_len) return 1;
    for (int i = 0; i < a->dim_len; i++) {
        if (BF_SHM_KEY_PTR_UINT64T(a->dcoords_o)[i] !=
            BF_SHM_KEY_PTR_UINT64T(b->dcoords_o)[i]) return 1;
    }

    if (strcmp(BF_SHM_KEY_PTR_CHAR(a->arrayname_o), BF_SHM_KEY_PTR_CHAR(b->arrayname_o)) != 0) return 1;
    if (strcmp(BF_SHM_KEY_PTR_CHAR(a->attrname_o), BF_SHM_KEY_PTR_CHAR(b->attrname_o)) != 0) return 1;

    return 0;
}