#ifndef __BF_STRUCT__H__
#define __BF_STRUCT__H__

#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>

#include "bf_type.h"
#include "arraykey.h"
#include "bf_malloc.h"

/*
 * most recent BF error code
 */
extern int BFerrno;

extern uint64_t BF_curr_ts;
extern uint8_t _bf_skiplist_level;

#define BF_TS_INF               (uint64_t) 0 - 1

// memory space used for dlmalloc (mspace_malloc).
extern BFmspace *_mspace_data;         // data for array data
extern BFmspace *_mspace_idata;        // data for input data
extern BFmspace *_mspace_key;
extern BFmspace *_mspace_bf;

// environmental variables
extern size_t _bf_env_data_size;           // BF_DATA_SIZE
extern size_t _bf_env_idata_size;          // BF_IDATA_SIZE
extern size_t _bf_env_keystore_size;       // BF_KEYSTORE_SIZE
extern size_t _bf_env_bfstore_size;        // BF_BFSTORE_SIZE


// When you test bf, it is recommand to set mem size small
// #define MAX_BUF_MEM_SIZE        ((off_t)1024 * 1024 * 64)   // for testing
#define MAX_BUF_MEM_SIZE        ((off_t)1024 * 1024 * 1024 * 16)    // data store
#define IDATA_MEM_SIZE          ((off_t)1024 * 1024 * 1024 * 12)    // idata store (input data)
#define KEYSTORE_MEM_SIZE       ((off_t)1024 * 1024 * 64)           // key store
#define BFSTORE_MEM_SIZE        ((off_t)1024 * 1024 * 128)           // bf store


#define BF_SHM_DATA_PATH    "buffertile_data"
#define BF_SHM_IDATA_PATH    "buffertile_idata"
#define BF_SHM_KEY_PATH     "buffertile_key"
#define BF_SHM_BF_PATH      "buffertile_bf"

#define BF_SHM_NULL 0

// calculate correct pointer from BFshm_offset_t, and vice versa.
// Be warned: the base is not mmap base; it is the base of the mspace.

// for datastore
#define _BF_SHM_OFFSET_AUTO(___mspace, ptr) ((void*) ___mspace == (void*) _mspace_data ? BF_SHM_DATA_OFFSET(ptr) : BF_SHM_IDATA_OFFSET(ptr))
#define _BF_SHM_PTR_AUTO(___mspace, ptr) ((void*) ___mspace == (void*) _mspace_data ? BF_SHM_DATA_PTR(ptr) : BF_SHM_IDATA_PTR(ptr))

#define BF_SHM_DATA_PTR(offset) (void *)((char *)_mspace_data + offset)
#define BF_SHM_DATA_PTR_UINT64T(offset) ((uint64_t *)BF_SHM_DATA_PTR(offset))
#define BF_SHM_DATA_OFFSET(ptr) (BFshm_offset_t)((char *)ptr - (char *) _mspace_data)

#define BF_SHM_IDATA_PTR(offset) (void *)((char *)_mspace_idata + offset)
#define BF_SHM_IDATA_PTR_UINT64T(offset) ((uint64_t *)BF_SHM_IDATA_PTR(offset))
#define BF_SHM_IDATA_OFFSET(ptr) (BFshm_offset_t)((char *)ptr - (char *) _mspace_idata)

// for keystore
#define BF_SHM_KEY_PTR(offset) (void *)((char *)_mspace_key + offset)
#define BF_SHM_KEY_PTR_KEY(offset) ((_array_key *)BF_SHM_KEY_PTR(offset))
#define BF_SHM_KEY_PTR_CHAR(offset) ((char *)BF_SHM_KEY_PTR(offset))
#define BF_SHM_KEY_PTR_UINT64T(offset) ((uint64_t *)BF_SHM_KEY_PTR(offset))
#define BF_SHM_KEY_OFFSET(ptr) (BFshm_offset_t)((char *)ptr - (char *) _mspace_key)

// for bfstore
#define BF_SHM_BF_PTR(offset) (void *)((char *)_mspace_bf + offset)
#define BF_SHM_BF_PTR_BFPAGE(offset) ((BFpage *)BF_SHM_BF_PTR(offset))
#define BF_SHM_BF_PTR_BFBUCKET(offset) ((BFbucket *)BF_SHM_BF_PTR(offset))
#define BF_SHM_BF_PTR_BFHASHENTRY(offset) ((offset == BF_SHM_NULL) ? NULL : ((BFhash_entry *)BF_SHM_BF_PTR(offset)))
#define BF_SHM_BF_PTR_VAL_INT(offset) (*((int *)BF_SHM_BF_PTR(offset)))
#define BF_SHM_BF_PTR_UINT64T(offset) ((uint64_t *)BF_SHM_BF_PTR(offset))
#define BF_SHM_BF_OFFSET(ptr) (BFshm_offset_t)((char *)ptr - (char *) _mspace_bf)


#define TMPARR_NAME_PREFIX "__temparr_"


/******************************************************************************/
// eviction policy
/******************************************************************************/
typedef enum bf_eviction_policy {
    BF_EVICTION_POLICY_MRU_ONLY         = 1,
    BF_EVICTION_POLICY_LRU_ONLY         = 2,
    BF_EVICTION_POLICY_OPT              = 8,
    BF_EVICTION_POLICY_LRU_K            = 9
} bf_eviction_policy;


extern bf_eviction_policy _bf_current_eviction_policy;
extern bool_t _bf_skiplist_on;

// environmental variable
// If bf uses multiple buffer spaces (i.e., using mspace_data and mspace_idata),
//    the policy is ignored.
extern bf_eviction_policy _bf_env_eviction_policy;      // BF_EVICTION_POLICY

// Preemptive Eviction
extern bf_eviction_policy _bf_env_preemptive_eviction;                       // default is on (1)

// LRU-K variables
extern uint8_t _bf_env_lruk_k;                                               // K
extern uint64_t _bf_env_lruk_crp;                                            // correlation reference period


/******************************************************************************/
/*   Type definition for pages of the PF layer.                               */
/******************************************************************************/

typedef enum PFpage_type
{
    DENSE_FIXED = 0,
    DENSE_FIXED_NULLABLE = 1,
    DENSE_VARIABLE = 2,
    DENSE_VARIABLE_NULLABLE = 3,
    SPARSE_FIXED = 4,
    SPARSE_FIXED_NULLABLE = 5,
    SPARSE_VARIABLE = 6,
    SPARSE_VARIABLE_NULLABLE = 7
} PFpage_type;

typedef enum PFpage_sparse_format
{
    PFPAGE_SPARSE_FORMAT_CSR = 0,
    PFPAGE_SPARSE_FORMAT_COO = 1
} PFpage_sparse_format;

// internal structure for PFpage
#define BF_TILE_DISCARD_OFF 0
typedef struct PFpage
{
    // the type of tile (page)
    PFpage_type type;
    uint64_t max_idx; // max length of arrays (pagebuf, coords[n], etc)

    // pagebuf: cell contents
    BFshm_offset_t pagebuf_o;         // (void*)
    uint64_t pagebuf_len;             // in bytes
    uint64_t unfilled_pagebuf_offset; // indicate the offset that user should fill the value next
    uint16_t _pagebuf_align_offset;   // offset to the original malloc-ed pointer. it is required to handle O_DIRECT.

    // coords: coordiantes of the cells
    BFshm_offset_t coords_o;               // (void**)
    BFshm_offset_t coords_lens_o;          // (uint64_t*)
    BFshm_offset_t _coord_align_offsets_o; // (uint16_t*)
    PFpage_sparse_format sparse_format;

    // for sparse array
    uint64_t unfilled_idx; // indicate the index that user should fill the value next

    // tile information
    BFshm_offset_t tile_extents_o;      // tile size (uint64_t*)
    uint32_t dim_len;                   // the number of dimensions
    uint8_t attrsize;                   // the size of attribute
    bool is_input;                      // is input tile?
    
    // dense mode
    double est_density;         // Estimated density. 
                                // When tile type is dense_fixed and est_density has a 0 ~ 1 value,
                                //      it is in a dense mode.
                                // If it is nan, the tile is truly a dense tile.
} PFpage;

typedef struct BFpage
{
    /* (_PFpage) page data from the file                                */
    BFshm_offset_t fpage_o;
    /* next in the linked list of buffer pages                          */
    BFshm_offset_t nextpage_o;
    /* (struct BFpage) prev in the linked list of buffer pages          */
    BFshm_offset_t prevpage_o;
    /* (struct BFpage) TRUE if page is dirty                            */
    bool_t dirty;
    /* pin count associated with the page                               */
    short count;
    /* (struct _array_key) array key to identify a tile                 */
    BFshm_offset_t key_o;

    bool is_input;                  // is it a tile of input array?
    uint64_t next_ts;               // timestamp 
    BFshm_offset_t s_nextpages_o;   // next BFpage's' used by skip list. 
    BFshm_offset_t s_prevpages_o;   // next BFpage's' used by skip list. 
} BFpage;

typedef struct BFList {
    BFshm_offset_t head_o;      // (BFpage*) head
    BFshm_offset_t tail_o;      // (BFpage*) tail
    int size;                   // size

    // skiplist
    BFshm_offset_t s_heads_o;   
    BFshm_offset_t s_tails_o;
} BFList;

// A header for the data structure of BF layer to share it to shared memory.
typedef struct BFshm_hdr
{
    // Hash table variable
    BFshm_offset_t BFhash_table_o;      // (BFbucket*) Hash table (pool)
    BFshm_offset_t BFDcnt_table_o;      // (BFbucket*) Hash table for dCnt
    BFshm_offset_t BFFuture_table_o;    // (BFbucket*) Hash table for future log (MIN)
    BFshm_offset_t BFHistory_table_o;   // (BFbucket*) Hash table for history log (LRU-K)

    // List
    BFshm_offset_t BFLRU_o;
    BFshm_offset_t BFMRU_o;

    BFshm_offset_t BFClean_Skiplist_o;   // clean list
    BFshm_offset_t BFDirty_Skiplist_o;   // dirty list

    // Latch (mutex)
    pthread_mutex_t latch; // (pthread_mutex_t*) Latch

} BFshm_hdr;

extern BFshm_hdr *BFshm_hdr_ptr; // header
// #define BF_HDR(VAR)     BFshm_hdr_ptr->(VAR)

#endif
