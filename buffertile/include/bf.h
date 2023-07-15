#ifndef __BF__H__
#define __BF__H__

/****************************************************************************
 * bf.h: external interface definition for the BF layer
 ****************************************************************************/

#include "arraykey.h"       // to expose array_key to user
#include "utils.h"          // to expose utils to user
#include "bf_struct.h"


// enable dense mode? (Not now)
// define BF_DENSE_MODE

// enable reporting memory footprint?
// #define BF_MEM_FOOTPRINT


/*
 * size of BF hash table
 */
#define BF_HASH_TBL_SIZE 20


/*
 * prototypes for BF-layer functions
 */
int BF_Init();          // Initialize BF layer
void BF_Free();         // Free BF layer
int BF_Attach();        // Attach to shm and create tile ctx
void BF_Detach();       // Detach from shm and free tile ctx

int BF_InitWithSize(
    size_t data_size, size_t idata_size, size_t key_size, size_t bf_size);
int BF_AttachWithSize(
    size_t data_size, size_t idata_size, size_t key_size, size_t bf_size);
void BF_FreeWithSize(
    size_t data_size, size_t idata_size, size_t key_size, size_t bf_size);
void BF_DetachWithSize(
    size_t data_size, size_t idata_size, size_t key_size, size_t bf_size);

void BF_QueryStart();
void BF_QueryEnd();
void BF_PrintQueryStats();

int BF_GetBuf(array_key bq, PFpage **fpage);
int BF_UnpinBuf(array_key bq);
int BF_TouchBuf(array_key bq);
int BF_ForceWriteBuf(array_key bq);
void BF_PrintError(const char *s);
void BF_PrintDebug();
void BF_ShowLists();
void BF_ShowBuf(array_key bq);
void BF_ShowDCnt();
int BF_FlushAll();

int BF_ResizeBuf(PFpage *page, size_t target_nnz);
int BF_HintDCnt(char *arrname, uint64_t num_tiles);
int BF_DecreaseDCnt(char *arrname);
int BF_AddFuture(array_key key, uint64_t ts);
int _BF_AddHistory(_array_key *key, uint64_t ts);

int BF_CheckValidity();
int BF_flush_dirty_victim(BFpage* target);
int BF_discard_page(array_key bq, bool write);      // discard a single page
int BF_discard_pages(const char* arrayname, int* count);    // discard pages about arrayname

// internal functions
int BF_init_bfpage(BFpage **bf_page);
int BF_free_bfpage(BFpage *bf_page);
int BF_claim_memory(BFmspace *mspace, size_t size); 
int BF_GetBuf_Impl(array_key bq, PFpage **fpage);

int BF_UpdateNextTs(array_key key);
void _BF_UpdateNextTs(BFpage *target, bool_t pagefault, bool_t skip_sl_update);       // TODO: naming

int BF_ResizeBuf_pagebuf(PFpage *page, size_t nnz, size_t datasize);
int BF_ResizeBuf_coords(PFpage *page, size_t nnz);
int BF_ResizeBuf_offset(PFpage *page, size_t nnz);
int BF_ResizeBuf_validty(PFpage *page, size_t nnz);

int BF_ConvertToDense(PFpage *fpage, bool is_densemode);
int BF_ConvertToCSR(PFpage *fpage);

int BF_evict_victim(BFpage *victim);
int BF_get_victim_from_list(BFmspace *mspace, BFpage** out);

PFpage* _BF_get_pfpage_ptr(BFpage *bfpage);

/******************************************************************************/
/*      BF Layer - Error codes definition                                     */
/******************************************************************************/
#define BFE_OK                  0
#define BFE_NOMEM               (-1)
#define BFE_NOBUF               (-2)
#define BFE_PAGEPINNED          (-3)
#define BFE_PAGEUNPINNED        (-4)
#define BFE_PAGEINBUF           (-5)
#define BFE_PAGENOTINBUF        (-6)
#define BFE_INCOMPLETEWRITE     (-7)
#define BFE_INCOMPLETEREAD      (-8)
#define BFE_MISSDIRTY           (-9)
#define BFE_INVALIDTID          (-10)
#define BFE_MSGERR              (-11)
#define BFE_HASHNOTFOUND        (-12)
#define BFE_HASHPAGEEXIST       (-13)

#define BFE_LRU_REMOVE_ERR                  (-101)
#define BFE_LRU_SET_MRU_ERR                 (-102)
#define BFE_BFPAGE_ALLOC_FAIL               (-103)
#define BFE_PAGE_REMOVE_FAIL                (-104)

#define BFE_PAGE_UNPIN_FAIL                 (-105)
#define BFE_PAGE_TRY_REMOVE_BUT_PINNED      (-106)

#define BFE_HASH_INSERT_ERR                 (-110)
#define BFE_HASH_FOUND                      (-111)

#define BFE_INIT_SHM_OPEN_ERR               (-121)
#define BFE_INIT_FTRUNCATE_ERR              (-122)
#define BFE_INIT_MMAP_ERR                   (-123)
#define BFE_ATTACH_SHM_OPEN_ERR             (-124)
#define BFE_ATTACH_MMAP_ERR                 (-125)

#define BFE_INIT_MUTEX_ERR                  (-131)

#define BFE_SCHEMA_ERR                      (-140)

#define BFE_VALIDITY_CHECK_ERR              (-150)

#define BFE_CONVERSION_WRONG_INPUT_TYPE     (-160)

/*
 * error in UNIX system call or library routine
 */
#define BFE_UNIX		(-100)

#endif
