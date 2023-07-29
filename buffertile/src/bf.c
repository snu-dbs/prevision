#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <sys/time.h>
#include <unistd.h>

#include "bf.h"
#include "list.h"
#include "hash.h"
#include "bf_struct.h"
#include "arraykey.h"
#include "tileio.h"
#include "bf_malloc.h"


int BFerrno;

uint64_t BF_curr_ts;
uint8_t _bf_skiplist_level;

BFmspace *_mspace_data;   
BFmspace *_mspace_idata;  
BFmspace *_mspace_key;
BFmspace *_mspace_bf;
BFshm_hdr *BFshm_hdr_ptr; 

const char *tmparr_name_prefix = "__temparr_";

struct timeval bf_query_start;

unsigned long long bftime = 0;
unsigned long long write_prevention_cnt = 0;
unsigned long long discarded_capacity = 0;
unsigned long long bf_tmpbuf_cnt = 0;

unsigned long long bf_getbuf_cnt_total = 0;
unsigned long long bf_getbuf_cnt_hit = 0;

unsigned long long bf_getbuf_io_total = 0;
unsigned long long bf_getbuf_io_hit = 0;

unsigned long long bf_this_query = 0;

extern unsigned long long bf_read_io_time, bf_iread_io_time, bf_read_io_size, bf_iread_io_size;
extern unsigned long long bf_write_io_time, bf_write_io_size;

extern unsigned long long bf_min_sl_update_time, bf_min_fl_retrival_time;


// environmental variables
size_t _bf_env_data_size = SIZE_MAX;           // BF_DATA_SIZE
size_t _bf_env_idata_size = SIZE_MAX;          // BF_IDATA_SIZE
size_t _bf_env_keystore_size = SIZE_MAX;       // BF_KEYSTORE_SIZE
size_t _bf_env_bfstore_size = SIZE_MAX;        // BF_BFSTORE_SIZE

/******************************************************************************/
// eviction policy
/******************************************************************************/
bf_eviction_policy _bf_current_eviction_policy = BF_EVICTION_POLICY_OPT;
bool_t _bf_skiplist_on = true;

// environmental variable
// If bf uses multiple buffer spaces (i.e., using mspace_data and mspace_idata),
//    the policy is ignored.
bf_eviction_policy _bf_env_eviction_policy = BF_EVICTION_POLICY_OPT;      // BF_EVICTION_POLICY

// Preemptive Eviction
bf_eviction_policy _bf_env_preemptive_eviction = 1;                       // default is on (1)

// LRU-K variables
uint8_t _bf_env_lruk_k = 2;                                               // K
uint64_t _bf_env_lruk_crp = 4;                                            // correlation reference period


#define __BF_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __BF_STAT_END()                                                                                 \
    struct timeval end;                                                                                 \
    gettimeofday(&end, NULL);                                                                           \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);  \
    bftime += diff;                                                                   
    // __lam_dm_conversion_cnt++;

#ifdef BF_MEM_FOOTPRINT
unsigned long long __bf_mem_footprint_last;
unsigned long long __bf_mem_footprint_printall_last;
#endif

int BF_CreateShm(const char* name, size_t size, BFmspace **shared_mem) {
    // Check it is already exists. If then, just skip the init procedure (err).
    // if not, create shared memory.
    int shared_fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
    if (shared_fd == -1) {
        fprintf(stderr, "[BufferTile] shm_open failed. You can ignore this if you intend it.\n");
        return BFE_INIT_SHM_OPEN_ERR;
    }

    // set the memory size
    if (ftruncate(shared_fd, size) == -1) {
        fprintf(stderr, "[BufferTile] ftruncate failed\n");
        return BFE_INIT_FTRUNCATE_ERR;
    }

    *shared_mem = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, shared_fd, 0);
    if (shared_mem == MAP_FAILED) {
        fprintf(stderr, "[BufferTile] mmap failed\n");
        return BFE_INIT_MMAP_ERR;
    }

    return BFE_OK;
}

int BF_OpenShm(const char* name, size_t size, BFmspace **shared_mem) {
    if (*shared_mem == NULL) {
        // open shared memory 
        int shared_fd = shm_open(name, O_RDWR, S_IRUSR | S_IWUSR);
        if (shared_fd == -1) {
            fprintf(stderr, "[BufferTile] shm_open failed\n");
            return BFE_ATTACH_SHM_OPEN_ERR;
        }

        *shared_mem = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, shared_fd, 0);
        if (shared_mem == MAP_FAILED) {
            fprintf(stderr, "[BufferTile] mmap failed\n");
            return BFE_ATTACH_MMAP_ERR;
        }

        // Close shm file
        close(shared_fd);
    }

    return BFE_OK;
}

int BF_ReadEnv() {
    _bf_env_data_size = atoll(getenv("BF_DATA_SIZE"));
    _bf_env_idata_size = atoll(getenv("BF_IDATA_SIZE"));
    _bf_env_keystore_size = atoll(getenv("BF_KEYSTORE_SIZE"));
    _bf_env_bfstore_size = atoll(getenv("BF_BFSTORE_SIZE"));

    _bf_env_eviction_policy = atoi(getenv("BF_EVICTION_POLICY"));
    _bf_env_preemptive_eviction = atoi(getenv("BF_PREEMPTIVE_EVICTION"));

    _bf_env_lruk_crp =  (uint64_t) atoi(getenv("BF_LRUK_CRP"));
    _bf_env_lruk_k = (uint8_t) atoi(getenv("BF_LRUK_K"));
}

int BF_InitWithSize(
        size_t data_size, size_t idata_size, size_t key_size, size_t bf_size) {
    BF_ReadEnv();

    /* initialize lists and hash table */
    BFerrno = 0;

    if (_bf_skiplist_on) 
        _bf_skiplist_level = 4;

    // create shared memory segments
    bool is_multiplt_bf = idata_size > 0;
    fprintf(stderr, "[BufferTile] BF_InitWithSize : Initialized with %lu, %lu, %lu, %lu\n",
        data_size, idata_size, key_size, bf_size);
    fprintf(stderr, "\t Eviction policy: %d\n", _bf_current_eviction_policy);

    BFmspace *bfstore = NULL;
    BF_CreateShm(BF_SHM_DATA_PATH, data_size, &_mspace_data);
    if (is_multiplt_bf) BF_CreateShm(BF_SHM_IDATA_PATH, idata_size, &_mspace_idata);
    BF_CreateShm(BF_SHM_KEY_PATH, key_size, &_mspace_key);
    BF_CreateShm(BF_SHM_BF_PATH, bf_size, &bfstore);

    // Init memory space
    // BF layer data structure header
    BFshm_hdr_ptr = (BFshm_hdr*) bfstore;

    // This creates anonymous and shared memory using mmap() so that 
    //   parent and all child processes may share the same _mspace after fork().
    // The memory alignment is required.  
    size_t aligned_hdr_size = BF_SHM_ALIGNED_SIZEOF(sizeof(BFshm_hdr));
    _mspace_bf = BF_shm_create_mspace((char*) bfstore + aligned_hdr_size, 
        bf_size - aligned_hdr_size);

    _mspace_data = BF_shm_create_mspace((char*) _mspace_data, data_size);
    if (is_multiplt_bf) _mspace_idata = BF_shm_create_mspace((char*) _mspace_idata, idata_size);
    else _mspace_idata = _mspace_data;
    _mspace_key = BF_shm_create_mspace((char*) _mspace_key, key_size);
    
    // Layer init
    BF_list_init();
    BF_hash_init();

    // Init latch (mutex)
    pthread_mutexattr_t attr;           // attr for shared mutex
    pthread_mutexattr_init(&attr);
    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0) {
        fprintf(stderr, "[BufferTile] pthread_mutexattr_setpshared failed\n");
        return BFE_INIT_MUTEX_ERR;
    }

    // init shared mutex
    if (pthread_mutex_init(&BFshm_hdr_ptr->latch, &attr) != 0) {
        fprintf(stderr, "[BufferTile] pthread_mutex_init failed\n");
        return BFE_INIT_MUTEX_ERR;
    }

    // destroy mutex attribute
    pthread_mutexattr_destroy(&attr);

#ifdef BF_MEM_FOOTPRINT
    // for memory footprint
    __bf_mem_footprint_last = 0;
    __bf_mem_footprint_printall_last = 0;
#endif

    return BFE_OK;
}

int BF_AttachWithSize(
        size_t data_size, size_t idata_size, size_t key_size, size_t bf_size) {
    BF_ReadEnv();

    bool is_multiplt_bf = idata_size > 0;

    BF_OpenShm(BF_SHM_DATA_PATH, data_size, &_mspace_data);
    if (is_multiplt_bf) BF_OpenShm(BF_SHM_IDATA_PATH, idata_size, &_mspace_idata);
    else _mspace_idata = _mspace_data;
    BF_OpenShm(BF_SHM_KEY_PATH, key_size, &_mspace_key);

    size_t aligned_hdr_size = BF_SHM_ALIGNED_SIZEOF(sizeof(BFshm_hdr));
    if (_mspace_bf == NULL) {
        BFmspace *bfstore = NULL;
        BF_OpenShm(BF_SHM_BF_PATH, bf_size, &bfstore);
        
        // BF layer data structure header
        BFshm_hdr_ptr = (BFshm_hdr*) bfstore;

        // Create mspace
        _mspace_bf = (BFmspace*) ((char *) bfstore + BF_SHM_ALIGNED_SIZEOF(sizeof(BFshm_hdr)));
    }

    return BFE_OK;
}

void BF_DetachWithSize(
        size_t data_size, size_t idata_size, size_t key_size, size_t bf_size) {
    BF_ReadEnv();
    bool is_multiplt_bf = idata_size > 0;

    if (_mspace_idata != NULL) {
        munmap(_mspace_idata, idata_size);
        _mspace_idata = NULL;
    }

    if (_mspace_data != NULL) {
        if (is_multiplt_bf) munmap(_mspace_data, data_size);
        _mspace_data = NULL;
    }

    if (_mspace_key != NULL) {
        munmap(_mspace_key, key_size);
        _mspace_key = NULL;
    }

    if (_mspace_bf != NULL) {
        munmap(_mspace_bf - BF_SHM_ALIGNED_SIZEOF(sizeof(BFshm_hdr)), bf_size);
        _mspace_bf = NULL;
    }
}

void BF_FreeWithSize(
        size_t data_size, size_t idata_size, size_t key_size, size_t bf_size) {
    BF_ReadEnv();
    bool is_multiplt_bf = idata_size > 0;

    __BF_STAT_START();

    BF_AttachWithSize(data_size, idata_size, key_size, bf_size);

    // Free BF structures
    BF_list_free();
    BF_hash_free();

#if BF_MALLOC_DEBUG
    fprintf(stderr, "========= mspace data =========\n");
    BF_shm_print_stats(_mspace_data);
    fprintf(stderr, "========= mspace key =========\n");
    BF_shm_print_stats(_mspace_key);
    fprintf(stderr, "========= mspace bf =========\n");
    BF_shm_print_stats(_mspace_bf);
#endif

    // free latch
    pthread_mutex_destroy(&BFshm_hdr_ptr->latch);

    BF_DetachWithSize(data_size, idata_size, key_size, bf_size);

    // close shared memory
    shm_unlink(BF_SHM_DATA_PATH);
    if (is_multiplt_bf) shm_unlink(BF_SHM_IDATA_PATH);
    shm_unlink(BF_SHM_KEY_PATH);
    shm_unlink(BF_SHM_BF_PATH);

    __BF_STAT_END();

    return;
}

void BF_DetermineSizesAndPolicy(
        size_t *datasize, size_t *idatasize, size_t *kssize, size_t *bfsize) {
    // priority: 1) env values 2) default value 
    *datasize = _bf_env_data_size == SIZE_MAX ? MAX_BUF_MEM_SIZE : _bf_env_data_size;
    *idatasize = _bf_env_idata_size == SIZE_MAX ? IDATA_MEM_SIZE : _bf_env_idata_size;
    *kssize = _bf_env_keystore_size == SIZE_MAX ? KEYSTORE_MEM_SIZE : _bf_env_keystore_size;
    *bfsize = _bf_env_bfstore_size == SIZE_MAX ? BFSTORE_MEM_SIZE : _bf_env_bfstore_size;

    // eviction policy
    _bf_current_eviction_policy = _bf_env_eviction_policy;

    _bf_skiplist_on = ((_bf_current_eviction_policy == BF_EVICTION_POLICY_OPT) 
        || (_bf_current_eviction_policy == BF_EVICTION_POLICY_LRU_K)) ?
        true : false;
}

int BF_Init() {
    size_t datasize, idatasize, kssize, bfsize;
    BF_ReadEnv();
    BF_DetermineSizesAndPolicy(&datasize, &idatasize, &kssize, &bfsize);

    return BF_InitWithSize(datasize, idatasize, kssize, bfsize);
}

int BF_Attach() {
    size_t datasize, idatasize, kssize, bfsize;
    BF_ReadEnv();
    BF_DetermineSizesAndPolicy(&datasize, &idatasize, &kssize, &bfsize);

    return BF_AttachWithSize(datasize, idatasize, kssize, bfsize);
}

void BF_Free() {
    size_t datasize, idatasize, kssize, bfsize;
    BF_ReadEnv();
    BF_DetermineSizesAndPolicy(&datasize, &idatasize, &kssize, &bfsize);

    BF_FreeWithSize(datasize, idatasize, kssize, bfsize);
}

void BF_Detach() {
    size_t datasize, idatasize, kssize, bfsize;
    BF_ReadEnv();
    BF_DetermineSizesAndPolicy(&datasize, &idatasize, &kssize, &bfsize);

    BF_DetachWithSize(datasize, idatasize, kssize, bfsize);
}

int BF_GetBuf(array_key key, PFpage **fpage) {      
    __BF_STAT_START();

	// fprintf(stderr, "Getbuf: %s {%lu, %lu}\n", key.arrayname, key.dcoords[0],key.dcoords[1]);
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);      // lock mutex

    int res = BF_GetBuf_Impl(key, fpage);

#ifdef BF_MEM_FOOTPRINT
    // for memory footprint
    if ((BF_curr_ts - __bf_mem_footprint_last) > 10) {
        size_t num_chunks, num_used, num_unused, unused_size, used_size;
        // diff
        fprintf(stderr, "[BufferTile] __bf_mem_footprint : %lu,", BF_curr_ts);
        // idata
        BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);
        BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);
        _BF_print_size_of_data_or_idata_in_list(mru);
        _BF_print_size_of_data_or_idata_in_list(lru);

        // fprintf(stderr, "** clean skip list **\n");
        BFList *clean_sl = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFClean_Skiplist_o);
        _BF_print_size_of_data_or_idata_in_skiplist(clean_sl);

        // fprintf(stderr, "** dirty skip list **\n");
        BFList *dirty_sl = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);
        _BF_print_size_of_data_or_idata_in_skiplist(dirty_sl);
        fprintf(stderr, "0\n");     // 0 is just padding

        BF_list_printall();

        __bf_mem_footprint_last = BF_curr_ts;
    }

    if ((BF_curr_ts - __bf_mem_footprint_printall_last) > 50) {
        __bf_mem_footprint_printall_last = BF_curr_ts;
        BF_shm_print_stats(_mspace_data);
    }
#endif

    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);    // unlock mutex

    __BF_STAT_END();

    // _BF_list_printall_skiplist(BF_SHM_BF_PTR(BFshm_hdr_ptr->BFSkip_list_o));

    return res;
}

int BF_UpdateNextTs(array_key key) {
    BFpage* target;
    BFhash_entry* hash_entry;
    _array_key *_key;
    int error = 0;

    __BF_STAT_START();

    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    init_internal_array_key(key, &_key);    

    if ((hash_entry = BF_hash_search(_key)) == NULL) {
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    target = BF_SHM_BF_PTR(hash_entry->bpage_o);
    _BF_UpdateNextTs(target, false, false);

DEALLOC:
    // free _array_key
    free_internal_array_key(_key);

    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }

    return BFE_OK;
}

int BF_DecreaseDCnt(char *arrname) {
    BFDCnt_entry *entry = BF_dCnt_search(arrname);
    if (entry == NULL) {
        fprintf(stderr, "[BufferTile] dCnt - entry not found for %s\n", arrname);
        return BFE_HASH_FOUND;
    }
    
    assert(entry->remains > 0);
    entry->remains--;

    fprintf(stderr, "[BufferTile] dCnt - remains=%lu\n", entry->remains);

    // remove object
    if (entry->remains == 0) {
        fprintf(stderr, "[BufferTile] dCnt - array delete!\n");
        tilestore_delete_array(arrname);
        BF_dCnt_remove(arrname);
    }

    return BFE_OK;
}

int BF_UnpinBuf(array_key bq) {
    // NOTE: This function use a goto statement to unlock the mutex.

    __BF_STAT_START();

    /* find target in hash */
    BFhash_entry* hash_entry;
    BFpage* target;
    _array_key *_key;
    int error = 0;
    uint8_t mutex_already_unlock = 0;       // needed when discard page

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    init_internal_array_key(bq, &_key);
    hash_entry = BF_hash_search(_key);

    if (hash_entry == NULL) {
        // This case can be possible since another process sharing the memory
        //      may evicts the page of the key.
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    if ((target = BF_SHM_BF_PTR(hash_entry->bpage_o)) == NULL) { /* not in hash */
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    if (target->count == 0) { /* if count is already 0 */
        fprintf(stderr, "[BufferTile] Too much Unpin() called!!\n");
        error = BFE_PAGE_UNPIN_FAIL;
        goto DEALLOC;
    }

    /* success */
    target->count--;
    
    // to support write prevention of tmp array
    PFpage *page = _BF_get_pfpage_ptr(target);
    
    // preemptive victimization!
    if (_bf_env_preemptive_eviction) {
        fprintf(stderr, "[BufferTile] preemptive eviction - arrname=%s, next_ts=%lu, count=%d\n", (char*) BF_SHM_KEY_PTR(((_array_key*)BF_SHM_KEY_PTR(target->key_o))->arrayname_o), target->next_ts, target->count);
        if (target->next_ts == BF_TS_INF && target->count == 0) {
            fprintf(stderr, "[BufferTile] preemptive eviction - try to discard\n");
            mutex_already_unlock = 1;
            pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

            // try to decrease dCnt
            bool in_dcnt = BF_DecreaseDCnt(bq.arrayname) == BFE_OK;
            if ((error = BF_discard_page(bq, !in_dcnt)) != BFE_OK) {
                fprintf(stderr, "[BufferTile] preemptive eviction - discard failed!!!!\n");
                return error;
            }
            write_prevention_cnt += 1;
        }
    }

DEALLOC:
    // free _array_key
    free_internal_array_key(_key);

    // unlock mutex
    if (!mutex_already_unlock) pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }
    return BFE_OK;
}

int BF_TouchBuf(array_key bq) {
    __BF_STAT_START();

    // NOTE: This function use a goto statement to unlock the mutex.

    // fprintf(stderr, "BF_TouthBuf(): LRU_HEAD= %ld\n", BFshm_hdr_ptr->BFLRU_head_o);
    BFpage* target;
    BFhash_entry* hash_entry;
    _array_key *_key;
    int error = 0;

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    init_internal_array_key(bq, &_key);
    hash_entry = BF_hash_search(_key);

    if (hash_entry == NULL) {
        // This case can be possible since another process sharing the memory
        //      may evicts the page of the key.
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    if ((target = BF_SHM_BF_PTR(hash_entry->bpage_o)) == NULL) { /* not in hash */
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    if (target->count == 0) { /* if not have been pinned */
        error = BFE_PAGE_UNPIN_FAIL;
        goto DEALLOC;
    }

    /* successsss */
    if (_bf_skiplist_on) 
        _BF_list_delete_skiplist(target);   

    target->dirty = TRUE;

    if (_bf_skiplist_on) 
        _BF_list_insert_skiplist(target);

    if (BF_list_remove(target) == NULL) {
        error = BFE_PAGE_REMOVE_FAIL;
        goto DEALLOC;
    }

    if (BF_list_set_last(target) != 0){
        error = BFE_LRU_SET_MRU_ERR;
        goto DEALLOC;
    }

    // mark it is needed to sort
    PFpage *page = _BF_get_pfpage_ptr(target);

DEALLOC:
    // free _array_key
    free_internal_array_key(_key);

    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }
    return BFE_OK;
}

int BF_FlushAll() {
    int error = 0;

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    while (true) {
        // if there is no enough memory, evict page
        BFpage* victim;
        if ((error = BF_get_victim_from_list(_mspace_data, &victim)) != BFE_OK) break;
        if ((error = BF_evict_victim(victim)) != BFE_OK) {
            fprintf(stderr, "BF_FlushAll failed!\n");
            break;
        }
    }

DEALLOC:
    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }
    return BFE_OK;
}

int BF_ForceWriteBuf(array_key bq) {
    // a function for testing purpose

    BFpage* target;
    BFhash_entry* hash_entry;
    _array_key *_key;
    int error = 0;

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    init_internal_array_key(bq, &_key);
    hash_entry = BF_hash_search(_key);

    if (hash_entry == NULL) {
        // This case can be possible since another process sharing the memory
        //      may evicts the page of the key.
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    if ((target = BF_SHM_BF_PTR(hash_entry->bpage_o)) == NULL) { /* not in hash */
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    // write to the disk
    if (write_to_tilestore(target) != 0) {
        error = BFE_INCOMPLETEWRITE;
        goto DEALLOC;
    }

    // set dirty to false
    target->dirty = FALSE;

    // do not set MRU

DEALLOC:
    // free _array_key
    free_internal_array_key(_key);

    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }
    return BFE_OK;
}

void BF_PrintDebug() {
#if BF_MALLOC_DEBUG
    fprintf(stderr, "========= mspace data =========\n");
    BF_shm_print_stats(_mspace_data);
    fprintf(stderr, "========= mspace key =========\n");
    BF_shm_print_stats(_mspace_key);
    fprintf(stderr, "========= mspace bf =========\n");
    BF_shm_print_stats(_mspace_bf);
#endif
}

void BF_ShowLists() {
    // lock mutex
    // pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    BF_list_printall();

    // unlock mutex
    // pthread_mutex_unlock(&BFshm_hdr_ptr->latch);
}

void BF_ShowBuf(array_key key){
    BFpage* ptr;
    BFhash_entry* hash_entry;
    _array_key *_key;

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    // init for internal array key
    // The _key should not be free when the page newly allocated
    //  , since it is flowed into the internal structure.
    init_internal_array_key(key, &_key);    
    
    // find the page in the pool
    if ((hash_entry = BF_hash_search(_key)) == NULL) {
        fprintf(stderr, "BF_ShowBuf(): A buffer for the key not found.\n");
        goto DEALLOC;
    }

    // print
    ptr = BF_SHM_BF_PTR_BFPAGE(hash_entry->bpage_o);
    fprintf(stderr, "BF_ShowBuf(): pin=%d, dirty=%d, fpage=%p(%ld), key=%p(%ld), nextpage=%p(%ld), prevpage=%p(%ld)\n",
        ptr->count, ptr->dirty,
        (void *) _BF_get_pfpage_ptr(ptr), ptr->fpage_o, 
        (void *) BF_SHM_KEY_PTR(ptr->key_o), ptr->key_o, 
        (void *) BF_SHM_BF_PTR(ptr->nextpage_o), ptr->nextpage_o, 
        (void *) BF_SHM_BF_PTR(ptr->prevpage_o), ptr->prevpage_o
    );

DEALLOC:
    // free _array_key
    free_internal_array_key(_key);

    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    return;
}

void BF_PrintError(const char *errstr){
    fprintf(stderr, "%s\n", errstr);
    switch(BFerrno) {
        case BFE_LRU_REMOVE_ERR:
            errstr = "BFE_LRU_REMOVE_ERR";
            break;
        case BFE_LRU_SET_MRU_ERR:
            errstr = "BFE_LRU_SET_MRU_ERR";
            break;
        case BFE_BFPAGE_ALLOC_FAIL:
            errstr = "BFE_BFPAGE_ALLOC_FAIL";
            break;
        case BFE_PAGE_REMOVE_FAIL:
            errstr = "BFE_PAGE_REMOVE_FAIL";
            break;

        case BFE_PAGE_UNPIN_FAIL:
            errstr = "BFE_PAGE_UNPIN_FAIL";
            break;
        case BFE_PAGE_TRY_REMOVE_BUT_PINNED:
            errstr = "BFE_PAGE_TRY_REMOVE_BUT_PINNED";
            break;

        case BFE_INCOMPLETEWRITE:
            errstr = "fail to write bfpage";
            break;
        case BFE_INCOMPLETEREAD:
            errstr = "fail to read bfpage";
            break;

        case BFE_HASHNOTFOUND :
            errstr = "CANNOT FIND HASH";
            break;
        case BFE_HASH_INSERT_ERR :
            errstr = "CANNOT INSERT HASH";
            break;
        case BFE_HASH_FOUND:
            errstr = "BFE_HASH_FOUND";
            break;
        default:
            errstr = "Error..";
            break;
    }
    fprintf(stderr, "BFerror(%d): %s\n", BFerrno, errstr);
    return;
}

int BF_CheckValidity() {
    // Count the number of elements in lists and hash table, and
    //    compare it with the BF_MAX_BUFS.
    // Hash Table
    BFbucket *bfhash_table = BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFhash_table_o);
    int total_hash_entry_count = 0;
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        // head check
        int hash_entry_count = 0;
        if (bfhash_table[i].head_o == BF_SHM_NULL) {
            continue;
        }

        // list check
        hash_entry_count++;
        BFhash_entry *head = BF_SHM_BF_PTR_BFHASHENTRY(bfhash_table[i].head_o);
        while (head->nextentry_o != BF_SHM_NULL) {
            BFhash_entry *next = BF_SHM_BF_PTR_BFHASHENTRY(head->nextentry_o);
            if (BF_SHM_BF_PTR_BFHASHENTRY(next->preventry_o) != head) {
                fprintf(stderr,
                    "BF_CheckValidity(): hash entry's prev entry is not correct\n");
                return BFE_VALIDITY_CHECK_ERR;
            }

            head = next;
            hash_entry_count++;
        }

        // tail check
        BFhash_entry *tail = BF_SHM_BF_PTR_BFHASHENTRY(bfhash_table[i].tail_o);
        if (tail != head) {
            fprintf(stderr,
                "BF_CheckValidity(): tail != head\n");
            return BFE_VALIDITY_CHECK_ERR;
        }

        if (bfhash_table[i].count != hash_entry_count) {
            fprintf(stderr,
                "BF_CheckValidity(): hash_entry_count != hash_entry_count\n");
            return BFE_VALIDITY_CHECK_ERR;
        }

        total_hash_entry_count += hash_entry_count;
    }

    // Nice!
    return BFE_OK;
}

int BF_flush_dirty_victim(BFpage* target){
    int error;

#ifdef BF_DENSE_MODE
    // only work when "dense mode conversion feature" is on
    PFpage *page = _BF_get_pfpage_ptr(target);
    bool_t is_densemode = page->est_density >= 0 && page->est_density <= 1;
    if (page->type == DENSE_FIXED && is_densemode) {
        // fprintf(stderr, "dm_conversion called!\n");

        // estimate the size of CSR format
        uint64_t *tile_extents = page->is_input ? BF_SHM_IDATA_PTR(page->tile_extents_o) : BF_SHM_DATA_PTR(page->tile_extents_o);
        uint64_t nnz = page->est_density * tile_extents[0] * tile_extents[1];
        uint64_t csr_size = nnz * (sizeof(double) + sizeof(uint64_t)) + (tile_extents[0] + 1) * sizeof(uint64_t);
        // fprintf(stderr, "csr_size=%lu, pagebuf_len=%lu\n", csr_size, page->pagebuf_len);

        // if saving with csr format is benefit
        if (csr_size < page->pagebuf_len) {
            // conversion to csr
            // fprintf(stderr, "conversion! called!\n");

            // unlock mutex
            pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

            struct timeval start;
            gettimeofday(&start, NULL);

            BF_ConvertToCSR(page);

            struct timeval end;                                                                                 \
            gettimeofday(&end, NULL);                                                                           \
            unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);  \
            bf_dm_conversion_time += diff;   
            bf_dm_conversion_count++;

            bftime -= diff;


            // lock mutex
            pthread_mutex_lock(&BFshm_hdr_ptr->latch);
        }
    }
#endif

    // write to tiledb
    if ((error = write_to_tilestore(target)) != 0) {
        BFerrno = BFE_INCOMPLETEWRITE;
        return BFerrno;
    }
    
    // remove from hash table (pool)
    if ((error = BF_hash_remove(target)) != 0) {
        BFerrno = BFE_HASHNOTFOUND;
        return BFerrno;
    }

    return BFE_OK;
}

/*
 * Remove pages having arrayname from the BF without writes.
 */
int BF_discard_pages(const char* arrayname, int* count) {
    assert(0);      // FIXME:

    int error = 0;

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    // remove all matching pages
    BFpage* target;
    if (count != NULL) (*count) = 0;       // adjust count 
    while ((target = BF_get_first_page(arrayname)) != NULL) {
        // remove from LRU list
        if (BF_list_remove(target) == NULL) {
            error = BFE_LRU_REMOVE_ERR;
            goto DEALLOC;
        }

        // remove from hash table (buffer pool)
        if (BF_hash_remove(target) != 0) {
            error = BFE_HASHNOTFOUND;
            goto DEALLOC;
        }

        // clean BF page
        BF_free_bfpage(target);

        if (count != NULL) (*count)++;       // increment count
    }

DEALLOC:
    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }
    return BFE_OK;
}

int BF_discard_page(array_key bq, bool write) {
    // NOTE: This function use a goto statement to unlock the mutex.
    BFpage* target;
    BFhash_entry* hash_entry;
    _array_key *_key;
    int error = 0;

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    init_internal_array_key(bq, &_key);

    hash_entry = BF_hash_search(_key);
    if (hash_entry == NULL) {
        // This case can be possible since another process sharing the memory
        //      may evicts the page of the key.
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    if ((target = BF_SHM_BF_PTR(hash_entry->bpage_o)) == NULL) { /* not in hash */
        error = BFE_HASHNOTFOUND;
        goto DEALLOC;
    }

    // we find target! so remove it from BF layer and free it
    // remove from LRU list
    if (BF_list_remove(target) == NULL) {
        error = BFE_LRU_REMOVE_ERR;
        goto DEALLOC;
    }

    if (_bf_skiplist_on) 
        _BF_list_delete_skiplist(target);

    if (write) BF_evict_victim(target);     // try to write if write is allowed
    else {
        // remove from hash table (buffer pool)
        if (BF_hash_remove(target) != 0) {
            error = BFE_HASHNOTFOUND;
            goto DEALLOC;
        }

        // clean BF page
        BF_free_bfpage(target);
    }

DEALLOC:
    // free _array_key
    free_internal_array_key(_key);

    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }
    return BFE_OK;
}

int BF_ResizeBuf(PFpage *page, size_t target_nnz) {
    // NOTE: This function use a goto statement to unlock the mutex.
    int error = 0;

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    // resize
    size_t datasize = page->pagebuf_len / page->max_idx;
    if (page->pagebuf_o != BF_SHM_NULL)
        BF_ResizeBuf_pagebuf(page, target_nnz, datasize);
    if (page->coords_o != BF_SHM_NULL)
        BF_ResizeBuf_coords(page, target_nnz);

    page->max_idx = target_nnz;

DEALLOC:
    // unlock mutex
    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    // return
    if (error != 0) {
        BFerrno = error; 
        return BFerrno;
    }

    return BFE_OK;
}


int BF_ConvertToDense(PFpage *fpage, bool is_densemode) {
    if (fpage->type != SPARSE_FIXED) {
        fprintf(stderr, "[BufferTile] BF_ConvertToDense - Wrong input type\n");
        return BFE_CONVERSION_WRONG_INPUT_TYPE;
    }

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    BFmspace *mspace = fpage->is_input ? _mspace_idata : _mspace_data;

    uint64_t nnz = fpage->max_idx;
    uint64_t dim_len = fpage->dim_len;
    uint64_t *tile_extents = BF_shm_malloc(mspace, sizeof(uint64_t) * dim_len);
    memcpy(tile_extents, _BF_SHM_PTR_AUTO(mspace, fpage->tile_extents_o), sizeof(uint64_t) * dim_len);

    uint64_t attrsize = fpage->attrsize;
    uint64_t data_size = 1;
    for (uint32_t d = 0; d < dim_len; d++) {
        data_size *= tile_extents[d];
    }

    uint64_t max_idx = data_size;
    data_size *= attrsize;
    
    // FIXME: assume double
    double *data_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(data_size) + 511);
    double *data = aligned_512bytes_ptr(data_unaligned);
    memset(data, 0, data_size);

    // conversion
    uint64_t *csr_coord_sizes = bf_util_pagebuf_get_coords_lens(fpage);
    uint64_t *csr_indptr = bf_util_pagebuf_get_coords(fpage, 0);
    uint64_t *csr_indices = bf_util_pagebuf_get_coords(fpage, 1);
    double *csr_buf = (double *)_BF_SHM_PTR_AUTO(mspace, fpage->pagebuf_o);

    bf_util_csr_to_dense(data, csr_buf, csr_indptr, csr_indices, tile_extents, 2);

    free_pfpage_content(fpage);

    // set pfpage
    fpage->type = DENSE_FIXED;
    fpage->dim_len = dim_len;
    fpage->max_idx = max_idx;
    fpage->tile_extents_o = _BF_SHM_OFFSET_AUTO(mspace, tile_extents);
    fpage->attrsize = attrsize;
    
    fpage->pagebuf_o = _BF_SHM_OFFSET_AUTO(mspace, data);
    fpage->pagebuf_len = data_size;
    fpage->_pagebuf_align_offset = (uintptr_t) data - (uintptr_t) data_unaligned;

    // normal read or dense read 
    fpage->unfilled_idx = max_idx;
    fpage->unfilled_pagebuf_offset = data_size;

    // dense mode
    fpage->est_density = (double) nnz / (data_size / sizeof(double));

    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);

    __BF_STAT_END();

    return 0;
}

int BF_ConvertToCSR(PFpage *fpage) {
    if (fpage->type != DENSE_FIXED) {
        fprintf(stderr, "[BufferTile] BF_ConvertToCSR - Wrong input type\n");
        return BFE_CONVERSION_WRONG_INPUT_TYPE;
    }

    __BF_STAT_START();

    // lock mutex
    pthread_mutex_lock(&BFshm_hdr_ptr->latch);

    BFmspace *mspace = fpage->is_input ? _mspace_idata : _mspace_data;

    // convert a dense tile to csr tile.
    // preparing variables
    double *pagebuf = _BF_SHM_PTR_AUTO(mspace, fpage->pagebuf_o);     // FIXME: assume double
    uint64_t nnz = 0;
    uint32_t dim_len = 2;
    uint64_t *tile_extents = _BF_SHM_PTR_AUTO(mspace, fpage->tile_extents_o);

    // 1. scan to get nnz per row (assuming double type) 
    for (uint64_t row_idx = 0; row_idx < tile_extents[0]; row_idx++) {
        for (uint64_t col_idx = 0; col_idx < tile_extents[1]; col_idx++) {
            uint64_t idx = row_idx * tile_extents[1] + col_idx;
            if (pagebuf[idx] != 0) {
                nnz++;
            }
        }
    }

    // 2. init tile
    // calculate the length of row
    uint64_t row_length = tile_extents[0];
    
    // coords
    uint64_t indptr_size = sizeof(uint64_t) * (row_length + 1);
    uint64_t *indptr_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(indptr_size) + 511);
    uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);

    uint64_t indices_size = sizeof(uint64_t) * nnz;
    uint64_t *indices_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(indices_size) + 511);
    uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);
  
    BFshm_offset_t *coords = BF_shm_malloc(mspace, sizeof(BFshm_offset_t) * dim_len);
    uint64_t *coord_sizes = BF_shm_malloc(mspace, sizeof(uint64_t) * dim_len);
    uint16_t *coord_offsets = BF_shm_malloc(mspace, sizeof(uint16_t) * dim_len);

    coords[0] = _BF_SHM_OFFSET_AUTO(mspace, indptr);
    coords[1] = _BF_SHM_OFFSET_AUTO(mspace, indices); 

    coord_sizes[0] = indptr_size;
    coord_sizes[1] = indices_size;

    coord_offsets[0] = (uintptr_t) indptr - (uintptr_t) indptr_unaligned;
    coord_offsets[1] = (uintptr_t) indices - (uintptr_t) indices_unaligned;

    // data
    uint64_t data_size, max_idx;
    data_size = nnz * fpage->attrsize;

    double *data_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(data_size) + 511);
    double *data = aligned_512bytes_ptr(data_unaligned);

    // we don't have to query, but fill a default value
    memset(data, 0, data_size);
    memset(indptr, 0, indptr_size);
    memset(indices, 0, indices_size);

    max_idx = nnz;

    // 3. conversion
    bf_util_dense_to_csr(pagebuf, data, indptr, indices, tile_extents, 2);

    // 4. finish to make the result tile
    free_pfpage_content(fpage);

    fpage->type = SPARSE_FIXED;
    fpage->dim_len = dim_len;
    fpage->max_idx = max_idx;
    
    fpage->pagebuf_o = _BF_SHM_OFFSET_AUTO(mspace, data);
    fpage->pagebuf_len = data_size;
    fpage->_pagebuf_align_offset = (uintptr_t) data - (uintptr_t) data_unaligned;
    
    fpage->unfilled_idx = nnz;
    fpage->unfilled_pagebuf_offset = data_size;

    fpage->coords_o = _BF_SHM_OFFSET_AUTO(mspace, coords);
    fpage->coords_lens_o = _BF_SHM_OFFSET_AUTO(mspace, coord_sizes);
    fpage->_coord_align_offsets_o = _BF_SHM_OFFSET_AUTO(mspace, coord_offsets);
    fpage->sparse_format = PFPAGE_SPARSE_FORMAT_CSR;

    fpage->est_density = NAN;

    pthread_mutex_unlock(&BFshm_hdr_ptr->latch);
    
    __BF_STAT_END();

    return 0;
}

int BF_HintDCnt(char *arrname, uint64_t num_tiles) {
    BFDCnt_entry *entry = BF_shm_malloc(_mspace_bf, sizeof(BFDCnt_entry));
    entry->remains = num_tiles;
    // arrname
    char *arrname_clone = BF_shm_malloc(_mspace_bf, strlen(arrname) * sizeof(char));
    memcpy(arrname_clone, arrname, strlen(arrname));
    entry->arrname_o = BF_SHM_BF_OFFSET(arrname_clone);

    BF_dCnt_insert(entry);
    fprintf(stderr, "[BufferTile] dCnt - inserted: %s,%lu\n", arrname, num_tiles);
}

void BF_ShowDCnt() {
    BF_dCnt_printall();
}

int BF_AddFuture(array_key key, uint64_t ts) {
    _array_key *_key;
    init_internal_array_key(key, &_key);

    BFFuture_entry_listentry *new = BF_shm_malloc(_mspace_bf, sizeof(BFFuture_entry_listentry));
    new->next_o = oNULL;
    new->ts = ts;

    BFFuture_entry *entry = BF_future_search(_key);
    if (entry == NULL) {
        entry = BF_shm_malloc(_mspace_bf, sizeof(BFFuture_entry));
        entry->key_o = BF_SHM_KEY_OFFSET(_key);
        
        entry->head_o = entry->tail_o = entry->curr_o = BF_SHM_BF_OFFSET(new);
        entry->len = 1;
        entry->last = 0;

        BF_future_insert(entry);
        return BFE_OK;
    } 

    // exists
    BFFuture_entry_listentry *tail = BF_SHM_BF_PTR(entry->tail_o);
    entry->tail_o = tail->next_o = BF_SHM_BF_OFFSET(new);
    entry->len++;
    entry->last = 0;

    free_internal_array_key(_key);
    return BFE_OK;
}


int _BF_AddHistory(_array_key *key, uint64_t ts) {
    array_key __key;
    __key.arrayname = BF_SHM_KEY_PTR_CHAR(key->arrayname_o);
    __key.attrname = BF_SHM_KEY_PTR_CHAR(key->attrname_o);
    __key.dcoords = BF_SHM_KEY_PTR_UINT64T(key->dcoords_o);
    __key.dim_len = key->dim_len;
    __key.emptytile_template = BF_EMPTYTILE_DENSE; // don't care
    
    _array_key *_key;
    init_internal_array_key(__key, &_key);

    BFFuture_entry_listentry *new = BF_shm_malloc(_mspace_bf, sizeof(BFFuture_entry_listentry));
    new->next_o = oNULL;
    new->ts = ts;

    BFFuture_entry *entry = BF_history_search(_key);
    if (entry == NULL) {
        entry = BF_shm_malloc(_mspace_bf, sizeof(BFFuture_entry));
        entry->key_o = BF_SHM_KEY_OFFSET(_key);
        
        entry->head_o = entry->tail_o = entry->curr_o = BF_SHM_BF_OFFSET(new);
        entry->len = 1;
        entry->last = ts;

        BF_history_insert(entry);
        return BFE_OK;
    } 

    // exists
    BFFuture_entry_listentry *head = BF_SHM_BF_PTR(entry->head_o);
    new->next_o = entry->head_o;
    entry->head_o = BF_SHM_BF_OFFSET(new);
    entry->len++;
    entry->last = ts;

    free_internal_array_key(_key);
    return BFE_OK;
}

void BF_QueryStart() {
    // Clear stats
    bf_this_query = bftime = bf_read_io_time = bf_iread_io_time = bf_write_io_time = 
        bf_getbuf_cnt_hit = bf_getbuf_cnt_total =
        bf_min_fl_retrival_time = bf_min_sl_update_time = 0;
    bf_read_io_size = bf_iread_io_size = bf_write_io_size = bf_getbuf_io_hit = bf_getbuf_io_total = 0;
    
    gettimeofday(&bf_query_start, NULL);
    
    BF_dCnt_init();
    BF_future_init();

    if (_bf_current_eviction_policy == BF_EVICTION_POLICY_LRU_K) {
        BF_history_init();
    }
}

void BF_PrintQueryStats() {
    printf("totla,buffer,io_read,io_iread,io_write,buf_hit,buf_total,fl_retrival,sl_update\n");
    printf("%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld\n", 
        bf_this_query, bf_read_io_time, bf_iread_io_time, bf_write_io_time, bf_getbuf_cnt_hit, bf_getbuf_cnt_total, bf_min_fl_retrival_time, bf_min_sl_update_time);
    printf("%d,%lld,%lld,%lld,%lld,%lld,%d,%d\n", 
        0, bf_read_io_size, bf_iread_io_size, bf_write_io_size, bf_getbuf_io_hit, bf_getbuf_io_total, 0, 0);
}

void BF_QueryEnd() {
    BF_ShowDCnt();

    BF_dCnt_free();
    BF_future_free();

    if (_bf_current_eviction_policy == BF_EVICTION_POLICY_LRU_K) {
        BF_history_printall();
        BF_history_free();
    }

    BF_FlushAll();

    // Debugging
    fprintf(stderr, "============_mspace_data ========\n");
    BF_shm_print_stats(_mspace_data);
    fprintf(stderr, "============_mspace_idata ========\n");
    BF_shm_print_stats(_mspace_idata);
    BF_ShowLists();

    struct timeval bf_query_end;
    gettimeofday(&bf_query_end, NULL);
    bf_this_query = ((bf_query_end.tv_sec - bf_query_start.tv_sec) * 1000000) + (bf_query_end.tv_usec - bf_query_start.tv_usec);
}