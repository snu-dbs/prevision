#include "bf.h"
#include "bf_struct.h"
#include "arraykey.h"
#include "bf_malloc.h"
#include "list.h"
#include "hash.h"
#include <sys/time.h>

extern BFmspace *_mspace_data, *_mspace_bf;

extern unsigned long long bf_getbuf_cnt_total;
extern unsigned long long bf_getbuf_cnt_hit;

extern unsigned long long bf_getbuf_io_total;
extern unsigned long long bf_getbuf_io_hit;

unsigned long long bf_min_fl_retrival_time;

PFpage* _BF_get_pfpage_ptr(BFpage *bfpage) {
    if (bfpage->is_input) return BF_SHM_IDATA_PTR(bfpage->fpage_o);
    else return BF_SHM_DATA_PTR(bfpage->fpage_o);
}

int BF_init_bfpage(BFpage **bf_page) {
    BFpage* ptr = BF_shm_malloc(_mspace_bf, sizeof(BFpage));
    ptr->fpage_o = BF_SHM_NULL;
    ptr->nextpage_o = BF_SHM_NULL;
    ptr->prevpage_o = BF_SHM_NULL;
    ptr->key_o = BF_SHM_NULL;
    ptr->dirty = FALSE;
    ptr->count = 0;
    ptr->is_input = false;
    
    if (_bf_skiplist_on) {
        BFshm_offset_t *nextpages_o = BF_shm_malloc(_mspace_bf, sizeof(BFshm_offset_t) * _bf_skiplist_level);
        for (uint8_t i = 0; i < _bf_skiplist_level; i++) {
            nextpages_o[i] = oNULL;
        }

        BFshm_offset_t *prevpages_o = BF_shm_malloc(_mspace_bf, sizeof(BFshm_offset_t) * _bf_skiplist_level);
        for (uint8_t i = 0; i < _bf_skiplist_level; i++) {
            prevpages_o[i] = oNULL;
        }

        ptr->s_nextpages_o = BF_SHM_BF_OFFSET(nextpages_o);
        ptr->s_prevpages_o = BF_SHM_BF_OFFSET(prevpages_o);
    } else {
        ptr->s_nextpages_o = BF_SHM_NULL;
        ptr->s_prevpages_o = BF_SHM_NULL;
    }

    ptr->next_ts = 0;

    (*bf_page) = ptr;
    
    return 0;
}

int BF_free_bfpage(BFpage *bf_page) {
    if (bf_page == NULL) return 0;
    
    if (bf_page->fpage_o != BF_SHM_NULL) {
        PFpage *page = _BF_get_pfpage_ptr(bf_page);

        free_pfpage(page);
    }

    if (_bf_skiplist_on) {
        if (bf_page->s_nextpages_o != BF_SHM_NULL)
            BF_shm_free(_mspace_bf, BF_SHM_BF_PTR(bf_page->s_nextpages_o));
        if (bf_page->s_prevpages_o != BF_SHM_NULL) 
            BF_shm_free(_mspace_bf, BF_SHM_BF_PTR(bf_page->s_prevpages_o));
    }

    free_internal_array_key(BF_SHM_KEY_PTR_KEY(bf_page->key_o));
    BF_shm_free(_mspace_bf, bf_page);
    return 0;
}

size_t _get_pagebuf_size(PFpage *page) {
    size_t size = 0;
    if (page->type == SPARSE_FIXED) {
        uint64_t *lens = bf_util_pagebuf_get_coords_lens(page);
        size += lens[0];
        size += lens[1];
    }

    size += page->pagebuf_len;

    return size;
}

int BF_get_victim_from_list(BFmspace *mspace, BFpage** out) {
    bool_t debug_correctness = false;

    if (debug_correctness) {
        if (_bf_skiplist_on) {
            fprintf(stderr, "[SIMULATOR] dirty skiplist:\n");
            BFList *skiplist = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);
            _BF_list_printall_skiplist(skiplist);
        }
    }

    BFpage *victim;
    if (_bf_current_eviction_policy == BF_EVICTION_POLICY_LRU_K) {
        if ((victim = BF_list_evict_page_lruk()) == NULL) {
            return BFE_BFPAGE_ALLOC_FAIL;
        }
    } else if (_bf_current_eviction_policy == BF_EVICTION_POLICY_OPT) {
        if ((victim = BF_list_evict_page_opt()) == NULL) {
            return BFE_BFPAGE_ALLOC_FAIL;
        }
    } else if (_bf_current_eviction_policy == BF_EVICTION_POLICY_MRU_ONLY) {
        if ((victim = BF_list_evict_page_mru()) == NULL) {
            return BFE_BFPAGE_ALLOC_FAIL;
        }
    } else if (_bf_current_eviction_policy == BF_EVICTION_POLICY_LRU_ONLY) {
        if ((victim = BF_list_evict_page_lru()) == NULL) {
            return BFE_BFPAGE_ALLOC_FAIL;
        }
    }

    if (debug_correctness) { 
        _array_key *_key = BF_SHM_KEY_PTR(victim->key_o);
        char *arrname = BF_SHM_KEY_PTR_CHAR(_key->arrayname_o);
        uint64_t *dcoords = BF_SHM_KEY_PTR(_key->dcoords_o);
        fprintf(stderr, "[SIMULATOR] victim - %lu,{%s,%lu,%lu},\n", BF_curr_ts, arrname, dcoords[0], dcoords[1]);
    }

    *out = victim;
    return BFE_OK;
}

int BF_evict_victim(BFpage *victim) {
    PFpage *evicted_page = _BF_get_pfpage_ptr(victim);
    _array_key *keyy = BF_SHM_KEY_PTR(victim->key_o);
    bool is_temp_req = strcmp(BF_SHM_KEY_PTR(keyy->attrname_o), "t") == 0;

    // remove the page
    if (!is_temp_req && victim->dirty) {
        // If the evicted page is dirty, flush it
        if (BF_flush_dirty_victim(victim) != 0) {
            return BFE_INCOMPLETEWRITE;
        }
    } else {
        // clean page
        if (BF_hash_remove(victim) != 0) {
            return BFE_HASHNOTFOUND;
        }
    }

    BF_free_bfpage(victim);
    return BFE_OK;
}

int BF_claim_memory(BFmspace *mspace, size_t size) {    
    int error = 0;    
    size_t evicted_clean_size = 0, evicted_dirty_size = 0;

    // Evict pages until memory has enough space
    while (true) {
        // check if memory is enough to get a new page
        bool_t is_memory_free_enough = BF_shm_does_fittable_chunk_exists(
            mspace, size);
        if (is_memory_free_enough) break;

        // if there is no enough memory, evict page
        BFpage* victim;
        if ((error = BF_get_victim_from_list(mspace, &victim)) != BFE_OK) {
            fprintf(stderr, "============_mspace_data ========\n");
            BF_shm_print_stats(_mspace_data);
            fprintf(stderr, "============_mspace_idata ========\n");
            BF_shm_print_stats(_mspace_idata);
            BF_ShowLists();

            return error;
        }

        PFpage *page = BF_SHM_DATA_PTR(victim->fpage_o);
        if (victim->dirty) evicted_dirty_size += (size_t) page->pagebuf_len;
        else evicted_clean_size += (size_t) page->pagebuf_len;

        if ((error = BF_evict_victim(victim)) != BFE_OK) return error;
    }
    
    if (evicted_clean_size > 0 || evicted_dirty_size > 0)
        // time, requested, evicted_dirty, evicted_clean 
        fprintf(stderr, "[BufferTile] BF_claim_memory - bf_claim: %lu,%f,%f,%f\n", 
            BF_curr_ts, (float) size * 0.000001, (float) evicted_dirty_size * 0.000001, (float) evicted_clean_size * 0.000001);
    return error;
}

uint64_t _BF_lruk_update(BFpage *target, bool_t pagefault) {
    uint64_t crp = _bf_env_lruk_crp;
    uint8_t _k_cnt = _bf_env_lruk_k;

    // find entry
    BFFuture_entry *entry = BF_history_search(BF_SHM_KEY_PTR(target->key_o));
    if (!pagefault && entry != NULL) {      // entry should not be NULL if page hit
        // for the case when uncorrelated pagehit  
        // check if correlated reference
        bool_t is_ucr = (BF_curr_ts - entry->last) > crp;
        if (is_ucr) {
            BFFuture_entry_listentry *item = BF_SHM_BF_PTR(entry->head_o);
            uint8_t k_cnt = _k_cnt - 1;
            uint64_t cporp = entry->last - item->ts;
            
            // "close out" the old period
            for (;(void*) item != _mspace_bf && k_cnt > 0;
                    item = BF_SHM_BF_PTR(item->next_o), k_cnt--) {
                item->ts += cporp;
            }

            _BF_AddHistory(BF_SHM_KEY_PTR(target->key_o), BF_curr_ts);
        } else {
            entry->last = BF_curr_ts;
        }
    } else {
        // for cases when a pagefault
        // we does not care RIP, so just follow history log
        _BF_AddHistory(BF_SHM_KEY_PTR(target->key_o), BF_curr_ts);
        entry = BF_history_search(BF_SHM_KEY_PTR(target->key_o));
    }

    fprintf(stderr, "[BufferTile] LRU-K - HIST=[");
    for (BFFuture_entry_listentry *i = BF_SHM_BF_PTR(entry->head_o);
            (void*) i != _mspace_bf;
            i = BF_SHM_BF_PTR(i->next_o)) {
        fprintf(stderr, "%lu,", i->ts);
    }
    fprintf(stderr, "] (K in ascending order)\n");
    
    // calculate backward-k distance
    uint8_t k_cnt = _k_cnt - 1;
    BFFuture_entry_listentry *item;
    for (item = BF_SHM_BF_PTR(entry->head_o);
            (void*) item != _mspace_bf && k_cnt > 0;
            item = BF_SHM_BF_PTR(item->next_o), k_cnt--) {}

    // return the value of backward-k for victim selection 
    if ((void*) item != _mspace_bf && k_cnt == 0) {
        fprintf(stderr, "[BufferTile] LRU-K - Backward-K distance found, K=%u, ts=%lu\n", _k_cnt, item->ts);
        // I added one, since Bf start clock from zero. 
        // That is, it could not be distinguished from infinity distance.
        return item->ts + 1;         
    } 

    fprintf(stderr, "[BufferTile] LRU-K - Backward-K distance not found, K=%u, ts=0(inf)\n", _k_cnt);
    return 0;
}

void _BF_UpdateNextTs(BFpage *target, bool_t pagefault, bool_t skip_sl_update) {
    // stats
    struct timeval fl_start;
    gettimeofday(&fl_start, NULL);

    // find and set next_ts
    BFFuture_entry *entry = BF_future_search(BF_SHM_KEY_PTR(target->key_o));
    if (entry == NULL) return;
    
    uint64_t bf_ts = BF_TS_INF;
    for (BFFuture_entry_listentry *item = BF_SHM_BF_PTR(entry->curr_o);
            (void*) item != _mspace_bf;
            item = BF_SHM_BF_PTR(item->next_o)) {
        if (item->ts <= BF_curr_ts) continue;

        entry->curr_o = BF_SHM_BF_OFFSET(item);
        bf_ts = item->ts;
        break;
    }

    // not found: it will be vicimized after unpint
    if (bf_ts == BF_TS_INF)
        entry->curr_o = oNULL;

    /*                      LRU-K                      */ 
    if (_bf_current_eviction_policy == BF_EVICTION_POLICY_LRU_K) {
        uint64_t btpk = _BF_lruk_update(target, pagefault);

        // update next ts
        // it is not problem even the replacement algorithm is LRU-K, since 
        //      the page for this entry is going to be pinned, and  
        //      it is immediately (preemptive) evicted after unpinned.
        if (bf_ts == BF_TS_INF) target->next_ts = bf_ts;
        else target->next_ts = btpk;
    } else {
        target->next_ts = bf_ts;
    }

    struct timeval fl_end;
    gettimeofday(&fl_end, NULL);

    unsigned long long fl_diff = ((fl_end.tv_sec - fl_start.tv_sec) * 1000000) + (fl_end.tv_usec - fl_start.tv_usec); 
    bf_min_fl_retrival_time += fl_diff;

    fprintf(stderr, "[BufferTile] future - update curr=%p(%lu), bf_ts=%lu\n", BF_SHM_BF_PTR(entry->curr_o), entry->curr_o, bf_ts);

    // update SORTED_LIST
    if (!_bf_skiplist_on || skip_sl_update) return;

    // remove bfpage and re-insert it
    // deleting it after ts is updated is fine since delete function does not care ts value
    if (!pagefault) _BF_list_delete_skiplist(target);   
    _BF_list_insert_skiplist(target);
}


int BF_GetBuf_Impl(array_key key, PFpage **fpage) {
    BFpage* target;
    BFhash_entry* hash_entry;
    _array_key *_key;
    int error = 0;

    init_internal_array_key(key, &_key);    
    
    // find the page in the pool
    if ((hash_entry = BF_hash_search(_key)) != NULL) {
        /* If the page in the pool
         * if there is bfpage 
         * just count++ and set the BFpage as mru
        */

        // the _array_key is now useless so free it.
        free_internal_array_key(_key);   

        // BF page 
        target = BF_SHM_BF_PTR(hash_entry->bpage_o);
        target->count++;

        // deal with only input when next_ts is over
        if (target->next_ts <= BF_curr_ts)
            _BF_UpdateNextTs(target, false, false);

        // LRU / MRU
        if (BF_list_remove(target) == NULL) {
            fprintf(stderr, "[BufferTile] Internal Error (LRU REMOVE FAILED).\n");
            return BFE_LRU_REMOVE_ERR;
        } 
        
        if (BF_list_set_last(target) != 0) {
            fprintf(stderr, "[BufferTile] Internal Error (SETTING MRU FAILED).\n");
            return BFE_LRU_SET_MRU_ERR; 
        }

        // get PFpage
        *fpage = _BF_get_pfpage_ptr(target);
        
        // Statistics        
        uint64_t size = bf_util_pagebuf_get_len(*fpage);
        if ((*fpage)->type == SPARSE_FIXED) {
            uint64_t *lens = bf_util_pagebuf_get_coords_lens(*fpage);
            size += (lens[0] + lens[1]);
        }

        bf_getbuf_cnt_total++;
        bf_getbuf_cnt_hit++;

        bf_getbuf_io_total += size;
        bf_getbuf_io_hit += size;

        // return
        return BFE_OK;
    }

    // If the page not in the pool, eivict some pages and get new BFpage.
    // malloc new bfpage
    BFpage* free_BFpage;
    if (BF_init_bfpage(&free_BFpage) != 0) {
        fprintf(stderr, "[BufferTile] Internal Error (Page allocation failed).\n");
        return BFE_BFPAGE_ALLOC_FAIL;
    }

    // read array
    bf_tileio_ret_t tio_ret = read_from_tilestore(free_BFpage, _key);
    if (tio_ret == BF_TILEIO_NONE){
        // This is an empty tile as well as user requested for NULL page
        
        // update only future log
        _BF_UpdateNextTs(free_BFpage, true, true);
        BF_free_bfpage(free_BFpage);

        bf_getbuf_cnt_total++;

        *fpage = NULL;

        return BFE_OK;
    } else if (tio_ret == BF_TILEIO_ERR) {
        fprintf(stderr, "[BufferTile] Read failed.\n");
        return BFE_INCOMPLETEREAD;
    }

    // update eviction list
    _BF_UpdateNextTs(free_BFpage, true, false);
    
    if (BF_list_set_last(free_BFpage) != 0){
        fprintf(stderr, "[BufferTile] Internal Error (SETTING MRU FAILED).\n");
        return BFE_LRU_SET_MRU_ERR;
    }
    
    // insert into hash table (pool)
    if (BF_hash_insert(free_BFpage) != 0) {
        fprintf(stderr, "[BufferTile] Internal Error (Hash Insert Failed).\n");
        return BFE_HASH_INSERT_ERR;
    }

    // set return value
    *fpage = _BF_get_pfpage_ptr(free_BFpage);

    // Statistics        
    uint64_t size = bf_util_pagebuf_get_len(*fpage);
    if ((*fpage)->type == SPARSE_FIXED) {
        uint64_t *lens = bf_util_pagebuf_get_coords_lens(*fpage);
        size += (lens[0] + lens[1]);
    }

    bf_getbuf_cnt_total++;
    bf_getbuf_io_total += size;

    // return
    return BFE_OK;
}

int BF_ResizeBuf_pagebuf(PFpage *page, size_t nnz, size_t datasize)
{
    int error = 0;
    size_t nbytes = nnz * datasize;
    
    assert(page->unfilled_pagebuf_offset <= nbytes);

    // claim
    size_t request_size = ceil_to_512bytes(nbytes) + 511;
    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;
    if ((error = BF_claim_memory(mspace, request_size)) != 0) {
        // BF_shm_print_stats(mspace);
        fprintf(stderr, "[BufferTile] Memory Claim Failed. It probably means that no memory available.\n");
        return error;
    }

    // get original ptr
    void *data = (void*) _BF_SHM_PTR_AUTO(mspace, page->pagebuf_o);
    void *data_unaligned = (void*) ((uintptr_t) _BF_SHM_PTR_AUTO(mspace, page->pagebuf_o) - page->_pagebuf_align_offset);
    
    // malloc new ptr
    void *data_new_unaligned = BF_shm_malloc(mspace, request_size);
    void *data_new = aligned_512bytes_ptr(data_new_unaligned);
    
    // copy
    memcpy(data_new, data, page->unfilled_pagebuf_offset);

    // set new ptr to page
    page->pagebuf_o = _BF_SHM_OFFSET_AUTO(mspace, data_new);
    page->_pagebuf_align_offset = (uintptr_t) data_new - (uintptr_t) data_new_unaligned;
    page->pagebuf_len = nbytes;
    
    // free
    BF_shm_free(mspace, data_unaligned);

    return error;
}

int BF_ResizeBuf_coords(PFpage *page, size_t nnz)
{
    int error = 0;

    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;

    // get pointers
    uint64_t *coords_lens = (uint64_t*) _BF_SHM_PTR_AUTO(mspace, page->coords_lens_o);
    BFshm_offset_t *coords = _BF_SHM_PTR_AUTO(mspace, page->coords_o);
    uint16_t *coord_offsets = _BF_SHM_PTR_AUTO(mspace, page->_coord_align_offsets_o);

    size_t nbytes = nnz * sizeof(uint64_t);
    assert(page->sparse_format == PFPAGE_SPARSE_FORMAT_CSR);
    // uint32_t d = (page->sparse_format == PFPAGE_SPARSE_FORMAT_CSR) ? 
    //     1 : 0;
    uint32_t d = 1;
    for (; d < page->dim_len; d++)
    {
        // assert(coords_lens[d] < nbytes);
        assert(page->unfilled_idx * sizeof(uint64_t) <= nbytes);

        // claim
        size_t request_size = ceil_to_512bytes(nbytes) + 511;
        if ((error = BF_claim_memory(mspace, request_size)) != 0) {
            // BF_shm_print_stats(mspace);
            fprintf(stderr, "[BufferTile] Memory Claim Failed. It probably means that no memory available.\n");
            return error;
        }

        // get pointers
        void *data = (void*) _BF_SHM_PTR_AUTO(mspace, coords[d]);
        void *data_unaligned = (void*) ((uintptr_t) _BF_SHM_PTR_AUTO(mspace, coords[d]) - coord_offsets[d]);
        
        // get new ptrs
        void *data_new_unaligned = BF_shm_malloc(mspace, request_size);
        void *data_new = aligned_512bytes_ptr(data_new_unaligned);
        
        // copy
        memcpy(data_new, data, page->unfilled_idx * sizeof(uint64_t));
        
        // set new ptr to page
        coords[d] = _BF_SHM_OFFSET_AUTO(mspace, data_new);
        coord_offsets[d] = (uintptr_t) data_new - (uintptr_t) data_new_unaligned;
        coords_lens[d] = nbytes;

        // free
        BF_shm_free(mspace, data_unaligned);
    }

    return 0;
}

int BF_ResizeBuf_offset(PFpage *page, size_t nnz)
{
    assert(0);
    return 0;
}

int BF_ResizeBuf_validty(PFpage *page, size_t nnz)
{
    assert(0);
    return 0;
}
