#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include "bf.h"
#include "list.h"
#include "arraykey.h"
#include "hash.h"
#include "bf_malloc.h"


extern BFmspace *_mspace_bf, *_mspace_key, *_mspace_data;
unsigned long long freed_capacity = 0;

unsigned long long bf_min_sl_update_time;

void _BF_list_init(BFList *list) {
    list->head_o = BF_SHM_NULL;
    list->tail_o = BF_SHM_NULL;
    list->size = 0;
}

void BF_list_init(){
    BFList *lru = BF_shm_malloc(_mspace_bf, sizeof(BFList));
    BFList *mru = BF_shm_malloc(_mspace_bf, sizeof(BFList));

    _BF_list_init(lru);
    _BF_list_init(mru);

    BFshm_hdr_ptr->BFLRU_o = BF_SHM_BF_OFFSET(lru);
    BFshm_hdr_ptr->BFMRU_o = BF_SHM_BF_OFFSET(mru);

    if (_bf_skiplist_on) {
        BFList *dirty_sl = BF_shm_malloc(_mspace_bf, sizeof(BFList));
        
        _BF_list_init_skiplist(dirty_sl);

        BFshm_hdr_ptr->BFDirty_Skiplist_o = BF_SHM_BF_OFFSET(dirty_sl);
    }
}

void _BF_list_free(BFList *list) {
    BFshm_offset_t head_o = list->head_o;
    while (head_o != BF_SHM_NULL) {
        BFpage *head = BF_SHM_BF_PTR_BFPAGE(head_o);
        head_o = head->nextpage_o;

        if (head->dirty) {
            if (head->next_ts < BF_TS_INF) {
                BF_flush_dirty_victim(head);
            } else {
                PFpage *page = _BF_get_pfpage_ptr(head);
                freed_capacity += page->pagebuf_len;
            }
        }
        BF_free_bfpage(head);
    }

    list->head_o = BF_SHM_NULL;
    list->tail_o = BF_SHM_NULL;
    list->size = 0;
}

void BF_list_free(){
    BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);
    BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);

    _BF_list_free(lru);
    _BF_list_free(mru);

    if (_bf_skiplist_on) {
        BFList *dirty_sl = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);
        
        _BF_list_free_skiplist(dirty_sl);
    }
}

int _BF_list_set_last(BFList *list, BFpage* dataptr) {
    if (list->size == 0) { /* first time */
        dataptr->nextpage_o = BF_SHM_NULL;
        dataptr->prevpage_o = BF_SHM_NULL;
        list->tail_o = BF_SHM_BF_OFFSET(dataptr);
        list->head_o = BF_SHM_BF_OFFSET(dataptr);
        list->size++;
    } else { /* general case */
        dataptr->nextpage_o = BF_SHM_NULL;
        dataptr->prevpage_o = list->tail_o;
        ((BFpage*) BF_SHM_BF_PTR(list->tail_o))->nextpage_o 
            = BF_SHM_BF_OFFSET(dataptr);
        list->tail_o = BF_SHM_BF_OFFSET(dataptr);
        list->size++;
    }
    return 0;
}

/* add BFpage to LRU_list at back */
int BF_list_set_last(BFpage* dataptr) {
    BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);
    BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);

    // fprintf(stderr, "%d @ BF_list_set_last\n", _bf_current_eviction_policy);
    switch (_bf_current_eviction_policy) {
        case BF_EVICTION_POLICY_LRU_ONLY: return _BF_list_set_last(lru, dataptr);
        case BF_EVICTION_POLICY_MRU_ONLY: return _BF_list_set_last(mru, dataptr);
        case BF_EVICTION_POLICY_OPT: case BF_EVICTION_POLICY_LRU_K: return BFE_OK;
        default: assert(0);
    }
}

/* Evict bfpage and return it. 

This function find a most suitable bfpage to evict.
It first find unpinned and clean (dirty = 0) page. If a page is found, 
    the function returns it.
If there is no unpinned and clean page, it find unpinned and dirty page.
    If a page is found, the function returns the dirty one.
If there is no page to return, it returns NULL.
*/
BFpage* BF_list_evict_page_lru() {
    BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);

    // No page in LRU list
    if (lru->size == 0) {
        return NULL;
    }

    // get a unpinned and clean page from LRU //clean
    BFpage* ptr = NULL;
    for (ptr = BF_SHM_BF_PTR_BFPAGE(lru->head_o);
            (void *) ptr != (void *) _mspace_bf;    
            // if tail, nextpage_o will be oNULL, ptr == _msapce
            ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {
        if (ptr->count == 0) {      // is the page unpinned? 
            _array_key *key = BF_SHM_KEY_PTR_KEY(ptr->key_o);
            bool is_temp_req = strcmp(BF_SHM_KEY_PTR(key->attrname_o), "t") == 0;
            if (is_temp_req || ptr->dirty == FALSE) {
                // Is the page tmp page or clean?
                // Tmp page should be evicted first even though it is dirty
                break;
            }
        } 
    }

    // If it failed to find (i.e., ptr == _mspace), 
    //      restart lookup to find unpinned and dirty page
    bool_t failed_to_find = ((void *) ptr == (void *) _mspace_bf);
    if (failed_to_find) {
        for (ptr = BF_SHM_BF_PTR_BFPAGE(lru->head_o);
                (void *) ptr != (void *) _mspace_bf;    
                // if tail, nextpage_o will be oNULL, ptr == _msapce
                ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {
            if (ptr->count == 0 && ptr->dirty == TRUE) 
                break;     
        }
    }

    // If it failed to find again, it means that there are no suitable page
    //      to evict, so it returns NULL.
    failed_to_find = ((void *) ptr == (void *) _mspace_bf);
    if (failed_to_find) {
        return NULL;
    }

    // FIXME: duplicated
    // remove the one from LRU list
    lru->size--;  // adjust size
    if (ptr == BF_SHM_BF_PTR_BFPAGE(lru->head_o) &&
            ptr == BF_SHM_BF_PTR_BFPAGE(lru->tail_o)) {
        // only one page in LRU list

        // manipulate the hdr
        lru->head_o = BF_SHM_NULL;
        lru->tail_o = BF_SHM_NULL;
    } else if (ptr == BF_SHM_BF_PTR_BFPAGE(lru->head_o)) {
        // if it is the head of the list

        // manipulate the hdr
        lru->head_o = ptr->nextpage_o;

        // adjust the next
        BFpage *next = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o);
        next->prevpage_o = BF_SHM_NULL;
    } else if (ptr == BF_SHM_BF_PTR_BFPAGE(lru->tail_o)) {
        // if it is the tail of the list

        // manipulate the hdr
        lru->tail_o = ptr->prevpage_o;

        // adjust the prev
        BFpage *prev = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o);
        prev->nextpage_o = BF_SHM_NULL;
    } else {
        // adjust the next and prev
        BFpage *next = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o);
        BFpage *prev = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o);
        next->prevpage_o = ptr->prevpage_o;
        prev->nextpage_o = ptr->nextpage_o;
    }

    // adjust ptr
    ptr->nextpage_o = BF_SHM_NULL;
    ptr->prevpage_o = BF_SHM_NULL;

    return ptr;
}

BFpage* BF_list_evict_page_mru() {
    BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);

    if (mru->size == 0) {
        return NULL;
    }

    BFpage* ptr = NULL;
    for (ptr = BF_SHM_BF_PTR_BFPAGE(mru->tail_o);
            (void *) ptr != (void *) _mspace_bf;    
            ptr = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o)) {
        if (ptr->count == 0) {
            _array_key *key = BF_SHM_KEY_PTR_KEY(ptr->key_o);
            bool is_temp_req = strcmp(BF_SHM_KEY_PTR(key->attrname_o), "t") == 0;
            if (is_temp_req || ptr->dirty == FALSE) {
                break;
            }
        } 
    }

    bool_t failed_to_find = ((void *) ptr == (void *) _mspace_bf);
    if (failed_to_find) {
        for (ptr = BF_SHM_BF_PTR_BFPAGE(mru->tail_o);
                (void *) ptr != (void *) _mspace_bf;    
                ptr = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o)) {
            if (ptr->count == 0 && ptr->dirty == TRUE) 
                break;     
        }
    }

    failed_to_find = ((void *) ptr == (void *) _mspace_bf);
    if (failed_to_find) {
        return NULL;
    }

    // FIXME: duplicated
    mru->size--; 
    if (ptr == BF_SHM_BF_PTR_BFPAGE(mru->head_o) &&
            ptr == BF_SHM_BF_PTR_BFPAGE(mru->tail_o)) {
        mru->head_o = BF_SHM_NULL;
        mru->tail_o = BF_SHM_NULL;
    } else if (ptr == BF_SHM_BF_PTR_BFPAGE(mru->head_o)) {
        mru->head_o = ptr->nextpage_o;
        BFpage *next = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o);
        next->prevpage_o = BF_SHM_NULL;
    } else if (ptr == BF_SHM_BF_PTR_BFPAGE(mru->tail_o)) {
        mru->tail_o = ptr->prevpage_o;
        BFpage *prev = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o);
        prev->nextpage_o = BF_SHM_NULL;
    } else {
        BFpage *next = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o);
        BFpage *prev = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o);
        next->prevpage_o = ptr->prevpage_o;
        prev->nextpage_o = ptr->nextpage_o;
    }

    ptr->nextpage_o = BF_SHM_NULL;
    ptr->prevpage_o = BF_SHM_NULL;

    return ptr;
}

 /* Get the first page matching the arrayname from lists */
BFpage* BF_get_first_page(const char* arrayname){
    /* LRU first */
    BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);
    // No page in LRU list
    if (lru->size == 0) {
        return NULL;
    }

    // get the unpinned LRU
    BFpage* ptr;
    for (ptr = BF_SHM_BF_PTR_BFPAGE(lru->head_o);
         (void *) ptr != (void *) _mspace_bf;    
            // if tail, nextpage_o will be oNULL, ptr == _msapce
         ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {

        _array_key *key = BF_SHM_KEY_PTR_KEY(ptr->key_o);
        if (strcmp(BF_SHM_KEY_PTR_CHAR(key->arrayname_o), arrayname) == 0) {
            return ptr; // find the unpinned
        }
    }

    /* MRU */
    BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);
    if (mru->size == 0) {
        return NULL;
    }

    for (ptr = BF_SHM_BF_PTR_BFPAGE(mru->tail_o);
         (void *) ptr != (void *) _mspace_bf;    
         ptr = BF_SHM_BF_PTR_BFPAGE(ptr->prevpage_o)) {

        _array_key *key = BF_SHM_KEY_PTR_KEY(ptr->key_o);
        if (strcmp(BF_SHM_KEY_PTR_CHAR(key->arrayname_o), arrayname) == 0) {
            return ptr; 
        }
    }

    // if we reach here, we didn't find the arrayname
    return NULL;
}

BFpage* _BF_list_remove(BFList *list, BFpage *target) {
    /* if target == head */
    if (list->size == 0) {
        printf("not at list\n");
        // return BFE_LRU_REMOVE_ERR;
        return NULL;
    }

    // check the LRU list
    if (compare_array_key(
            BF_SHM_KEY_PTR_KEY(
                BF_SHM_BF_PTR_BFPAGE(list->head_o)->key_o),
            BF_SHM_KEY_PTR_KEY(target->key_o)) == 0) {
        // head case
        list->head_o = target->nextpage_o;
        list->size--;
        
        if (list->size == 0) {
            list->tail_o = BF_SHM_NULL;     /* nothing left */
        } else {
            BF_SHM_BF_PTR_BFPAGE(target->nextpage_o)->prevpage_o = BF_SHM_NULL;
        }

        return target;
    } else if (compare_array_key(
            BF_SHM_KEY_PTR_KEY(
                BF_SHM_BF_PTR_BFPAGE(list->tail_o)->key_o),
            BF_SHM_KEY_PTR_KEY(target->key_o)) == 0) {
        /* if target == tail */
        list->tail_o = target->prevpage_o;
        BF_SHM_BF_PTR_BFPAGE(list->tail_o)->nextpage_o = BF_SHM_NULL;
        list->size--;
        return target;
    } else { /* general case */
        BF_SHM_BF_PTR_BFPAGE(target->prevpage_o)->nextpage_o = target->nextpage_o;
        BF_SHM_BF_PTR_BFPAGE(target->nextpage_o)->prevpage_o = target->prevpage_o;
        list->size--;
        return target;
    }

    // return BFE_LRU_REMOVE_ERR;
    return NULL;
}

BFpage* BF_list_remove(BFpage* target){
    BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);
    BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);

    // fprintf(stderr, "%d @ BF_list_remove\n", _bf_current_eviction_policy);
    switch (_bf_current_eviction_policy) {
        case BF_EVICTION_POLICY_LRU_ONLY: return _BF_list_remove(lru, target);
        case BF_EVICTION_POLICY_MRU_ONLY: return _BF_list_remove(mru, target);
        case BF_EVICTION_POLICY_OPT: case BF_EVICTION_POLICY_LRU_K: return target;
        default: assert(0);
    }
}

void _BF_list_printall(BFList *list) {
    fprintf(stderr, " - # of pages: %d\n", list->size);
    
    BFpage* ptr;
    int i = 0;
    for (ptr = BF_SHM_BF_PTR_BFPAGE(list->head_o);
         (void *) ptr != (void *) _mspace_bf;    
         ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {

        _array_key *key = (_array_key *) BF_SHM_KEY_PTR(ptr->key_o);
        fprintf(stderr, "\t[%d] pin=%d, dirty=%d, fpage=%p(%ld), key=%p(%ld), nextpage=%p(%ld), prevpage=%p(%ld), key.arrayname=%s, key.dcoords={",
            i++, ptr->count, ptr->dirty,
            (void *) _BF_get_pfpage_ptr(ptr), ptr->fpage_o, 
            (void *) BF_SHM_KEY_PTR(ptr->key_o), ptr->key_o, 
            (void *) BF_SHM_BF_PTR(ptr->nextpage_o), ptr->nextpage_o, 
            (void *) BF_SHM_BF_PTR(ptr->prevpage_o), ptr->prevpage_o,
            BF_SHM_KEY_PTR_CHAR(key->arrayname_o)
        );

        for (int d = 0; d < key->dim_len; d++) {
            fprintf(stderr, "%lu,", BF_SHM_KEY_PTR_UINT64T(key->dcoords_o)[d]);    
        }

        fprintf(stderr, "}, key.attr=%s, next_ts=%lu\n", BF_SHM_KEY_PTR_CHAR(key->attrname_o), ptr->next_ts); 
    }
}

void _BF_print_size_of_data_or_idata_in_list(BFList *list) {
    BFpage* ptr;
    size_t isize_clean = 0, isize_dirty = 0, size_clean = 0, size_dirty = 0;
    for (ptr = BF_SHM_BF_PTR_BFPAGE(list->head_o);
         (void *) ptr != (void *) _mspace_bf;    
         ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {

        PFpage *page = _BF_get_pfpage_ptr(ptr);
        size_t s = 0;
        if (page->type == DENSE_FIXED) {
            s += page->pagebuf_len;
        } else {
            uint64_t *lens = bf_util_pagebuf_get_coords_lens(page);
            s += lens[0];
            s += lens[1];
            s += page->pagebuf_len;
        }

        if (page->is_input) {
            if (ptr->dirty) isize_dirty += s;
            else isize_clean += s;
        } else {
            if (ptr->dirty) size_dirty += s;
            else size_clean += s;
            
        } 
    }

    fprintf(stderr, "%lu,%lu,%lu,%lu,", isize_clean, isize_dirty, size_clean, size_dirty);
}


size_t _BF_list_getbuffersize(BFList *list) {    
    BFpage* ptr;
    size_t size = 0;
    for (ptr = BF_SHM_BF_PTR_BFPAGE(list->head_o);
         (void *) ptr != (void *) _mspace_bf;    
         ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {

        PFpage *page = _BF_get_pfpage_ptr(ptr);
        if (page->type == DENSE_FIXED) {
            size += page->pagebuf_len;
        } else {
            uint64_t *lens = bf_util_pagebuf_get_coords_lens(page);
            size += lens[0];
            size += lens[1];
            size += page->pagebuf_len;
        }
    }

    return size;
}

void BF_list_printall() {
    BFList *lru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFLRU_o);
    BFList *mru = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFMRU_o);

    fprintf(stderr, "LRU");
    _BF_list_printall(lru);

    fprintf(stderr, "MRU");
    _BF_list_printall(mru);

    if (_bf_skiplist_on) {
        BFList *dirty_sl = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);

        fprintf(stderr, "SKIPLIST DIRTY");
        _BF_list_printall_skiplist(dirty_sl);
    }
}

void _BF_list_printall_skiplist(BFList *list) {
    fprintf(stderr, " - # of pages: %d\n", list->size);

    BFshm_offset_t *heads_o = BF_SHM_BF_PTR(list->s_heads_o);
    fprintf(stderr, "\t head=\n");
    for (uint8_t level = 0; level < _bf_skiplist_level; level++) {
        fprintf(stderr, "\t\t level=%d, nextpage=%p(%ld)\n",
            level,
            (void *) BF_SHM_BF_PTR(heads_o[level]), 
            heads_o[level]);
    }

    BFshm_offset_t *tails_o = BF_SHM_BF_PTR(list->s_tails_o);
    fprintf(stderr, "\t tail=\n");
    for (uint8_t level = 0; level < _bf_skiplist_level; level++) {
        fprintf(stderr, "\t\t level=%d, prevpage=%p(%ld)\n",
            level,
            (void *) BF_SHM_BF_PTR(tails_o[level]), 
            tails_o[level]);
    }

    // int latest_level = 0;        // for validity
    for (uint8_t level = 0; level < _bf_skiplist_level; level++) {
        fprintf(stderr, "\t level=%d\n", level);
        BFpage *ptr = BF_SHM_BF_PTR(heads_o[level]);
        int i = 0;
        // uint64_t latest = 0;        // for validity
        while ((void *) ptr != (void *) _mspace_bf) {
            /* print */
            BFshm_offset_t *nextpages_o = BF_SHM_BF_PTR(ptr->s_nextpages_o);
            BFshm_offset_t *prevpages_o = BF_SHM_BF_PTR(ptr->s_prevpages_o);

            _array_key *key = (_array_key *) BF_SHM_KEY_PTR(ptr->key_o);
            fprintf(stderr, "\t\t [%d] ptr=%p(%ld), pin=%d, dirty=%d, fpage=%p(%ld), key=%p(%ld), nextpage=%p(%ld), prevpage=%p(%ld), key.arrayname=%s, key.dcoords={",
                i++, (void *) ptr, BF_SHM_BF_OFFSET(ptr),
                ptr->count, ptr->dirty,
                (void *) _BF_get_pfpage_ptr(ptr), ptr->fpage_o, 
                (void *) BF_SHM_KEY_PTR(ptr->key_o), ptr->key_o, 
                (void *) BF_SHM_BF_PTR(nextpages_o[level]), nextpages_o[level], 
                (void *) BF_SHM_BF_PTR(prevpages_o[level]), prevpages_o[level],
                BF_SHM_KEY_PTR_CHAR(key->arrayname_o)
            );

            for (int d = 0; d < key->dim_len; d++) {
                fprintf(stderr, "%lu,", BF_SHM_KEY_PTR_UINT64T(key->dcoords_o)[d]);    
            }

            fprintf(stderr, "}, key.attr=%s, next_ts=%lu\n", BF_SHM_KEY_PTR_CHAR(key->attrname_o), ptr->next_ts); 

            // /* validity */
            // assert(latest <= ptr->next_ts);
            // assert(ptr->dirty == __test);
            // latest = ptr->next_ts;
            
            /* next */
            ptr = BF_SHM_BF_PTR(nextpages_o[level]);
        }

        // /* validity */
        // assert(latest_level <= i);
        // latest_level = i;
        // if (level == _bf_skiplist_level - 1) assert(i == list->size);
    }
}

void _BF_print_size_of_data_or_idata_in_skiplist(BFList *list) {
    size_t isize_clean = 0, isize_dirty = 0, size_clean = 0, size_dirty = 0;

    BFshm_offset_t *heads_o = BF_SHM_BF_PTR(list->s_heads_o);
    BFpage *ptr = BF_SHM_BF_PTR(heads_o[_bf_skiplist_level - 1]);
    int i = 0;
    while ((void *) ptr != (void *) _mspace_bf) {
        /* calculate */
        PFpage *page = _BF_get_pfpage_ptr(ptr);
        size_t s = 0;
        if (page->type == DENSE_FIXED) {
            s += page->pagebuf_len;
        } else {
            uint64_t *lens = bf_util_pagebuf_get_coords_lens(page);
            s += lens[0];
            s += lens[1];
            s += page->pagebuf_len;
        }

        if (page->is_input) {
            if (ptr->dirty) isize_dirty += s;
            else isize_clean += s;
        } else {
            if (ptr->dirty) size_dirty += s;
            else size_clean += s;  
        } 

        /* next */
        BFshm_offset_t *nextpages_o = BF_SHM_BF_PTR(ptr->s_nextpages_o);
        ptr = BF_SHM_BF_PTR(nextpages_o[_bf_skiplist_level - 1]);
    }

    fprintf(stderr, "%lu,%lu,%lu,%lu,", isize_clean, isize_dirty, size_clean, size_dirty);
}

/* Skip List */

void _BF_list_init_skiplist(BFList *list) {
    BFshm_offset_t *heads_o = BF_shm_malloc(_mspace_bf, sizeof(BFshm_offset_t) * _bf_skiplist_level);
    BFshm_offset_t *tails_o = BF_shm_malloc(_mspace_bf, sizeof(BFshm_offset_t) * _bf_skiplist_level);

    for (uint8_t level = 0; level < _bf_skiplist_level; level++) {
        heads_o[level] = oNULL;
        tails_o[level] = oNULL;
    }

    list->s_heads_o = BF_SHM_BF_OFFSET(heads_o);
    list->s_tails_o = BF_SHM_BF_OFFSET(tails_o);

    list->size = 0;
}

void _BF_list_free_skiplist(BFList *list) {
    BFshm_offset_t head_o = ((BFshm_offset_t*) BF_SHM_BF_PTR(list->s_heads_o))[_bf_skiplist_level - 1];
    while (head_o != oNULL) {
        BFpage *head = BF_SHM_BF_PTR_BFPAGE(head_o);
        head_o = ((BFshm_offset_t*) BF_SHM_BF_PTR(head->s_nextpages_o))[_bf_skiplist_level - 1];

        if (head->dirty) {
            if (head->next_ts < BF_TS_INF) {
                BF_flush_dirty_victim(head);
            } else {
                PFpage *page = _BF_get_pfpage_ptr(head);
                freed_capacity += page->pagebuf_len;
            }
        }

        BF_free_bfpage(head);
    }

    list->s_heads_o = oNULL;
    list->s_tails_o = oNULL;
    list->size = 0;
}

void _BF_list_delete_skiplist(BFpage* page) {
    BFList *list = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);

    if (list->size == 0) assert(0);

    // stats
    struct timeval sl_start;
    gettimeofday(&sl_start, NULL);

    BFshm_offset_t* heads_o = BF_SHM_BF_PTR(list->s_heads_o);
    BFshm_offset_t* tails_o = BF_SHM_BF_PTR(list->s_tails_o);
    BFshm_offset_t* nexts_o = BF_SHM_BF_PTR(page->s_nextpages_o);
    BFshm_offset_t* prevs_o = BF_SHM_BF_PTR(page->s_prevpages_o);

    for (int16_t level = _bf_skiplist_level - 1;
            level >= 0; 
            level--) {
        
        BFpage *next = BF_SHM_BF_PTR(nexts_o[level]);
        BFpage *prev = BF_SHM_BF_PTR(prevs_o[level]);

        if (prevs_o[level] == oNULL && nexts_o[level] == oNULL) {
            // this is the only element in the level,
            if (BF_SHM_BF_PTR(heads_o[level]) == page)
                heads_o[level] = tails_o[level] = oNULL;

            //      or this element is not in the level
        } else if (prevs_o[level] == oNULL && nexts_o[level] != oNULL) {
            // the first element in the level
            heads_o[level] = nexts_o[level];
            BFshm_offset_t *next_prevs_o = BF_SHM_BF_PTR(next->s_prevpages_o);
            next_prevs_o[level] = oNULL;
        } else if (prevs_o[level] != oNULL && nexts_o[level] == oNULL) {
            // the last element in the level
            tails_o[level] = prevs_o[level];
            BFshm_offset_t *prev_nexts_o = BF_SHM_BF_PTR(prev->s_nextpages_o);
            prev_nexts_o[level] = oNULL;
        } else {
            BFshm_offset_t *next_prevs_o = BF_SHM_BF_PTR(next->s_prevpages_o);
            BFshm_offset_t *prev_nexts_o = BF_SHM_BF_PTR(prev->s_nextpages_o);

            next_prevs_o[level] = prevs_o[level];
            prev_nexts_o[level] = nexts_o[level];
        }
    }

    list->size--;

    for (uint8_t level = 0; level < _bf_skiplist_level; level++)
        nexts_o[level] = prevs_o[level] = oNULL;

    // stats
    struct timeval sl_end;
    gettimeofday(&sl_end, NULL);

    unsigned long long sl_diff = ((sl_end.tv_sec - sl_start.tv_sec) * 1000000) + (sl_end.tv_usec - sl_start.tv_usec); 
    bf_min_sl_update_time += sl_diff;

    // fprintf(stderr, "after delete: %ld\n", BF_SHM_BF_OFFSET(page));
    // _BF_list_printall_skiplist(list);
}

void _BF_list_insert_skiplist(BFpage* page) {
    BFList *list = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);

    // stats
    struct timeval sl_start;
    gettimeofday(&sl_start, NULL);

    BFpage **targets = _BF_list_search_skiplist(list, page->next_ts);
    BFshm_offset_t* heads_o = BF_SHM_BF_PTR(list->s_heads_o);
    BFshm_offset_t* tails_o = BF_SHM_BF_PTR(list->s_tails_o);

    int top = 1;
    for (int16_t level = _bf_skiplist_level - 1;
            level >= 0; 
            level--) {

        BFshm_offset_t *nextpages_o = BF_SHM_BF_PTR(page->s_nextpages_o);
        BFshm_offset_t *prevpages_o = BF_SHM_BF_PTR(page->s_prevpages_o);
        if (!top) {
            // nextpages_o[level] = prevpages_o[level] = oNULL;
            continue;
        }
        
        if (targets[level] == BF_SHM_NULL) {
            // non entry have been inserted, or this is the first element
            if (heads_o[level] == oNULL) {
                heads_o[level] = tails_o[level] = BF_SHM_BF_OFFSET(page);
                nextpages_o[level] = prevpages_o[level] = oNULL;
            } else {
                BFpage *next = BF_SHM_BF_PTR(heads_o[level]);
                BFshm_offset_t *next_prevpages_o = BF_SHM_BF_PTR(next->s_prevpages_o);
                next_prevpages_o[level] = BF_SHM_BF_OFFSET(page);
                nextpages_o[level] = BF_SHM_BF_OFFSET(next);

                heads_o[level] =  BF_SHM_BF_OFFSET(page);
                prevpages_o[level] = oNULL;
            }
        } else {
            BFshm_offset_t *target_nextpages_o = BF_SHM_BF_PTR(targets[level]->s_nextpages_o);
            if (target_nextpages_o[level] != oNULL) {
                BFpage *target_next = BF_SHM_BF_PTR(target_nextpages_o[level]);
                BFshm_offset_t *target_next_prevpages_o = BF_SHM_BF_PTR(target_next->s_prevpages_o);
                target_next_prevpages_o[level] = BF_SHM_BF_OFFSET(page);
            } else {
                tails_o[level] = BF_SHM_BF_OFFSET(page);
            }

            nextpages_o[level] = target_nextpages_o[level];
            prevpages_o[level] = BF_SHM_BF_OFFSET(targets[level]);

            target_nextpages_o[level] = BF_SHM_BF_OFFSET(page);
        }

        // coin flip
        top = rand() % 2;
    }

    BF_shm_free(_mspace_bf, targets);
    list->size++;

    // stats
    struct timeval sl_end;
    gettimeofday(&sl_end, NULL);

    unsigned long long sl_diff = ((sl_end.tv_sec - sl_start.tv_sec) * 1000000) + (sl_end.tv_usec - sl_start.tv_usec); 
    bf_min_sl_update_time += sl_diff;

    // fprintf(stderr, "after insert:%ld\n", BF_SHM_BF_OFFSET(page));
    // _BF_list_printall_skiplist(list);
}


// it returns a list of BFpage* that having exactly same ts or greatest lower element for each level
BFpage** _BF_list_search_skiplist(BFList *list, uint64_t ts) {
    BFpage **ret = BF_shm_malloc(_mspace_bf, sizeof(BFpage*) * _bf_skiplist_level);

    BFshm_offset_t* heads_o = BF_SHM_BF_PTR(list->s_heads_o);
    BFpage *curr;

    // find start point from the header (maybe it can be merged with below logic)
    uint8_t level = 0;
    for (level = 0; level < _bf_skiplist_level; level++) {
        curr = BF_SHM_BF_PTR(heads_o[level]);
        if ((void*) curr == (void*) _mspace_bf) {
            ret[level] = BF_SHM_NULL;
            continue;
        } else if (curr->next_ts > ts) {
            ret[level] = BF_SHM_NULL;
            continue;
        }

        break;
    }
    if ((void*) curr != (void*) _mspace_bf) {
        while (level < _bf_skiplist_level) {
            BFshm_offset_t *nexts_o = BF_SHM_BF_PTR(curr->s_nextpages_o);
            BFpage *next = BF_SHM_BF_PTR(nexts_o[level]);

            if ((void*) next == (void*) _mspace_bf) {   // NULL
                ret[level++] = curr;
            } else {
                if (ts >= next->next_ts) curr = next;
                else ret[level++] = curr;
            }
        }
    }

    // fprintf(stderr, "[BufferTile] skiplist _BF_list_search_skiplist - ts=%lu,ret=[", ts);
    // level = 0;
    // for (level = 0; level < _bf_skiplist_level; level++) {
    //     fprintf(stderr, "%p(%lu),", (void*) ret[level], BF_SHM_BF_OFFSET(ret[level]));
    // }
    // fprintf(stderr, "]\n");

    return ret;
}

BFpage* BF_list_evict_page_opt() {
    BFList *list = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);
    if (list->size == 0) return NULL;

    BFshm_offset_t *tails_o = BF_SHM_BF_PTR(list->s_tails_o);
    BFpage* ptr = BF_SHM_BF_PTR_BFPAGE(tails_o[_bf_skiplist_level - 1]);
    while ((void *) ptr != (void *) _mspace_bf) {
        if (ptr->count == 0) break;
        
        BFshm_offset_t *prevs_o = BF_SHM_BF_PTR(ptr->s_prevpages_o);
        ptr = BF_SHM_BF_PTR_BFPAGE(prevs_o[_bf_skiplist_level - 1]);
    }

    bool failed_to_find = ((void *) ptr == (void *) _mspace_bf);
    if (failed_to_find) {
        return NULL;
    }

    _array_key *key = BF_SHM_KEY_PTR(ptr->key_o);
    uint64_t *dcoords = BF_SHM_KEY_PTR(key->dcoords_o);
    fprintf(stderr, "[BufferTile] Future - Find farest victim: %s,%lu,%lu,%lu\n", 
        (char *) BF_SHM_KEY_PTR(key->arrayname_o), dcoords[0], dcoords[1], ptr->next_ts);

    _BF_list_delete_skiplist(ptr);
    return ptr;
}

BFpage* BF_list_evict_page_lruk() {
    BFList *list = BF_SHM_BF_PTR(BFshm_hdr_ptr->BFDirty_Skiplist_o);

    if (list->size == 0) return NULL;

    uint64_t crp = _bf_env_lruk_crp;
    BFpage *candidate = NULL;
    bool_t tie = false;

    // start from head since we need to find min value
    BFshm_offset_t *heads_o = BF_SHM_BF_PTR(list->s_heads_o);
    BFpage* ptr = BF_SHM_BF_PTR_BFPAGE(heads_o[_bf_skiplist_level - 1]);
    while ((void *) ptr != (void *) _mspace_bf) {
        if (ptr->count == 0) {
            // we need to check correlated reference period
            BFFuture_entry *entry = BF_history_search(BF_SHM_KEY_PTR(ptr->key_o));
            if ((BF_curr_ts - entry->last) > crp) {
                if (candidate == NULL) candidate = ptr;
                else {
                    if (candidate->next_ts == ptr->next_ts) {
                        // find!
                        tie = true;
                    }
                    break;
                }
            }
        }
        
        BFshm_offset_t *nexts_o = BF_SHM_BF_PTR(ptr->s_nextpages_o);
        ptr = BF_SHM_BF_PTR_BFPAGE(nexts_o[_bf_skiplist_level - 1]);
    }

    if (tie == true) {
        // we need to tie-breaking
        // subsidiary policy: LRU
        // fprintf(stderr, "[BufferTile] LRU-K - Tie Found!!\n");

        // scan from candidate
        ptr = candidate;
        BFpage* lru = NULL;
        uint64_t min_lru_ts = 99999999;
        while ((void *) ptr != (void *) _mspace_bf) {
            if (ptr->count == 0) {
                // we need to check correlated reference period
                BFFuture_entry *entry = BF_history_search(BF_SHM_KEY_PTR(ptr->key_o));
                // fprintf(stderr, "[BufferTile] LRU-K - Tie-Breaking - (t - LAST(p))=%lu, crp=%lu\n", BF_curr_ts - entry->last, crp);
                
                // _array_key *__key = BF_SHM_KEY_PTR(ptr->key_o);
                // uint64_t lru1 = ((BFFuture_entry_listentry*) BF_SHM_BF_PTR(entry->head_o))->ts;
                // fprintf(stderr, "[BufferTile] LRU-K - Tie list - %s,%lu,%lu = %lu / %lu,%lu,%lu\n",
                //     BF_SHM_KEY_PTR_CHAR(__key->arrayname_o),
                //     BF_SHM_KEY_PTR_UINT64T(__key->dcoords_o)[0],
                //     BF_SHM_KEY_PTR_UINT64T(__key->dcoords_o)[1],
                //     lru1,
                //     BF_curr_ts, entry->last, crp);
                    
                if ((BF_curr_ts - entry->last) > crp) {
                    if (candidate->next_ts == ptr->next_ts) {
                        uint64_t lru1 = ((BFFuture_entry_listentry*) BF_SHM_BF_PTR(entry->head_o))->ts;
                        if (min_lru_ts > lru1) {
                            min_lru_ts = lru1;
                            lru = ptr;
                        }
                    } else {
                        break;
                    }
                }
            }
            
            BFshm_offset_t *nexts_o = BF_SHM_BF_PTR(ptr->s_nextpages_o);
            ptr = BF_SHM_BF_PTR_BFPAGE(nexts_o[_bf_skiplist_level - 1]);
        }
        ptr = lru;
    } else {
        ptr = candidate;
    }

    bool failed_to_find = ((void *) ptr == (void *) _mspace_bf);
    if (failed_to_find) {
        return NULL;
    }

    _array_key *key = BF_SHM_KEY_PTR(ptr->key_o);
    uint64_t *dcoords = BF_SHM_KEY_PTR(key->dcoords_o);
    fprintf(stderr, "[BufferTile] History - Find farest victim: %s,%lu,%lu,%lu\n", 
        (char *) BF_SHM_KEY_PTR(key->arrayname_o), dcoords[0], dcoords[1], ptr->next_ts);

    _BF_list_delete_skiplist(ptr);
    return ptr;
}
