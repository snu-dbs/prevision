#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "bf.h"
#include "hash.h"
#include "tileio.h"
#include "bf_malloc.h"

extern int BFerrno;
extern BFmspace *_mspace_bf, *_mspace_key;
unsigned long long evicted_but_no_write = 0;

void BF_hash_init(){
    BFbucket *bfhash_table =
        BF_shm_malloc(_mspace_bf, sizeof(BFbucket) * BF_HASH_TBL_SIZE);
    
    int i;
    for (i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        bfhash_table[i].head_o = BF_SHM_NULL;
        bfhash_table[i].tail_o = BF_SHM_NULL;
        bfhash_table[i].count = 0;
    }

    BFshm_hdr_ptr->BFhash_table_o = BF_SHM_BF_OFFSET(bfhash_table);
}

void BF_hash_free(){
    BFbucket *bfhash_table = BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFhash_table_o);
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        BFshm_offset_t head_o = bfhash_table[i].head_o;
        while (head_o != BF_SHM_NULL) {
            BFhash_entry *head = BF_SHM_BF_PTR_BFHASHENTRY(head_o);
            head_o = head->nextentry_o;
            BF_shm_free(_mspace_bf, BF_SHM_BF_PTR(head->bpage_o));
            BF_shm_free(_mspace_bf, head);
        }
    }

    BF_shm_free(_mspace_bf, BF_SHM_BF_PTR(BFshm_hdr_ptr->BFhash_table_o));
    BFshm_hdr_ptr->BFhash_table_o = BF_SHM_NULL;
}

int BF_hash_function(_array_key *key){ /* TO DO THINK */
    // generating id.
    int x = strlen(BF_SHM_KEY_PTR_CHAR(key->arrayname_o)) +
        strlen(BF_SHM_KEY_PTR_CHAR(key->attrname_o)) + key->dim_len;

    for (size_t i = 0; i < key->dim_len; i++) {
        x += BF_SHM_KEY_PTR_UINT64T(key->dcoords_o)[i];
    }

    return x % BF_HASH_TBL_SIZE;
}

int BF_hash_insert(BFpage* ptr){
    /* allocate */
    BFhash_entry* hash_node;
    hash_node = BF_shm_malloc(_mspace_bf, sizeof(BFhash_entry));
    hash_node->bpage_o = BF_SHM_BF_OFFSET(ptr);
    hash_node->preventry_o = BF_SHM_NULL;

    /* insert */
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFhash_table_o)[
            BF_hash_function(BF_SHM_KEY_PTR_KEY(ptr->key_o))]);
    
    if (baguni->count == 0) {
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->tail_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = BF_SHM_NULL;
        return 0;
    } else {
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = baguni->head_o;
        BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o)->preventry_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        return 0;
    }
    
    return 1;
}

int BF_hash_remove(BFpage* target) {
    PFpage *evicted_page = _BF_get_pfpage_ptr(target);
    uint64_t evicted_size = evicted_page->pagebuf_len;
    evicted_but_no_write += evicted_size;            

    BFhash_entry* ptr;
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFhash_table_o)[
            BF_hash_function(BF_SHM_KEY_PTR_KEY(target->key_o))]);

    for (ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
         ptr != NULL;
         ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
        // TODO: this logic is correct?
        if (compare_array_key(
            BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(ptr->bpage_o)->key_o),
            BF_SHM_KEY_PTR_KEY(target->key_o)) == 0) {

            if (baguni->count == 1) {   /* only target in bucket */
                baguni->head_o = BF_SHM_NULL;
                baguni->tail_o = BF_SHM_NULL;
                baguni->count = 0;
                BF_shm_free(_mspace_bf, ptr);
                return 0;
            } else if (compare_array_key(BF_SHM_KEY_PTR_KEY(target->key_o),
                BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(BF_SHM_BF_PTR_BFHASHENTRY(
                    baguni->tail_o)->bpage_o)->key_o)) == 0) {
                /* target at the end */
                BF_SHM_BF_PTR_BFHASHENTRY(ptr->preventry_o)->nextentry_o = BF_SHM_NULL;
                baguni->tail_o = ptr->preventry_o;
                baguni->count--;
                BF_shm_free(_mspace_bf, ptr);
                return 0;
            } else if (compare_array_key(
                BF_SHM_KEY_PTR_KEY(target->key_o),
                BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(BF_SHM_BF_PTR_BFHASHENTRY(
                    baguni->head_o)->bpage_o)->key_o)) == 0) { 
                /* target at the head */
                BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)->preventry_o = BF_SHM_NULL;
                baguni->head_o = ptr->nextentry_o;
                baguni->count--;
                BF_shm_free(_mspace_bf, ptr);
                return 0;
            } else { 
                /* general case */
                BF_SHM_BF_PTR_BFHASHENTRY(ptr->preventry_o)->nextentry_o = ptr->nextentry_o;
                BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)->preventry_o = ptr->preventry_o;
                baguni->count--;
                BF_shm_free(_mspace_bf, ptr);
                return 0;
            }
        }
    }

    printf("no target there\n");
    return 1; 
}

int BF_hash_remove2(BFhash_entry* target) {
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFhash_table_o)[
            BF_hash_function(BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(
                target->bpage_o)->key_o))]);
    /*
    printf("count : %d \n",baguni->count);
    printf("head : %d\n",baguni->head->pageNum );
    printf("tail : %d\n",baguni->tail->pageNum );
    printf("target : %d\n", target->pageNum);
    */
    if (baguni->count == 1) { /* only target in bucket */
        baguni->head_o = BF_SHM_NULL;
        baguni->tail_o = BF_SHM_NULL;
        baguni->count = 0;
        BF_shm_free(_mspace_bf, target);
        return 0;      
    } else if (compare_array_key(
        BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(target->bpage_o)->key_o),
        BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(BF_SHM_BF_PTR_BFHASHENTRY(
            baguni->tail_o)->bpage_o)->key_o)) == 0) { 
        /* target at the end */
        BF_SHM_BF_PTR_BFHASHENTRY(target->preventry_o)->nextentry_o = BF_SHM_NULL;
        baguni->tail_o = target->preventry_o;
        baguni->count--;
        BF_shm_free(_mspace_bf, target);
        return 0;
    } else if (compare_array_key(
        BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(target->bpage_o)->key_o),
        BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(BF_SHM_BF_PTR_BFHASHENTRY(
            baguni->head_o)->bpage_o)->key_o)) == 0) {
        /* target at the head */
        BF_SHM_BF_PTR_BFHASHENTRY(target->nextentry_o)->preventry_o = BF_SHM_NULL;
        baguni->head_o = target->nextentry_o;
        baguni->count--;
        BF_shm_free(_mspace_bf, target);
        return 0;
    } else { /* general case */
        BF_SHM_BF_PTR_BFHASHENTRY(target->preventry_o)->nextentry_o =
            target->nextentry_o;
        BF_SHM_BF_PTR_BFHASHENTRY(target->nextentry_o)->preventry_o =
            target->preventry_o;
        baguni->count--;
        BF_shm_free(_mspace_bf, target);
        return 0;
    }
    
    printf("no target there\n");
    return 1; 
}

BFhash_entry* BF_hash_search(_array_key *key) {
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFhash_table_o)[BF_hash_function(key)]);
    BFhash_entry* ptr;
    for(ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
        ptr != NULL;
        ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
        if (compare_array_key(BF_SHM_KEY_PTR_KEY(BF_SHM_BF_PTR_BFPAGE(ptr->bpage_o)->key_o), key) == 0) {
            return ptr;
        }
    }

    return NULL;
}


/* dCnt */

int BF_dCnt_hash_function(char *key){
    // generating id.
    int val = 0;
    for (int i = 0; i < strlen(key); i++) {
        val += key[i];
    }

    // fprintf(stderr, "[BufferTile] dCnt - %s -> %d\n", key, val % BF_HASH_TBL_SIZE);
    return val % BF_HASH_TBL_SIZE;
}

void BF_dCnt_init(){
    BFbucket *bfdcnt_table =
        BF_shm_malloc(_mspace_bf, sizeof(BFbucket) * BF_HASH_TBL_SIZE);
    
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        bfdcnt_table[i].head_o = BF_SHM_NULL;
        bfdcnt_table[i].tail_o = BF_SHM_NULL;
        bfdcnt_table[i].count = 0;
    }

    BFshm_hdr_ptr->BFDcnt_table_o = BF_SHM_BF_OFFSET(bfdcnt_table);
}

void BF_dCnt_free(){
    BFbucket *bfdcnt_table = BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFDcnt_table_o);
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        BFshm_offset_t head_o = bfdcnt_table[i].head_o;
        while (head_o != BF_SHM_NULL) {
            BFhash_entry *head = BF_SHM_BF_PTR_BFHASHENTRY(head_o);
            BFDCnt_entry *entry = BF_SHM_BF_PTR(head->bpage_o);
            BF_shm_free(_mspace_bf, BF_SHM_BF_PTR(entry->arrname_o));
            head_o = head->nextentry_o;
            BF_shm_free(_mspace_bf, BF_SHM_BF_PTR(head->bpage_o));
            BF_shm_free(_mspace_bf, head);
        }
    }

    BF_shm_free(_mspace_bf, bfdcnt_table);
    BFshm_hdr_ptr->BFDcnt_table_o = BF_SHM_NULL;
}

int BF_dCnt_insert(BFDCnt_entry *ptr){
    /* allocate */
    BFhash_entry* hash_node;
    hash_node = BF_shm_malloc(_mspace_bf, sizeof(BFhash_entry));
    hash_node->bpage_o = BF_SHM_BF_OFFSET(ptr);
    hash_node->preventry_o = BF_SHM_NULL;

    /* insert */
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFDcnt_table_o)[
            BF_dCnt_hash_function(BF_SHM_BF_PTR(ptr->arrname_o))]);
    
    if (baguni->count == 0) {
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->tail_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = BF_SHM_NULL;
        return 0;
    } else {
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = baguni->head_o;
        BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o)->preventry_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        return 0;
    }
    
    return 1;
}

int BF_dCnt_remove(char *arrname) {
    BFhash_entry* ptr;
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFDcnt_table_o)[
            BF_dCnt_hash_function(arrname)]);

    for (ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
            ptr != NULL;
            ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {

        BFDCnt_entry *entry = (BFDCnt_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
        if (strcmp(arrname, BF_SHM_BF_PTR(entry->arrname_o)) == 0) {
            if (baguni->count == 1) {   /* only target in bucket */
                baguni->head_o = BF_SHM_NULL;
                baguni->tail_o = BF_SHM_NULL;
                baguni->count = 0;
                BF_shm_free(_mspace_bf, ptr);
                return 0;
            } else {
                BFhash_entry *head = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
                BFhash_entry *tail = BF_SHM_BF_PTR_BFHASHENTRY(baguni->tail_o);

                BFDCnt_entry *headentry = (BFDCnt_entry*) BF_SHM_BF_PTR(head->bpage_o);
                BFDCnt_entry *tailentry = (BFDCnt_entry*) BF_SHM_BF_PTR(tail->bpage_o);

                if (strcmp(arrname, BF_SHM_BF_PTR(tailentry->arrname_o)) == 0) {
                    /* target at the end */
                    BF_SHM_BF_PTR_BFHASHENTRY(ptr->preventry_o)->nextentry_o = BF_SHM_NULL;
                    baguni->tail_o = ptr->preventry_o;
                    baguni->count--;
                    BF_shm_free(_mspace_bf, ptr);
                    return 0;
                } else if (strcmp(arrname, BF_SHM_BF_PTR(headentry->arrname_o)) == 0) { 
                    /* target at the head */
                    BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)->preventry_o = BF_SHM_NULL;
                    baguni->head_o = ptr->nextentry_o;
                    baguni->count--;
                    BF_shm_free(_mspace_bf, ptr);
                    return 0;
                } else { 
                    /* general case */
                    BF_SHM_BF_PTR_BFHASHENTRY(ptr->preventry_o)->nextentry_o = ptr->nextentry_o;
                    BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)->preventry_o = ptr->preventry_o;
                    baguni->count--;
                    BF_shm_free(_mspace_bf, ptr);
                    return 0;
                }
            }
        }
    }

    printf("no target there\n");
    return 1; 
}

BFDCnt_entry* BF_dCnt_search(char *arrname) {
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFDcnt_table_o)[
            BF_dCnt_hash_function(arrname)]);
    BFhash_entry* ptr;
    for(ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
            ptr != NULL;
            ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
        BFDCnt_entry *entry = (BFDCnt_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
        if (strcmp(arrname, BF_SHM_BF_PTR(entry->arrname_o)) == 0) {
            return entry;
        }
    }

    return NULL;
}

void BF_dCnt_printall() {
    fprintf(stderr, "BF_dCnt_printall\n");
    for (int i = 0; i < BF_HASH_TBL_SIZE; i++) {
        BFbucket* baguni = &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFDcnt_table_o)[i]);
        for(BFhash_entry* ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
                ptr != NULL;
                ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
            BFDCnt_entry *entry = (BFDCnt_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
            char* arrname = BF_SHM_BF_PTR(entry->arrname_o);
            uint64_t remains = entry->remains;

            fprintf(stderr, "\t%s,%lu", arrname, remains);
            fprintf(stderr, "\n");
        }
    } 
}

/* future history */
int BF_future_hash_function(_array_key *key) {
    // generating id.
    int x = strlen(BF_SHM_KEY_PTR_CHAR(key->arrayname_o)) +
        strlen(BF_SHM_KEY_PTR_CHAR(key->attrname_o)) + key->dim_len;

    for (size_t i = 0; i < key->dim_len; i++) {
        x += BF_SHM_KEY_PTR_UINT64T(key->dcoords_o)[i];
    }

    return x % BF_HASH_TBL_SIZE;
}

void BF_future_init(){
    BFbucket *bffuture_table =
        BF_shm_malloc(_mspace_bf, sizeof(BFbucket) * BF_HASH_TBL_SIZE);
    
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        bffuture_table[i].head_o = BF_SHM_NULL;
        bffuture_table[i].tail_o = BF_SHM_NULL;
        bffuture_table[i].count = 0;
    }

    BFshm_hdr_ptr->BFFuture_table_o = BF_SHM_BF_OFFSET(bffuture_table);
}

void BF_future_free(){
    BFbucket *bffuture_table = BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFFuture_table_o);
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        BFshm_offset_t head_o = bffuture_table[i].head_o;
        while (head_o != BF_SHM_NULL) {
            BFhash_entry *head = BF_SHM_BF_PTR_BFHASHENTRY(head_o);
            head_o = head->nextentry_o;
            // TODO: 
            BFFuture_entry *entry = BF_SHM_BF_PTR(head->bpage_o);

            for (BFFuture_entry_listentry *item = BF_SHM_BF_PTR(entry->head_o);
                    (void*) item != _mspace_bf;) {
                BFFuture_entry_listentry *next = BF_SHM_BF_PTR(item->next_o);
                BF_shm_free(_mspace_bf, item);
                item = next;
            }

            BF_shm_free(_mspace_key, BF_SHM_KEY_PTR(entry->key_o));      

            BF_shm_free(_mspace_bf, entry);
            BF_shm_free(_mspace_bf, head);
        }
    }

    BF_shm_free(_mspace_bf, bffuture_table);
    BFshm_hdr_ptr->BFFuture_table_o = BF_SHM_NULL;
}

int BF_future_insert(BFFuture_entry *ptr){
    /* allocate */
    BFhash_entry *hash_node = BF_shm_malloc(_mspace_bf, sizeof(BFhash_entry));
    hash_node->bpage_o = BF_SHM_BF_OFFSET(ptr);
    hash_node->preventry_o = BF_SHM_NULL;

    /* insert */
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFFuture_table_o)[
            BF_future_hash_function(BF_SHM_KEY_PTR(ptr->key_o))]);
    
    if (baguni->count == 0) {
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->tail_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = BF_SHM_NULL;
        return 0;
    } else {
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = baguni->head_o;
        BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o)->preventry_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        return 0;
    }
    
    return 1;
}

BFFuture_entry* BF_future_search(_array_key *key) {
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFFuture_table_o)[
            BF_future_hash_function(key)]);
    BFhash_entry* ptr;
    for(ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
            ptr != NULL;
            ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
        BFFuture_entry *entry = (BFFuture_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
        if (compare_array_key(key, BF_SHM_KEY_PTR(entry->key_o)) == 0) {
            return entry;
        }
    }

    return NULL;
}

void BF_future_printall() {
    fprintf(stderr, "BF_future_printall\n");
    for (int i = 0; i < BF_HASH_TBL_SIZE; i++) {
        fprintf(stderr, "\t%d\n", i);
        BFbucket* baguni = &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFFuture_table_o)[i]);
        for(BFhash_entry* ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
                ptr != NULL;
                ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
            BFFuture_entry *entry = (BFFuture_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
            _array_key *key = BF_SHM_KEY_PTR(entry->key_o);
            char* arrname = BF_SHM_KEY_PTR(key->arrayname_o);
            uint64_t *dcoords = BF_SHM_KEY_PTR(key->dcoords_o);
            
            fprintf(stderr, "\t\t%s,%lu,%lu,events=[", arrname, dcoords[0], dcoords[1]);
            for (BFFuture_entry_listentry *item = BF_SHM_BF_PTR(entry->head_o);
                    (void*) item != _mspace_bf;
                    item = BF_SHM_BF_PTR(item->next_o)) {
                fprintf(stderr, "%lu,", item->ts);
            }
            fprintf(stderr, "]\n");
        }
    } 
}

/* LRU-K */
void BF_history_init(){
    BFbucket *bffuture_table =
        BF_shm_malloc(_mspace_bf, sizeof(BFbucket) * BF_HASH_TBL_SIZE);
    
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        bffuture_table[i].head_o = BF_SHM_NULL;
        bffuture_table[i].tail_o = BF_SHM_NULL;
        bffuture_table[i].count = 0;
    }

    BFshm_hdr_ptr->BFHistory_table_o = BF_SHM_BF_OFFSET(bffuture_table);
}

void BF_history_free(){
    BFbucket *bffuture_table = BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFHistory_table_o);
    for (int i = 0 ; i < BF_HASH_TBL_SIZE; i++) {
        BFshm_offset_t head_o = bffuture_table[i].head_o;
        while (head_o != BF_SHM_NULL) {
            BFhash_entry *head = BF_SHM_BF_PTR_BFHASHENTRY(head_o);
            head_o = head->nextentry_o;
            // TODO: 
            BFFuture_entry *entry = BF_SHM_BF_PTR(head->bpage_o);

            for (BFFuture_entry_listentry *item = BF_SHM_BF_PTR(entry->head_o);
                    (void*) item != _mspace_bf;) {
                BFFuture_entry_listentry *next = BF_SHM_BF_PTR(item->next_o);
                BF_shm_free(_mspace_bf, item);
                item = next;
            }

            BF_shm_free(_mspace_key, BF_SHM_KEY_PTR(entry->key_o));      

            BF_shm_free(_mspace_bf, entry);
            BF_shm_free(_mspace_bf, head);
        }
    }

    BF_shm_free(_mspace_bf, bffuture_table);
    BFshm_hdr_ptr->BFHistory_table_o = BF_SHM_NULL;
}

int BF_history_insert(BFFuture_entry *ptr){
    /* allocate */
    BFhash_entry *hash_node = BF_shm_malloc(_mspace_bf, sizeof(BFhash_entry));
    hash_node->bpage_o = BF_SHM_BF_OFFSET(ptr);
    hash_node->preventry_o = BF_SHM_NULL;

    /* insert */
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFHistory_table_o)[
            BF_future_hash_function(BF_SHM_KEY_PTR(ptr->key_o))]);
    
    if (baguni->count == 0) {
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->tail_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = BF_SHM_NULL;
        return 0;
    } else {
        hash_node->preventry_o = BF_SHM_NULL;
        hash_node->nextentry_o = baguni->head_o;
        BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o)->preventry_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->head_o = BF_SHM_BF_OFFSET(hash_node);
        baguni->count++;
        return 0;
    }
    
    return 1;
}

BFFuture_entry* BF_history_search(_array_key *key) {
    BFbucket* baguni =
        &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFHistory_table_o)[
            BF_future_hash_function(key)]);
    BFhash_entry* ptr;
    for(ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
            ptr != NULL;
            ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
        BFFuture_entry *entry = (BFFuture_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
        if (compare_array_key(key, BF_SHM_KEY_PTR(entry->key_o)) == 0) {
            return entry;
        }
    }

    return NULL;
}

void BF_history_printall() {
    fprintf(stderr, "BF_future_printall\n");
    for (int i = 0; i < BF_HASH_TBL_SIZE; i++) {
        fprintf(stderr, "\t%d\n", i);
        BFbucket* baguni = &(BF_SHM_BF_PTR_BFBUCKET(BFshm_hdr_ptr->BFHistory_table_o)[i]);
        for(BFhash_entry* ptr = BF_SHM_BF_PTR_BFHASHENTRY(baguni->head_o);
                ptr != NULL;
                ptr = BF_SHM_BF_PTR_BFHASHENTRY(ptr->nextentry_o)) {
            BFFuture_entry *entry = (BFFuture_entry*) BF_SHM_BF_PTR(ptr->bpage_o);
            _array_key *key = BF_SHM_KEY_PTR(entry->key_o);
            char* arrname = BF_SHM_KEY_PTR(key->arrayname_o);
            uint64_t *dcoords = BF_SHM_KEY_PTR(key->dcoords_o);
            
            fprintf(stderr, "\t\t%s,%lu,%lu,events=[", arrname, dcoords[0], dcoords[1]);
            for (BFFuture_entry_listentry *item = BF_SHM_BF_PTR(entry->head_o);
                    (void*) item != _mspace_bf;
                    item = BF_SHM_BF_PTR(item->next_o)) {
                fprintf(stderr, "%lu,", item->ts);
            }
            fprintf(stderr, "]\n");
        }
    } 
}