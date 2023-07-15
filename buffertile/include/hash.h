#ifndef __HASH__H__
#define __HASH__H__

#include "bf_struct.h"

typedef struct BFFuture_entry_listentry {
    BFshm_offset_t next_o;              // BFFuture_entry_listentry*
    uint64_t ts;                        // timestamp
} BFFuture_entry_listentry;

typedef struct BFFuture_entry {
    BFshm_offset_t key_o;               // _array_key

    // for list `events`
    uint64_t len;
    BFshm_offset_t head_o;              // BFFuture_entry_listentry*
    BFshm_offset_t tail_o;              // BFFuture_entry_listentry*
    
    BFshm_offset_t curr_o;              // ptr for MIN, and HIST(1) for LRU-K 

    uint64_t last;                      // LAST(p) for LRU-K
} BFFuture_entry;

typedef struct BFDCnt_entry {
    uint64_t remains;                 // 
    BFshm_offset_t arrname_o;         // char*
} BFDCnt_entry;

typedef struct BFhash_entry {
    /* (struct BFhash_entry) next hash table element or NULL */
    BFshm_offset_t     nextentry_o;     
    /* (struct BFhash_entry) prev hash table element or NULL */
    BFshm_offset_t     preventry_o;     
    /* (struct BFpage), (struct BFDCnt_entry), or (struct BFFuture_entry) ptr to buffer holding this page */
    BFshm_offset_t     bpage_o;         
} BFhash_entry;

typedef struct BFbucket {
    BFshm_offset_t head_o;        // (BFhash_entry*)
    BFshm_offset_t tail_o;        // (BFhash_entry*)
    int count;
} BFbucket;

// hash table for searching
void BF_hash_init();
void BF_hash_free();
int BF_hash_function(_array_key *key);
int BF_hash_insert(BFpage* ptr);
int BF_hash_remove(BFpage* ptr);
BFhash_entry* BF_hash_search(_array_key *key);
int BF_hash_flush_fd(int fd);
int BF_flush_dirty_victim(BFpage* target);
int BF_hash_remove2(BFhash_entry* target);

// dCntEntry
int BF_dCnt_hash_function(char *key);
void BF_dCnt_init();
void BF_dCnt_free();
int BF_dCnt_insert(BFDCnt_entry *ptr);
int BF_dCnt_remove(char *arrname);
BFDCnt_entry* BF_dCnt_search(char *arrname);

void BF_dCnt_printall();

/* future log */
int BF_future_hash_function(_array_key *key);
void BF_future_init();
void BF_future_free();
int BF_future_insert(BFFuture_entry *ptr);
BFFuture_entry* BF_future_search(_array_key *key);
void BF_future_printall();

/* history log */
// just using future hash function
void BF_history_init();
void BF_history_free();
int BF_history_insert(BFFuture_entry *ptr);
BFFuture_entry* BF_history_search(_array_key *key);
void BF_history_printall();

#endif
