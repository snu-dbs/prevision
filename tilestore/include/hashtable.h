//
// Created by mxmdb on 22. 7. 8..
//

#ifndef TILESTORE_HASHINDEX_H
#define TILESTORE_HASHINDEX_H


#include <string.h>
#include <stdio.h>

#define INDEX_EXTENSION ".hdx"
#define INITIAL_NUM_BUCKETS 512
#define NUM_ENTRIES_PER_BUCKET 16
#define DEFAULT_ENTRY_SIZE 16  // sizeof(key = int64) + sizeof(value = int64)


typedef enum tilestore_ht_errcode_t {
    NO_ERR,
    ERR_CANNOT_CREATE_INDEX ,
    ERR_CANNOT_READ_INDEX ,
    ERR_CANNOT_READ_BUCKET,
    ERR_NO_EMPTY_PLACE,
    NO_ENTRY,
    RESIZE_OCCUR
} tilestore_ht_errcode_t ;


typedef struct StrDiskHT {
    char ht_name[512];
    FILE* tilestore_ht_buckets;
    FILE* tilestore_ht_entries;
    FILE* hashtable_log;
    int entry_size;
    int num_buckets;
    int next_entrybox_idx;
} StrDiskHT;


tilestore_ht_errcode_t tilestore_ht_open(char* ht_name, StrDiskHT* ht_ptr);
tilestore_ht_errcode_t tilestore_ht_open_wpayload(char* ht_name, StrDiskHT* ht_ptr, int payload_size);
tilestore_ht_errcode_t tilestore_ht_close(StrDiskHT* ht_ptr);
tilestore_ht_errcode_t tilestore_ht_put(StrDiskHT* ht_ptr, int64_t key, void* value);
tilestore_ht_errcode_t tilestore_ht_get(StrDiskHT* ht_ptr, int64_t key, void* value);
tilestore_ht_errcode_t tilestore_ht_resize(StrDiskHT* ht_ptr);
void tilestore_ht_print_all(StrDiskHT* ht_ptr);
#endif //TILESTORE_HASHINDEX_H
