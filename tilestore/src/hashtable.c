//
// Created by mxmdb on 22. 7. 11..
//
#include <stdint.h>
#include <unistd.h>
#include "hashtable.h"
#include <errno.h>
#include <stdbool.h>
#include <malloc.h>
#include <stdlib.h>
#include <time.h>

#define BUF_SIZE    512


void build_entries_name (char* buffer, char* name){
    strcpy(buffer, name);
    strcat(buffer,"_entires");
    strcat(buffer,INDEX_EXTENSION);
}

void build_bucket_name (char* buffer, char* name){
    strcpy(buffer, name);
    strcat(buffer,"_bucket");
    strcat(buffer,INDEX_EXTENSION);
}

int64_t tilesotre_hash_func(int64_t key, int64_t length){
    /**
     *  it ranges from  1 to length because 0 is reserved for the meta data
     */
    return key%length;
}

tilestore_ht_errcode_t tilestore_ht_open(char* ht_name, StrDiskHT* ht_ptr){

    FILE *ht_bucket;
    FILE *ht_entries;
    strcpy(ht_ptr->ht_name, ht_name);

    char bucket_name[BUF_SIZE];
    build_bucket_name(bucket_name, ht_name);

    if (access(bucket_name, F_OK) != 0) {
        /* Hashtable does not exist */
        /* Create new hashtable */

        errno = 0;
        // fprintf(stderr, "file name: %s\n", bucket_name);
        ht_bucket = fopen(bucket_name, "wb+");
        if(ht_bucket == NULL ) return  ERR_CANNOT_CREATE_INDEX;

        int initial_num_buckets = INITIAL_NUM_BUCKETS;
        int64_t buffer[INITIAL_NUM_BUCKETS];

        for(int i = 0; i < initial_num_buckets ; i++){
            buffer[i] = -1;
            if( i == 0 ) buffer[i] = 0;
            if( i == 1 ) buffer[i] = DEFAULT_ENTRY_SIZE;
        }
        fwrite(buffer, (INITIAL_NUM_BUCKETS)*sizeof(int64_t), 1, ht_bucket);
        fsync(fileno(ht_bucket));
        ht_ptr->num_buckets = initial_num_buckets;
        ht_ptr->next_entrybox_idx = 0;
        ht_ptr->entry_size = DEFAULT_ENTRY_SIZE;

        char entries_name[BUF_SIZE];
        build_entries_name(entries_name, ht_name);


        errno = 0;
        ht_entries = fopen(entries_name, "wb+");
        if(ht_entries == NULL ) return ERR_CANNOT_READ_BUCKET;

        ht_ptr->tilestore_ht_entries = ht_entries;
        ht_ptr->tilestore_ht_buckets = ht_bucket;


        return NO_ERR;


    } else {
        /* Hashtable exists */
        /* Read the hashtable */

        ht_bucket = fopen(bucket_name, "rb+");
        if(ht_bucket == NULL ) return ERR_CANNOT_READ_INDEX;

        fseek(ht_bucket, 0, SEEK_END); // seek to end of file
        long size = ftell(ht_bucket); // get current file pointer
        fseek(ht_bucket, 0, SEEK_SET); // seek back to beginning of file

        ht_ptr->num_buckets = size/sizeof(int64_t);
        // printf("number of buckets: %d\n", ht_ptr->num_buckets);

        fseek(ht_bucket, 0, SEEK_SET);
        fread(&(ht_ptr->next_entrybox_idx), sizeof(ht_ptr->next_entrybox_idx), 1, ht_bucket);
        fseek(ht_bucket, 8, SEEK_SET);
        fread(&(ht_ptr->entry_size), sizeof(ht_ptr->entry_size), 1, ht_bucket);

        char entries_name[BUF_SIZE];
        build_entries_name(entries_name, ht_name);


        ht_entries = fopen(entries_name, "rb+");
        if(ht_entries == NULL ) return ERR_CANNOT_READ_BUCKET;

        ht_ptr->tilestore_ht_entries = ht_entries;
        ht_ptr->tilestore_ht_buckets = ht_bucket;
        return NO_ERR;
    }
}


tilestore_ht_errcode_t tilestore_ht_open_wpayload(char* ht_name, StrDiskHT* ht_ptr, int payload_size){

    FILE *ht_bucket;
    FILE *ht_entries;
    strcpy(ht_ptr->ht_name, ht_name);


    char bucket_name[BUF_SIZE];
    build_bucket_name(bucket_name, ht_name);

    if (access(bucket_name, F_OK) != 0) {
        /* Hashtable does not exist */
        /* Create new hashtable */

        errno = 0;
        // fprintf(stderr, "file name: %s\n", bucket_name);
        ht_bucket = fopen(bucket_name, "wb+");
        if(ht_bucket == NULL ) return  ERR_CANNOT_CREATE_INDEX;

        int initial_num_buckets = INITIAL_NUM_BUCKETS;
        int64_t buffer[INITIAL_NUM_BUCKETS];

        for(int i = 0; i < initial_num_buckets ; i++){
            buffer[i] = -1;
            if( i == 0 ) buffer[i] = 0;
            if( i == 1 ) buffer[i] = payload_size+8;
        }
        fwrite(buffer, (INITIAL_NUM_BUCKETS)*sizeof(int64_t), 1, ht_bucket);
        fsync(fileno(ht_bucket));
        ht_ptr->num_buckets = initial_num_buckets;
        ht_ptr->next_entrybox_idx = 0;
        ht_ptr->entry_size = payload_size+8;

        char entries_name[BUF_SIZE];
        build_entries_name(entries_name, ht_name);


        errno = 0;
        ht_entries = fopen(entries_name, "wb+");
        if(ht_entries == NULL ) return ERR_CANNOT_READ_BUCKET;

        ht_ptr->tilestore_ht_entries = ht_entries;
        ht_ptr->tilestore_ht_buckets = ht_bucket;
        return NO_ERR;


    } else {
        /* Hashtable exists */
        /* Read the hashtable */

        ht_bucket = fopen(bucket_name, "rb+");
        if(ht_bucket == NULL ) return ERR_CANNOT_READ_INDEX;

        fseek(ht_bucket, 0, SEEK_END); // seek to end of file
        long size = ftell(ht_bucket); // get current file pointer
        fseek(ht_bucket, 0, SEEK_SET); // seek back to beginning of file

//        // fprintf(stderr, "file size %ld\n", size);
        ht_ptr->num_buckets = size/sizeof(int64_t);
        // printf("number of buckets: %d\n", ht_ptr->num_buckets);

        fseek(ht_bucket, 0, SEEK_SET);
        fread(&(ht_ptr->next_entrybox_idx), sizeof(ht_ptr->next_entrybox_idx), 1, ht_bucket);
        fseek(ht_bucket, 8, SEEK_SET);
        fread(&(ht_ptr->entry_size), sizeof(ht_ptr->entry_size), 1, ht_bucket);


        char entries_name[BUF_SIZE];
        build_entries_name(entries_name, ht_name);


        ht_entries = fopen(entries_name, "rb+");
        if(ht_entries == NULL ) return ERR_CANNOT_READ_BUCKET;

        ht_ptr->tilestore_ht_entries = ht_entries;
        ht_ptr->tilestore_ht_buckets = ht_bucket;
        return NO_ERR;
    }
}




tilestore_ht_errcode_t tilestore_ht_close(StrDiskHT* ht_ptr){
    fclose(ht_ptr->tilestore_ht_entries);
    fclose(ht_ptr->tilestore_ht_buckets);
    return NO_ERR;
}

tilestore_ht_errcode_t tilestore_ht_put(StrDiskHT* ht_ptr, int64_t key, void* value){

    // find entrybox_idx corresponding projected key
    int entry_size = ht_ptr->entry_size;
    int entrybox_idx = 0;
    int projected_key = tilesotre_hash_func(key, (ht_ptr->num_buckets - 2))+2; // Index 0,1 is reserved for the meta data


    fseek(ht_ptr->tilestore_ht_buckets, projected_key * sizeof(int64_t), SEEK_SET);
    fread(&entrybox_idx, sizeof(int), 1 , ht_ptr->tilestore_ht_buckets);


    if(entrybox_idx == -1 ) {
        // new bucket
        entrybox_idx = ht_ptr->next_entrybox_idx;
        fseek(ht_ptr->tilestore_ht_buckets, projected_key * sizeof(int64_t), SEEK_SET);
        fwrite(&entrybox_idx, sizeof(int), 1, ht_ptr->tilestore_ht_buckets);
        ht_ptr->next_entrybox_idx++;

        // Update the meta data at bucket 0
        fseek(ht_ptr->tilestore_ht_buckets, 0, SEEK_SET);
        fwrite(&(ht_ptr->next_entrybox_idx), sizeof(int), 1, ht_ptr->tilestore_ht_buckets);
        // put key and values in the entries
        char buffer[NUM_ENTRIES_PER_BUCKET * entry_size] ;  // #entries_per_bucket * (key, value)
        memset(buffer, 0, NUM_ENTRIES_PER_BUCKET * entry_size);

        memcpy(&buffer[entry_size], &key, sizeof (int64_t));
        memcpy(&buffer[entry_size+8], value, entry_size-sizeof (int64_t));

        int64_t* fill = (int64_t*) buffer;
        (*fill)++;


        fseek(ht_ptr->tilestore_ht_entries, entrybox_idx * NUM_ENTRIES_PER_BUCKET * entry_size, SEEK_SET);
        fwrite(buffer, NUM_ENTRIES_PER_BUCKET * entry_size, 1, ht_ptr->tilestore_ht_entries);

//        fsync(fileno(ht_ptr->tilestore_ht_buckets));
//        fsync(fileno(ht_ptr->tilestore_ht_entries));

        return NO_ERR;
    }
    else{
        // existing bucket
        // put key and values in the bucket
        fseek(ht_ptr->tilestore_ht_entries, entrybox_idx * NUM_ENTRIES_PER_BUCKET * entry_size, SEEK_SET);
        char buffer [NUM_ENTRIES_PER_BUCKET * entry_size] ;  // #entries_per_bucket * (key, value)
        memset(buffer, 0, NUM_ENTRIES_PER_BUCKET * entry_size);
        fread(buffer, NUM_ENTRIES_PER_BUCKET * entry_size, 1, ht_ptr->tilestore_ht_entries);


        int64_t* fill = (int64_t*) buffer;

        if((*fill) >= NUM_ENTRIES_PER_BUCKET - 1){
            // fprintf(stderr, "[TileStore:HashTable] Resize is required!\n");
            tilestore_ht_resize(ht_ptr);
            tilestore_ht_put(ht_ptr, key, value);
            // RESIZE_OCCUR is an error, so I changed it to NO_ERR 
            return NO_ERR;
        }


        bool duplicate = false;

        for( int i = 1 ; i <= (int) (*fill) ; i++){
            if( *((int64_t*) &buffer[i*entry_size]) == key ){
                *((int64_t*) &buffer[i*entry_size]) = key;
                memcpy(&buffer[i*entry_size+8], value, entry_size-sizeof (int64_t));
                duplicate = true;
                break;
            }
        }


        if( !duplicate) {
            *((int64_t*) &buffer[((*fill)+1)*entry_size]) = key;
            memcpy(&buffer[(*fill+1)*entry_size+8], value, entry_size-sizeof (int64_t));
            (*fill)++;
        }

        fseek(ht_ptr->tilestore_ht_entries, entrybox_idx * NUM_ENTRIES_PER_BUCKET * entry_size, SEEK_SET);
        fwrite(buffer, NUM_ENTRIES_PER_BUCKET * entry_size, 1, ht_ptr->tilestore_ht_entries);

//        fsync(fileno(ht_ptr->tilestore_ht_buckets));
//        fsync(fileno(ht_ptr->tilestore_ht_entries));
        return NO_ERR;
    }
}



tilestore_ht_errcode_t tilestore_ht_get(StrDiskHT* ht_ptr, int64_t key, void* value){

    int entrybox_idx = 0;
    int entry_size = ht_ptr->entry_size;
    int projected_key = (int) tilesotre_hash_func(key, (ht_ptr->num_buckets - 2)) +2;  // Index 0,1 is reserved for the meta data
    fseek(ht_ptr->tilestore_ht_buckets, projected_key * sizeof(int64_t), SEEK_SET);
    fread(&entrybox_idx, sizeof(int), 1 , ht_ptr->tilestore_ht_buckets);

    if(entrybox_idx == -1 ) {
       return NO_ENTRY;
    }

    // get key and values in the bucket
    fseek(ht_ptr->tilestore_ht_entries, entrybox_idx * NUM_ENTRIES_PER_BUCKET * entry_size, SEEK_SET);
    char buffer [NUM_ENTRIES_PER_BUCKET * entry_size] ;
    fread(buffer, NUM_ENTRIES_PER_BUCKET * entry_size, 1, ht_ptr->tilestore_ht_entries);

    int64_t* fill = (int64_t*) buffer;

    for ( int i = 1 ; i <= (int) (*fill) ; i++){
        if( *((int64_t*) &buffer[i*entry_size]) == key ){
            memcpy(value, &buffer[i*entry_size+8], entry_size-sizeof (int64_t));
            return NO_ERR;
        }
    }
    // fprintf(stderr, "no entry\n");
    return  NO_ENTRY;

}

void tilestore_ht_print_all(StrDiskHT* ht_ptr){
    int num_buckets = ht_ptr->num_buckets;
    int entry_size = ht_ptr->entry_size;
        for (int i = 0; i < num_buckets; i++){
        char buffer[NUM_ENTRIES_PER_BUCKET * entry_size];
        fseek(ht_ptr->tilestore_ht_entries, i * NUM_ENTRIES_PER_BUCKET * entry_size, SEEK_SET);
        fread(buffer, NUM_ENTRIES_PER_BUCKET * entry_size, 1, ht_ptr->tilestore_ht_entries );
        int64_t* fill = (int64_t*) buffer;
        for ( int64_t j = 1 ; j <= (*fill); j++){
            // fprintf(stderr, "PRINT (%ld, %ld)\n", *((int64_t*) &buffer[i*entry_size]), *((int64_t*) &buffer[i*entry_size+8]));
        }
    }
}



tilestore_ht_errcode_t tilestore_ht_create(char* ht_name, StrDiskHT* ht_ptr, int num_buckets, int entry_size){

    FILE *ht_buckets;
    FILE *ht_entries;
    strcpy(ht_ptr->ht_name, ht_name);


    char bucktes_name[BUF_SIZE];
    build_bucket_name(bucktes_name, ht_name);

    errno = 0;
    // // fprintf(stderr, "file name: %s\n", bucktes_name);
    ht_buckets = fopen(bucktes_name, "wb+");
    if(ht_buckets == NULL ) return  ERR_CANNOT_CREATE_INDEX;

    int initial_num_buckets = num_buckets;
    int64_t buffer[num_buckets];

    for(int i = 0; i < initial_num_buckets ; i++){
        buffer[i] = -1;
        if( i == 0 ) buffer[i] = 0;
        if( i == 1 ) buffer[i] = entry_size;
    }
    fwrite(buffer, (num_buckets)*sizeof(int64_t), 1, ht_buckets);
    fsync(fileno(ht_buckets));
    ht_ptr->num_buckets = initial_num_buckets;
    ht_ptr->next_entrybox_idx = 0;
    ht_ptr->entry_size = entry_size;

    char entries_name[BUF_SIZE];
    build_entries_name(entries_name, ht_name);

    errno = 0;
    ht_entries = fopen(entries_name, "wb+");
    if(ht_entries == NULL ) return ERR_CANNOT_READ_BUCKET;

    ht_ptr->tilestore_ht_entries = ht_entries;
    ht_ptr->tilestore_ht_buckets = ht_buckets;
    return NO_ERR;

}

tilestore_ht_errcode_t tilestore_ht_resize(StrDiskHT* ht_ptr){
    // printf("[TileSore->HashTable] Resize %d -> %d\n", ht_ptr->num_buckets, ht_ptr->num_buckets*2);
    StrDiskHT* resized_hashtable = malloc(sizeof(StrDiskHT));

    int entry_size = ht_ptr->entry_size;

    srand(time(0));
    char temp_name[BUF_SIZE];
    sprintf(temp_name, "temp_%d", rand()%1247720);

    // create new resized hash
    tilestore_ht_create(temp_name, resized_hashtable, ht_ptr->num_buckets*2, entry_size);


    // fill entry one by one
    int num_buckets = ht_ptr->num_buckets;
    for (int i = 0; i < num_buckets; i++){
        char buffer[NUM_ENTRIES_PER_BUCKET * entry_size];
        fseek(ht_ptr->tilestore_ht_entries, i * NUM_ENTRIES_PER_BUCKET * entry_size, SEEK_SET);
        fread(buffer, NUM_ENTRIES_PER_BUCKET * entry_size, 1, ht_ptr->tilestore_ht_entries );
        int64_t* fill = (int64_t*) buffer;
        for ( int j = 1 ; j <= (int) *fill; j++){
            tilestore_ht_put(resized_hashtable, *((int64_t*) &buffer[j*entry_size]), &buffer[j*entry_size+8]);
        }
    }

    // rename
    char orig_buckets_name[BUF_SIZE];
    build_bucket_name(orig_buckets_name, ht_ptr->ht_name);

    char temp_buckets_name[BUF_SIZE];
    build_bucket_name(temp_buckets_name, resized_hashtable->ht_name);

    char orig_entries_name[BUF_SIZE];
    build_entries_name(orig_entries_name, ht_ptr->ht_name);

    char temp_entries_name[BUF_SIZE];
    build_entries_name(temp_entries_name, resized_hashtable->ht_name);

    tilestore_ht_close(ht_ptr);
    tilestore_ht_close(resized_hashtable);

    // printf("rname %s => %s\n", ht_ptr->ht_name, resized_hashtable->ht_name);
    rename(temp_buckets_name, orig_buckets_name);
    rename(temp_entries_name, orig_entries_name);
    tilestore_ht_open(ht_ptr->ht_name, ht_ptr);

    return NO_ERR;
}
