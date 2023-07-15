#define _GNU_SOURCE     // This is included to use O_DIRECT

#include <fcntl.h>
#include <math.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>

#include "tilestore.h"
#include "hashtable.h"

// data type, # of dimenaions, array_size_in_cell, array_size_in_tile, tile_extents

#define TILESTORE_STORAGE_VERSION                           (uint64_t) 3

#define TILESTORE_STORAGE_SCHEMA_HDR_SIZE                   (sizeof(uint64_t) * 2) + sizeof(size_t)           // not aligned!!!
#define TILESTORE_STORAGE_SCHEMA_UNIQUE                     ((uint64_t) 97575761757033)
#define TILESTORE_STORAGE_INDEX_UNIQUE                      ((uint64_t) 97575761757034)
#define TILESTORE_STORAGE_DATA_UNIQUE                       ((uint64_t) 97575761757035)
#define TILESTORE_STORAGE_DATA_HDR_SIZE                     ceil_to_512bytes(sizeof(uint64_t) + sizeof(size_t))     // aligned!!!

#define BUF_SIZE    1024


tilestore_errcode_t 
tilestore_get_array_schema_v3(
        const char *array_name,
        tilestore_array_schema_t **array_schema) {

    // return value
    tilestore_errcode_t retval = TILESTORE_OK;
    
    // variables that should be initialized explicitly
    int fd = 0;
    void *schema_hdr = NULL;
    void *schema_data = NULL;

    //////////////////////////////////////////
    // Check if the directory exists
    //////////////////////////////////////////
    
    // to check directory
    char dirname[BUF_SIZE];
    sprintf(dirname, "%s.tilestore", array_name);

    // check directory
    struct stat st = {0};
    int directory_exists = stat(dirname, &st) != -1;
    if (!directory_exists) {
        fprintf(stderr, "[TileStore] The array does not exists.\n");
        retval = TILESTORE_ARRAY_NOT_EXISTS;
        goto DEALLOC;
    }

    // open schema file
    char schema_filename[BUF_SIZE];     // buf to store string
    sprintf(schema_filename, "%s/schema", dirname);
    fd = open(schema_filename, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to open a schema file\n");
        retval = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    //////////////////////////////////////////
    // read 
    //////////////////////////////////////////

    // read hdr
    schema_hdr = malloc(TILESTORE_STORAGE_SCHEMA_HDR_SIZE);
    pread(fd, schema_hdr, TILESTORE_STORAGE_SCHEMA_HDR_SIZE, 0);
    
    // validate file
    uint64_t schema_unique = *((uint64_t*) schema_hdr);
    if (schema_unique != TILESTORE_STORAGE_SCHEMA_UNIQUE) {
        fprintf(stderr, "[TileStore] The schema file broken.\n");
        retval = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    uint64_t version = *((size_t*) (((char *) schema_hdr) + sizeof(uint64_t)));
    if (version != TILESTORE_STORAGE_VERSION) {
        fprintf(stderr, "[TileStore] Version mismatched! tilestore: %lu vs. file: %lu \n", TILESTORE_STORAGE_VERSION, version);
        retval = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    // read schema
    size_t schema_size = *((size_t*) (((char *) schema_hdr) + (sizeof(uint64_t) * 2)));
    schema_data = malloc(schema_size);
    pread(fd, schema_data, schema_size, TILESTORE_STORAGE_SCHEMA_HDR_SIZE);

    //////////////////////////////////////////
    // parse data
    //////////////////////////////////////////
    char *schema_base = (char*) schema_data;
    tilestore_array_schema_t *ret = malloc(sizeof(tilestore_array_schema_t));
    
    ret->data_type = *((tilestore_datatype_t*) schema_base);
    schema_base += sizeof(tilestore_datatype_t);

    ret->num_of_dimensions = *((uint32_t*) schema_base);
    schema_base += sizeof(uint32_t);
    
    size_t nbytes = sizeof(uint64_t) * ret->num_of_dimensions;      // for array_size, array_size_in_tile, and tile_extents
    ret->array_size = malloc(nbytes);
    memcpy(ret->array_size, schema_base, nbytes);
    schema_base += nbytes;

    ret->array_size_in_tile = malloc(nbytes);
    memcpy(ret->array_size_in_tile, schema_base, nbytes);
    schema_base += nbytes;

    ret->tile_extents = malloc(nbytes);
    memcpy(ret->tile_extents, schema_base, nbytes);
    schema_base += nbytes;

    (*array_schema) = ret;

DEALLOC:
    //////////////////////////////////////////
    // close & free
    //////////////////////////////////////////

    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array.\n");
        if (retval == TILESTORE_OK) retval = TILESTORE_CLOSE_FAILED;
    }

    if (schema_hdr != NULL) free(schema_hdr);
    if (schema_data != NULL) free(schema_data);

    return retval;
}
