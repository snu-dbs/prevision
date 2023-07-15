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
#define TILESTORE_STORAGE_VERSION                           (uint64_t) 4

#define TILESTORE_STORAGE_SCHEMA_HDR_SIZE                   (sizeof(uint64_t) * 2) + sizeof(size_t)           // not aligned!!!
#define TILESTORE_STORAGE_SCHEMA_UNIQUE                     ((uint64_t) 97575761757033)
#define TILESTORE_STORAGE_INDEX_UNIQUE                      ((uint64_t) 97575761757034)
#define TILESTORE_STORAGE_DATA_UNIQUE                       ((uint64_t) 97575761757035)
#define TILESTORE_STORAGE_DATA_HDR_SIZE                     ceil_to_512bytes(sizeof(uint64_t) + sizeof(size_t))     // aligned!!!

#define BUF_SIZE    1024


tilestore_errcode_t 
tilestore_get_array_schema_v4(
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

    ret->format = *((tilestore_format_t*)schema_base);
    schema_base += sizeof(tilestore_format_t);
    
    size_t nbytes = sizeof(uint64_t) * ret->num_of_dimensions;      // for array_size, array_size_in_tile, and tile_extents
    ret->array_size = malloc(nbytes);
    memcpy(ret->array_size, schema_base, nbytes);
    schema_base += nbytes;

    ret->array_size_in_tile = malloc(nbytes);
    memcpy(ret->array_size_in_tile, schema_base, nbytes);
    schema_base += nbytes;

    ret->unpadded_array_size = malloc(nbytes);
    memcpy(ret->unpadded_array_size, schema_base, nbytes);
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


tilestore_errcode_t 
tilestore_read_metadata_of_tile_v4(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        tilestore_tile_metadata_t *metadata_of_tile) {
    
    tilestore_errcode_t ret = TILESTORE_OK;

    // variables that should be initialized explicitly
    tilestore_array_schema_t *schema = NULL;
    StrDiskHT* hashtable = NULL;
    
    // get schema. the function will check header
    tilestore_errcode_t res = tilestore_get_array_schema_v4(array_name, &schema);
    if (res != TILESTORE_OK) {
        ret = res;
        goto DEALLOC;
    }

    // open index files
    char index_name[BUF_SIZE];
    sprintf(index_name, "%s.tilestore/index", array_name);
    
    hashtable = malloc(sizeof(StrDiskHT));
    res = tilestore_ht_open_wpayload(index_name, hashtable, sizeof(tilestore_tile_metadata_t));
    if (res != NO_ERR) {
        fprintf(stderr, "[TileStore] Failed to open index files\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    // tile coordinates check
    uint8_t invalid_tile_coords = !_is_tile_coords_in_array(schema, tile_coords);
    if (invalid_tile_coords) {
        fprintf(stderr, "[TileStore] Invalid tile coords\n");
        ret = TILESTORE_INVALID_TILE_COORDS;
        goto DEALLOC;
    }

    // convert ND tile coordinates to 1D coordinate
    int64_t key = (int64_t) _tile_coords_to_1D_coord(schema, tile_coords);

    // get offset 
    int64_t offset = 0;
    tilestore_tile_metadata_t tile_meta;
    if (tilestore_ht_get(hashtable, key, &tile_meta) != NO_ENTRY) {
        // set metadata
        memcpy(metadata_of_tile, &tile_meta, sizeof(tile_meta));
    } else {
        // if not found, it just means that the tile isn't written yet.
        ret = TILESTORE_TILE_NOT_INSERTED;
    }
    
DEALLOC:
    // free
    if (schema != NULL) tilestore_clear_array_schema(schema);
    if (hashtable != NULL) tilestore_ht_close(hashtable);
    if (hashtable != NULL) free(hashtable);

    return ret;
}



tilestore_errcode_t
tilestore_write_dense_v4(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        void *data,
        size_t data_size,
        tilestore_io_mode_t iomode) {
    
    tilestore_errcode_t ret = TILESTORE_OK;

    // variables that should be initialized explicitly
    tilestore_array_schema_t *schema = NULL;
    StrDiskHT* hashtable = NULL;
    int fd = 0;
    

    ////////////////////////////
    // 1. array schema and files' header check
    ////////////////////////////
     
    // check array directory
    char dirname[BUF_SIZE];     // buf to store string
    sprintf(dirname, "%s.tilestore", array_name);

    struct stat st = {0};
    int directory_not_exists = stat(dirname, &st) == -1;
    if (directory_not_exists) {
        fprintf(stderr, "[TileStore] The array does not exists.\n");
        ret = TILESTORE_ARRAY_NOT_EXISTS;
        goto DEALLOC;
    }

    // get schema. the function will check header
    tilestore_errcode_t res = tilestore_get_array_schema(array_name, &schema);
    if (res != TILESTORE_OK) {
        ret = res;
        goto DEALLOC;
    }

    // open index files
    char index_name[BUF_SIZE];
    sprintf(index_name, "%s/index", dirname);
    
    hashtable = malloc(sizeof(StrDiskHT));
    res = tilestore_ht_open_wpayload(index_name, hashtable, sizeof(tilestore_tile_metadata_t));
    if (res != NO_ERR) {
        fprintf(stderr, "[TileStore] Failed to open index files\n");
        ret = TILESTORE_HASHTABLE_ERR;
        goto DEALLOC;
    }

    // open data file
    char data_filename[BUF_SIZE];
    sprintf(data_filename, "%s/data", dirname);
    
    int open_flag = O_RDWR;
    if (iomode == TILESTORE_DIRECT) open_flag |= O_DIRECT;
    fd = open(data_filename, open_flag, 0644);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to open a data file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    ////////////////////////////
    // 2. tile coordinates check
    ////////////////////////////
    uint8_t invalid_tile_coords = !_is_tile_coords_in_array(schema, tile_coords);
    if (invalid_tile_coords) {
        fprintf(stderr, "[TileStore] Invalid tile coords\n");
        ret = TILESTORE_INVALID_TILE_COORDS;
        goto DEALLOC;
    }
    
    ////////////////////////////
    // 3. search tile coordinates from index 
    ////////////////////////////

    // convert ND tile coordinates to 1D coordinate
    int64_t key = (int64_t) _tile_coords_to_1D_coord(schema, tile_coords);

    // get offset and write to the file
    uint8_t is_updated = 0;
    tilestore_tile_metadata_t tile_meta;

    // find the key in the hash table
    if (tilestore_ht_get(hashtable, key, &tile_meta) != NO_ENTRY) {
        // if found, determine the possibility of overwriting
        size_t estimated_size = ceil_to_512bytes(data_size);
        if (estimated_size <= tile_meta.nbytes) {
            // size is fit! so overwrite tile
            // data
            int64_t offset = (int64_t) tile_meta.offset;
            size_t aligned_data_size = ceil_to_512bytes(data_size);
            if (pwrite(fd, data, aligned_data_size, offset) == -1) {
                fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
                ret = TILESTORE_PWRITE_FAILED;
                goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
            }

            // update tile metadata to the hash table
            tile_meta.coord_nbytes = 0;
            tile_meta.cxs_indptr_nbytes = 0;
            tile_meta.nnz = 0;
            tile_meta.data_nbytes = data_size;
            tile_meta.nbytes = aligned_data_size;
            tile_meta.offset = (size_t) offset;
            tile_meta.tiletype = TILESTORE_DENSE;
            if (tilestore_ht_put(hashtable, key, &tile_meta) != NO_ERR) {
                fprintf(stderr, "[TileStore] Failed to put entry to the hashtable\n");
                ret = TILESTORE_HASHTABLE_ERR;
                goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
            }

            is_updated = 1;
        }
    }

    // if it is a new tile or overwriting is not possible
    if (!is_updated) {
        // append tile (meta + data) to the data file 
        // first, read header from data file
        size_t hdr_size = TILESTORE_STORAGE_DATA_HDR_SIZE;
        void *hdr_unaligned = NULL;
        hdr_unaligned = malloc(hdr_size + 511);
        void *hdr = aligned_512bytes_ptr(hdr_unaligned);
        if (pread(fd, hdr, hdr_size, 0) == -1) {
            fprintf(stderr, "[TileStore] Failed to read hdr of data. errno=%d\n", errno);
            ret = TILESTORE_PREAD_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // get dataarea_size of hdr and write tile
        size_t *dataarea_size = (size_t*) ((char *) hdr + sizeof(uint64_t));
        int64_t offset = (int64_t) *(dataarea_size) + TILESTORE_STORAGE_DATA_HDR_SIZE;
        size_t aligned_data_size = ceil_to_512bytes(data_size);
        if (pwrite(fd, data, aligned_data_size, offset) == -1) {
            fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
            ret = TILESTORE_PWRITE_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // put tile metadata to the hash table
        tile_meta.coord_nbytes = 0;
        tile_meta.cxs_indptr_nbytes = 0;
        tile_meta.nnz = 0;
        tile_meta.data_nbytes = data_size;
        tile_meta.nbytes = aligned_data_size;
        tile_meta.offset = (size_t) offset;
        tile_meta.tiletype = TILESTORE_DENSE;
        if (tilestore_ht_put(hashtable, key, &tile_meta) != NO_ERR) {
            fprintf(stderr, "[TileStore] Failed to put entry to the hashtable\n");
            ret = TILESTORE_HASHTABLE_ERR;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // increase dataarea_size and write hdr
        *(dataarea_size) += aligned_data_size;
        if (pwrite(fd, hdr, hdr_size, 0) == -1) {
            fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
            ret = TILESTORE_PWRITE_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

DEALLOC_WITH_FREE_HDR_UNALIGNED:
        free(hdr_unaligned);
    }

DEALLOC:
    //////////////////////////////////////////
    // close & free
    //////////////////////////////////////////

    // free
    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    if (schema != NULL) tilestore_clear_array_schema(schema);
    if (hashtable != NULL) tilestore_ht_close(hashtable);
    if (hashtable != NULL) free(hashtable);

    return ret;
}


tilestore_errcode_t 
tilestore_read_dense_v4(
        const char *array_name,
        tilestore_tile_metadata_t metadata_of_tile,
        void *data,
        size_t data_size,
        tilestore_io_mode_t iomode) {
    
    tilestore_errcode_t ret = TILESTORE_OK;

    int fd = 0;

    
    // open data file
    char data_filename[BUF_SIZE];
    sprintf(data_filename, "%s.tilestore/data", array_name);

    int open_flag = O_RDWR;
    if (iomode == TILESTORE_DIRECT) open_flag |= O_DIRECT;
    fd = open(data_filename, open_flag, 0644);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to open a data file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    // assert constraints 
    assert(metadata_of_tile.tiletype == TILESTORE_DENSE);

    size_t offset = (size_t) metadata_of_tile.offset;

    // read data from data file
    size_t aligned_data_size = ceil_to_512bytes(data_size);
    if (pread(fd, data, aligned_data_size, offset) == -1) {
        fprintf(stderr, "[TileStore] Failed to read tile. errno=%d\n", errno);
        ret = TILESTORE_PREAD_FAILED;
        goto DEALLOC;
    }

DEALLOC:
    // free
    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array schema file.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    return ret;
}

tilestore_errcode_t
tilestore_write_sparse_csr_v4(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        void *data,
        size_t data_size,
        uint64_t **coords,
        size_t *coord_sizes,
        tilestore_io_mode_t iomode) {

    tilestore_errcode_t ret = TILESTORE_OK;

    // variables that should be initialized explicitly
    tilestore_array_schema_t *schema = NULL;
    StrDiskHT* hashtable = NULL;
    int fd = 0;

    // assert constraints 
    assert(num_of_dimensions == 2);     // support only 2D in CSR

    ////////////////////////////
    // 1. array schema and files' header check
    ////////////////////////////
     
    // check array directory
    char dirname[BUF_SIZE];     // buf to store string
    sprintf(dirname, "%s.tilestore", array_name);

    struct stat st = {0};
    int directory_not_exists = stat(dirname, &st) == -1;
    if (directory_not_exists) {
        fprintf(stderr, "[TileStore] The array does not exists.\n");
        ret = TILESTORE_ARRAY_NOT_EXISTS;
        goto DEALLOC;
    }

    // get schema. the function will check header
    tilestore_errcode_t res = tilestore_get_array_schema(array_name, &schema);
    if (res != TILESTORE_OK) {
        ret = res;
        goto DEALLOC;
    }

    // open index files
    char index_name[BUF_SIZE];
    sprintf(index_name, "%s/index", dirname);
    
    hashtable = malloc(sizeof(StrDiskHT));
    res = tilestore_ht_open_wpayload(index_name, hashtable, sizeof(tilestore_tile_metadata_t));
    if (res != NO_ERR) {
        fprintf(stderr, "[TileStore] Failed to open index files\n");
        ret = TILESTORE_HASHTABLE_ERR;
        goto DEALLOC;
    }

    // open data file
    char data_filename[BUF_SIZE];
    sprintf(data_filename, "%s/data", dirname);
    
    int open_flag = O_RDWR;
    if (iomode == TILESTORE_DIRECT) open_flag |= O_DIRECT;
    fd = open(data_filename, open_flag, 0644);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to open a data file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    ////////////////////////////
    // 2. tile coordinates check
    ////////////////////////////
    uint8_t invalid_tile_coords = !_is_tile_coords_in_array(schema, tile_coords);
    if (invalid_tile_coords) {
        fprintf(stderr, "[TileStore] Invalid tile coords\n");
        ret = TILESTORE_INVALID_TILE_COORDS;
        goto DEALLOC;
    }
    
    ////////////////////////////
    // 3. search tile coordinates from index 
    ////////////////////////////

    // convert ND tile coordinates to 1D coordinate
    int64_t key = (int64_t) _tile_coords_to_1D_coord(schema, tile_coords);

    // get offset and write to the file
    uint8_t is_updated = 0;
    tilestore_tile_metadata_t tile_meta;

    // find the key in the hash table
    if (tilestore_ht_get(hashtable, key, &tile_meta) != NO_ENTRY) {
        // if found, determine the possibility of overwriting
        size_t estimated_size = ceil_to_512bytes(data_size) + ceil_to_512bytes(coord_sizes[0]) + ceil_to_512bytes(coord_sizes[1]);
        if (estimated_size <= tile_meta.nbytes) {
            // size is fit! so overwrite tile
            // data
            int64_t offset = (int64_t) tile_meta.offset;
            size_t aligned_data_size = ceil_to_512bytes(data_size);
            if (pwrite(fd, data, aligned_data_size, offset) == -1) {
                fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
                ret = TILESTORE_PWRITE_FAILED;
                goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
            }

            // indptr
            size_t aligned_indptr_size = ceil_to_512bytes(coord_sizes[0]);
            if (pwrite(fd, coords[0], aligned_indptr_size, offset + aligned_data_size) == -1) {
                fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
                ret = TILESTORE_PWRITE_FAILED;
                goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
            }
            
            // indices
            size_t aligned_indices_size = ceil_to_512bytes(coord_sizes[1]);
            if (pwrite(fd, coords[1], aligned_indices_size,
                    offset + aligned_data_size + aligned_indptr_size) == -1) {
                fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
                ret = TILESTORE_PWRITE_FAILED;
                goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
            }

            // update tile metadata to the hash table
            tile_meta.coord_nbytes = coord_sizes[1];
            tile_meta.cxs_indptr_nbytes = coord_sizes[0];
            tile_meta.nnz = coord_sizes[1] / sizeof(uint64_t);
            tile_meta.data_nbytes = data_size;
            tile_meta.nbytes = aligned_data_size + aligned_indptr_size + aligned_indices_size;
            tile_meta.offset = (size_t) offset;
            tile_meta.tiletype = TILESTORE_SPARSE_CSR;
            if (tilestore_ht_put(hashtable, key, &tile_meta) != NO_ERR) {
                fprintf(stderr, "[TileStore] Failed to put entry to the hashtable\n");
                ret = TILESTORE_HASHTABLE_ERR;
                goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
            }

            is_updated = 1;
        }
    }

    // if it is a new tile or overwriting is not possible
    if (!is_updated) {
        // append tile (meta + data + coords) to the data file 
        // first, read header from data file
        size_t hdr_size = TILESTORE_STORAGE_DATA_HDR_SIZE;
        void *hdr_unaligned = NULL;
        hdr_unaligned = malloc(hdr_size + 511);
        void *hdr = aligned_512bytes_ptr(hdr_unaligned);
        if (pread(fd, hdr, hdr_size, 0) == -1) {
            fprintf(stderr, "[TileStore] Failed to read hdr of data. errno=%d\n", errno);
            ret = TILESTORE_PREAD_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // get dataarea_size of hdr
        size_t *dataarea_size = (size_t*) ((char *) hdr + sizeof(uint64_t));
        
        // write tile
        // data
        int64_t offset = (int64_t) *(dataarea_size) + TILESTORE_STORAGE_DATA_HDR_SIZE;
        size_t aligned_data_size = ceil_to_512bytes(data_size);
        if (pwrite(fd, data, aligned_data_size, offset) == -1) {
            fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
            ret = TILESTORE_PWRITE_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // indptr
        size_t aligned_indptr_size = ceil_to_512bytes(coord_sizes[0]);
        if (pwrite(fd, coords[0], aligned_indptr_size, offset + aligned_data_size) == -1) {
            fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
            ret = TILESTORE_PWRITE_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }
        
        // indices
        size_t aligned_indices_size = ceil_to_512bytes(coord_sizes[1]);
        if (pwrite(fd, coords[1], aligned_indices_size,
                offset + aligned_data_size + aligned_indptr_size) == -1) {
            fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
            ret = TILESTORE_PWRITE_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // put tile metadata to the hash table
        tile_meta.coord_nbytes = coord_sizes[1];
        tile_meta.cxs_indptr_nbytes = coord_sizes[0];
        tile_meta.nnz = coord_sizes[1] / sizeof(uint64_t);
        tile_meta.data_nbytes = data_size;
        tile_meta.nbytes = aligned_data_size + aligned_indptr_size + aligned_indices_size;
        tile_meta.offset = (size_t) offset;
        tile_meta.tiletype = TILESTORE_SPARSE_CSR;
        if (tilestore_ht_put(hashtable, key, &tile_meta) != NO_ERR) {
            fprintf(stderr, "[TileStore] Failed to put entry to the hashtable\n");
            ret = TILESTORE_HASHTABLE_ERR;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

        // increase dataarea_size and write hdr
        *(dataarea_size) += (aligned_data_size + aligned_indptr_size + aligned_indices_size);
        if (pwrite(fd, hdr, hdr_size, 0) == -1) {
            fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
            ret = TILESTORE_PWRITE_FAILED;
            goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
        }

DEALLOC_WITH_FREE_HDR_UNALIGNED:
        free(hdr_unaligned);
    }

DEALLOC:
    //////////////////////////////////////////
    // close & free
    //////////////////////////////////////////

    // free
    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    if (schema != NULL) tilestore_clear_array_schema(schema);
    if (hashtable != NULL) tilestore_ht_close(hashtable);
    if (hashtable != NULL) free(hashtable);

    return ret;
}


tilestore_errcode_t
tilestore_read_sparse_csr_v4(
        const char *array_name,
        tilestore_tile_metadata_t metadata_of_tile,
        void *data,
        size_t data_size,
        uint64_t **coords,
        size_t *coord_sizes,
        tilestore_io_mode_t iomode) {

    tilestore_errcode_t ret = TILESTORE_OK;

    int fd = 0;

    
    // open data file
    char data_filename[BUF_SIZE];
    sprintf(data_filename, "%s.tilestore/data", array_name);

    int open_flag = O_RDWR;
    if (iomode == TILESTORE_DIRECT) open_flag |= O_DIRECT;
    fd = open(data_filename, open_flag, 0644);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to open a data file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    // assert constraints 
    assert(metadata_of_tile.tiletype == TILESTORE_SPARSE_CSR);

    size_t offset = (size_t) metadata_of_tile.offset;

    // read data from data file
    size_t aligned_data_size = ceil_to_512bytes(data_size);
    if (pread(fd, data, aligned_data_size, offset) == -1) {
        fprintf(stderr, "[TileStore] Failed to read tile. errno=%d\n", errno);
        ret = TILESTORE_PREAD_FAILED;
        goto DEALLOC;
    }

    size_t aligned_indptr_size = ceil_to_512bytes(coord_sizes[0]);
    if (pread(fd, coords[0], aligned_indptr_size, offset + aligned_data_size) == -1) {
        fprintf(stderr, "[TileStore] Failed to read tile. errno=%d\n", errno);
        ret = TILESTORE_PREAD_FAILED;
        goto DEALLOC;
    }

    size_t aligned_indices_size = ceil_to_512bytes(coord_sizes[1]);
    if (pread(fd, coords[1], aligned_indices_size, 
            offset + aligned_data_size + aligned_indptr_size) == -1) {
        fprintf(stderr, "[TileStore] Failed to read tile. errno=%d\n", errno);
        ret = TILESTORE_PREAD_FAILED;
        goto DEALLOC;
    }

DEALLOC:
    // free
    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array schema file.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    return ret;
}
