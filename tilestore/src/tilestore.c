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

#define TILESTORE_STORAGE_VERSION                           (uint64_t) 5

#define TILESTORE_STORAGE_SCHEMA_HDR_SIZE                   (sizeof(uint64_t) * 2) + sizeof(size_t)           // not aligned!!!
#define TILESTORE_STORAGE_SCHEMA_UNIQUE                     ((uint64_t) 97575761757033)
#define TILESTORE_STORAGE_INDEX_UNIQUE                      ((uint64_t) 97575761757034)
#define TILESTORE_STORAGE_DATA_UNIQUE                       ((uint64_t) 97575761757035)
#define TILESTORE_STORAGE_DATA_HDR_SIZE                     1024     // aligned!!! / enough space

#define BUF_SIZE    1024

inline void* aligned_512bytes_ptr(void* ptr) {
    // we must use align memory address when write to a file opened in O_DIRECT mode.
    return (void*) (((uintptr_t) ptr + 511) & ~((uintptr_t) 0x1FF)); 
}

inline size_t ceil_to_512bytes(size_t original) {
    // we must use align memory address when write to a file opened in O_DIRECT mode.
    return (size_t) ((original + 511) & ~(0x1FF)); 
}

uint8_t _is_tile_coords_in_array(
        tilestore_array_schema_t *schema, uint64_t *tile_coords) {
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        if (schema->array_size_in_tile[d] <= tile_coords[d]) {
            // fprintf(stderr, "[TileStore] Invalid tile coords\n");
            return 0;
        }
    }

    return 1;
}

uint64_t _tile_coords_to_1D_coord(
        tilestore_array_schema_t *schema,
        uint64_t *tile_coords) {
    uint64_t val = 0;
    uint64_t base = 1;
    for (uint64_t d = 0; d < schema->num_of_dimensions; d++) {
        val += (base * tile_coords[d]); 
        base *= schema->array_size_in_tile[d];
    }
    
    return val;
}

tilestore_errcode_t 
tilestore_create_array(
        const char *array_name,
        uint64_t *array_size,
        uint64_t *tile_extents,
        uint32_t num_of_dimensions,
        tilestore_datatype_t data_type,
        tilestore_format_t format) {
    
    // return value
    tilestore_errcode_t ret = TILESTORE_OK;

    // variables that should be initialized explicitly
    int fd = 0;
    int data_fd = 0;
    uint64_t *array_size_in_tile = NULL;
    StrDiskHT* hashtable = NULL;
    void *data_data = NULL;
    void *schema_data = NULL;
    
    //////////////////////////////////////////
    // Create array directory.
    //////////////////////////////////////////
    
    // to create directory
    char dirname[BUF_SIZE];     // buf to store string
    sprintf(dirname, "%s.tilestore", array_name);

    // check directory
    struct stat st = {0};
    int directory_exists = stat(dirname, &st) != -1;
    if (directory_exists) {
        fprintf(stderr, "[TileStore] The array already exists.\n");
        ret = TILESTORE_ARRAY_ALREADY_EXISTS;
        goto DEALLOC;
    }

    // create directory
    mkdir(dirname, 0775);


    //////////////////////////////////////////
    // Create schema file
    //////////////////////////////////////////

    // create schema file
    char schema_filename[BUF_SIZE];     // buf to store string
    sprintf(schema_filename, "%s/schema", dirname);
    fd = open(
        schema_filename, O_RDWR | O_CREAT | O_EXCL | O_DIRECT | O_SYNC, 0644);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to create a schema file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    // calculate schema_size
    size_t schema_size = 1024;      // reserved enough space
    // schema_size += sizeof(uint32_t) * 2;    // DATA_TYPE, NUM_OF_DIM
    // schema_size += (sizeof(uint64_t) * 3) * num_of_dimensions;  // ARRAY_SIZE, ARRAY_SIZE_IN_TILE, TILE_EXTENTS * num_of_dims

    // calculate array_size_in_tile
    uint64_t *padded_arrsize = malloc(sizeof(uint64_t) * num_of_dimensions);
    array_size_in_tile = malloc(sizeof(uint64_t) * num_of_dimensions);
    for (uint32_t d = 0; d < num_of_dimensions; d++) {
        array_size_in_tile[d] = (uint64_t) ceil((double) array_size[d] / tile_extents[d]);
        padded_arrsize[d] = array_size_in_tile[d] * tile_extents[d];
    }

    // construct content of schema file
    size_t schema_filesize = ceil_to_512bytes(TILESTORE_STORAGE_SCHEMA_HDR_SIZE + schema_size);
    schema_data = malloc(schema_filesize + 511);                  // hdr + schema + align margin
    void *schema_data_aligned = aligned_512bytes_ptr(schema_data);
    char *schema_base = (char *) schema_data_aligned;

    // copy data
    // ====== hdr ======
    uint64_t schema_unique = TILESTORE_STORAGE_SCHEMA_UNIQUE;
    uint64_t version = TILESTORE_STORAGE_VERSION;
    // UNIQUE
    memcpy(schema_base, &schema_unique, sizeof(uint64_t)); 
    schema_base += sizeof(uint64_t);  
    // VERSION
    memcpy(schema_base, &version, sizeof(uint64_t)); 
    schema_base += sizeof(uint64_t);       
    // SCHEMA_SIZE
    memcpy(schema_base, &schema_filesize, sizeof(size_t)); 
    schema_base += sizeof(size_t);      

    // ====== schema ======
    // DATA_TYPE
    memcpy(schema_base, &data_type, sizeof(tilestore_datatype_t)); 
    schema_base += sizeof(tilestore_datatype_t);     
    // NUM_OF_DIM
    memcpy(schema_base, &num_of_dimensions, sizeof(uint32_t)); 
    schema_base += sizeof(uint32_t);
    // FORMAT
    memcpy(schema_base, &format, sizeof(tilestore_format_t));
    schema_base += sizeof(tilestore_format_t);
    
    // ====== arrays ======
    // ARRAY_SIZE
    memcpy(schema_base, padded_arrsize, sizeof(uint64_t) * num_of_dimensions); 
    schema_base += (sizeof(uint64_t) * num_of_dimensions);

    // ARRAY_SIZE_IN_TILE
    memcpy(schema_base, array_size_in_tile, sizeof(uint64_t) * num_of_dimensions); 
    schema_base += (sizeof(uint64_t) * num_of_dimensions);

    // UNPADDED_ARRAY_SIZE 
    memcpy(schema_base, array_size, sizeof(uint64_t) * num_of_dimensions); 
    schema_base += (sizeof(uint64_t) * num_of_dimensions);

    // TILE_EXTENTS
    memcpy(schema_base, tile_extents, sizeof(uint64_t) * num_of_dimensions);

    // write 
    if (pwrite(fd, schema_data_aligned, schema_filesize, 0) == -1) {
        fprintf(stderr, "[TileStore] Failed to write array schema. errno=%d\n", errno);
        ret = TILESTORE_PWRITE_FAILED;
        goto DEALLOC;
    }

    //////////////////////////////////////////
    // create index file (hash table)
    //////////////////////////////////////////
    char index_name[BUF_SIZE];
    sprintf(index_name, "%s/index", dirname);
    // Note that two files are created: index_entries and index_bucket
    
    // OPEN and CLOSE to ensure the file creation
    hashtable = malloc(sizeof(StrDiskHT));
    tilestore_ht_open_wpayload(index_name, hashtable, sizeof(tilestore_tile_metadata_t));

    //////////////////////////////////////////
    // create data file
    //////////////////////////////////////////
    char data_name[BUF_SIZE];
    sprintf(data_name, "%s/data", dirname);
    data_fd = open(
        data_name, O_RDWR | O_CREAT | O_EXCL | O_DIRECT | O_SYNC, 0644);
    if (data_fd == -1) {
        fprintf(stderr, "[TileStore] Failed to create a data file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }

    // the header of data file only contains UNIQUE
    size_t data_filesize = TILESTORE_STORAGE_DATA_HDR_SIZE;   // this is already
    data_data = malloc(data_filesize + 511);  // size + align margin
    void *data_data_aligned = aligned_512bytes_ptr(data_data);
    char *data_base = (char *) data_data_aligned;

    // copy header
    uint64_t data_unique = TILESTORE_STORAGE_DATA_UNIQUE;
    memcpy(data_base, &data_unique, sizeof(uint64_t));
    data_base += sizeof(uint64_t);

    // data file size
    size_t data_file_size = 0;
    memcpy(data_base, &data_file_size, sizeof(size_t));
    
    // fragmentation size
    size_t frag_size = 0;
    memcpy(data_base, &frag_size, sizeof(size_t));

    // write 
    if (pwrite(data_fd, data_data_aligned, data_filesize, 0) == -1) {
        fprintf(stderr, "[TileStore] Failed to write data header. errno=%d\n", errno);
        ret = TILESTORE_PWRITE_FAILED;
        goto DEALLOC;
    }

DEALLOC:
    //////////////////////////////////////////
    // close & free
    //////////////////////////////////////////

    if (hashtable != NULL) tilestore_ht_close(hashtable);
    
    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array schema file.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    if (data_fd != 0 && close(data_fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array data file.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    if (schema_data != NULL) free(schema_data);
    if (array_size_in_tile != NULL) free(array_size_in_tile);
    if (data_data != NULL) free(data_data);
    if (hashtable != NULL) free(hashtable);

    return ret;
}

tilestore_errcode_t 
tilestore_delete_array(const char *arrayname) {
    char buf[BUF_SIZE];
    sprintf(buf, "/bin/rm -rf %s.tilestore", arrayname);
    if (system(buf) != 0) {
        fprintf(stderr, "[TileStore] Array remove failed!!\n");
        return TILESTORE_REMOVE_ARRAY_FAILED;
    }

    return TILESTORE_OK;
}



tilestore_errcode_t 
tilestore_get_array_schema(
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
tilestore_clear_array_schema(
    tilestore_array_schema_t *array_schema) {

    free(array_schema->array_size);
    free(array_schema->array_size_in_tile);
    free(array_schema->tile_extents);

    free(array_schema);

    return TILESTORE_OK;
}

tilestore_errcode_t
tilestore_write_dense(
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

    size_t *dataarea_size = (size_t*) ((char *) hdr + sizeof(uint64_t));
    size_t *frag_size = (size_t*) ((char *) hdr + sizeof(uint64_t) + sizeof(size_t));

    // find the key in the hash table
    if (tilestore_ht_get(hashtable, key, &tile_meta) != NO_ENTRY) {
        // if found, determine the possibility of overwriting
        size_t estimated_size = ceil_to_512bytes(data_size);
        if (estimated_size <= tile_meta.nbytes) {
            // size is fit! so overwrite tile

            // fragmentation 
            *frag_size += tile_meta.nbytes - estimated_size;

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
        } else {
            // fragmentation 
            *frag_size += tile_meta.nbytes;
        }
    }

    // if it is a new tile or overwriting is not possible
    if (!is_updated) {
        // append tile (meta + data) to the data file 
        // get dataarea_size of hdr and write tile
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
    }

    // update header
    if (pwrite(fd, hdr, hdr_size, 0) == -1) {
        fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
        ret = TILESTORE_PWRITE_FAILED;
        goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
    }

DEALLOC_WITH_FREE_HDR_UNALIGNED:
    free(hdr_unaligned);

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
tilestore_read_metadata_of_tile(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        tilestore_tile_metadata_t *metadata_of_tile) {
    
    tilestore_errcode_t ret = TILESTORE_OK;

    // variables that should be initialized explicitly
    tilestore_array_schema_t *schema = NULL;
    StrDiskHT* hashtable = NULL;
    
    // get schema. the function will check header
    tilestore_errcode_t res = tilestore_get_array_schema(array_name, &schema);
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
tilestore_read_dense(
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
tilestore_write_sparse_csr(
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
    size_t *frag_size = (size_t*) ((char *) hdr + sizeof(uint64_t) + sizeof(size_t));

    // find the key in the hash table
    if (tilestore_ht_get(hashtable, key, &tile_meta) != NO_ENTRY) {
        // if found, determine the possibility of overwriting
        size_t estimated_size = ceil_to_512bytes(data_size) + ceil_to_512bytes(coord_sizes[0]) + ceil_to_512bytes(coord_sizes[1]);
        if (estimated_size <= tile_meta.nbytes) {
            // fragmentation 
            *frag_size += tile_meta.nbytes - estimated_size;
            // fprintf(stderr, "frag_size=%lu, nbytes=%lu, est_size=%lu\n", (*frag_size), tile_meta.nbytes, estimated_size);

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
        } else {
            // fragmentation 
            *frag_size += tile_meta.nbytes;
        }
    }

    // if it is a new tile or overwriting is not possible
    if (!is_updated) {
        // append tile (meta + data + coords) to the data file 
        
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
    }

    if (pwrite(fd, hdr, hdr_size, 0) == -1) {
        fprintf(stderr, "[TileStore] Failed to write tile. errno=%d\n", errno);
        ret = TILESTORE_PWRITE_FAILED;
        goto DEALLOC_WITH_FREE_HDR_UNALIGNED;
    }

DEALLOC_WITH_FREE_HDR_UNALIGNED:
    free(hdr_unaligned);


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
tilestore_read_sparse_csr(
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


tilestore_errcode_t
tilestore_compaction(const char *array_name) {
    tilestore_errcode_t ret = TILESTORE_OK;

    int fd = 0;

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

    // open data file
    char data_filename[BUF_SIZE];
    sprintf(data_filename, "%s/data", dirname);
    
    int open_flag = O_RDWR;
    // if (iomode == TILESTORE_DIRECT) open_flag |= ;
    fd = open(data_filename, open_flag, 0644);
    if (fd == -1) {
        fprintf(stderr, "[TileStore] Failed to open a data file\n");
        ret = TILESTORE_OPEN_FAILED;
        goto DEALLOC;
    }    
    
    // first, read header from data file
    size_t hdr_size = TILESTORE_STORAGE_DATA_HDR_SIZE;
    void *hdr_unaligned = NULL;
    hdr_unaligned = malloc(hdr_size + 511);
    void *hdr = aligned_512bytes_ptr(hdr_unaligned);
    if (pread(fd, hdr, hdr_size, 0) == -1) {
        fprintf(stderr, "[TileStore] Failed to read hdr of data. errno=%d\n", errno);
        ret = TILESTORE_PREAD_FAILED;
        goto DEALLOC;
    }

    // get dataarea_size of hdr
    size_t *dataarea_size = (size_t*) ((char *) hdr + sizeof(uint64_t));
    size_t *frag_size = (size_t*) ((char *) hdr + sizeof(uint64_t) + sizeof(size_t));

    // condition
    // fprintf(stderr, "%lu %lu %lu\n", (*dataarea_size), ((*dataarea_size) - (*frag_size)), (*frag_size));
    short is_required = (*dataarea_size) >= 1000000000 && ((*dataarea_size) - (*frag_size)) <= (*frag_size);
    if (!is_required) {
        // we dont need to do compaction
        fprintf(stderr, "[TileStore] Compaction not required\n");
        goto DEALLOC;
    }

    // read original 
    tilestore_array_schema_t *schema; 
    if (tilestore_get_array_schema(array_name, &schema) != TILESTORE_OK) {
        return 1;
    }

    // iterate and copy
    uint64_t len1d = 1;
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        len1d *= schema->array_size_in_tile[d];
    }

    size_t *ids = malloc(sizeof(size_t) * len1d);
    size_t *offsets = malloc(sizeof(size_t) * len1d);
    size_t *nbytes = malloc(sizeof(size_t) * len1d);

    uint64_t numtiles = 0;

    uint64_t *tile_coords = malloc(sizeof(uint64_t) * schema->num_of_dimensions);
    for (uint64_t idx1d = 0; idx1d < len1d; idx1d++) {
        // 1-d -> n-d
        // fprintf(stderr, "coords={");
        uint64_t base = idx1d;
        for (int32_t d = schema->num_of_dimensions - 1; d >= 0; d--) {
            uint64_t num_of_tiles_in_dim = (uint64_t) ceil((double) schema->array_size[d] / schema->tile_extents[d]);
            tile_coords[d] = base % num_of_tiles_in_dim; 
            base = base / num_of_tiles_in_dim;
        }
        // fprintf(stderr, "}\n");

        tilestore_tile_metadata_t metadata;
        tilestore_errcode_t err = tilestore_read_metadata_of_tile(
            array_name, tile_coords, schema->num_of_dimensions, &metadata);
        if (err == TILESTORE_TILE_NOT_INSERTED) continue;
        if (err != TILESTORE_OK) {
            return 1;
        }

        offsets[numtiles] = metadata.offset;
        nbytes[numtiles] = metadata.nbytes;
        // ids[numtiles++] = idx1d;
        ids[numtiles++] = _tile_coords_to_1D_coord(schema, tile_coords);
    }

    // sort by offsets; just bubble
    for (uint64_t i = 0; i < numtiles; i++) {
        for (uint64_t j = 0; j < numtiles - 1 - i; j++) {
            if (offsets[j] < offsets[j + 1]) {
                size_t tmp = offsets[j + 1];
                offsets[j + 1] = offsets[j];
                offsets[j] = tmp;

                tmp = nbytes[j + 1];
                nbytes[j + 1] = nbytes[j];
                nbytes[j] = tmp;

                tmp = ids[j + 1];
                ids[j + 1] = ids[j];
                ids[j] = tmp;
            }
        }
    }

    for (int64_t i = 0; i < numtiles; i++) {
        fprintf(stderr, "%lu ", offsets[i]);
    }
    fprintf(stderr, "\n");

    // check from the back
    size_t block_size = 4096;       // assume that block size is 4096
    for (int64_t i = 0; i < numtiles - 1; i++) {
        // fprintf(stderr, "%ld, %lu\n", i, frag_size);

        size_t frag_size = offsets[i] - (offsets[i + 1] + nbytes[i + 1]);
        if (frag_size <= 1000000) {      // if the fragmentation is larger than 1MB (1MB is cherry picked)
            continue;
        }

        // calculate address
        size_t valid_start = offsets[i + 1] + nbytes[i + 1];
        valid_start = (valid_start - (valid_start % block_size)) + block_size;      // align

        size_t valid_len = offsets[i] - valid_start;
        valid_len -= (valid_len % block_size);

        // compaction
        if (fallocate(fd, FALLOC_FL_COLLAPSE_RANGE, valid_start, valid_len) == -1) {
            fprintf(stderr, "[TileStore] Compaction failed during fallocate!!!! ERRNO=%d\n", errno);
            fprintf(stderr, "[TileStore] offsets[i+1]=%lu, nbytes[i+1]=%lu, block_size=%lu, valid_start=%lu, offsets[i]=%lu, valid_len=%lu\n", 
                offsets[i+1], nbytes[i+1], block_size, valid_start, offsets[i], valid_len);
            goto DEALLOC;
        }

        // adjust
        for (int64_t j = 0; j <= i; j++) {
            offsets[j] -= valid_len;
        }
    }


    //////////////////////////////////////////
    // open index file (hash table)
    //////////////////////////////////////////
    char index_name[BUF_SIZE];
    sprintf(index_name, "%s/index", dirname);
    // Note that two files are created: index_entries and index_bucket
    
    // OPEN and CLOSE to ensure the file creation
    StrDiskHT* hashtable = malloc(sizeof(StrDiskHT));
    tilestore_ht_open_wpayload(index_name, hashtable, sizeof(tilestore_tile_metadata_t));

    // update index
    tilestore_tile_metadata_t tile_meta;
    for (uint64_t i = 0; i < numtiles; i++) {
        // find the key in the hash table
        fprintf(stderr, "%lu, %ld\n", i, (int64_t) ids[i]);
        if (tilestore_ht_get(hashtable, (int64_t) ids[i], &tile_meta) != NO_ERR) {
            fprintf(stderr, "[TileStore] Compaction failed during index update (get)!!!!\n");
            goto DEALLOC;
        }

        tile_meta.offset = offsets[i];
        if (tilestore_ht_put(hashtable, (int64_t) ids[i], &tile_meta) != NO_ERR) {
            fprintf(stderr, "[TileStore] Compaction failed during index update (set)!!!!\n");
            // ret = TILESTORE_HASHTABLE_ERR;
            goto DEALLOC;
        }
    }

    tilestore_ht_close(hashtable);

    // FIXME: dealloc position
    free(ids);
    free(offsets);
    free(nbytes);
    free(tile_coords);

    tilestore_clear_array_schema(schema);

DEALLOC:
    // free
    if (fd != 0 && close(fd) == -1) {
        fprintf(stderr, "[TileStore] Failed to close array data file.\n");
        if (ret == TILESTORE_OK) ret = TILESTORE_CLOSE_FAILED;
    }

    return ret;
}


size_t 
tilestore_datatype_size(tilestore_datatype_t input) {
    switch (input) {
        case TILESTORE_CHAR: return sizeof(char);
        case TILESTORE_INT32: return sizeof(int32_t);
        case TILESTORE_FLOAT64: return sizeof(double); 
        default: return 0;
    }
}