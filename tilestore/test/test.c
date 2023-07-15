
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


#include <stdio.h>
#include <stdint.h>
#include <malloc.h>
#include <assert.h>
#include <math.h>
#include <sys/time.h>

#include "tilestore.h"
#include "hashtable.h"

void test_hash_table_interface(){
    char* newht_name = "interface";

    // OPEN
    StrDiskHT* hashtable = malloc(sizeof(StrDiskHT));
    tilestore_ht_open_wpayload(newht_name, hashtable, 24);

    int64_t  key = 1;
    int64_t  value[3] = {4,8,6};

    // PUT
    tilestore_ht_put(hashtable, key, value);

    memset(value, 0, 24);
    
    // GET
    tilestore_ht_get(hashtable, key, value);

    printf("(%ld, %ld, %ld )\n", value[0], value[1], value[2] );
    assert ( value[0] == 4 );
    assert ( value[1] == 8 );
    assert ( value[2] == 6 ); 
    // CLOSE
    tilestore_ht_close(hashtable);
    free(hashtable);

    printf("Hash Table Interace Test finished!!\n");
}

void test_hash_table_simple(){
    char* newht_name = "simple";
    StrDiskHT* newHT = malloc(sizeof(StrDiskHT));
    if( tilestore_ht_open(newht_name, newHT) != NO_ERR ) printf("ERROR\n");

    int64_t values[6] = {3, 2, 6, 23, 4, 6};

    assert( tilestore_ht_put(newHT, 1, &values[0]) == NO_ERR);
    assert( tilestore_ht_put(newHT, 1, &values[1]) == NO_ERR);
    assert( tilestore_ht_put(newHT, 1, &values[2]) == NO_ERR);
    assert( tilestore_ht_put(newHT, 100, &values[3]) == NO_ERR);
    assert( tilestore_ht_put(newHT, 21, &values[4]) == NO_ERR);
    assert( tilestore_ht_put(newHT, 2, &values[5]) == NO_ERR);

    //tilestore_ht_print_all(newHT);
    int value;

    assert(tilestore_ht_get(newHT, 4, &value) == NO_ENTRY);

    tilestore_ht_get(newHT, 1, &value);
    assert(value == 6);

    tilestore_ht_get(newHT, 100, &value);
    assert(value == 23);

    assert(tilestore_ht_get(newHT, 23, &value) == NO_ENTRY);


    tilestore_ht_close(newHT);
    free(newHT);
    printf("Hash Table Simple Test finished!!\n");

}

void test_hash_table_latency(){
    int total_iteration = 1000;

    char* newht_name = "latency";
    StrDiskHT* hashtable = malloc(sizeof(StrDiskHT));
    if(tilestore_ht_open_wpayload(newht_name, hashtable, 24) != NO_ERR ) printf("ERROR\n");

    long start, end;
    struct timeval timecheck;

    gettimeofday(&timecheck, NULL);
    start = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;


    for( int i = 0 ; i < total_iteration; i++){
        int value[6] = {0};
	value[0] =  i * 12691 % 12952;
        tilestore_ht_put(hashtable, i, value);
    }

    gettimeofday(&timecheck, NULL);
    end = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;

    printf("PUT operation consumed %f microseconds\n", (double)(end - start)/ (double) total_iteration);

    gettimeofday(&timecheck, NULL);
    start = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;

    for( int i = 0 ; i < total_iteration; i++){
        int value[6] = {0} ;
        tilestore_ht_get(hashtable, i, value);
        assert(value[0] == i*12691%12952 );
    }

    gettimeofday(&timecheck, NULL);
    end = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;
    printf("GET operation consumed %f microseconds\n", (double)(end - start)/ (double) total_iteration);

    tilestore_ht_close(hashtable);
    free(hashtable);
    printf("Hash Table Latency Test finished!!\n");

}


void test_hash_table_resize(){
    int total_iteration = 100000;

    char* newht_name = "resize";
    StrDiskHT* hashtable = malloc(sizeof(StrDiskHT));
    if(tilestore_ht_open(newht_name, hashtable) != NO_ERR ) printf("ERROR\n");

    long start, end;
    struct timeval timecheck;

    gettimeofday(&timecheck, NULL);
    start = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;


    for( int i = 0 ; i < total_iteration; i++){
        int value =  i * 12691 % 12952;
        tilestore_ht_put(hashtable, i, &value);
    }

    gettimeofday(&timecheck, NULL);
    end = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;

    printf("PUT operation consumed %f microseconds\n", (double)(end - start)/ (double) total_iteration);

    gettimeofday(&timecheck, NULL);
    start = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;

    for( int i = 0 ; i < total_iteration; i++){
        int value;
        tilestore_ht_get(hashtable, i, &value);
        assert(value == i*12691%12952 );
    }

    gettimeofday(&timecheck, NULL);
    end = (long)timecheck.tv_sec * 1000000 + (long)timecheck.tv_usec ;
    printf("GET operation consumed %f microseconds\n", (double)(end - start)/ (double) total_iteration);

    tilestore_ht_close(hashtable);
    free(hashtable);
    printf("Hash Table Resize Test finished!!\n");

}

int test_create_array_and_get_schema() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {100, 200, 300};
    uint64_t tile_extents[] = {10, 50, 60};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);

    // test
    assert(res == TILESTORE_OK);

    // get schema
    tilestore_array_schema_t *schema;
    res = tilestore_get_array_schema(arrname, &schema);

    // test
    assert(res == TILESTORE_OK);
    assert(schema->num_of_dimensions == num_of_dim);
    assert(schema->array_size[0] == array_size[0]);
    assert(schema->array_size[1] == array_size[1]);
    assert(schema->array_size[2] == array_size[2]);
    assert(schema->array_size_in_tile[0] == (uint64_t) ceil(array_size[0] / tile_extents[0]));
    assert(schema->array_size_in_tile[1] == (uint64_t) ceil(array_size[1] / tile_extents[1]));
    assert(schema->array_size_in_tile[2] == (uint64_t) ceil(array_size[2] / tile_extents[2]));
    assert(schema->data_type == TILESTORE_INT32);
    assert(schema->tile_extents[0] == tile_extents[0]);
    assert(schema->tile_extents[1] == tile_extents[1]);
    assert(schema->tile_extents[2] == tile_extents[2]);

    printf("%s passed\n", __func__);
}


int test_write_dense_and_read_dense() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {100, 200, 300};
    uint64_t tile_extents[] = {10, 50, 60};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);
    assert(res == TILESTORE_OK);

    // write_dense - normal case
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1] * tile_extents[2]);
    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }

    // write
    uint64_t tile_coords[] = {1, 2, 3};
    res = tilestore_write_dense(
        arrname, tile_coords, num_of_dim,
        data, data_size_unaligned, TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // read
    // read has two phase
    // first, read metadata of tile
    tilestore_tile_metadata_t metadata;
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords, num_of_dim, &metadata);
    assert(res == TILESTORE_OK);

    // second, read tile data from data file
    int *readdata_unaligned = malloc(data_size + 511);
    int *readdata = (int*) aligned_512bytes_ptr(readdata_unaligned);
    res = tilestore_read_dense(
        arrname, metadata,
        readdata, data_size_unaligned, TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // test
    for (size_t i = 0; i < num_of_elems; i++) {
        assert(data[i] == readdata[i]);
    }

    // free
    free(readdata_unaligned);
    free(data_unaligned);

    printf("%s passed\n", __func__);
}

int test_write_dense_update_existing_tile() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {100, 200, 300};
    uint64_t tile_extents[] = {10, 50, 60};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);
    assert(res == TILESTORE_OK);

    // write_dense - normal case
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1] * tile_extents[2]);
    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }

    // write
    uint64_t tile_coords[] = {1, 2, 3};
    res = tilestore_write_dense(
        arrname, tile_coords, num_of_dim,
        data, data_size_unaligned, TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // write different data to the same tile coordinates
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i * 100;
    }
    res = tilestore_write_dense(
        arrname, tile_coords, num_of_dim,
        data, data_size_unaligned, TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // read
    tilestore_tile_metadata_t metadata;
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords, num_of_dim, &metadata);
    assert(res == TILESTORE_OK);

    int *readdata_unaligned = malloc(data_size + 511);
    int *readdata = (int*) aligned_512bytes_ptr(readdata_unaligned);
    res = tilestore_read_dense(
        arrname, metadata,
        readdata, data_size_unaligned, TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // test
    // data from the read function must be same with second written data
    for (size_t i = 0; i < num_of_elems; i++) {
        assert(data[i] == readdata[i]);
    }

    // free
    free(readdata_unaligned);
    free(data_unaligned);

    printf("%s passed\n", __func__);
}


int test_read_empty_tile() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {100, 200, 300};
    uint64_t tile_extents[] = {10, 50, 60};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);
    assert(res == TILESTORE_OK);

    // read metadata
    uint64_t tile_coords[] = {1, 2, 3};
    tilestore_tile_metadata_t metadata;
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords, num_of_dim, &metadata);
    assert(res == TILESTORE_TILE_NOT_INSERTED);

    printf("%s passed\n", __func__);
}


int test_wrong_write_dense() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {100, 200, 300};
    uint64_t tile_extents[] = {10, 50, 60};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);
    assert(res == TILESTORE_OK);

    // write to wrong array
    const char *wrong_arrname = "adsfasfaw";
    uint64_t tile_coords_1[] = {0, 0, 0};
    res = tilestore_write_dense(wrong_arrname, tile_coords_1, num_of_dim, NULL, 0, TILESTORE_DIRECT);
    assert(res == TILESTORE_ARRAY_NOT_EXISTS);

    // write with invalid coords
    uint64_t tile_coords_2[] = {121300, 224200, 300404};
    res = tilestore_write_dense(arrname, tile_coords_2, num_of_dim, NULL, 0, TILESTORE_DIRECT);
    assert(res == TILESTORE_INVALID_TILE_COORDS);

    printf("%s passed\n", __func__);
}

int test_wrong_read_dense() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {100, 200, 300};
    uint64_t tile_extents[] = {10, 50, 60};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);
    assert(res == TILESTORE_OK);

    // read from array that does not exist
    const char *wrong_arrname = "adsfasfaw";
    uint64_t tile_coords[] = {1, 2, 3};
    res = tilestore_read_metadata_of_tile(
        wrong_arrname, tile_coords, num_of_dim, NULL);
    assert(res == TILESTORE_ARRAY_NOT_EXISTS);

    // read with invalid coords
    uint64_t tile_coords_2[] = {214515, 123124, 4222223};
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords_2, num_of_dim, NULL);
    assert(res == TILESTORE_INVALID_TILE_COORDS);

    // read from array that does not exist
    tilestore_tile_metadata_t tm;
    res = tilestore_read_dense(wrong_arrname, tm, NULL, 0, TILESTORE_DIRECT);
    assert(res == TILESTORE_OPEN_FAILED);

    printf("%s passed\n", __func__);
}

int test_write_sparse_csr_and_read_sparse_csr() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {100, 200};
    uint64_t tile_extents[] = {10, 50};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_SPARSE_CSR);
    assert(res == TILESTORE_OK);

    // write_sparse - normal case
    uint64_t tile_coords[] = {1, 2};

    /////////////////////////////////
    // prepare memory space
    /////////////////////////////////
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);
    // data
    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }
    
    // coords
    // indices (column indices)
    size_t indices_size_unaligned = sizeof(uint64_t) * num_of_elems;
    size_t indices_size = ceil_to_512bytes(indices_size_unaligned);
    uint64_t *indices_unaligned = malloc(indices_size + 511);
    uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        indices[i] = ((uint64_t) i % tile_extents[1]) + (tile_extents[1] * tile_coords[1]);
    }

    // indptr (row ptr)
    size_t indptr_size_unaligned = sizeof(uint64_t) * (tile_extents[0] + 1);
    size_t indptr_size = ceil_to_512bytes(indptr_size_unaligned);
    uint64_t *indptr_unaligned = malloc(indptr_size + 511);
    uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);
    
    indptr[0] = 0;
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        indptr[i] = i * tile_extents[1];
    }

    // make input for tilestore using indices and indptr
    uint64_t **coords = malloc(sizeof(uint64_t*) * num_of_dim);         // no need to align
    size_t *coord_sizes = malloc(sizeof(size_t) * num_of_dim);      // no need to align
    coords[0] = indptr;         // the first dimension should be a indptr (row ptr) in CSR 
    coords[1] = indices;        // the second dimension should be a indices (column indices) in CSR
    coord_sizes[0] = indptr_size;
    coord_sizes[1] = indices_size;

    /////////////////////////////////
    // write
    /////////////////////////////////
    res = tilestore_write_sparse_csr(
        arrname, tile_coords, num_of_dim,
        data, data_size_unaligned,
        coords, coord_sizes,
        TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    /////////////////////////////////
    // read
    /////////////////////////////////
    tilestore_tile_metadata_t metadata;
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords, num_of_dim, &metadata);
    assert(res == TILESTORE_OK);

    // prepare memory space
    size_t readdata_size = metadata.data_nbytes;
    int *readdata_unaligned = malloc(ceil_to_512bytes(readdata_size) + 511);
    int *readdata = aligned_512bytes_ptr(readdata_unaligned);

    size_t readindptr_size = metadata.cxs_indptr_nbytes;
    uint64_t *readindptr_unaligned = malloc(ceil_to_512bytes(readindptr_size) + 511);
    uint64_t *readindptr = aligned_512bytes_ptr(readindptr_unaligned);

    size_t readindices_size = metadata.coord_nbytes;
    uint64_t *readindices_unaligned = malloc(ceil_to_512bytes(readindices_size) + 511);
    uint64_t *readindices = aligned_512bytes_ptr(readindices_unaligned);

    uint64_t **readcoords = malloc(sizeof(uint64_t*) * num_of_dim);
    size_t *readcoord_sizes = malloc(sizeof(size_t) * num_of_dim);

    readcoords[0] = readindptr;
    readcoords[1] = readindices;

    readcoord_sizes[0] = readindptr_size;
    readcoord_sizes[1] = readindices_size;

    res = tilestore_read_sparse_csr(
        arrname, metadata,
        readdata, readdata_size, 
        readcoords, readcoord_sizes,
        TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // test
    // data from the read function must be same with second written data
    for (size_t i = 0; i < num_of_elems; i++) {
        assert(readdata[i] == (int) i);
        assert(readcoords[1][i] == ((uint64_t) i % tile_extents[1]) + (tile_extents[1] * tile_coords[1]));
    }
    
    assert(readcoords[0][0] == 0);
    for (size_t i = 1; i < (tile_extents[0] + 1); i++) {
        assert(readcoords[0][i] == (i * tile_extents[1]));
    }

    // free
    free(data_unaligned);
    free(indptr_unaligned);
    free(indices_unaligned);
    free(coords);
    free(coord_sizes);

    free(readdata_unaligned);
    free(readindptr_unaligned);
    free(readindices_unaligned);
    free(readcoords);
    free(readcoord_sizes);

    printf("%s passed\n", __func__);
}


int test_write_sparse_csr_update_existing_tile() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {200, 200};
    uint64_t tile_extents[] = {50, 50};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_SPARSE_CSR);
    assert(res == TILESTORE_OK);

    // write_sparse - normal case
    uint64_t tile_coords[] = {1, 2};

    /////////////////////////////////
    // prepare memory space
    /////////////////////////////////
    // input data should be aligned to 512 byte memory address
    // fill only diagonal
    size_t num_of_elems = 50;
    // data
    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }
    
    // coords
    // indices (column indices)
    size_t indices_size_unaligned = sizeof(uint64_t) * num_of_elems;
    size_t indices_size = ceil_to_512bytes(indices_size_unaligned);
    uint64_t *indices_unaligned = malloc(indices_size + 511);
    uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        indices[i] = i + (tile_extents[1] * tile_coords[1]);
    }

    // indptr (row ptr)
    size_t indptr_size_unaligned = sizeof(uint64_t) * (tile_extents[0] + 1);
    size_t indptr_size = ceil_to_512bytes(indptr_size_unaligned);
    uint64_t *indptr_unaligned = malloc(indptr_size + 511);
    uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);
    
    indptr[0] = 0;
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        indptr[i] = i;
    }

    // make input for tilestore using indices and indptr
    uint64_t **coords = malloc(sizeof(uint64_t*) * num_of_dim);         // no need to align
    size_t *coord_sizes = malloc(sizeof(size_t) * num_of_dim);      // no need to align
    coords[0] = indptr;         // the first dimension should be a indptr (row ptr) in CSR 
    coords[1] = indices;        // the second dimension should be a indices (column indices) in CSR
    coord_sizes[0] = indptr_size;
    coord_sizes[1] = indices_size;

    /////////////////////////////////
    // write
    /////////////////////////////////
    res = tilestore_write_sparse_csr(
        arrname, tile_coords, num_of_dim,
        data, data_size_unaligned,
        coords, coord_sizes,
        TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    /////////////////////////////////
    // clean up memory and malloc again with full sparse tile
    /////////////////////////////////
    free(data_unaligned);
    free(indptr_unaligned);
    free(indices_unaligned);
    free(coords);
    free(coord_sizes);

    // input data should be aligned to 512 byte memory address
    // fill all 
    num_of_elems = tile_extents[0] * tile_extents[1];
    // data
    data_size_unaligned = sizeof(int) * num_of_elems;
    data_size = ceil_to_512bytes(data_size_unaligned);
    data_unaligned = malloc(data_size + 511);
    data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }
    
    // coords
    // indices (column indices)
    indices_size_unaligned = sizeof(uint64_t) * num_of_elems;
    indices_size = ceil_to_512bytes(indices_size_unaligned);
    indices_unaligned = malloc(indices_size + 511);
    indices = aligned_512bytes_ptr(indices_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        indices[i] = (i % tile_extents[1]) + (tile_extents[1] * tile_coords[1]);
    }

    // indptr (row ptr)
    indptr_size_unaligned = sizeof(uint64_t) * (tile_extents[0] + 1);
    indptr_size = ceil_to_512bytes(indptr_size_unaligned);
    indptr_unaligned = malloc(indptr_size + 511);
    indptr = aligned_512bytes_ptr(indptr_unaligned);
    
    indptr[0] = 0;
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        indptr[i] = i * tile_extents[1];
    }

    // make input for tilestore using indices and indptr
    coords = malloc(sizeof(uint64_t*) * num_of_dim);         // no need to align
    coord_sizes = malloc(sizeof(size_t) * num_of_dim);      // no need to align
    coords[0] = indptr;         // the first dimension should be a indptr (row ptr) in CSR 
    coords[1] = indices;        // the second dimension should be a indices (column indices) in CSR
    coord_sizes[0] = indptr_size;
    coord_sizes[1] = indices_size;


    /////////////////////////////////
    // write again
    /////////////////////////////////
    res = tilestore_write_sparse_csr(
        arrname, tile_coords, num_of_dim,
        data, data_size_unaligned,
        coords, coord_sizes,
        TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    /////////////////////////////////
    // read
    /////////////////////////////////
    tilestore_tile_metadata_t metadata;
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords, num_of_dim, &metadata);
    assert(res == TILESTORE_OK);

    // prepare memory space
    size_t readdata_size = metadata.data_nbytes;
    int *readdata_unaligned = malloc(ceil_to_512bytes(readdata_size) + 511);
    int *readdata = aligned_512bytes_ptr(readdata_unaligned);

    size_t readindptr_size = metadata.cxs_indptr_nbytes;
    uint64_t *readindptr_unaligned = malloc(ceil_to_512bytes(readindptr_size) + 511);
    uint64_t *readindptr = aligned_512bytes_ptr(readindptr_unaligned);

    size_t readindices_size = metadata.coord_nbytes;
    uint64_t *readindices_unaligned = malloc(ceil_to_512bytes(readindices_size) + 511);
    uint64_t *readindices = aligned_512bytes_ptr(readindices_unaligned);

    uint64_t **readcoords = malloc(sizeof(uint64_t*) * num_of_dim);
    size_t *readcoord_sizes = malloc(sizeof(size_t) * num_of_dim);

    readcoords[0] = readindptr;
    readcoords[1] = readindices;

    readcoord_sizes[0] = readindptr_size;
    readcoord_sizes[1] = readindices_size;

    res = tilestore_read_sparse_csr(
        arrname, metadata,
        readdata, readdata_size, 
        readcoords, readcoord_sizes,
        TILESTORE_DIRECT);
    assert(res == TILESTORE_OK);

    // test
    // data from the read function must be same with second written data
    for (size_t i = 0; i < num_of_elems; i++) {
        assert(readdata[i] == (int) i);
        assert(readcoords[1][i] == ((uint64_t) i % tile_extents[1]) + (tile_extents[1] * tile_coords[1]));
    }
    
    assert(readcoords[0][0] == 0);
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        assert(readcoords[0][i] == (i * tile_extents[1]));
    }

    // free
    free(data_unaligned);
    free(indptr_unaligned);
    free(indices_unaligned);
    free(coords);
    free(coord_sizes);

    free(readdata_unaligned);
    free(readindptr_unaligned);
    free(readindices_unaligned);
    free(readcoords);
    free(readcoord_sizes);

    printf("%s passed\n", __func__);
}


int test_wrong_write_sparse_csr() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {100, 200};
    uint64_t tile_extents[] = {10, 50};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_SPARSE_CSR);
    assert(res == TILESTORE_OK);

    // write to wrong array
    const char *wrong_arrname = "adsfasfaw";
    uint64_t tile_coords_1[] = {0, 0};
    res = tilestore_write_sparse_csr(wrong_arrname, tile_coords_1, num_of_dim, NULL, 0, NULL, NULL, TILESTORE_DIRECT);
    assert(res == TILESTORE_ARRAY_NOT_EXISTS);

    // write with invalid coords
    uint64_t tile_coords_2[] = {121300, 224200};
    res = tilestore_write_sparse_csr(arrname, tile_coords_2, num_of_dim, NULL, 0, NULL, NULL, TILESTORE_DIRECT);
    assert(res == TILESTORE_INVALID_TILE_COORDS);

    printf("%s passed\n", __func__);
}

int test_wrong_read_sparse_csr() {
    printf("%s started\n", __func__);

    const char *arrname = __func__;

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {100, 200};
    uint64_t tile_extents[] = {10, 50};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_SPARSE_CSR);

    assert(res == TILESTORE_OK);

    // read from array that does not exist
    const char *wrong_arrname = "adsfasfaw";
    uint64_t tile_coords[] = {1, 2};
    res = tilestore_read_metadata_of_tile(
        wrong_arrname, tile_coords, num_of_dim, NULL);
    assert(res == TILESTORE_ARRAY_NOT_EXISTS);

    // read with invalid coords
    uint64_t tile_coords_2[] = {214515, 123124};
    res = tilestore_read_metadata_of_tile(
        arrname, tile_coords_2, num_of_dim, NULL);
    assert(res == TILESTORE_INVALID_TILE_COORDS);

    // read from array that does not exist
    tilestore_tile_metadata_t tm;
    res = tilestore_read_sparse_csr(wrong_arrname, tm, NULL, 0, NULL, NULL, TILESTORE_DIRECT);
    assert(res == TILESTORE_OPEN_FAILED);

    printf("%s passed\n", __func__);
}

int perf_test_dense_write() {
    const char *arrname = "perf_test";

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {50000, 50000};
    uint64_t tile_extents[] = {500, 500};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);
    
    // write to the array
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);
    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);

    // write
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            uint64_t tile_coords[] = {i, j};

            // test
            // printf("%d\n", (int) (i * 5 + j) * 1000);
            // for (size_t k = 0; k < num_of_elems; k++) {
            //     data[k] = (int) (i * 5 + j) * 1000 + (k % 1000);
            // }
            
            tilestore_write_dense(
                arrname, tile_coords, num_of_dim,
                data, data_size_unaligned, TILESTORE_NORMAL);
        
            sync();
        }
    }

    // sync();

    gettimeofday(&end, NULL);
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
    fprintf(stderr, "%llu\n", diff);
}

int perf_test_dense_read() {
    const char *arrname = "perf_test";

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {50000, 50000};
    uint64_t tile_extents[] = {500, 500};
    
    // write to the array
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);
    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);

    struct timeval start, end;
    gettimeofday(&start, NULL);

    // read
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            uint64_t tile_coords[] = {i, j};
            // read
            // read has two phase
            // first, read metadata of tile
            tilestore_tile_metadata_t metadata;
            tilestore_read_metadata_of_tile(
                arrname, tile_coords, num_of_dim, &metadata);

            // second, read tile data from data file
            tilestore_read_dense(
                arrname, metadata, data, data_size_unaligned, TILESTORE_DIRECT);

            // test
            // printf("%d\n", (int) (i * 5 + j) * 1000);
            // for (size_t k = 0; k < num_of_elems; k++) {
            //     assert(data[k] == (int) (i * 5 + j) * 1000 + (k % 1000));   
            // }
        }
    }

    // sync();

    gettimeofday(&end, NULL);
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
    fprintf(stderr, "%llu\n", diff);
}


int perf_test_sparse_write() {
    const char *arrname = "perf_test";

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {50000, 50000};
    uint64_t tile_extents[] = {500, 500};

    // create array
    int res = tilestore_create_array(
        arrname,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_SPARSE_CSR);
    
    // write to the array
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);

    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (int) i;
    }

    size_t indptr_size_unaligned = sizeof(uint64_t) * (tile_extents[0] + 1);
    size_t indptr_size = ceil_to_512bytes(indptr_size_unaligned);
    uint64_t *indptr_unaligned = malloc(indptr_size + 511);
    uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);
    indptr[0] = 0;
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        indptr[i] = i * tile_extents[1];
    }
    
    size_t indices_size_unaligned = sizeof(uint64_t) * num_of_elems;
    size_t indices_size = ceil_to_512bytes(indices_size_unaligned);
    uint64_t *indices_unaligned = malloc(indices_size + 511);
    uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        indices[i] = i % tile_extents[1];
    }

    uint64_t *coords[2] = {indptr, indices};
    uint64_t coord_sizes[2] = {indptr_size_unaligned, indices_size_unaligned};

    struct timeval start, end;
    gettimeofday(&start, NULL);

    // write
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            uint64_t tile_coords[] = {i, j};

            // test
            // printf("%d\n", (int) (i * 5 + j) * 1000);
            // for (size_t k = 0; k < num_of_elems; k++) {
            //     data[k] = (int) (i * 5 + j) * 1000 + (k % 1000);
            //     indices[k] = (uint64_t) (j * tile_extents[1]) + (k % tile_extents[1]);
            // }
            
            tilestore_write_sparse_csr(
                arrname, tile_coords, num_of_dim, 
                data, data_size_unaligned, 
                coords, coord_sizes, TILESTORE_DIRECT);
        
            // sync();
        }
    }

    // sync();

    gettimeofday(&end, NULL);
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
    fprintf(stderr, "%llu\n", diff);
}

int perf_test_sparse_read() {
    const char *arrname = "perf_test";

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {50000, 50000};
    uint64_t tile_extents[] = {500, 500};

    // write to the array
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);

    size_t data_size_unaligned = sizeof(int) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    int *data_unaligned = malloc(data_size + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = 0;
    }

    size_t indptr_size_unaligned = sizeof(uint64_t) * (tile_extents[0] + 1);
    size_t indptr_size = ceil_to_512bytes(indptr_size_unaligned);
    uint64_t *indptr_unaligned = malloc(indptr_size + 511);
    uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);
    indptr[0] = 0;
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        indptr[i] = 0;
    }
    
    size_t indices_size_unaligned = sizeof(uint64_t) * num_of_elems;
    size_t indices_size = ceil_to_512bytes(indices_size_unaligned);
    uint64_t *indices_unaligned = malloc(indices_size + 511);
    uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        indices[i] = 0;
    }

    uint64_t *coords[2] = {indptr, indices};
    uint64_t coord_sizes[2] = {indptr_size_unaligned, indices_size_unaligned};

    struct timeval start, end;
    gettimeofday(&start, NULL);

    // read
    for (uint32_t i = 0; i < 100; i++) {
        for (uint32_t j = 0; j < 100; j++) {
            uint64_t tile_coords[] = {i, j};
            // read
            // read has two phase
            // first, read metadata of tile
            tilestore_tile_metadata_t metadata;
            tilestore_read_metadata_of_tile(
                arrname, tile_coords, num_of_dim, &metadata);

            // second, read tile data from data file
            tilestore_read_sparse_csr(
                arrname, metadata, 
                data, data_size_unaligned, 
                coords, coord_sizes, TILESTORE_DIRECT);

            // test
            // for (size_t k = 0; k < num_of_elems; k++) {
            //     assert(data[k] == (int) (i * 5 + j) * 1000 + (k % 1000));
            //     assert(coords[1][k] == ((j * tile_extents[1]) + (k % tile_extents[1])));   
            //     // printf("%lu,%d\n", coords[1][k], data[k]);
            // }
            // for (size_t k = 0; k < (tile_extents[0] + 1); k++) {
            //     assert(coords[0][k] == (k * tile_extents[1])); 
            //     // printf("%lu\n", coords[0][k]);
            // }
        }
    }

    // sync();

    gettimeofday(&end, NULL);
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
    fprintf(stderr, "%llu\n", diff);
}

void perf_test(int split) {
    int open_flag = O_RDWR | O_DIRECT;
    int fd = open("test_data", open_flag, 0644);
    printf("%d\n", fd);

    size_t data_size_unaligned = 1024 * 1024 * 1024;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    void *data_unaligned = malloc(data_size + 511);
    void *data = aligned_512bytes_ptr(data_unaligned);

    // int split = 256;
    
    struct timeval start, end;
    gettimeofday(&start, NULL);

    for (int i = 0; i < split; i++) {
        size_t chunk_size = data_size / split;
        size_t domain_high = 16 * split;
        int idx = (rand() % domain_high);
        int res = pread(fd, data, chunk_size, chunk_size * idx);
        printf("chunk_size=%lu,domain_high=%lu,idx=%d,res=%d\n", chunk_size, domain_high, idx, res);
    }

    gettimeofday(&end, NULL);
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
    fprintf(stderr, "%llu\n", diff);
    
    close(fd);

    return;

    // int open_flag = O_RDWR | O_CREAT | O_EXCL;
    // // if (iomode == TILESTORE_DIRECT) open_flag |= O_DIRECT;
    // int fd = open("test_data", open_flag, 0644);
    // printf("%d\n", fd);

    // size_t data_size = 1024 * 1024 * 1024;
    // void *data = malloc(data_size);
    
    // for (int i = 0; i < 16; i++) {
    //     pwrite(fd, data, data_size, data_size * i);
    // }
    // close(fd);
}

int main(int argv, char* argc[]) { 
    // perf_test(atoi(argc[1]));
    // return 0;

    // test_create_array_and_get_schema();

    // test_write_dense_and_read_dense();
    // test_write_dense_update_existing_tile();
    // test_wrong_write_dense();
    // test_wrong_read_dense();

    // test_read_empty_tile();

    // test_write_sparse_csr_and_read_sparse_csr();
    // test_write_sparse_csr_update_existing_tile();
    // test_wrong_write_sparse_csr();
    // test_wrong_read_sparse_csr();

    // perf_test_dense_write();
    // perf_test_dense_read();

    // perf_test_sparse_write();
    // perf_test_sparse_read();

    return 0;
}
