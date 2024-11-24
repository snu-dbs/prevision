#include <stdio.h>
#include <math.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>

#include "bf.h"
#include "chunk_interface.h"
#include "array_struct.h"
#include "exec_interface.h"
#include "node_interface.h"
#include "planner.h"

void test_transpose_csr_write_data(char *arrname1) {
    /******************************
     prepare tile data below

     data(dense) = [[100, 0, 101, 102, 0, 200, 0, 0],
        [0, 103, 0, 0, 201, 0, 202, 203],
        [0, 0, 104, 105, 204, 205, 0, 0],
        [0, 0, 0, 106, 206, 207, 0, 0],
        [300, 0, 0, 0, 0, 0, 0, 400],
        [0, 301, 0, 0, 0, 0, 401, 0],
        [0, 0, 302, 0, 0, 402, 0, 0],
        [0, 0, 0, 303, 403, 0, 0, 0]]

    ******************************/

    // {0, 0}
    // data
    uint64_t data_0_size_unaligned = sizeof(double) * 7;
    uint64_t data_0_size = ceil_to_512bytes(data_0_size_unaligned);
    double *data_0_unaligned = malloc(data_0_size + 511);
    double *data_0 = aligned_512bytes_ptr(data_0_unaligned);
    // indptr
    uint64_t indptr_0_size_unaligned = sizeof(uint64_t) * 5;
    uint64_t indptr_0_size = ceil_to_512bytes(indptr_0_size_unaligned);
    uint64_t *indptr_0_unaligned = malloc(indptr_0_size + 511);
    uint64_t *indptr_0 = aligned_512bytes_ptr(indptr_0_unaligned);
    // indices
    uint64_t indices_0_size_unaligned = sizeof(uint64_t) * 7;
    uint64_t indices_0_size = ceil_to_512bytes(indices_0_size_unaligned);
    uint64_t *indices_0_unaligned = malloc(indices_0_size + 511);
    uint64_t *indices_0 = aligned_512bytes_ptr(indices_0_unaligned);
    // coords
    uint64_t **coords_0 = malloc(sizeof(uint64_t*) * 2);
    uint64_t coord_sizes_0[2] = {indptr_0_size_unaligned, indices_0_size_unaligned};
    coords_0[0] = indptr_0;
    coords_0[1] = indices_0;
    
    // filling
    for (int i = 0; i < 7; i++) {
        data_0[i] = (double) (100 + i);
    }
    indices_0[0] = 0;
    indices_0[1] = 2;
    indices_0[2] = 3;
    indices_0[3] = 1;
    indices_0[4] = 2;
    indices_0[5] = 3;
    indices_0[6] = 3;

    indptr_0[0] = 0;
    indptr_0[1] = 3;
    indptr_0[2] = 4;
    indptr_0[3] = 6;
    indptr_0[4] = 7;


    // {0, 1}
    // data
    uint64_t data_1_size_unaligned = sizeof(double) * 8;
    uint64_t data_1_size = ceil_to_512bytes(data_1_size_unaligned);
    double *data_1_unaligned = malloc(data_1_size + 511);
    double *data_1 = aligned_512bytes_ptr(data_1_unaligned);
    // indptr
    uint64_t indptr_1_size_unaligned = sizeof(uint64_t) * 5;
    uint64_t indptr_1_size = ceil_to_512bytes(indptr_1_size_unaligned);
    uint64_t *indptr_1_unaligned = malloc(indptr_1_size + 511);
    uint64_t *indptr_1 = aligned_512bytes_ptr(indptr_1_unaligned);
    // indices
    uint64_t indices_1_size_unaligned = sizeof(uint64_t) * 8;
    uint64_t indices_1_size = ceil_to_512bytes(indices_1_size_unaligned);
    uint64_t *indices_1_unaligned = malloc(indices_1_size + 511);
    uint64_t *indices_1 = aligned_512bytes_ptr(indices_1_unaligned);
    // coords
    uint64_t **coords_1 = malloc(sizeof(uint64_t*) * 2);
    uint64_t coord_sizes_1[2] = {indptr_1_size_unaligned, indices_1_size_unaligned};
    coords_1[0] = indptr_1;
    coords_1[1] = indices_1;
    
    // filling
    for (int i = 0; i < 8; i++) {
        data_1[i] = (double) (200 + i);
    }
    indices_1[0] = 1;
    indices_1[1] = 0;
    indices_1[2] = 2;
    indices_1[3] = 3;
    indices_1[4] = 0;
    indices_1[5] = 1;
    indices_1[6] = 0;
    indices_1[7] = 1;

    indptr_1[0] = 0;
    indptr_1[1] = 1;
    indptr_1[2] = 4;
    indptr_1[3] = 6;
    indptr_1[4] = 8;

    // {1, 0}
    // data
    uint64_t data_2_size_unaligned = sizeof(double) * 4;
    uint64_t data_2_size = ceil_to_512bytes(data_2_size_unaligned);
    double *data_2_unaligned = malloc(data_2_size + 511);
    double *data_2 = aligned_512bytes_ptr(data_2_unaligned);
    // indptr
    uint64_t indptr_2_size_unaligned = sizeof(uint64_t) * 5;
    uint64_t indptr_2_size = ceil_to_512bytes(indptr_2_size_unaligned);
    uint64_t *indptr_2_unaligned = malloc(indptr_2_size + 511);
    uint64_t *indptr_2 = aligned_512bytes_ptr(indptr_2_unaligned);
    // indices
    uint64_t indices_2_size_unaligned = sizeof(uint64_t) * 4;
    uint64_t indices_2_size = ceil_to_512bytes(indices_2_size_unaligned);
    uint64_t *indices_2_unaligned = malloc(indices_2_size + 511);
    uint64_t *indices_2 = aligned_512bytes_ptr(indices_2_unaligned);
    // coords
    uint64_t **coords_2 = malloc(sizeof(uint64_t*) * 2);
    uint64_t coord_sizes_2[2] = {indptr_2_size_unaligned, indices_2_size_unaligned};
    coords_2[0] = indptr_2;
    coords_2[1] = indices_2;
    
    // filling
    for (int i = 0; i < 4; i++) {
        data_2[i] = (double) (300 + i);
    }
    indices_2[0] = 0;
    indices_2[1] = 1;
    indices_2[2] = 2;
    indices_2[3] = 3;

    indptr_2[0] = 0;
    indptr_2[1] = 1;
    indptr_2[2] = 2;
    indptr_2[3] = 3;
    indptr_2[4] = 4;

    // {1, 1}
    // data
    uint64_t data_3_size_unaligned = sizeof(double) * 4;
    uint64_t data_3_size = ceil_to_512bytes(data_3_size_unaligned);
    double *data_3_unaligned = malloc(data_3_size + 511);
    double *data_3 = aligned_512bytes_ptr(data_3_unaligned);
    // indptr
    uint64_t indptr_3_size_unaligned = sizeof(uint64_t) * 5;
    uint64_t indptr_3_size = ceil_to_512bytes(indptr_3_size_unaligned);
    uint64_t *indptr_3_unaligned = malloc(indptr_3_size + 511);
    uint64_t *indptr_3 = aligned_512bytes_ptr(indptr_3_unaligned);
    // indices
    uint64_t indices_3_size_unaligned = sizeof(uint64_t) * 4;
    uint64_t indices_3_size = ceil_to_512bytes(indices_3_size_unaligned);
    uint64_t *indices_3_unaligned = malloc(indices_3_size + 511);
    uint64_t *indices_3 = aligned_512bytes_ptr(indices_3_unaligned);
    // coords
    uint64_t **coords_3 = malloc(sizeof(uint64_t*) * 2);
    uint64_t coord_sizes_3[2] = {indptr_3_size_unaligned, indices_3_size_unaligned};
    coords_3[0] = indptr_3;
    coords_3[1] = indices_3;
    
    // filling
    for (int i = 0; i < 4; i++) {
        data_3[i] = (double) (400 + i);
    }
    indices_3[0] = 3;
    indices_3[1] = 2;
    indices_3[2] = 1;
    indices_3[3] = 0;

    indptr_3[0] = 0;
    indptr_3[1] = 1;
    indptr_3[2] = 2;
    indptr_3[3] = 3;
    indptr_3[4] = 4;

    int res;
    uint64_t tile_coords[] = {0, 0};
    
    // write
    res = tilestore_write_sparse_csr(
        arrname1, tile_coords, 2,
        data_0, data_0_size_unaligned, 
        coords_0, coord_sizes_0, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);

    tile_coords[0] = 0;
    tile_coords[1] = 1;
    res = tilestore_write_sparse_csr(
        arrname1, tile_coords, 2,
        data_1, data_1_size_unaligned, 
        coords_1, coord_sizes_1, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);

    tile_coords[0] = 1;
    tile_coords[1] = 0;
    res = tilestore_write_sparse_csr(
        arrname1, tile_coords, 2,
        data_2, data_2_size_unaligned, 
        coords_2, coord_sizes_2, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);

    tile_coords[0] = 1;
    tile_coords[1] = 1;
    res = tilestore_write_sparse_csr(
        arrname1, tile_coords, 2,
        data_3, data_3_size_unaligned, 
        coords_3, coord_sizes_3, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);


    // free
    free(data_0_unaligned);
    free(indices_0_unaligned);
    free(indptr_0_unaligned);
    free(coords_0);
    
    free(data_1_unaligned);
    free(indices_1_unaligned);
    free(indptr_1_unaligned);
    free(coords_1);

    free(data_2_unaligned);
    free(indices_2_unaligned);
    free(indptr_2_unaligned);
    free(coords_2);

    free(data_3_unaligned);
    free(indices_3_unaligned);
    free(indptr_3_unaligned);
    free(coords_3);
}


void test_transpose_dense_write_data(char *arrname1) {
    /******************************
     prepare tile data below

        data = [[100.0, 101.0, 102.0, 103.0, 200.0, 201.0, 202.0, 203.0],
        [104.0, 105.0, 106.0, 107.0, 204.0, 205.0, 206.0, 207.0, ],
        [108.0, 109.0, 110.0, 111.0, 208.0, 209.0, 210.0, 211.0, ],
        [112.0, 113.0, 114.0, 115.0, 212.0, 213.0, 214.0, 215.0, ],
        [300.0, 301.0, 302.0, 303.0, 400.0, 401.0, 402.0, 403.0, ],
        [304.0, 305.0, 306.0, 307.0, 404.0, 405.0, 406.0, 407.0, ],
        [308.0, 309.0, 310.0, 311.0, 408.0, 409.0, 410.0, 411.0, ],
        [312.0, 313.0, 314.0, 315.0, 412.0, 413.0, 414.0, 415.0, ]]

    ******************************/


    // {0, 0}
    // data
    uint64_t data_0_size_unaligned = sizeof(double) * 16;
    uint64_t data_0_size = ceil_to_512bytes(data_0_size_unaligned);
    double *data_0_unaligned = malloc(data_0_size + 511);
    double *data_0 = aligned_512bytes_ptr(data_0_unaligned);

    // {0, 1}
    // data
    uint64_t data_1_size_unaligned = sizeof(double) * 16;
    uint64_t data_1_size = ceil_to_512bytes(data_1_size_unaligned);
    double *data_1_unaligned = malloc(data_1_size + 511);
    double *data_1 = aligned_512bytes_ptr(data_1_unaligned);
  

    // {1, 0}
    // data
    uint64_t data_2_size_unaligned = sizeof(double) * 16;
    uint64_t data_2_size = ceil_to_512bytes(data_2_size_unaligned);
    double *data_2_unaligned = malloc(data_2_size + 511);
    double *data_2 = aligned_512bytes_ptr(data_2_unaligned);
   
    // {1, 1}   
    // data
    uint64_t data_3_size_unaligned = sizeof(double) * 16;
    uint64_t data_3_size = ceil_to_512bytes(data_3_size_unaligned);
    double *data_3_unaligned = malloc(data_3_size + 511);
    double *data_3 = aligned_512bytes_ptr(data_3_unaligned);

    // filling
    for (int i = 0; i < 16; i++) {
        data_0[i] = (double) (100 + i);
        data_1[i] = (double) (200 + i);
        data_2[i] = (double) (300 + i);
        data_3[i] = (double) (400 + i);
    }

    int res;
    uint64_t tile_coords[] = {0, 0};
    
    // write
    res = tilestore_write_dense(
        arrname1, tile_coords, 2,
        data_0, data_0_size_unaligned, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);

    tile_coords[0] = 0;
    tile_coords[1] = 1;
    res = tilestore_write_dense(
        arrname1, tile_coords, 2,
        data_1, data_1_size_unaligned, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);

    tile_coords[0] = 1;
    tile_coords[1] = 0;
    res = tilestore_write_dense(
        arrname1, tile_coords, 2,
        data_2, data_2_size_unaligned, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);

    tile_coords[0] = 1;
    tile_coords[1] = 1;
    res = tilestore_write_dense(
        arrname1, tile_coords, 2,
        data_3, data_3_size_unaligned, TILESTORE_NORMAL);
    assert(res == TILESTORE_OK);


    // free
    free(data_0_unaligned);
    free(data_1_unaligned);
    free(data_2_unaligned);
    free(data_3_unaligned);
}

void test_print_dense(char *arrname) {
    double *data = malloc(8 * 8 * sizeof(double));
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < 2; j++) {
            uint64_t tile_coords[] = {(uint64_t) i, (uint64_t) j};
            tilestore_tile_metadata_t tile_meta;

            tilestore_errcode_t res = tilestore_read_metadata_of_tile(
                arrname, tile_coords, 2, &tile_meta);
            assert(res == TILESTORE_OK);

            double *tile_data = malloc(tile_meta.data_nbytes * sizeof(double) + 511);
            res = tilestore_read_dense(
                arrname, tile_meta, 
                tile_data, tile_meta.data_nbytes, 
                TILESTORE_NORMAL);
            assert(res == TILESTORE_OK);

            for (int k = 0; k < 16; k++) {
                int sub_i = k / 4;
                int sub_j = k % 4;
                int dest = (8 * (4 * i + sub_i)) + (4 * j) + sub_j;
                data[dest] = tile_data[k];
            }

            free(tile_data);
        }
    }

    for (int i = 0; i < 8; i++) {
        for (int j = 0; j < 8; j++) {
            printf("%lf ", data[i * 8 + j]);
        }
        printf("\n");
    }

    free(data);
}


void test_print_sparse(char *arrname) {
    // double *data = malloc(8 * 8 * sizeof(double));
    double *data = calloc(8 * 8, sizeof(double));
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < 2; j++) {
            uint64_t tile_coords[] = {(uint64_t) i, (uint64_t) j};
            tilestore_tile_metadata_t tile_meta;

            tilestore_errcode_t res = tilestore_read_metadata_of_tile(
                arrname, tile_coords, 2, &tile_meta);
            assert(res == TILESTORE_OK);

            double *tile_data = malloc(tile_meta.data_nbytes * sizeof(double) + 511);
            uint64_t *indptr = malloc(tile_meta.cxs_indptr_nbytes + 511);
            uint64_t *indices = malloc(tile_meta.coord_nbytes + 511);

            uint64_t **req_coords = malloc(sizeof(uint64_t*) * 2);
            uint64_t *coord_sizes = malloc(sizeof(uint64_t) * 2);

            req_coords[0] = indptr;
            req_coords[1] = indices;

            coord_sizes[0] = tile_meta.cxs_indptr_nbytes;
            coord_sizes[1] = tile_meta.coord_nbytes;

            // query
            tilestore_read_sparse_csr(
                    arrname, tile_meta, 
                    tile_data, tile_meta.data_nbytes, 
                    req_coords, coord_sizes,
                    TILESTORE_NORMAL);
            assert(res == TILESTORE_OK);

            for (int sub_i = 0; sub_i < 4; sub_i++) {
                for (int l = indptr[sub_i]; l < indptr[sub_i + 1]; l++) {
                    int sub_j = indices[l];
                    int dest = (8 * (4 * i + sub_i)) + (4 * j) + sub_j;
                    data[dest] = tile_data[l];
                }
            }

            free(tile_data);
            free(coord_sizes);
            free(req_coords);
            free(indices);
            free(indptr);
        }
    }

    for (int i = 0; i < 8; i++) {
        for (int j = 0; j < 8; j++) {
            printf("%lf ", data[i * 8 + j]);
        }
        printf("\n");
    }

    free(data);
}

void test_bypass_a_m_b_1_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_1_a";
    char *array_name_2 = "__test_bypass_a_m_b_1_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    // uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *out = matmul(a, b);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[ 10000  60200  20604  31617  41616  61819  80200      0]
        [ 60300  10609  61004  61509 102512  81204  20806 101309]
        [ 61200  61705  10816  22050  42846  43055  82205  81600]
        [ 61800  62307      0  11236  21836  21942  83007  82400]
        [ 30000      0  30300 151800 161200  60000      0      0]
        [     0  31003 121102      0  60501 161202  60802  61103]
        [     0 121002  31408  31710  61608  61910 161202      0]
        [120900      0      0  32118  62418  62721      0 161200]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_2_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_2_a";
    char *array_name_2 = "__test_bypass_a_m_b_2_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *at = transpose(a, dim_order);
    Array *b = scan(array_name_2);
    Array *out = matmul(at, b);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[100000      0  10100  10200      0  20000      0 120000]
        [     0 101210      0      0  20703      0 141507  20909]
        [ 10100      0 112221  21222  21216 162924      0      0]
        [ 10200      0  21222 124474 165365  63867      0      0]
        [     0  20703  21216 165365 286862  84462  40602  40803]
        [ 20000      0 162924  63867  84462 286478      0      0]
        [     0 141507      0      0  40602      0 201605  41006]
        [120000  20909      0      0  40803      0  41006 201209]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_3_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_3_a";
    char *array_name_2 = "__test_bypass_a_m_b_3_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *bt = transpose(b, dim_order);
    Array *out = matmul(a, bt);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[ 70605      0  62214  52212  30000      0 110902  30906]
        [     0 133023  41004  41406  81200 112005      0  81003]
        [ 62214  41004 105482  95589      0      0 113818 114027]
        [ 52212  41406  95589  96521      0      0  83214 115136]
        [ 30000  81200      0      0 250000      0      0      0]
        [     0 112005      0      0      0 251402      0      0]
        [110902      0 113818  83214      0      0 252808      0]
        [ 30906  81003 114027 115136      0      0      0 254218]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_4_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_4_a";
    char *array_name_2 = "__test_bypass_a_m_b_4_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *at = transpose(a, dim_order);
    Array *b = scan(array_name_2);
    Array *bt = transpose(b, dim_order);
    Array *out = matmul(at, bt);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[ 10000  60300  61200  61800  30000      0      0 120900]
        [ 60200  10609  61705  62307      0  31003 121002      0]
        [ 20604  61004  10816      0  30300 121102  31408      0]
        [ 31617  61509  22050  11236 151800      0  31710  32118]
        [ 41616 102512  42846  21836 161200  60501  61608  62418]
        [ 61819  81204  43055  21942  60000 161202  61910  62721]
        [ 80200  20806  82205  83007      0  60802 161202      0]
        [     0 101309  81600  82400      0  61103      0 161200]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}

void test_bypass_a_m_b_1()
{
    char *array_name_1 = "__test_bypass_a_m_b_1_a";
    char *array_name_2 = "__test_bypass_a_m_b_1_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    // uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *out = matmul(a, b);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[289712. 290924. 292136. 293348. 410912. 412124. 413336. 414548.]
        [296304. 297548. 298792. 300036. 420704. 421948. 423192. 424436.]
        [302896. 304172. 305448. 306724. 430496. 431772. 433048. 434324.]
        [309488. 310796. 312104. 313412. 440288. 441596. 442904. 444212.]
        [619312. 622124. 624936. 627748. 900512. 903324. 906136. 908948.]
        [625904. 628748. 631592. 634436. 910304. 913148. 915992. 918836.]
        [632496. 635372. 638248. 641124. 920096. 922972. 925848. 928724.]
        [639088. 641996. 644904. 647812. 929888. 932796. 935704. 938612.]]
     */

    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_2()
{
    char *array_name_1 = "__test_bypass_a_m_b_2_a";
    char *array_name_2 = "__test_bypass_a_m_b_2_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *at = transpose(a, dim_order);
    Array *b = scan(array_name_2);
    Array *out = matmul(at, b);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[419648. 421296. 422944. 424592. 584448. 586096. 587744. 589392.]
        [421296. 422952. 424608. 426264. 586896. 588552. 590208. 591864.]
        [422944. 424608. 426272. 427936. 589344. 591008. 592672. 594336.]
        [424592. 426264. 427936. 429608. 591792. 593464. 595136. 596808.]
        [584448. 586896. 589344. 591792. 829248. 831696. 834144. 836592.]
        [586096. 588552. 591008. 593464. 831696. 834152. 836608. 839064.]
        [587744. 590208. 592672. 595136. 834144. 836608. 839072. 841536.]
        [589392. 591864. 594336. 596808. 836592. 839064. 841536. 844008.]]
     */

    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_3()
{
    char *array_name_1 = "__test_bypass_a_m_b_3_a";
    char *array_name_2 = "__test_bypass_a_m_b_3_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *bt = transpose(b, dim_order);
    Array *out = matmul(a, bt);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[ 203628.  208476.  213324.  218172.  446028.  450876.  455724.  460572.]
        [ 208476.  213452.  218428.  223404.  457276.  462252.  467228.  472204.]
        [ 213324.  218428.  223532.  228636.  468524.  473628.  478732.  483836.]
        [ 218172.  223404.  228636.  233868.  479772.  485004.  490236.  495468.]
        [ 446028.  457276.  468524.  479772. 1008428. 1019676. 1030924. 1042172.]
        [ 450876.  462252.  473628.  485004. 1019676. 1031052. 1042428. 1053804.]
        [ 455724.  467228.  478732.  490236. 1030924. 1042428. 1053932. 1065436.]
        [ 460572.  472204.  483836.  495468. 1042172. 1053804. 1065436. 1077068.]]
     */
    
    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_4()
{
    char *array_name_1 = "__test_bypass_a_m_b_4_a";
    char *array_name_2 = "__test_bypass_a_m_b_4_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *at = transpose(a, dim_order);
    Array *b = scan(array_name_2);
    Array *bt = transpose(b, dim_order);
    Array *out = matmul(at, bt);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[289712. 296304. 302896. 309488. 619312. 625904. 632496. 639088.]
        [290924. 297548. 304172. 310796. 622124. 628748. 635372. 641996.]
        [292136. 298792. 305448. 312104. 624936. 631592. 638248. 644904.]
        [293348. 300036. 306724. 313412. 627748. 634436. 641124. 647812.]
        [410912. 420704. 430496. 440288. 900512. 910304. 920096. 929888.]
        [412124. 421948. 431772. 441596. 903324. 913148. 922972. 932796.]
        [413336. 423192. 433048. 442904. 906136. 915992. 925848. 935704.]
        [414548. 424436. 434324. 444212. 908948. 918836. 928724. 938612.]]
     */

    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}

void test_bypass_a_m_b() {
    // dense
    test_bypass_a_m_b_1();          // A @ B
    test_bypass_a_m_b_2();          // A^T @ B
    test_bypass_a_m_b_3();          // A @ B^T
    test_bypass_a_m_b_4();          // A^T @ B^T

    // sparse
    test_bypass_a_m_b_1_sparse();   // A @ B
    test_bypass_a_m_b_2_sparse();   // A^T @ B
    test_bypass_a_m_b_3_sparse();   // A @ B^T
    test_bypass_a_m_b_4_sparse();   // A^T @ B^T
}


void test_bypass_a_m_b_p_1_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_1_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_1_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    // uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *out = elemwise_op(matmul(a, b), a, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[ 10100  60200  20705  31719  41616  62019  80200      0]
        [ 60300  10712  61004  61509 102713  81204  21008 101512]
        [ 61200  61705  10920  22155  43050  43260  82205  81600]
        [ 61800  62307      0  11342  22042  22149  83007  82400]
        [ 30300      0  30300 151800 161200  60000      0    400]
        [     0  31304 121102      0  60501 161202  61203  61103]
        [     0 121002  31710  31710  61608  62312 161202      0]
        [120900      0      0  32421  62821  62721      0 161200]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_p_2_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_2_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_2_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *out = elemwise_op(matmul(a, b), b, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[ 10100  60200  20705  31719  41616  62019  80200      0]
        [ 60300  10712  61004  61509 102713  81204  21008 101512]
        [ 61200  61705  10920  22155  43050  43260  82205  81600]
        [ 61800  62307      0  11342  22042  22149  83007  82400]
        [ 30300      0  30300 151800 161200  60000      0    400]
        [     0  31304 121102      0  60501 161202  61203  61103]
        [     0 121002  31710  31710  61608  62312 161202      0]
        [120900      0      0  32421  62821  62721      0 161200]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_p_3_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_3_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_3_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *at = transpose(a, dim_order);
    Array *out = elemwise_op(matmul(at, b), at, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[100100      0  10100  10200    300  20000      0 120000]
        [     0 101313      0      0  20703    301 141507  20909]
        [ 10201      0 112325  21222  21216 162924    302      0]
        [ 10302      0  21327 124580 165365  63867      0    303]
        [     0  20904  21420 165571 286862  84462  40602  41206]
        [ 20200      0 163129  64074  84462 286478    402      0]
        [     0 141709      0      0  40602    401 201605  41006]
        [120000  21112      0      0  41203      0  41006 201209]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_p_4_sparse()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_4_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_4_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_csr_write_data(array_name_1);
    test_transpose_csr_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *bt = transpose(b, dim_order);
    Array *out = elemwise_op(matmul(a, bt), bt, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer


    /* expecting result 
     *
     * [[ 70705      0  62214  52212  30300      0 110902  30906]
        [     0 133126  41004  41406  81200 112306      0  81003]
        [ 62315  41004 105586  95589      0      0 114120 114027]
        [ 52314  41406  95694  96627      0      0  83214 115439]
        [ 30000  81401    204    206 250000      0      0    403]
        [   200 112005    205    207      0 251402    402      0]
        [110902    202 113818  83214      0    401 252808      0]
        [ 30906  81206 114027 115136    400      0      0 254218]]
     */

    test_print_sparse(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}

void test_bypass_a_m_b_p_1()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_1_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_1_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    // uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *out = elemwise_op(matmul(a, b), a, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[289812. 291025. 292238. 293451. 411112. 412325. 413538. 414751.]
        [296408. 297653. 298898. 300143. 420908. 422153. 423398. 424643.]
        [303004. 304281. 305558. 306835. 430704. 431981. 433258. 434535.]
        [309600. 310909. 312218. 313527. 440500. 441809. 443118. 444427.]
        [619612. 622425. 625238. 628051. 900912. 903725. 906538. 909351.]
        [626208. 629053. 631898. 634743. 910708. 913553. 916398. 919243.]
        [632804. 635681. 638558. 641435. 920504. 923381. 926258. 929135.]
        [639400. 642309. 645218. 648127. 930300. 933209. 936118. 939027.]]
     */

    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_p_2()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_2_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_2_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *out = elemwise_op(matmul(a, b), b, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[289812. 291025. 292238. 293451. 411112. 412325. 413538. 414751.]
        [296408. 297653. 298898. 300143. 420908. 422153. 423398. 424643.]
        [303004. 304281. 305558. 306835. 430704. 431981. 433258. 434535.]
        [309600. 310909. 312218. 313527. 440500. 441809. 443118. 444427.]
        [619612. 622425. 625238. 628051. 900912. 903725. 906538. 909351.]
        [626208. 629053. 631898. 634743. 910708. 913553. 916398. 919243.]
        [632804. 635681. 638558. 641435. 920504. 923381. 926258. 929135.]
        [639400. 642309. 645218. 648127. 930300. 933209. 936118. 939027.]]
     */

    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_p_3()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_3_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_3_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *at = transpose(a, dim_order);
    Array *out = elemwise_op(matmul(at, b), at, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[419748. 421400. 423052. 424704. 584748. 586400. 588052. 589704.]
        [421397. 423057. 424717. 426377. 587197. 588857. 590517. 592177.]
        [423046. 424714. 426382. 428050. 589646. 591314. 592982. 594650.]
        [424695. 426371. 428047. 429723. 592095. 593771. 595447. 597123.]
        [584648. 587100. 589552. 592004. 829648. 832100. 834552. 837004.]
        [586297. 588757. 591217. 593677. 832097. 834557. 837017. 839477.]
        [587946. 590414. 592882. 595350. 834546. 837014. 839482. 841950.]
        [589595. 592071. 594547. 597023. 836995. 839471. 841947. 844423.]]
     */
    
    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}


void test_bypass_a_m_b_p_4()
{
    char *array_name_1 = "__test_bypass_a_m_b_p_4_a";
    char *array_name_2 = "__test_bypass_a_m_b_p_4_b";
    printf("%s started\n", __func__);

    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};

    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_FLOAT64, 0);

    test_transpose_dense_write_data(array_name_1);
    test_transpose_dense_write_data(array_name_2);


    /*******************************
     * Test 
     *******************************/
    uint32_t dim_order[2] = {1, 0};

    BF_Init();   // Init BF Layer
    BF_Attach(); // Attach to BF Layer

    // create dummy array structures
    Array *a = scan(array_name_1);
    Array *b = scan(array_name_2);
    Array *bt = transpose(b, dim_order);
    Array *out = elemwise_op(matmul(a, bt), bt, OP_ADD);
    
    out = execute(out);

    BF_Detach(); // Detach from BF Layer
    BF_Free();   // Free BF Layer

    /* expecting result 
     *
     * [[ 203728.  208580.  213432.  218284.  446328.  451180.  456032.  460884.]
        [ 208577.  213557.  218537.  223517.  457577.  462557.  467537.  472517.]
        [ 213426.  218534.  223642.  228750.  468826.  473934.  479042.  484150.]
        [ 218275.  223511.  228747.  233983.  480075.  485311.  490547.  495783.]
        [ 446228.  457480.  468732.  479984. 1008828. 1020080. 1031332. 1042584.]
        [ 451077.  462457.  473837.  485217. 1020077. 1031457. 1042837. 1054217.]
        [ 455926.  467434.  478942.  490450. 1031326. 1042834. 1054342. 1065850.]
        [ 460775.  472411.  484047.  495683. 1042575. 1054211. 1065847. 1077483.]]
     */
    
    test_print_dense(out->desc.object_name);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(out->desc.object_name);

    printf("%s passed\n", __func__);
}

void test_bypass_a_m_b_p_something() {
    // dense
    test_bypass_a_m_b_p_1();          // (A @ B) + A
    test_bypass_a_m_b_p_2();          // (A @ B) + B
    test_bypass_a_m_b_p_3();          // (A^T @ B) + A^T
    test_bypass_a_m_b_p_4();          // (A @ B^T) + B^T

    // sparse
    test_bypass_a_m_b_p_1_sparse();   // (A @ B) + A
    test_bypass_a_m_b_p_2_sparse();   // (A @ B) + B
    test_bypass_a_m_b_p_3_sparse();   // (A^T @ B) + A^T
    test_bypass_a_m_b_p_4_sparse();   // (A @ B^T) + B^T
}

int main(int argc, char **argv)
{
    // test_bypass_a_m_b();
    test_bypass_a_m_b_p_something();

    return 0;
}
