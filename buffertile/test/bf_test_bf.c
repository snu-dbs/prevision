#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

#include "bf.h"
#include "utils.h"


void test_lru_pinned()
{
    char *array_name = "__test_lru_pinned";
    printf("test_lru_pinned started\n");

    // BF init
    BF_Init();
    BF_Attach();

    /*************************
     * Creating and writing array
     *************************/
    int tile_size = 10000;
    int num_of_tiles = (MAX_BUF_MEM_SIZE / (tile_size * sizeof(int))) + 1;   // approximated
    int dim_domain[] = {1, tile_size * num_of_tiles};
    int tile_extents[] = {tile_size};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    /*************************
     * Test BufferTile
     *************************/

    // fill BF layer
    for (int zz = 0; zz < num_of_tiles; zz++)
    {
        PFpage *page;
        uint64_t dcoords[1] = {zz};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };

        BF_GetBuf(key, &page);
        if (zz != 0)
            BF_UnpinBuf(key); // pin the first tile only
    }

    // read the first loaded tile
    PFpage *page;
    uint64_t dcoords[1] = {0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        1           // length of the coordinates.
    };
    assert(BF_GetBuf(key, &page) == BFE_OK);

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_lru_pinned passed\n");
    return;
}

void test_op_discard_pages() {
    char *array_name = "__test_op_discard_pages";
    printf("test_op_discard_pages\n");

    // BF init
    BF_Init();
    BF_Attach();

    /*************************
     * Creating and writing array
     *************************/
    int dim_domain[] = {1, 100};
    int tile_extents[] = {10};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);

    /*************************
     * Test BufferTile
     *************************/

    // fill BF layer
    for (int zz = 0; zz < 10; zz++)
    {
        PFpage *page;
        uint64_t dcoords[1] = {zz};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };

        BF_GetBuf(key, &page);
        for (int zzz = 0; zzz < 10; zzz++) {
            bf_util_pagebuf_set_int(page, zzz, 1);
        }
        BF_UnpinBuf(key); 
    }

    // remove the array
    storage_util_delete_array(array_name);

    // BF_ShowLists();     // show

    // discard pages for the wrong array_name
    int removed_count = 0;
    BF_discard_pages("helloworld!", &removed_count);
    assert(removed_count == 0);

    assert(BF_discard_pages("helloworld!", NULL) == BFE_OK);

    // discard pages for the array_name
    removed_count = 0;
    BF_discard_pages(array_name, &removed_count);
    assert(removed_count == 10);

    // BF_ShowLists();     // show the lists

    // create the array again
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);

    // read and test
    for (int zz = 0; zz < 10; zz++)
    {
        PFpage *page;
        uint64_t dcoords[1] = {zz};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };

        BF_GetBuf(key, &page);
        for (int zzz = 0; zzz < 10; zzz++) {
            assert(bf_util_pagebuf_get_int(page, zzz) != 1);
        }
        BF_UnpinBuf(key); 
    }

    // BF_ShowLists();     // show the lists

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);
    
    BF_Detach();
    BF_Free();

    printf("test_op_discard_pages passed\n");
    return;
}

void test_double_unpin_and_touch_unpinned_buf() {    
    char *array_name = "__test_double_unpin_and_touch_unpinned_buf";
    printf("test_double_unpin_and_touch_unpinned_buf started; this should make one bf error.\n");

    // Init
    BF_Init();
    BF_Attach();

    // create array
    int dim_domain[] = {1, 128, 1, 128};
    int tile_extents[] = {16, 16};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_INT32, false);

    // GetBuf
    PFpage *page;
    uint64_t dcoords[2] = {0, 0};
    struct array_key key = {array_name, "a", dcoords, 2};

    BF_GetBuf(key, &page);
    BF_UnpinBuf(key); 

    // test
    assert(BF_UnpinBuf(key) != BFE_OK);
    assert(BF_TouchBuf(key) != BFE_OK);

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    // Free
    BF_Detach();
    BF_Free();

    printf("test_double_unpin_and_touch_unpinned_buf ended\n");
}

void test_bigload_bf() {
    char *array_name = "__test_bigload_bf";
    printf("test_bigload_bf started\n");

    // BF init
    BF_Init();
    BF_Attach();

    /*************************
     * Creating and writing array
     *************************/
    int dim_domain[] = {1, 160000, 1, 400000};
    int tile_extents[] = {4000, 4000};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_INT32, false);

    /*************************
     * Test BufferTile
     *************************/

    for (int i = 0; i < 40; i++) {
        for (int j = 0; j < 100; j++) {
            PFpage *page;
            uint64_t dcoords[2] = {i, j};
            struct array_key key = {array_name, "a", dcoords, 2};
            BF_GetBuf(key, &page);
            BF_UnpinBuf(key); 

            // printf("%d,%d\n", i, j);
            // BF_ShowLists();     // show the lists
        }
    }

    // remove the array
    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_bigload_bf passed\n");
    return;
}

int test_getbufwithoutread() {
    char *array_name_dense = "__test_getbufwithoutread_dense";
    char *array_name_densenull = "__test_getbufwithoutread_densenull";
    char *array_name_sparse = "__test_getbufwithoutread_sparse";
    char *array_name_sparsenull = "__test_getbufwithoutread_sparsenull";

    printf("test_getbufwithoutread started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach(); 

    PFpage *page;

    /////////////////////////// 
    // test DENSE_FIXED
    ///////////////////////////
    int dim_domain_dense[] = {1, 64, 1, 64};
    int tile_extents_dense[] = {32, 32};
    storage_util_create_array(array_name_dense, TILESTORE_DENSE, dim_domain_dense, tile_extents_dense, 2, 1, TILESTORE_INT32, false);

    // this getbuf should be wo/ read
    uint64_t dcoords_dense[2] = {0, 0};
    struct array_key key_dense = {array_name_dense, "a", dcoords_dense, 2};
    assert(BF_GetBuf(key_dense, &page) == BFE_OK); 

    // tests
    uint64_t num_of_cells_dense = tile_extents_dense[0] * tile_extents_dense[1];
    assert(bf_util_pagebuf_get_unfilled_idx(page) == num_of_cells_dense);
    assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (num_of_cells_dense * sizeof(int)));
    assert(bf_util_pagebuf_get_len(page) == (num_of_cells_dense * sizeof(int)));

    BF_UnpinBuf(key_dense);

    /////////////////////////// 
    // test DENSE_FIXED_NULLABLE
    ///////////////////////////
    // int dim_domain_densenull[] = {1, 64, 1, 64};
    // int tile_extents_densenull[] = {16, 16};
    // storage_util_create_array(array_name_densenull, TILESTORE_DENSE, dim_domain_densenull, tile_extents_densenull, 2, 1, TILESTORE_FLOAT64, true);

    // // this getbuf should be wo/ read
    // uint64_t dcoords_densenull[2] = {0, 0};
    // struct array_key key_densenull = {array_name_densenull, "a", dcoords_densenull, 2};
    // assert(BF_GetBuf(key_densenull, &page) == BFE_OK); 

    // // tests
    // uint64_t num_of_cells_densenull = tile_extents_densenull[0] * tile_extents_densenull[1];
    // assert(bf_util_pagebuf_get_unfilled_idx(page) == num_of_cells_densenull);
    // assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (num_of_cells_densenull * sizeof(double)));
    // assert(bf_util_pagebuf_get_len(page) == (num_of_cells_densenull * sizeof(double)));
    // assert(bf_util_pagebuf_get_validity_len(page) == (num_of_cells_densenull * sizeof(uint8_t)));

    // BF_UnpinBuf(key_densenull);

    // /////////////////////////// 
    // // test SPARSE_FIXED
    // ///////////////////////////
    // int dim_domain_sparse[] = {1, 128, 1, 128};
    // int tile_extents_sparse[] = {8, 8};
    // storage_util_create_array(array_name_sparse, TILESTORE_SPARSE, dim_domain_sparse, tile_extents_sparse, 2, 1, TILESTORE_CHAR, false);

    // // this getbuf should be wo/ read
    // uint64_t dcoords_sparse[2] = {0, 0};
    // struct array_key key_sparse = {array_name_sparse, "a", dcoords_sparse, 2};
    // assert(BF_GetBuf(key_sparse, &page) == BFE_OK); 

    // // tests
    // uint64_t num_of_cells_sparse = 4;       // initial num of cells of empty tile is 4
    // assert(bf_util_pagebuf_get_unfilled_idx(page) == num_of_cells_sparse);
    // assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (num_of_cells_sparse * sizeof(char)));
    // assert(bf_util_pagebuf_get_len(page) == (num_of_cells_sparse * sizeof(char)));
    // assert(bf_util_pagebuf_get_coords_lens(page)[0] == num_of_cells_sparse * sizeof(int32_t));
    // assert(bf_util_pagebuf_get_coords_lens(page)[1] == num_of_cells_sparse * sizeof(int32_t));

    // BF_UnpinBuf(key_sparse);

    // /////////////////////////// 
    // // test SPARSE_FIXED_NULLABLE
    // ///////////////////////////
    // int dim_domain_sparsenull[] = {1, 256, 1, 256};
    // int tile_extents_sparsenull[] = {128, 128};
    // storage_util_create_array(array_name_sparsenull, TILESTORE_SPARSE, dim_domain_sparsenull, tile_extents_sparsenull, 2, 1, TILESTORE_FLOAT64, true);

    // // this getbuf should be wo/ read
    // uint64_t dcoords_sparsenull[2] = {0, 0};
    // struct array_key key_sparsenull = {array_name_sparsenull, "a", dcoords_sparsenull, 2};
    // assert(BF_GetBuf(key_sparsenull, &page) == BFE_OK); 

    // // tests
    // uint64_t num_of_cells_sparsenull = 4;          // initial num of cells of empty tile is 4
    // assert(bf_util_pagebuf_get_unfilled_idx(page) == num_of_cells_sparsenull);
    // assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (num_of_cells_sparsenull * sizeof(float)));
    // assert(bf_util_pagebuf_get_len(page) == (num_of_cells_sparsenull * sizeof(float)));
    // assert(bf_util_pagebuf_get_coords_lens(page)[0] == num_of_cells_sparsenull * sizeof(int32_t));
    // assert(bf_util_pagebuf_get_coords_lens(page)[1] == num_of_cells_sparsenull * sizeof(int32_t));
    // assert(bf_util_pagebuf_get_validity_len(page) == (num_of_cells_sparsenull * sizeof(uint8_t)));

    // BF_UnpinBuf(key_sparsenull);

    BF_discard_pages(array_name_dense, NULL);
    // BF_discard_pages(array_name_densenull, NULL);
    // BF_discard_pages(array_name_sparse, NULL);
    // BF_discard_pages(array_name_sparsenull, NULL);

    storage_util_delete_array(array_name_dense);
    // storage_util_delete_array(array_name_densenull);
    // storage_util_delete_array(array_name_sparse);
    // storage_util_delete_array(array_name_sparsenull);

    BF_Detach();
    BF_Free();

    printf("test_getbufwithoutread passed\n");
    return 0;
}

int test_getbufwithoutread_eviction() {
    char *array_name = "__test_getbufwithoutread_eviction";
    printf("test_getbufwithoutread_eviction started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach(); 

    /*************************
     * Create and write array
     *************************/
    int dim_domain[] = {1, 128};
    int tile_extents[] = {16};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    /*************************
     * Test BufferTile
     *************************/
    PFpage *page;
    uint64_t dcoords[1] = {0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        1           // length of the coordinates.
    };

    // Get Buffer without read
    // this is getbuf without read
    assert(BF_GetBuf(key, &page) == BFE_OK); 
    
    // write to buffer
    for (int i = 0; i < 16; i++) {
        bf_util_pagebuf_set_int(page, i, 0);
    }
    
    // touch and unpin
    BF_TouchBuf(key);
    BF_UnpinBuf(key);
    
    // reset BF layer to flush the dirty page
    BF_Detach();
    BF_Free();
    BF_Init();
    BF_Attach();

    // Get Buffer with read
    assert(BF_GetBuf(key, &page) == BFE_OK); 
    
    // read from buffer and test data
    for (int i = 0; i < 16; i++) {
        assert(bf_util_pagebuf_get_int(page, i) == 0);
    }

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_getbufwithoutread_eviction passed\n");

    return 0;
}

void test_edgetile() {
    char *array_name = "__test_edgetile";
    printf("test_edgetile started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach(); 

    /*************************
     * Create and write array
     *************************/
    int dim_domain[] = {1, 100, 1, 100};
    int tile_extents[] = {64, 64};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 2, 1);

    /*************************
     * Test BufferTile
     *************************/
    PFpage *page;
    uint64_t dcoords[2] = {1, 1};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        2           // length of the coordinates.
    };

    // Get Buffer without read
    // this is getbuf without read
    assert(BF_GetBuf(key, &page) == BFE_OK); 
    // (100 - 64) * (100 - 64) * sizeof(int) = 5184
    assert(bf_util_pagebuf_get_len(page) == 5184);
    
    // touch and unpin
    BF_TouchBuf(key);
    BF_UnpinBuf(key);
    
    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_edgetile passed\n");

    return;
}

void test_lru_policy() {
    // test LRU policy:
    // current policy is the following:
    //      1. get LRU page that unpinned & clean
    //      2. if there is no candidate, get LRU page that unpinned & dirty
    //      3. if there is no candidate, fail to get buffer.

    char *array_name_1 = "__test_lru_policy_1";
    char *array_name_2 = "__test_lru_policy_2";
    char *array_name_3 = "__test_lru_policy_3";
    printf("test_lru_policy started; this test may occur BF errors\n");

    BF_Init();
    BF_Attach();

    // 0. initialize: prepare two arrays
    int tile_size = 4000;
    int num_of_tiles = (MAX_BUF_MEM_SIZE / (tile_size * sizeof(int))) + 1;   // approximated
    int dim_domain[] = {1, num_of_tiles * tile_size};
    int tile_extents[] = {tile_size};
    storage_util_create_array(array_name_1, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    storage_util_create_array(array_name_2, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    storage_util_create_array(array_name_3, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);

    // these will be re-used
    PFpage *page;
    uint64_t dcoords[1] = {0};
    struct array_key key = {array_name_1, "a", dcoords, 1};

    // 1. fill some pinned pages (clean and dirty) 
    int pinned_thres = 8;
    for (int i = 0; i < pinned_thres; i++) {
        dcoords[0] = i;
        BF_GetBuf(key, &page);
        if (i % 2 == 0) BF_TouchBuf(key);
    }

    // 2. fill the others, with unpinned pages (clean and dirty).
    for (int i = pinned_thres; i < num_of_tiles; i++) {
        dcoords[0] = i;
        BF_GetBuf(key, &page);
        if (i % 2 == 0) BF_TouchBuf(key);
        BF_UnpinBuf(key);
    }

    // 3. fill more dirty pages (it would evict clean pages) until the size of 
    //      more filled pages is over the size of buffer size.
    key.arrayname = array_name_2;
    for (int i = 0; i < num_of_tiles; i++) {
        dcoords[0] = i;
        BF_GetBuf(key, &page);
        BF_TouchBuf(key);
        BF_UnpinBuf(key);
    }

    // 4. test there are some pinned pages and All of the other pages are dirty.
    // BFpage* ptr;
    // int i = 0;
    // for (ptr = BF_SHM_BF_PTR_BFPAGE(BFshm_hdr_ptr->BFLRU_head_o);
    //         (void *) ptr != (void *) _mspace_bf;    
    //             // if tail, nextpage_o will be oNULL, ptr == _msapce
    //         ptr = BF_SHM_BF_PTR_BFPAGE(ptr->nextpage_o)) {
    //     if (i < pinned_thres) {
    //         assert(ptr->count != 0);
    //     } else {
    //         assert(ptr->count == 0);
    //         assert(ptr->dirty == TRUE);
    //     }
        
    //     i++;
    // }

    // 5. fill more pinned pages and see the error is returned from GetBuf at 
    //      some time.
    key.arrayname = array_name_3;
    for (int i = 0; i < num_of_tiles - 1; i++) {
        dcoords[0] = i;
        BF_GetBuf(key, &page);
    }

    // after the loop, GetBuf should return err
    dcoords[0] = num_of_tiles - 1;
    assert(BF_GetBuf(key, &page) != BFE_OK);
    
    BF_discard_pages(array_name_1, NULL);
    BF_discard_pages(array_name_2, NULL);
    BF_discard_pages(array_name_3, NULL);

    storage_util_delete_array(array_name_1);
    storage_util_delete_array(array_name_2);
    storage_util_delete_array(array_name_3);

    BF_Detach();
    BF_Free();

    printf("test_lru_policy passed\n");

    return;
}


// void test_discard_unpin_cnt() {
//     // get buffer and set discard_unpin_cnt to 2.  
//     // after two calling BF_Unpin(), the pagebuf of pfpage immediately be NULL.  

//     char *array_name = "__test_discard_unpin_cnt";
//     printf("test_discard_unpin_cnt started\n");

//     // Init BF Layer
//     BF_Init();
//     BF_Attach(); 

//     /*************************
//      * Create and write array
//      *************************/
//     int dim_domain[] = {1, 100, 1, 100};
//     int tile_extents[] = {64, 64};
//     storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 2, 1, TILESTORE_INT32, false);
//     bf_util_write_array_with_incremental_values(array_name, dim_domain, 2, 1);

//     /*************************
//      * Test BufferTile
//      *************************/
//     PFpage *page;
//     uint64_t dcoords[2] = {0, 0};
//     struct array_key key = {array_name, "a", dcoords, 2};

//     // get buffer
//     BF_GetBuf(key, &page);

//     // default value of discard_unpin_cnt is BF_TILE_DISCARD_OFF (-1).
//     assert(page->discard_unpin_cnt == BF_TILE_DISCARD_OFF);

//     // set discard_unpin_cnt
//     page->discard_unpin_cnt = 2;

//     // unpin it
//     BF_UnpinBuf(key);

//     // the pagebuf of pfpage will not NULL since BufferTile does not evict the
//     //  page.
//     assert(page->pagebuf_o != BF_SHM_NULL);

//     // get buffer and unpin it again
//     BF_GetBuf(key, &page);
//     BF_UnpinBuf(key);

//     // the pagebuf of pfpage will be NULL since BufferTile discard it immediately.
//     assert(page->pagebuf_o == BF_SHM_NULL);

//     // done!

//     storage_util_delete_array(array_name);

//     BF_Detach();
//     BF_Free();

//     printf("test_discard_unpin_cnt passed\n");

//     return;
// }

void test_densemode() {
    printf("%s started\n", "test_densemode");
    
    // the test case works only when the mem size is small.
    assert(MAX_BUF_MEM_SIZE < 10000*10000*8);
    
    BF_Init();
    BF_Attach();

    // make a temp array
    int array_domain[] = {0, 10000 - 1, 0, 10000 - 1};
    int tile_size[] = {100, 100};
    storage_util_create_array(
        "test_densemode", 
        TILESTORE_DENSE,        // obsolete
        array_domain,
        tile_size,
        2,
        1,
        TILESTORE_FLOAT64,
        false);

    // we will test that the {0, 0} tile will be converted to CSR format.
    
    // array_key and coords
    array_key key;
    key.arrayname = "test_densemode";
    key.attrname = "a";
    key.dim_len = 2;
    key.emptytile_template = BF_EMPTYTILE_DENSE;
    uint64_t coord[] = {0, 0};
    key.dcoords = coord;

    // getbuf the {0, 0} tile
    PFpage *tile;
    assert(BF_GetBuf(key, &tile) == BFE_OK);

    // I will fill in the 33% of the pagebuf
    double *pagebuf = bf_util_get_pagebuf(tile);
    for (uint64_t i = 0; i < tile->pagebuf_len / sizeof(double); i += 3) {
        pagebuf[i] = 1;
    }
    tile->est_density = 0.3;

    // Unpin the {0, 0} tile
    assert(BF_TouchBuf(key) == BFE_OK);
    assert(BF_UnpinBuf(key) == BFE_OK);

    // evict the {0, 0} tile.
    for (uint64_t i = 0; i < 100; i++) {
        for (uint64_t j = 0; j < 100; j++) {
            // fprintf(stderr, "%lu,%lu\n", i, j);

            // skip the {0, 0} tile
            if (i == 0 && j == 0) continue;

            // fill in the tile and set to dirty
            coord[0] = i;
            coord[1] = j;

            assert(BF_GetBuf(key, &tile) == BFE_OK);

            double *pagebuf = bf_util_get_pagebuf(tile);
            for (uint64_t i = 0; i < tile->pagebuf_len / sizeof(double); i++) {
                pagebuf[i] = 1;
            }

            assert(BF_TouchBuf(key) == BFE_OK);
            assert(BF_UnpinBuf(key) == BFE_OK);
        }
    }

    // now, the {0, 0} tile would be evicted.
    coord[0] = 0;
    coord[1] = 0;
    assert(BF_GetBuf(key, &tile) == BFE_OK);

    // test the tile
    assert(tile->type == SPARSE_FIXED);
    assert(tile->sparse_format == PFPAGE_SPARSE_FORMAT_CSR);

    uint64_t *indptr = bf_util_pagebuf_get_coords(tile, 0);
    uint64_t *indices = bf_util_pagebuf_get_coords(tile, 1);
    pagebuf = bf_util_get_pagebuf(tile);
    
    // test pagebuf and indices 
    for (uint64_t i = 0; i < tile->pagebuf_len / sizeof(double); i++) {
        assert(pagebuf[i] == 1);
        assert(indices[i] == (i * 3) % tile_size[1]);
    }

    // test indptr
    for (uint64_t i = 0; i < tile_size[0]; i++) {
        assert(indptr[i + 1] - indptr[i] == 33 || indptr[i + 1] - indptr[i] == 34);
    }

    BF_Detach();
    BF_Free();

    storage_util_delete_array("test_densemode");

    printf("%s passed\n", "test_densemode");
}

int bf_tests()
{
    printf("============= BF tests =============\n");
    test_lru_pinned();
    // test_lru_policy();
    test_op_discard_pages();
    // test_double_unpin_and_touch_unpinned_buf();
    test_bigload_bf();
    test_getbufwithoutread();
    test_getbufwithoutread_eviction();
    test_edgetile();
    // test_discard_unpin_cnt();
    // test_densemode();
}