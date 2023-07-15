#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

#include "bf.h"
#include "utils.h"

/*********************
 * Dense tests
 *********************/

void test_dense_normal()
{
    char *array_name = "__test_dense_normal";
    printf("test_dense_normal started\n");

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

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 8) == 8);
    assert(bf_util_pagebuf_get_int(page, 15) == 15);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    key.dcoords[0] = 7;
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get other buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 112);
    assert(bf_util_pagebuf_get_int(page, 8) == 120);
    assert(bf_util_pagebuf_get_int(page, 15) == 127);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_dense_normal passed\n");
    return;
}

void test_dense_3d()
{
    char *array_name = "__test_dense_3d";
    printf("test_dense_3d started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach(); 

    /*************************
     * Create and write array
     *************************/
    int dim_domain[] = {1, 16, 1, 16, 1, 16};
    int tile_extents[] = {2, 2, 2};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 3, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 3, 1);

    /*************************
     * Test BufferTile
     *************************/
    PFpage *page;
    uint64_t dcoords[3] = {0, 0, 0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        3           // length of the coordinates.
    };

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 1) == 1);
    assert(bf_util_pagebuf_get_int(page, 2) == 16);
    assert(bf_util_pagebuf_get_int(page, 3) == 17);
    assert(bf_util_pagebuf_get_int(page, 4) == 256);
    assert(bf_util_pagebuf_get_int(page, 5) == 257);
    assert(bf_util_pagebuf_get_int(page, 6) == 272);
    assert(bf_util_pagebuf_get_int(page, 7) == 273);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    key.dcoords[0] = 1;
    key.dcoords[1] = 2;
    key.dcoords[2] = 3;
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get other buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 582);
    assert(bf_util_pagebuf_get_int(page, 7) == 855);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_dense_3d passed\n");
    return;
}

void test_dense_highdim()
{
    char *array_name = "__test_dense_highdim";
    printf("test_dense_highdim started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach(); 

    /*************************
     * Create and write array
     *************************/
    int dim_domain[] = {1, 16, 1, 16, 1, 16, 1, 16, 1, 16}; // 5D array
    int tile_extents[] = {2, 2, 2, 2, 2};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 5, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 5, 1);

    /*************************
     * Test BufferTile
     *************************/
    PFpage *page;
    uint64_t dcoords[5] = {0, 0, 0, 0, 0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        5           // length of the coordinates.
    };

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 1) == 1);
    assert(bf_util_pagebuf_get_int(page, 2) == 16);
    assert(bf_util_pagebuf_get_int(page, 3) == 17);
    assert(bf_util_pagebuf_get_int(page, 4) == 256);
    assert(bf_util_pagebuf_get_int(page, 5) == 257);
    assert(bf_util_pagebuf_get_int(page, 6) == 272);
    assert(bf_util_pagebuf_get_int(page, 7) == 273);
    assert(bf_util_pagebuf_get_int(page, 8) == 4096);
    assert(bf_util_pagebuf_get_int(page, 16) == 65536);
    assert(bf_util_pagebuf_get_int(page, 31) == 69905);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    key.dcoords[0] = 1;
    key.dcoords[1] = 2;
    key.dcoords[2] = 3;
    key.dcoords[3] = 4;
    key.dcoords[4] = 5;
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get other buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 149130);
    assert(bf_util_pagebuf_get_int(page, 8) == 153226);
    assert(bf_util_pagebuf_get_int(page, 16) == 214666);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_dense_highdim passed\n");
    return;
}

void test_dense_bffree_write()
{
    char *array_name = "__test_dense_bffree_write";
    printf("test_dense_bffree_write started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach();

    /*************************
     * Create Array
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
        "a",       // attribute name
        dcoords,    // data tile coordinates
        1           // length of the coordinates.
    };

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer

    // write
    for (int i = 0; i < 16; i++)
    {
        bf_util_pagebuf_set_int(page, i, -i);
    }
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, 16 * sizeof(int));
    bf_util_pagebuf_set_unfilled_idx(page, 16);
    assert(BF_TouchBuf(key) == BFE_OK); // Touch the buffer
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    // BF free
    BF_Detach();
    BF_Free();

    /*************************
     * Reading array
     *************************/
    uint64_t data_size1 = sizeof(int) * 16;
    int *data1_unaligned = malloc(ceil_to_512bytes(data_size1) + 511);
    int *data1 = aligned_512bytes_ptr(data1_unaligned);
    memset(data1, 0, 16 * sizeof(int));

    uint64_t tile_coords[1] = {0};
    tilestore_tile_metadata_t tile_meta;

    tilestore_read_metadata_of_tile(array_name, tile_coords, 1, &tile_meta);
    tilestore_read_dense(array_name, tile_meta, data1, data_size1, TILESTORE_NORMAL);

    // test
    for (int i = 0; i < 16; i++) assert(data1[i] == -i);

    tile_coords[0] = 1;
    tilestore_read_metadata_of_tile(array_name, tile_coords, 1, &tile_meta);
    tilestore_read_dense(array_name, tile_meta, data1, data_size1, TILESTORE_NORMAL);

    // test
    for (int i = 0; i < 16; i++) assert(data1[i] == (i + 16));

    free(data1_unaligned);

    storage_util_delete_array(array_name);

    printf("test_dense_bffree_write passed\n");
    return;
}

void test_dense_write_reload()
{
    char *array_name = "__test_dense_write_reload";
    printf("test_dense_write_reload started\n");

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

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer

    bf_util_pagebuf_set_int(page, 8, 999); // write a value

    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 8) == 999);
    assert(bf_util_pagebuf_get_int(page, 15) == 15);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get other buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 8) == 999);
    assert(bf_util_pagebuf_get_int(page, 15) == 15);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_dense_write_reload passed\n");
    return;
}

void test_dense_write_flush()
{
    char *array_name = "__test_dense_write_flush";
    printf("test_dense_write_flush started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach(); 

    /*************************
     * Create and write array
     *************************/
    int tile_size = 4000;
    int num_of_tiles = (MAX_BUF_MEM_SIZE / (tile_size * sizeof(int))) + 1;   // approximated
    int dim_domain[] = {1, num_of_tiles * tile_size};
    int tile_extents[] = {tile_size};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    /*************************
     * Test BufferTile
     *************************/
    PFpage *page;
    uint64_t dcoords[1] = {0};
    struct array_key key = {
        array_name, // array name
        "a",       // attribute name
        dcoords,    // data tile coordinates
        1           // length of the coordinates.
    };

    // BF_ShowLists();

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer

    // write
    for (int i = 0; i < bf_util_pagebuf_get_len(page) / sizeof(int); i++)
    {
        bf_util_pagebuf_set_int(page, i, 0);
    }
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, tile_size * sizeof(int));
    bf_util_pagebuf_set_unfilled_idx(page, tile_size);
    assert(BF_TouchBuf(key) == BFE_OK); // Touch the buffer
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    // read buffers to flush the above write
    for (int i = 1; i < num_of_tiles; i++)
    {
        key.dcoords[0] = i;
        assert(BF_GetBuf(key, &page) == BFE_OK);
        assert(BF_UnpinBuf(key) == BFE_OK);
    }

    // read first page again
    key.dcoords[0] = 0;
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    // test
    for (int i = 0; i < tile_size; i++)
        assert(bf_util_pagebuf_get_int(page, i) == 0);

    // BF_ShowLists();
    // BF_ShowBuf(key);

    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    // BF_ShowLists();

    BF_Detach();
    BF_Free();

    /*************************
     * Reading array
     *************************/

    uint64_t data_size1 = sizeof(int) * tile_size;
    int *data1_unaligned = malloc(ceil_to_512bytes(data_size1) + 511);
    int *data1 = aligned_512bytes_ptr(data1_unaligned);

    for (int i = 0; i < num_of_tiles; i++) {
        uint64_t tile_coords[1] = {i};
        tilestore_tile_metadata_t tile_meta;

        memset(data1, 0, tile_size * sizeof(int));

        tilestore_read_metadata_of_tile(array_name, tile_coords, 1, &tile_meta);
        tilestore_read_dense(array_name, tile_meta, data1, data_size1, TILESTORE_NORMAL);
        
        for (int j = 0; j < tile_size; j++) {
            if (i == 0)
                assert(data1[j] == 0);
            else
                assert(data1[j] == (i * tile_size) + j);
        
        }
    }

    free(data1_unaligned);

    storage_util_delete_array(array_name);

    printf("test_dense_write_flush passed\n");
    return;
}

void test_dense_forcewrite()
{
    char *array_name = "__test_dense_forcewrite";
    printf("test_dense_forcewrite started\n");

    // Init
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

    assert(BF_GetBuf(key, &page) == BFE_OK);   // Get Buffer
    bf_util_pagebuf_set_int(page, 0, 999); // Set value
    assert(BF_TouchBuf(key) == BFE_OK);        // Touch the buffer
    assert(BF_UnpinBuf(key) == BFE_OK);        // Unpin the buffer
    assert(BF_ForceWriteBuf(key) == BFE_OK);   // Sync content to the disk

    BF_Detach();
    BF_Free();

    /*************************
     * Reading array
     *************************/

    uint64_t data_size = sizeof(int) * 16;
    int *data_unaligned = malloc(ceil_to_512bytes(data_size) + 511);
    int *data = aligned_512bytes_ptr(data_unaligned);
    memset(data, 0, 16 * sizeof(int));

    uint64_t tile_coords[1] = {0};
    tilestore_tile_metadata_t tile_meta;

    tilestore_read_metadata_of_tile(array_name, tile_coords, 1, &tile_meta);
    tilestore_read_dense(array_name, tile_meta, data, data_size, TILESTORE_NORMAL);

    // test
    assert(data[0] == 999);

    free(data_unaligned);

    storage_util_delete_array(array_name);

    printf("test_dense_forcewrite passed\n");
    return;
}

void test_dense_odd_tile_extent()
{
    char *array_name = "__test_dense_odd_tile_extent";

    printf("test_dense_odd_tile_extent started\n");

    // Init
    BF_Init();
    BF_Attach();

    /*************************
     * Create and write array
     *************************/
    int dim_domain[] = {1, 8};
    int tile_extents[] = {5};
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

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 4) == 4);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    key.dcoords[0] = 1;
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get other buffer
    // Some tests
    assert(bf_util_pagebuf_get_int(page, 0) == 5);
    assert(bf_util_pagebuf_get_int(page, 2) == 7);
    assert(page->pagebuf_len / sizeof(int) == 3);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_discard_pages(array_name, NULL);
    storage_util_delete_array(array_name);

    BF_Detach();
    BF_Free();

    printf("test_dense_odd_tile_extent passed\n");
    return;
}

int dense_tests()
{
    /*
     * Dense tests
     */

    printf("============= Dense tests =============\n");

    // array dimension test
    test_dense_normal();
    test_dense_3d();
    test_dense_highdim();

    // writing test
    test_dense_bffree_write();
    test_dense_write_reload();
    test_dense_write_flush();
    test_dense_forcewrite();

    // mise test
    test_dense_odd_tile_extent();

    return 0;
}