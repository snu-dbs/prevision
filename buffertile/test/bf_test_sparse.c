#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"

/*********************
 * Sparse tests
 *********************/

void test_sparse_normal()
{
    char *array_name = "__test_sparse_normal";
    printf("test_sparse_normal started\n");

    // Init BF Layer
    BF_Init();
    BF_Attach();

    /*************************
     * Create array
     *************************/
    int dim_domain[] = {0, 127, 0, 127};
    int tile_extents[] = {16, 16};
    storage_util_create_array(array_name, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_INT32, false);

    // empty read
    PFpage *page;
    uint64_t dcoords[2] = {1, 2};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        2,           // length of the coordinates.
        BF_EMPTYTILE_SPARSE_CSR     // a request MUST contain expected template of the tile when reading an empty tile 
    };
    
    // test
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    assert(bf_util_pagebuf_get_unfilled_idx(page) == 0); // unfilled idx of empty tile is 4
    assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == 0); // same reason
    
    assert(bf_util_pagebuf_get_len(page) == (sizeof(int32_t) * 4)); // default size of empty tile is 4

    uint64_t *coord_sizes = bf_util_pagebuf_get_coords_lens(page);  // the sizes of indptr and indices

    assert(coord_sizes[0] == ((tile_extents[0] + 1) * sizeof(uint64_t)));    // the size of row ptr == the length of row + 1
    assert(coord_sizes[1] == (4 * sizeof(uint64_t)));  // default size of empty tile is 4

    // fill diagonal    
    // default size of pagebuf is 4, so we need to double the pagebuf and the second dim of coords twice.
    //      the first dim is indptr so we don't need to double it up.
    BF_ResizeBuf(page, 16);
    
    // fill
    // expected pagebuf, indptr, and indices:
    //  - pagebuf:  [0, 1, 2, 3, 4, ... 15] (total 16 items)
    //  - indptr:   [0, 1, 2, 3, 4, ..., 15, 16] (total 17 items)
    //  - indices:  [0, 1, 2, 3, 4, ..., 15] (total 16 items)
    uint64_t *indptr = bf_util_pagebuf_get_coords(page, 0);         // row ptr
    uint64_t *indices = bf_util_pagebuf_get_coords(page, 1);        // column indices
    for (int i = 0; i < 16; i++) {
        bf_util_pagebuf_set_int(page, i, i);    // indicate data value
        indices[i] = (uint64_t) i;              // indicate column indices
        indptr[i] = (uint64_t) i;               // indicate indptr
    }
    indptr[16] = tile_extents[0];
    
    bf_util_pagebuf_set_unfilled_idx(page, 16);        // only for column indices
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, 16 * sizeof(int32_t));

    // write
    assert(BF_TouchBuf(key) == BFE_OK);
    assert(BF_UnpinBuf(key) == BFE_OK);

    // close bf and re-open bf
    BF_Detach();
    BF_Free();
    BF_Init();
    BF_Attach();

    // read
    struct array_key key2 = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        2,           // length of the coordinates.
    };
    
    // test
    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    assert(bf_util_pagebuf_get_unfilled_idx(page) == 16); // unfilled idx of the tile is 16
    assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (16 * sizeof(int32_t)));
    
    assert(bf_util_pagebuf_get_len(page) == (sizeof(int32_t) * 16)); // 16 elements are filled

    uint64_t *coord_sizes_2 = bf_util_pagebuf_get_coords_lens(page);  // the sizes of indptr and indices

    assert(coord_sizes_2[0] == ((tile_extents[0] + 1) * sizeof(uint64_t)));    // the size of row ptr == the length of row + 1
    assert(coord_sizes_2[1] == (16 * sizeof(uint64_t)));  // 16 elements are filled

    uint64_t *indptr_2 = bf_util_pagebuf_get_coords(page, 0);         // row ptr
    uint64_t *indices_2 = bf_util_pagebuf_get_coords(page, 1);        // column indices
    for (int i = 0; i < 16; i++) {
        assert(bf_util_pagebuf_get_int(page, i) == i);
        assert(indices_2[i] == (uint64_t) i);              
        assert(indptr_2[i] == (uint64_t) i);                       
    }
    assert(indptr_2[16] == tile_extents[0]);

    BF_Detach();
    BF_Free();

    printf("test_sparse_normal passed\n");
    return;
}

void test_sparse_many()
{
    char *array_name = "__test_sparse_many";
    printf("%s started\n", __func__);

    // Init BF Layer
    BF_Init();
    BF_Attach();

    /*************************
     * Create array
     *************************/
    int dim_domain[] = {0, 7, 0, 7};
    int tile_extents[] = {4, 4};
    storage_util_create_array(array_name, TILESTORE_SPARSE_CSR, dim_domain, tile_extents, 2, 1, TILESTORE_INT32, false);

    // empty read
    PFpage *page;
    uint64_t dcoords[2] = {0, 0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        2,           // length of the coordinates.
        BF_EMPTYTILE_SPARSE_CSR     // a request MUST contain expected template of the tile when reading an empty tile 
    };

    // fill only 4 elements per tile
    for (uint64_t i = 0; i < 2; i++) {
        for (uint64_t j = 0; j < 2; j++) {
            dcoords[0] = i;
            dcoords[1] = j;

            BF_GetBuf(key, &page);

            uint64_t *indptr = bf_util_pagebuf_get_coords(page, 0);
            uint64_t *indices = bf_util_pagebuf_get_coords(page, 1);
            for (int k = 0; k < 4; k++) {
                bf_util_pagebuf_set_int(page, k, k);
                indices[k] = (uint64_t) k;
                indptr[k] = (uint64_t) k;
            }
            indptr[4] = tile_extents[0];

            bf_util_pagebuf_set_unfilled_pagebuf_offset(page, sizeof(int32_t) * 4);
            bf_util_pagebuf_set_unfilled_idx(page, 4);

            BF_TouchBuf(key);
            BF_UnpinBuf(key);
        }
    }

    // reset bf layer
    BF_Detach();
    BF_Free();
    BF_Init();
    BF_Attach();

    // replace {0, 0} to smaller tile
    dcoords[0] = 0;
    dcoords[1] = 0;

    BF_GetBuf(key, &page);

    uint64_t *indptr = bf_util_pagebuf_get_coords(page, 0);
    uint64_t *indices = bf_util_pagebuf_get_coords(page, 1);
    for (int k = 0; k < 2; k++) {
        bf_util_pagebuf_set_int(page, k, k * 2);
        indices[k] = (uint64_t) k;
        indptr[k] = (uint64_t) k;
    }
    indptr[2] = 2;
    indptr[3] = 2;
    indptr[4] = 2;

    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, sizeof(int32_t) * 2);
    bf_util_pagebuf_set_unfilled_idx(page, 2);

    BF_TouchBuf(key);
    BF_UnpinBuf(key);

    // replace {0, 1} to larger tile
    dcoords[0] = 0;
    dcoords[1] = 1;

    BF_GetBuf(key, &page);

    BF_ResizeBuf(page, 8);

    indptr = bf_util_pagebuf_get_coords(page, 0);
    indices = bf_util_pagebuf_get_coords(page, 1);
    for (int k = 0; k < 8; k++) {
        bf_util_pagebuf_set_int(page, k, k * 2);
        indices[k] = (uint64_t) (k % 4);
        if (k < 2) {
            indptr[k] = (uint64_t) (k * tile_extents[0]);
        } else if (k <= 4) {
            indptr[k] = 8;
        }
    }

    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, sizeof(int32_t) * 8);
    bf_util_pagebuf_set_unfilled_idx(page, 8);

    BF_TouchBuf(key);
    BF_UnpinBuf(key);

    // reset bf layer
    BF_Detach();
    BF_Free();
    BF_Init();
    BF_Attach();

    // test {0, 0}
    dcoords[0] = 0;
    dcoords[1] = 0;
    assert(BF_GetBuf(key, &page) == BFE_OK);
    assert(bf_util_pagebuf_get_unfilled_idx(page) == 2);
    assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (2 * sizeof(int32_t)));
    assert(bf_util_pagebuf_get_len(page) == (sizeof(int32_t) * 2));

    uint64_t *coord_sizes = bf_util_pagebuf_get_coords_lens(page);
    assert(coord_sizes[0] == ((tile_extents[0] + 1) * sizeof(uint64_t)));
    assert(coord_sizes[1] == (2 * sizeof(uint64_t)));

    assert(bf_util_pagebuf_get_int(page, 0) == 0);
    assert(bf_util_pagebuf_get_int(page, 1) == 2);

    assert(BF_UnpinBuf(key) == BFE_OK);

    // test {0, 1}
    dcoords[0] = 0;
    dcoords[1] = 1;
    assert(BF_GetBuf(key, &page) == BFE_OK);
    assert(bf_util_pagebuf_get_unfilled_idx(page) == 8);
    assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (8 * sizeof(int32_t)));
    assert(bf_util_pagebuf_get_len(page) == (sizeof(int32_t) * 8));

    coord_sizes = bf_util_pagebuf_get_coords_lens(page);
    assert(coord_sizes[0] == ((tile_extents[0] + 1) * sizeof(uint64_t)));
    assert(coord_sizes[1] == (8 * sizeof(uint64_t)));

    for (int i = 0; i < 8; i++) {
        assert(bf_util_pagebuf_get_int(page, i) == (i * 2));
    }

    assert(BF_UnpinBuf(key) == BFE_OK);

    // test {1, 0} and {1, 1}
    for (int i = 0; i < 2; i++) {
        dcoords[0] = 1;
        dcoords[1] = i;
        assert(BF_GetBuf(key, &page) == BFE_OK);
        assert(bf_util_pagebuf_get_unfilled_idx(page) == 4);
        assert(bf_util_pagebuf_get_unfilled_pagebuf_offset(page) == (4 * sizeof(int32_t)));
        assert(bf_util_pagebuf_get_len(page) == (sizeof(int32_t) * 4));

        coord_sizes = bf_util_pagebuf_get_coords_lens(page);
        assert(coord_sizes[0] == ((tile_extents[0] + 1) * sizeof(uint64_t)));
        assert(coord_sizes[1] == (4 * sizeof(uint64_t)));

        for (int i = 0; i < 4; i++) {
            assert(bf_util_pagebuf_get_int(page, i) == i);
        }

        assert(BF_UnpinBuf(key) == BFE_OK);
    }

    BF_Detach();
    BF_Free();

    printf("test_sparse_many passed\n");
    return;
}

int sparse_tests()
{
    /*
     * Sparse tests
     */

    printf("============= Sparse tests =============\n");

    // array dimension test
    test_sparse_normal();
    test_sparse_many();

    // sparse functionality test
    // test_sparse_double_page_size();

    return 0;
}