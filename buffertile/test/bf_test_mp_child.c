#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"

// test for test_read_shared_data_of_other_process()
int main(const int argc, const char *argv[]) {
    int test_num = atoi(argv[1]);
    // fprintf(stderr, "test_num: %d\n", test_num);
    if (test_num == 1) {
        char * array_name = "__test_read_shared_data_of_other_process";

        // Child
        PFpage *page;
        uint64_t dcoords[1] = {0};
        struct array_key key = {
            array_name,             // array name
            "a",                    // attribute name
            dcoords,                // data tile coordinates
            1                       // length of the coordinates.
        };

        BF_Attach();

        assert(BF_GetBuf(key, &page) == BFE_OK);            // Get Buffer

        assert(bf_util_pagebuf_get_int(page, 0) == 0);
        assert(bf_util_pagebuf_get_int(page, 8) == 8);
        assert(bf_util_pagebuf_get_int(page, 15) == 15);

        // make dirty
        bf_util_pagebuf_set_int(page, 0, 999);

        assert(BF_TouchBuf(key) == BFE_OK);                 // Touch the buffer
        assert(BF_UnpinBuf(key) == BFE_OK);                 // Unpin the buffer

        BF_Detach();
    } else if (test_num == 2) {
        char * array_name = "__test_latch";

        BF_Attach();

        // dirty the data structure of the BF layer
        int max_buf = 128;
        for (int i = 0; i < max_buf * 4; i++) {
            PFpage *page;
            uint64_t dcoords[1] = {i % (max_buf * 2)};
            struct array_key key = {
                array_name,             // array name
                "a",                    // attribute name
                dcoords,                // data tile coordinates
                1                       // length of the coordinates.
            };
            BF_GetBuf(key, &page);            // Get Buffer
            BF_TouchBuf(key);                 // Touch the buffer
            BF_UnpinBuf(key);                 // Unpin the buffer
            // fprintf(stderr, "child(%d): %d\n", getpid(), i);
        }

        BF_Detach();
    }
    // fprintf(stderr, "child(%d) done!\n", getpid());

    exit(0);
}