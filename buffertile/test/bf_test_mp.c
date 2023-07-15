#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"

void delete_array(char *array_name) {
    BF_Init(); BF_Attach();
    storage_util_delete_array(array_name);
    BF_Detach(); BF_Free();
}

void test_read_shared_data_of_child_from_parent()
{
    char *array_name = "__test_read_shared_data_of_child_from_parent";
    printf("test_read_shared_data_of_child_from_parent started\n");

    /*************************
     * Creating and writing array
     *************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {1, 128};
    int tile_extents[] = {16};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    BF_Detach();
    BF_Free();

    /*************************
     * Test BufferTile
     *************************/
    BF_Init(); // Init BF Layer

    pid_t pid = fork();
    if (pid == 0)
    {
        // Child
        PFpage *page;
        uint64_t dcoords[1] = {0};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };

        BF_Attach();

        assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
        assert(bf_util_pagebuf_get_int(page, 0) == 0);
        assert(bf_util_pagebuf_get_int(page, 8) == 8);
        assert(bf_util_pagebuf_get_int(page, 15) == 15);

        // make dirty
        bf_util_pagebuf_set_int(page, 0, 999);

        assert(BF_TouchBuf(key) == BFE_OK); // Touch the buffer
        assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

        BF_Detach();
        _exit(0);
    }

    // parent
    waitpid(pid, NULL, 0);

    // Some tests
    PFpage *page;
    uint64_t dcoords[1] = {0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        1           // length of the coordinates.
    };

    BF_Attach();

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    assert(bf_util_pagebuf_get_int(page, 0) == 999);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_Detach();
    BF_Free();

    delete_array(array_name);

    printf("test_read_shared_data_of_child_from_parent passed\n");
    return;
}

void test_read_shared_data_of_sibiling()
{
    char *array_name = "__test_read_shared_data_of_sibiling";
    printf("test_read_shared_data_of_sibiling started\n");

    /*************************
     * Creating and writing array
     *************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {1, 128};
    int tile_extents[] = {16};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    BF_Detach();
    BF_Free();

    /*************************
     * Test BufferTile
     *************************/

    BF_Init(); // Init BF Layer

    pid_t pid = fork();
    if (pid == 0)
    {
        // Child
        PFpage *page;
        uint64_t dcoords[1] = {0};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };

        BF_Attach();

        assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
        assert(bf_util_pagebuf_get_int(page, 0) == 0);
        assert(bf_util_pagebuf_get_int(page, 8) == 8);
        assert(bf_util_pagebuf_get_int(page, 15) == 15);

        // make dirty
        bf_util_pagebuf_set_int(page, 0, 999);

        assert(BF_TouchBuf(key) == BFE_OK); // Touch the buffer
        assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

        BF_Detach();
        _exit(0);
    }

    // parent
    waitpid(pid, NULL, 0);

    // tests
    pid = fork();
    if (pid == 0)
    {
        // Child
        PFpage *page;
        uint64_t dcoords[1] = {0};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };

        BF_Attach();

        assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
        assert(bf_util_pagebuf_get_int(page, 0) == 999);
        assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

        BF_Detach();
        _exit(0);
    }

    waitpid(pid, NULL, 0);
    BF_Free();

    delete_array(array_name);

    printf("test_read_shared_data_of_sibiling passed\n");
    return;
}

#include <errno.h>
void test_read_shared_data_of_other_process()
{
    char *array_name = "__test_read_shared_data_of_other_process";
    printf("test_read_shared_data_of_other_process started\n");

    /*************************
     * Creating and writing array
     *************************/
    BF_Init();
    BF_Attach();

    int dim_domain[] = {1, 128};
    int tile_extents[] = {16};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    BF_Detach();
    BF_Free();

    /*************************
     * Test BufferTile
     *************************/

    BF_Init(); // Init BF Layer

    // fprintf(stderr, "Now!\n");
    BF_Attach();

    pid_t pid = fork();
    if (pid == 0)
    {
        // execute the executable (from bftest_mp.c)
        int res = execlp("bf_test_mp_child", "bf_test_mp_child", "1", NULL);
        fprintf(stderr, "res=%d\n", errno);
    }

    // parent
    waitpid(pid, NULL, 0);

    // tests
    PFpage *page;
    uint64_t dcoords[1] = {0};
    struct array_key key = {
        array_name, // array name
        "a",        // attribute name
        dcoords,    // data tile coordinates
        1           // length of the coordinates.
    };

    BF_Attach();

    assert(BF_GetBuf(key, &page) == BFE_OK); // Get Buffer
    assert(bf_util_pagebuf_get_int(page, 0) == 999);
    assert(BF_UnpinBuf(key) == BFE_OK); // Unpin the buffer

    BF_Detach();
    BF_Free();

    delete_array(array_name);

    printf("test_read_shared_data_of_other_process passed\n");
}

void test_latch()
{
    char *array_name = "__test_latch";

    printf("test_latch started\n");
    printf("\tIt might takes a long time\n");
    printf("\tIf you got fail, then remove arrays and /dev/shm/buffertile,");
    printf(" kill both bftest and bftest_mp processes on the background,");
    printf(" and then try again.\n");

    /*************************
     * Creating and writing array
     *************************/
    BF_Init();
    BF_Attach();

    int max_buf = 128;
    int dim_domain[] = {1, (max_buf * 2) * 16}; // large enough
    int tile_extents[] = {16};
    storage_util_create_array(array_name, TILESTORE_DENSE, dim_domain, tile_extents, 1, 1, TILESTORE_INT32, false);
    bf_util_write_array_with_incremental_values(array_name, dim_domain, 1, 1);

    BF_Detach();
    BF_Free();

    /*************************
     * Test BufferTile
     *************************/
    BF_Init(); // Init BF Layer

    // run four processes (bftest_mp.c)
    pid_t pid[4];
    for (int i = 0; i < 4; i++)
    {
        pid[i] = fork();
        // fprintf(stderr, "fork()!\n");
        // fprintf(stderr, "pid[i]=%d\n", pid[i]);
        if (pid[i] == 0)
        {
            int res = execlp("bf_test_mp_child", "bf_test_mp_child", "2", NULL);
            // int res = execl("./bftest_mp", "bftest_mp", "2", NULL);
            fprintf(stderr, "res=%d\n", errno);
        }
    }

    // parent
    BF_Attach();

    // dirty the data structure of the BF layer
    for (int i = 0; i < max_buf * 4; i++)
    {
        PFpage *page;
        uint64_t dcoords[1] = {i % (max_buf * 2)};
        struct array_key key = {
            array_name, // array name
            "a",        // attribute name
            dcoords,    // data tile coordinates
            1           // length of the coordinates.
        };
        BF_GetBuf(key, &page); // Get Buffer
        BF_TouchBuf(key);      // Touch the buffer
        BF_UnpinBuf(key);      // Unpin the buffer
        // fprintf(stderr, "parent: %d\n", i);
        fprintf(stderr, "\r\tprogress: %03d/%03d (approximate)", i + 1,
                max_buf * 4);
    }
    fprintf(stderr, "\n");

    // wait for childs' dirty actions
    for (int i = 0; i < 4; i++)
        waitpid(pid[i], NULL, 0);

    // test the data structure of BF layer are fine
    assert(BF_CheckValidity() == 0);

    BF_Detach();
    BF_Free();

    // delete array
    delete_array(array_name);

    printf("test_latch passed\n");
}

int shared_mem_tests()
{
    printf("============= Shared mem tests =============\n");
    // test_dlmalloc_limit_memory_space();
    test_read_shared_data_of_child_from_parent();
    test_read_shared_data_of_sibiling();
    test_read_shared_data_of_other_process();
    test_latch();

    return 0;
}