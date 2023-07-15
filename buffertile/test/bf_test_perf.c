// #include <stdio.h>
// #include <math.h>
// #include <sys/time.h>
// #include <unistd.h>
// #include <sys/wait.h>

// #include <tiledb/tiledb.h>

// #include "bf.h"
// #include "utils.h"

// // FIXME: these test cases are carelessly modified, so you have to review the 
// //      test cases before do test.

// void test_perf_pg_get_index_tuple()
// {
//     char *array_name = "__test_perf_pg_get_index_tuple";
//     tiledb_ctx_t *ctx;

//     printf("test_perf_pg_get_index_tuple started\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1, *d2;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int dim_domain[] = {1, 10000, 1, 10000};
//     int tile_extents[] = {1000, 1000};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "idx1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_dimension_alloc(ctx, "idx2", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_domain_add_dimension(ctx, domain, d2);
//     tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

//     tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
//     tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_domain(ctx, array_schema, domain);
//     tiledb_array_schema_add_attribute(ctx, array_schema, a);
//     tiledb_array_create(ctx, array_name, array_schema);

//     tiledb_attribute_free(&a);
//     tiledb_dimension_free(&d1);
//     tiledb_dimension_free(&d2);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Writing array
//      *************************/
//     tiledb_array_t *array;
//     tiledb_query_t *query;
//     tiledb_array_alloc(ctx, array_name, &array);
//     tiledb_array_open(ctx, array, TILEDB_WRITE);

//     uint64_t data_size = sizeof(int) * 10000 * 10000;
//     int *data = malloc(data_size);

//     tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
//     tiledb_query_set_layout(ctx, query, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
//     tiledb_query_submit(ctx, query);

//     tiledb_array_close(ctx, array);
//     tiledb_array_free(&array);
//     tiledb_query_free(&query);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Test BufferTile
//      *************************/

//     BF_Init(); //BF_Attach();                  // Init BF Layer
//     BF_Attach();

//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0, us3 = 0;
//     int max = 128;
//     for (int zz = 0; zz < max; zz++)
//     {
//         BF_Attach();

//         int x = rand() % 9000;
//         int y = rand() % 9000;

//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         for (int i = 0; i < 1000; i++)
//         {
//             for (int j = 0; j < 1000; j++)
//             {
//                 PFpage *page;
//                 uint64_t dcoords[2] = {x / 1000, y / 1000};
//                 struct array_key key = {
//                     array_name, // array name
//                     "a",        // attribute name
//                     dcoords,    // data tile coordinates
//                     2           // length of the coordinates.
//                 };
//                 BF_GetBuf(key, &page);
//                 BF_UnpinBuf(key);
//             }
//         }

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         us3 += (us2 - us1);
//         fprintf(stderr, "\tprogress(approx): %03d/%03d\r", zz + 1, max);
//     }
//     fprintf(stderr, "\n");

//     BF_Detach();
//     BF_Free();

//     printf("test_perf_pg_get_index_tuple passed");
//     printf(", average perf of get_index_tuple() = %f us ",
//            (float)us3 / (max * 1000 * 1000));
//     printf("(Make key, BF_GetBuf(), and BF_UnpinBuf())\n");
//     return;
// }

// void test_perf_pg_get_index_tuple_with_touch()
// {
//     char *array_name = "__test_perf_pg_get_index_tuple_with_touch";
//     tiledb_ctx_t *ctx;

//     printf("test_perf_pg_get_index_tuple_with_touch started\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1, *d2;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int dim_domain[] = {1, 10000, 1, 10000};
//     int tile_extents[] = {1000, 1000};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "idx1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_dimension_alloc(ctx, "idx2", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_domain_add_dimension(ctx, domain, d2);
//     tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

//     tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
//     tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_domain(ctx, array_schema, domain);
//     tiledb_array_schema_add_attribute(ctx, array_schema, a);
//     tiledb_array_create(ctx, array_name, array_schema);

//     tiledb_attribute_free(&a);
//     tiledb_dimension_free(&d1);
//     tiledb_dimension_free(&d2);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Writing array
//      *************************/
//     tiledb_array_t *array;
//     tiledb_query_t *query;
//     tiledb_array_alloc(ctx, array_name, &array);
//     tiledb_array_open(ctx, array, TILEDB_WRITE);

//     uint64_t data_size = sizeof(int) * 10000 * 10000;
//     int *data = malloc(data_size);

//     tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
//     tiledb_query_set_layout(ctx, query, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
//     tiledb_query_submit(ctx, query);

//     tiledb_array_close(ctx, array);
//     tiledb_array_free(&array);
//     tiledb_query_free(&query);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Test BufferTile
//      *************************/

//     BF_Init();
//     BF_Attach(); // Init BF Layer

//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0, us3 = 0;
//     int max = 128;
//     for (int zz = 0; zz < max; zz++)
//     {
//         BF_Attach();

//         int x = rand() % 9000;
//         int y = rand() % 9000;

//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         for (int i = 0; i < 1000; i++)
//         {
//             for (int j = 0; j < 1000; j++)
//             {
//                 PFpage *page;
//                 uint64_t dcoords[2] = {x / 1000, y / 1000};
//                 struct array_key key = {
//                     array_name, // array name
//                     "a",        // attribute name
//                     dcoords,    // data tile coordinates
//                     2           // length of the coordinates.
//                 };
//                 BF_GetBuf(key, &page);
//                 BF_TouchBuf(key);
//                 BF_UnpinBuf(key);
//             }
//         }

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         us3 += (us2 - us1);
//         fprintf(stderr, "\tprogress(approx): %03d/%03d\r", zz + 1, max);
//     }
//     fprintf(stderr, "\n");

//     BF_Detach();
//     BF_Free();

//     printf("test_perf_pg_get_index_tuple_with_touch passed");
//     printf(", average perf of get_index_tuple() with a touch = %f us ",
//            (float)us3 / (max * 1000 * 1000));
//     printf("(Make key, BF_GetBuf(), BF_UnpinBuf(), and BF_TouchBuf())\n");
//     return;
// }

// void test_perf_getbuf_exist()
// {
//     char *array_name = "__test_perf_getbuf_exist";
//     tiledb_ctx_t *ctx;

//     printf("test_perf_getbuf_exist\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int max = 128;
//     int dim_domain[] = {1, 1000 * max};
//     int tile_extents[] = {1000};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "idx1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

//     tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
//     tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_domain(ctx, array_schema, domain);
//     tiledb_array_schema_add_attribute(ctx, array_schema, a);
//     tiledb_array_create(ctx, array_name, array_schema);

//     tiledb_attribute_free(&a);
//     tiledb_dimension_free(&d1);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Writing array
//      *************************/
//     tiledb_array_t *array;
//     tiledb_query_t *query;
//     tiledb_array_alloc(ctx, array_name, &array);
//     tiledb_array_open(ctx, array, TILEDB_WRITE);

//     uint64_t data_size = sizeof(int) * 1000 * max;
//     int *data = malloc(data_size);

//     tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
//     tiledb_query_set_layout(ctx, query, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
//     tiledb_query_submit(ctx, query);

//     tiledb_array_close(ctx, array);
//     tiledb_array_free(&array);
//     tiledb_query_free(&query);

//     /*************************
//      * Test BufferTile
//      *************************/

//     int trial = 4096;
//     int report = 1;
//     FILE *fp = NULL;

//     if (report)
//     {
//         fp = fopen("report_perf_getbuf_exist.csv", "w");
//     }

//     BF_Init();
//     BF_Attach(); // Init BF Layer

//     // read one tile
//     PFpage *page;
//     uint64_t dcoords[1] = {0};
//     struct array_key key = {
//         array_name, // array name
//         "a",        // attribute name
//         dcoords,    // data tile coordinates
//         1           // length of the coordinates.
//     };
//     BF_GetBuf(key, &page);
//     BF_UnpinBuf(key);

//     // perf test
//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0, us3 = 0;
//     for (int zz = 0; zz < trial; zz++)
//     {

//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         // read already loaded tile (no I/O)
//         BF_GetBuf(key, &page);
//         BF_UnpinBuf(key);

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         us3 += (us2 - us1);

//         fprintf(fp, "%d,%lu\n", zz, us2 - us1);
//         fprintf(stderr, "\tprogress(approx): %05d/%05d\r", zz + 1, trial);
//     }
//     fprintf(stderr, "\n");

//     BF_Detach();
//     BF_Free();

//     if (report)
//     {
//         fclose(fp);
//     }

//     printf("test_perf_getbuf_exist passed");
//     printf(", average perf = %f us\n", (float)us3 / trial);
//     return;
// }

// void test_perf_getbuf_no_evict_and_read()
// {
//     char *array_name = "__test_perf_getbuf_no_evict_and_read";
//     tiledb_ctx_t *ctx;

//     printf("test_perf_getbuf_no_evict_and_read\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int tile_size = 1000;
//     int num_of_tiles = (MAX_BUF_MEM_SIZE / (tile_size * sizeof(int)));   // approximated
//     int dim_domain[] = {1, tile_size * num_of_tiles};
//     int tile_extents[] = {tile_size};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "idx1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

//     tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
//     tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_domain(ctx, array_schema, domain);
//     tiledb_array_schema_add_attribute(ctx, array_schema, a);
//     tiledb_array_create(ctx, array_name, array_schema);

//     tiledb_attribute_free(&a);
//     tiledb_dimension_free(&d1);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Writing array
//      *************************/
//     tiledb_array_t *array;
//     tiledb_query_t *query;
//     tiledb_array_alloc(ctx, array_name, &array);
//     tiledb_array_open(ctx, array, TILEDB_WRITE);

//     uint64_t data_size = sizeof(int) * tile_size * num_of_tiles;
//     int *data = malloc(data_size);

//     tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
//     tiledb_query_set_layout(ctx, query, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
//     tiledb_query_submit(ctx, query);

//     tiledb_array_close(ctx, array);
//     tiledb_array_free(&array);
//     tiledb_query_free(&query);

//     /*************************
//      * Test BufferTile
//      *************************/

//     int trial = 4096;
//     int report = 1;
//     FILE *fp = NULL;

//     if (report)
//     {
//         fp = fopen("report_perf_getbuf_no_evict_and_read.csv", "w");
//     }

//     BF_Init();
//     BF_Attach(); // Init BF Layer

//     // perf test
//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0, us3 = 0;
//     for (int zz = 0; zz < trial; zz++)
//     {
//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         // read a tile
//         PFpage *page;
//         uint64_t dcoords[1] = {zz % num_of_tiles};
//         struct array_key key = {
//             array_name, // array name
//             "a",        // attribute name
//             dcoords,    // data tile coordinates
//             1           // length of the coordinates.
//         };
//         BF_GetBuf(key, &page);
//         BF_UnpinBuf(key);

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         us3 += (us2 - us1);

//         fprintf(fp, "%d,%lu\n", zz, us2 - us1);
//         fprintf(stderr, "\tprogress(approx): %05d/%05d\r", zz + 1, trial);

//         // reset bf before bf layer fully filled
//         if (zz % (num_of_tiles / 8) == 0)
//         {
//             // reset BF layer
//             BF_Detach();
//             BF_Free();
//             BF_Init();
//             BF_Attach();
//         }
//     }
//     fprintf(stderr, "\n");

//     BF_Detach();
//     BF_Free();

//     if (report)
//     {
//         fclose(fp);
//     }

//     printf("test_perf_getbuf_no_evict_and_read passed");
//     printf(", average perf = %f us\n", (float)us3 / trial);
//     return;
// }

// void test_perf_getbuf_evict_clean_and_read()
// {
//     char *array_name = "__test_perf_getbuf_evict_clean_and_read";
//     tiledb_ctx_t *ctx;

//     printf("test_perf_getbuf_evict_clean_and_read\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int tile_size = 1000;
//     int num_of_tiles = (MAX_BUF_MEM_SIZE / (tile_size * sizeof(int))) + 1;   // approximated
//     int dim_domain[] = {1, tile_size * num_of_tiles};
//     int tile_extents[] = {tile_size};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "idx1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

//     tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
//     tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_domain(ctx, array_schema, domain);
//     tiledb_array_schema_add_attribute(ctx, array_schema, a);
//     tiledb_array_create(ctx, array_name, array_schema);

//     tiledb_attribute_free(&a);
//     tiledb_dimension_free(&d1);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Writing array
//      *************************/
//     tiledb_array_t *array;
//     tiledb_query_t *query;
//     tiledb_array_alloc(ctx, array_name, &array);
//     tiledb_array_open(ctx, array, TILEDB_WRITE);

//     uint64_t data_size = sizeof(int) * tile_size * num_of_tiles;
//     int *data = malloc(data_size);

//     tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
//     tiledb_query_set_layout(ctx, query, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
//     tiledb_query_submit(ctx, query);

//     tiledb_array_close(ctx, array);
//     tiledb_array_free(&array);
//     tiledb_query_free(&query);

//     /*************************
//      * Test BufferTile
//      *************************/

//     int trial = 4096;
//     int report = 1;
//     FILE *fp = NULL;

//     if (report)
//     {
//         fp = fopen("report_perf_getbuf_evict_clean_and_read.csv", "w");
//     }

//     BF_Init();
//     BF_Attach(); // Init BF Layer

//     // fill BF layer
//     for (int zz = 1; zz < num_of_tiles; zz++)
//     {
//         PFpage *page;
//         uint64_t dcoords[1] = {zz};
//         struct array_key key = {
//             array_name, // array name
//             "a",        // attribute name
//             dcoords,    // data tile coordinates
//             1           // length of the coordinates.
//         };
//         BF_GetBuf(key, &page);
//         BF_UnpinBuf(key);
//     }

//     // fprintf(stderr, "start!!!\n"); // for case 2
//     // perf test
//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0, us3 = 0;
//     for (int zz = 0; zz < trial; zz++)
//     {
//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         // read a tile
//         PFpage *page;
//         uint64_t dcoords[1] = {zz % num_of_tiles};
//         struct array_key key = {
//             array_name, // array name
//             "a",        // attribute name
//             dcoords,    // data tile coordinates
//             1           // length of the coordinates.
//         };
//         BF_GetBuf(key, &page);
//         BF_UnpinBuf(key);

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         us3 += (us2 - us1);

//         fprintf(fp, "%d,%lu\n", zz, us2 - us1);
//         fprintf(stderr, "\tprogress(approx): %05d/%05d\r", zz + 1, trial);
//     }
//     fprintf(stderr, "\n");

//     BF_Detach();
//     BF_Free();

//     if (report)
//     {
//         fclose(fp);
//     }

//     printf("test_perf_getbuf_evict_clean_and_read passed");
//     printf(", average perf = %f us\n", (float)us3 / trial);
//     return;
// }

// void test_perf_getbuf_evict_dirty_and_read()
// {
//     char *array_name = "__test_perf_getbuf_evict_dirty_and_read";
//     tiledb_ctx_t *ctx;

//     printf("test_perf_getbuf_evict_dirty_and_read\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int tile_size = 1000;
//     int num_of_tiles = (MAX_BUF_MEM_SIZE / (tile_size * sizeof(int))) + 1;   // approximated
//     int dim_domain[] = {1, tile_size * num_of_tiles};
//     int tile_extents[] = {tile_size};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "idx1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

//     tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
//     tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_array_schema_set_domain(ctx, array_schema, domain);
//     tiledb_array_schema_add_attribute(ctx, array_schema, a);
//     tiledb_array_create(ctx, array_name, array_schema);

//     tiledb_attribute_free(&a);
//     tiledb_dimension_free(&d1);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);

//     // tiledb_ctx_free(&ctx);

//     /*************************
//      * Writing array
//      *************************/
//     tiledb_array_t *array;
//     tiledb_query_t *query;
//     tiledb_array_alloc(ctx, array_name, &array);
//     tiledb_array_open(ctx, array, TILEDB_WRITE);

//     uint64_t data_size = sizeof(int) * tile_size * num_of_tiles;
//     int *data = malloc(data_size);

//     tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
//     tiledb_query_set_layout(ctx, query, TILEDB_TILESTORE_ROW_MAJOR);
//     tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
//     tiledb_query_submit(ctx, query);

//     tiledb_array_close(ctx, array);
//     tiledb_array_free(&array);
//     tiledb_query_free(&query);

//     /*************************
//      * Test BufferTile
//      *************************/

//     int trial = 1024;
//     int report = 1;
//     FILE *fp = NULL;

//     if (report)
//     {
//         fp = fopen("report_perf_getbuf_evict_dirty_and_read.csv", "w");
//     }

//     BF_Init();
//     BF_Attach(); // Init BF Layer

//     // fill BF layer
//     for (int zz = 1; zz < num_of_tiles; zz++)
//     {
//         PFpage *page;
//         uint64_t dcoords[1] = {zz};
//         struct array_key key = {
//             array_name, // array name
//             "a",        // attribute name
//             dcoords,    // data tile coordinates
//             1           // length of the coordinates.
//         };
//         BF_GetBuf(key, &page);
//         BF_TouchBuf(key);
//         BF_UnpinBuf(key);
//     }

//     // perf test
//     // fprintf(stderr, "start!!!\n"); // for case 2
//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0, us3 = 0;
//     for (int zz = 0; zz < trial; zz++)
//     {
//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         // write a tile and read a tile
//         PFpage *page;
//         uint64_t dcoords[1] = {zz % num_of_tiles};
//         struct array_key key = {
//             array_name, // array name
//             "a",        // attribute name
//             dcoords,    // data tile coordinates
//             1           // length of the coordinates.
//         };
//         BF_GetBuf(key, &page);
//         BF_TouchBuf(key);
//         BF_UnpinBuf(key);

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         us3 += (us2 - us1);

//         if (report)
//             fprintf(fp, "%d,%lu\n", zz, us2 - us1);
//         fprintf(stderr, "\tprogress(approx): %05d/%05d\r", zz + 1, trial);
//     }
//     fprintf(stderr, "\n");

//     BF_Detach();
//     BF_Free();

//     if (report)
//     {
//         fclose(fp);
//     }

//     printf("test_perf_getbuf_evict_dirty_and_read passed");
//     printf(", average perf = %f us\n", (float)us3 / trial);
//     return;
// }

// /*
// void speed_test() {
//     BF_Init();                          // BF init
//     // read_array2("hello_world");
//     struct timeval tv;
//     unsigned long us1 = 0, us2 = 0;
//     unsigned long bf = 0, bf2 = 0;
//     for (int i = 0; i < 128; i++) {
//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         read_array("hello_world");

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         // system("echo 3 > /proc/sys/vm/drop_caches");
//         bf2 += (us2 - us1);
//     }

//     for (int i = 0; i < 128; i++) {
//         gettimeofday(&tv, NULL);
//         us1 = 1000000 * tv.tv_sec + tv.tv_usec;

//         test_read_all();

//         gettimeofday(&tv, NULL);
//         us2 = 1000000 * tv.tv_sec + tv.tv_usec;
//         // system("echo 3 > /proc/sys/vm/drop_caches");
//         bf += (us2 - us1);
//     }

//     printf("BF: %ld\n", bf);
//     printf("Normal: %ld\n", bf2);

//     BF_Free();

//     return 0;
// }
// */

// void perf_test()
// {
//     test_perf_pg_get_index_tuple();
//     test_perf_pg_get_index_tuple_with_touch();

//     test_perf_getbuf_exist();                // no I/O
//     test_perf_getbuf_no_evict_and_read();    // one read
//     test_perf_getbuf_evict_clean_and_read(); // one read
//     test_perf_getbuf_evict_dirty_and_read(); // one write and one read
// }