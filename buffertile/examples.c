#include <stdio.h>
#include <math.h>
#include <sys/time.h>

#include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"


void create_array(char *array_name) {
    // Create TileDB context
    tiledb_ctx_t *ctx;
    int dim_domain[] = {1, 128, 1, 128};
    int tile_extents[] = {16, 16};

    tiledb_ctx_alloc(NULL, &ctx);
    tiledb_dimension_t *d1;
    tiledb_dimension_alloc(ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
    tiledb_dimension_t *d2;
    tiledb_dimension_alloc(ctx, "cols", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);

    // Create domain
    tiledb_domain_t *domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, d1);
    tiledb_domain_add_dimension(ctx, domain, d2);

    // Create a single attribute "a" so each (i,j) cell can store an integer
    tiledb_attribute_t *a1, *a2;
    tiledb_attribute_alloc(ctx, "a1", TILEDB_INT32, &a1);
    tiledb_attribute_alloc(ctx, "a2", TILEDB_INT32, &a2);

    // Create array schema
    tiledb_array_schema_t *array_schema;
    tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
    tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
    tiledb_array_schema_set_domain(ctx, array_schema, domain);
    tiledb_array_schema_add_attribute(ctx, array_schema, a1);
    tiledb_array_schema_add_attribute(ctx, array_schema, a2);

    // Create array
    tiledb_array_create(ctx, array_name, array_schema);

    // Clean up
    tiledb_attribute_free(&a1);
    tiledb_attribute_free(&a2);
    tiledb_dimension_free(&d1);
    tiledb_dimension_free(&d2);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);
    tiledb_ctx_free(&ctx);
}

void write_array(const char *array_name) {
    // Create TileDB context
    tiledb_ctx_t *ctx;
    tiledb_ctx_alloc(NULL, &ctx);

    // Open array for writing
    tiledb_array_t *array;
    tiledb_array_alloc(ctx, array_name, &array);
    tiledb_array_open(ctx, array, TILEDB_WRITE);

    // Prepare some data for the array
    int data1[16384], data2[16384];
    for (int i = 0; i < 16384; i++) {
        data1[i] = -i;
        data2[i] = i;
    }
    uint64_t data_size = sizeof(data1);

    // Create the query
    tiledb_query_t *query;
    tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
    tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
    tiledb_query_set_data_buffer(ctx, query, "a1", data1, &data_size);
    tiledb_query_set_data_buffer(ctx, query, "a2", data2, &data_size);

    // Submit query
    tiledb_query_submit(ctx, query);

    // Close array
    tiledb_array_close(ctx, array);

    // Clean up
    tiledb_array_free(&array);
    tiledb_query_free(&query);
    tiledb_ctx_free(&ctx);
}

void read_array(const char* array_name) {
    // Create TileDB context
    static tiledb_ctx_t* ctx;
    if (ctx == NULL) tiledb_ctx_alloc(NULL, &ctx);

    // Open array for reading
    tiledb_array_t* array;
    tiledb_array_alloc(ctx, array_name, &array);
    tiledb_array_open(ctx, array, TILEDB_READ);

    // Slice only rows 1, 2 and cols 2, 3, 4
    int subarray[] = {1, 128, 1, 128};

    // Prepare the vector that will hold the result (of size 6 elements)
    int data1[1048576];
    uint64_t data_size = sizeof(data1);

    // Create query
    tiledb_query_t* query;
    tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
    tiledb_query_set_subarray(ctx, query, subarray);
    tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
    tiledb_query_set_data_buffer(ctx, query, "a1", data1, &data_size);
    // tiledb_query_set_data_buffer(ctx, query, "a2", data2, &data_size);

    // Submit query
    tiledb_query_submit(ctx, query);

    // Close array
    tiledb_array_close(ctx, array);

    // Print out the results.
    uint64_t test = 0;
    for (int i = 0; i < 16384; i++) test++;
    // printf("\n");

    // Clean up
    tiledb_array_free(&array);
    tiledb_query_free(&query);
    // tiledb_ctx_free(&ctx);

    printf("%ld\n", test);
}

void read_array2(const char* array_name) {
    // Create TileDB context
    tiledb_ctx_t* ctx;
    tiledb_ctx_alloc(NULL, &ctx);

    // Open array for reading
    tiledb_array_t* array;
    tiledb_array_alloc(ctx, array_name, &array);
    tiledb_array_open(ctx, array, TILEDB_READ);

    int subarray[] = {1, 16, 1, 16};

    // Prepare the vector that will hold the result (of size 6 elements)
    int data[256];
    uint64_t data_size = sizeof(data);

    // Create query
    tiledb_query_t* query;
    tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
    tiledb_query_set_subarray(ctx, query, subarray);
    tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
    tiledb_query_set_data_buffer(ctx, query, "a1", data, &data_size);

    // Submit query
    tiledb_query_submit(ctx, query);

    // Close array
    tiledb_array_close(ctx, array);

    // Print out the results.
    for (int i = 0; i < 256; i++) printf("%d ", data[i]);
    printf("\n");

    // Clean up
    tiledb_array_free(&array);
    tiledb_query_free(&query);
    tiledb_ctx_free(&ctx);
}