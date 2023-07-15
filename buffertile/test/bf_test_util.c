#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"


/*********************
 * Util tests
 *********************/


void test_get_dcoords_from_ccoords()
{
    char *array_name = "__test_get_dcoords_from_ccoords";

    printf("test_get_dcoords_from_ccoords started\n");

    /*************************
     * Create Array
     *************************/
    uint32_t num_of_dim = 3;
    uint64_t array_size[] = {16, 16, 16};
    uint64_t tile_extents[] = {2, 2, 2};

    // create array
    tilestore_create_array(
        array_name,
        array_size,
        tile_extents,
        num_of_dim,
        TILESTORE_INT32,
        TILESTORE_DENSE);

    // test

    uint64_t *dcoords;
    int64_t ccoords[3] = {0, 0, 0};
    storage_util_get_dcoords_from_ccoords(array_name, "a", ccoords, &dcoords);

    assert(dcoords[0] == 0);
    assert(dcoords[1] == 0);
    assert(dcoords[2] == 0);

    free(dcoords);

    ccoords[0] = 15, ccoords[1] = 15, ccoords[2] = 15;
    storage_util_get_dcoords_from_ccoords(array_name, "a", ccoords, &dcoords);

    assert(dcoords[0] == 7);
    assert(dcoords[1] == 7);
    assert(dcoords[2] == 7);

    free(dcoords);

    storage_util_delete_array(array_name);
    printf("test_get_dcoords_from_ccoords passed\n");
}

// void test_get_selected_dim_by_names()
// {

//     const char *array_name = "__test_get_selected_dim_by_names";
//     tiledb_ctx_t *ctx;

//     printf("test_get_selected_dim_by_names started\n");

//     /*************************
//      * Create Array
//      *************************/
//     tiledb_dimension_t *d1, *d2, *d3, *d4, *d5;
//     tiledb_domain_t *domain;
//     tiledb_attribute_t *a;
//     tiledb_array_schema_t *array_schema;

//     int dim_domain[] = {1, 128, 1, 128, 1, 128, 1, 128, 1, 128};
//     int tile_extents[] = {16, 16, 16, 16, 16};

//     tiledb_ctx_alloc(NULL, &ctx);
//     tiledb_dimension_alloc(ctx, "d1", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
//     tiledb_dimension_alloc(ctx, "d2", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);
//     tiledb_dimension_alloc(ctx, "d3", TILEDB_INT32, &dim_domain[4], &tile_extents[2], &d3);
//     tiledb_dimension_alloc(ctx, "d4", TILEDB_INT32, &dim_domain[6], &tile_extents[3], &d4);
//     tiledb_dimension_alloc(ctx, "d5", TILEDB_INT32, &dim_domain[8], &tile_extents[4], &d5);
//     tiledb_domain_alloc(ctx, &domain);
//     tiledb_domain_add_dimension(ctx, domain, d1);
//     tiledb_domain_add_dimension(ctx, domain, d2);
//     tiledb_domain_add_dimension(ctx, domain, d3);
//     tiledb_domain_add_dimension(ctx, domain, d4);
//     tiledb_domain_add_dimension(ctx, domain, d5);
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
//     tiledb_dimension_free(&d3);
//     tiledb_dimension_free(&d4);
//     tiledb_dimension_free(&d5);
//     tiledb_domain_free(&domain);
//     tiledb_array_schema_free(&array_schema);
//     tiledb_ctx_free(&ctx);

//     /*************************
//      * Test functionality
//      *************************/
//     const char *selected_dim_names[] = {"d1", "d4", "d5"};
//     uint8_t *selected_dim;

//     BF_Init();
//     BF_Attach();
//     storage_util_get_selected_dim_by_names(array_name, selected_dim_names, 5, 3, &selected_dim);
//     assert(selected_dim[0] == 1);
//     assert(selected_dim[1] == 0);
//     assert(selected_dim[2] == 0);
//     assert(selected_dim[3] == 1);
//     assert(selected_dim[4] == 1);

//     storage_util_free_selected(&selected_dim);

//     storage_util_delete_array(array_name);
//     tiledb_free_global_context();

//     BF_Detach();
//     BF_Free();

//     printf("test_get_selected_dim_by_names passed\n");
//     return;
// }

void test_util_get_attribute_nullability()
{
    char *array_name = "__test_util_get_attribute_nullability";

    printf("test_util_get_attribute_nullability started\n");

    /*************************
     * Test functionality
     *************************/
    uint8_t nullability;
    storage_util_get_attribute_nullability("whatever", "whatever", &nullability);
    assert(nullability == 0);

    storage_util_delete_array(array_name);

    printf("test_util_get_attribute_nullability passed\n");

    return;
}

void test_util_comapre() {
    printf("test_util_comapre started\n");

    // several tests...
    uint64_t lhs_coordinates[3], rhs_coordinates[3];
    
    // 1D
    lhs_coordinates[0] = 3, rhs_coordinates[0] = 3;
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 1, TILESTORE_ROW_MAJOR) == COMP_EQUAL);

    lhs_coordinates[0] = 1, rhs_coordinates[0] = 3;
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 1, TILESTORE_ROW_MAJOR) == COMP_RHS_IS_BIGGER);

    lhs_coordinates[0] = 3, rhs_coordinates[0] = 1;
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 1, TILESTORE_ROW_MAJOR) == COMP_LHS_IS_BIGGER);

    lhs_coordinates[0] = 3, rhs_coordinates[0] = 3;
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 1, TILESTORE_COL_MAJOR) == COMP_EQUAL);

    lhs_coordinates[0] = 1, rhs_coordinates[0] = 3;
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 1, TILESTORE_COL_MAJOR) == COMP_RHS_IS_BIGGER);

    lhs_coordinates[0] = 3, rhs_coordinates[0] = 1;
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 1, TILESTORE_COL_MAJOR) == COMP_LHS_IS_BIGGER);

    // 3D
    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_ROW_MAJOR) == COMP_EQUAL);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 4; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_ROW_MAJOR) == COMP_RHS_IS_BIGGER);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 4, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_ROW_MAJOR) == COMP_RHS_IS_BIGGER);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 4; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_ROW_MAJOR) == COMP_LHS_IS_BIGGER);

    lhs_coordinates[0] = 4, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_ROW_MAJOR) == COMP_LHS_IS_BIGGER);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_COL_MAJOR) == COMP_EQUAL);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 4; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_COL_MAJOR) == COMP_RHS_IS_BIGGER);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 4, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_COL_MAJOR) == COMP_RHS_IS_BIGGER);

    lhs_coordinates[0] = 3, lhs_coordinates[1] = 3, lhs_coordinates[2] = 4; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_COL_MAJOR) == COMP_LHS_IS_BIGGER);

    lhs_coordinates[0] = 4, lhs_coordinates[1] = 3, lhs_coordinates[2] = 3; 
    rhs_coordinates[0] = 3, rhs_coordinates[1] = 3, rhs_coordinates[2] = 3; 
    assert(bf_util_compare_coords(lhs_coordinates, rhs_coordinates, 3, TILESTORE_COL_MAJOR) == COMP_LHS_IS_BIGGER);

    printf("test_util_comapre passed\n");
}


int util_tests()
{
    printf("============= Util tests =============\n");
    test_get_dcoords_from_ccoords();
    // test_get_selected_dim_by_names();
    test_util_get_attribute_nullability();
    test_util_comapre();
}