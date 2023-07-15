#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "tilestore.h"
#include "bf_struct.h"
#include "tileio.h"
#include "tilestore.h"


// define for the compare function
#define COMP_EQUAL 0
#define COMP_LHS_IS_BIGGER -1
#define COMP_RHS_IS_BIGGER 1


/**************************************************
 * Utils for Storage Engine
 **************************************************/

/**
 * @brief Get attribute names in an array.
 *
 * @param array_name The name of the array.
 * @param attr_names (return) The names of the attributes. It should be freed
 *      by storage_util_free_attr_names() or the caller.
 * @param attr_len (return) The number of attributes. It should be freed by
 *      storage_util_free_attr_names() or the caller.
 * @return int error code (0 if successful).
 */
int storage_util_get_attr_names(
    const char *array_name,
    char ***attr_names,
    uint32_t *attr_len);

/**
 * @brief Get the domain of dimensions in an array.
 *
 * @param array_name The name of the array.
 * @param dim_domains (return) The domains of the dimensions. It is a 2D array.
 *      It should be freed by storage_util_free_dim_domains() or the caller.
 * @param dim_len (return) The number of dimensions. It should be freed by
 *      storage_util_free_dim_domains() or the caller.
 * @return int error code (0 if successful).
 */
int storage_util_get_dim_domains(
    const char *array_name,
    uint64_t ***dim_domains,
    uint32_t *dim_len);

/**
 * @brief Get the real (unpadded) domain of dimensions in an array.
 *
 * @param array_name The name of the array.
 * @param dim_domains (return) The domains of the dimensions. It is a 2D array.
 *      It should be freed by storage_util_free_dim_domains() or the caller.
 * @param dim_len (return) The number of dimensions. It should be freed by
 *      storage_util_free_dim_domains() or the caller.
 * @return int error code (0 if successful).
 */
int storage_util_get_unpadded_dim_domains(
    const char *array_name, 
    uint64_t ***dim_domains, 
    uint32_t *dim_len);

/**
 * @brief Get the tile extents of dimensions in a TileDB array.
 *
 * @param array_name The name of the TileDB array.
 * @param tile_extents (return) The tile extents of the dimensions. It is a
 *      1D array. It should be freed by storage_util_free_tile_extents() or the
 *      caller.
 * @param dim_len (return) The number of dimensions. It should be freed by
 *      storage_util_free_tile_extents() or the caller.
 * @return int error code (0 if successful).
 */
int storage_util_get_tile_extents(
    const char *array_name,
    uint64_t **tile_extents,
    uint32_t *dim_len);

/**
 * @brief Get the size of an array in terms of tile.
 * For example, if an array size is 6x6 and the tile size is 2x3, then the
 *      return value is [3, 2].
 *
 * @param dim_domains The domains of the dimensions. It is a 2D array.
 * @param tile_extents The tile extents of the dimensions. It is a 1D array.
 * @param dim_len The number of dimensions.
 * @param dcoord_lens (return) The size of the array in terms of space tile.
 *      It should be freed by storage_util_free_dcoord_lens() or the caller.
 * @return int error code (0 if successful).
 */
int storage_util_get_dcoord_lens(
    uint64_t **dim_domains,
    uint64_t *tile_extents,
    uint32_t dim_len,
    uint64_t **dcoord_lens);

/**
 * @brief Get the length in each cell.
 *
 * @param array_name The name of the array.
 * @param attr_name The name of the attribute.
 * @param cell_len (return) The length of each cell. If the cell is
 *      variable-length, then the length is TILEDB_VAR_NUM.
 * @return int error code (0 if successful).
 */
int storage_util_get_cell_length(
    const char *array_name,
    char *attr_name,
    uint32_t *cell_len);

/**
 * @brief Get the array type of an array.
 *
 * @param array_name The name of the TileDB array.
 * @param array_type (return) The array type.
 * @return int error code (0 if successful).
 */
int storage_util_get_array_type(
    const char *array_name,
    tilestore_format_t *array_type);

/**
 * @brief Get the data type of the arrtibute.
 *
 * @param array_name The name of the array.
 * @param attr_name The attribute name.
 * @param attr_type (return) The data type of the attribute.
 * @return int error code (0 if successful).
 */
int storage_util_get_attribute_type(
    const char *array_name,
    const char *attr_name,
    tilestore_datatype_t *attr_type);

/**
 * @brief Get the data type of the arrtibute.
 *
 * @param array_name The name of the TileDB array.
 * @param attr_name The attribute name.
 * @param attr_type (return) The data type of the attribute.
 * @return int error code (0 if successful).
 */
int storage_util_get_attribute_nullability(
    const char *array_name,
    const char *attr_name,
    uint8_t *nullable);

/**
 * @brief Get the cell order of the array.
 *
 * @param array_name The name of the TileDB array.
 * @param cell_order (return) The cell order of the array. (row/column major)
 * @return int error code (0 if successful).
 */
int storage_util_get_cell_order(
    const char *array_name,
    tilestore_layout_t *cell_order);

/**
 * @brief Get the tile coordinates from the cell coordinates.
 *
 * TODO: Should we consider the case where the cell coordinates are not integer?
 * TODO: int64_t for the cell coordinates is fine?
 *
 * @param array_name The name of the TileDB array.
 * @param attr_name The attribute name.
 * @param ccoords The cell coordinates.
 * @param dcoords (return) The tile coordinates.
 *      It should be freed by the caller.
 * @return int error code (0 if successful).
 */
int storage_util_get_dcoords_from_ccoords(
    const char *array_name,
    const char *attr_name,
    int64_t *ccoords,
    uint64_t **dcoords);

/**
 * @brief Deallocate the memory made by storage_util_get_attr_names().
 *
 * @param attr_names The names of the attributes.
 * @param attr_len The number of attributes.
 * @return int error code (0 if successful).
 */
int storage_util_free_attr_names(
    char ***attr_names,
    uint32_t attr_len);

/**
 * @brief Deallocate the memory made by tile_util_get_dim_domains().
 *
 * @param dim_domains The domains of the dimensions.
 * @param dim_len The number of dimensions.
 * @return int error code (0 if successful).
 */
int storage_util_free_dim_domains(
    uint64_t ***dim_domains,
    uint32_t dim_len);

/**
 * @brief Deallocate the memory made by storage_util_get_tile_extents().
 *
 * @param tile_extents The tile extents of the dimensions.
 * @param dim_len The number of dimensions.
 * @return int error code (0 if successful).
 */
int storage_util_free_tile_extents(
    uint64_t **tile_extents,
    uint32_t dim_len);

/**
 * @brief Deallocate the memory made by storage_util_get_dcoord_lens().
 *
 * @param dcoord_lens The size of the array in terms of space tile.
 * @return int error code (0 if successful).
 */
int storage_util_free_dcoord_lens(
    uint64_t **dcoord_lens);

/**
 * @brief Deallocate the memory made by storage_util_get_selected_dim_by_names().
 *
 * @param selected The list of 0 and 1 indicating whether the dimension is selected or not.
 * @return int error code (0 if successful).
 * */
int storage_util_free_selected(
    uint8_t **selected);

/**
 * @brief Return index of the selected dimensions
 *
 * @param array_name Name of the array.
 * @param dim_names List of dimension names.
 * @param dim_len The number of dimensions.
 * @param selected_len The number of selected dimensions.
 * @param selected (return) List of 0 or 1. 0 for not selected, 1 for selected.
 * @return int error code (0 if successful).
 */
int storage_util_get_selected_dim_by_names(
    const char *array_name,
    const char **selected_dim_names,
    uint32_t dim_len,
    uint32_t selected_len,
    uint8_t **selected);

// Array create/delete utils

/**
 * @brief Create a TileDB array.
 *
 * @param array_name the array name.
 * @param array_type the type of array (dense or sparse).
 * @param dim_domains the domains for each dimensions.
 * @param tile_extents the tile extent for each dimensions.
 * @param num_of_dimensions the number of dimensions.
 * @param attribute_length the length of attribute.
 * @param attribute_type the type of attribute.
 * @param nullable the boolean value indicates is it nullable aray.
 * @return int error code (0 if successful).
 */
int storage_util_create_array(
    const char *array_name,
    tilestore_format_t array_type,
    int *dim_domain,
    int *tile_extents,
    uint32_t num_of_dimensions,
    uint32_t attribute_length,
    tilestore_datatype_t attribute_type,
    uint8_t nullable);

/**
 * @brief Delete a TileDB array.
 *
 * @param array_name the array name.
 * @return int error code (0 if successful).
 */
int storage_util_delete_array(const char *arrayname);

/**************************************************
 * BufferTile page utils
 **************************************************/

/**
 * @brief Get the offset-th char value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @return char The offset-th value.
 */
char bf_util_pagebuf_get_char(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the offset-th chars values (char*) in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @return char* The offset-th value.
 */
char *bf_util_pagebuf_get_chars(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the offset-th int value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @return int The offset-th value.
 */
int bf_util_pagebuf_get_int(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the offset-th float value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @return float The offset-th value.
 */
float bf_util_pagebuf_get_float(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the offset-th double value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @return double The offset-th value.
 */
double bf_util_pagebuf_get_double(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the offset-th validity bit corresponding to the value in the
 *      pagebuf.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @return bool_t The offset-th validity bit.
 */
bool_t bf_util_pagebuf_get_validity(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the length of the validitiy values.
 *
 * @param page Page.
 * @return uint64_t The length of the validitiy values in bytes.
 */
uint64_t bf_util_pagebuf_get_validity_len(
    PFpage *page);

/**
 * @brief Get the coordinates list of the dimension. Please be aware that the
 *      coordinates are for the dimension rather than a cell.
 *
 * @param page Page.
 * @param dim_idx The index of the dimension.
 * @return void* The coordinates list of the dimension.
 */
void *bf_util_pagebuf_get_coords(
    PFpage *page,
    uint64_t dim_idx);

/**
 * @brief Get the lengths of each coordinates list of the dimension in bytes.
 *
 * @param page Page.
 * @return uint64_t* the lengths of each coordinates.
 */
uint64_t *bf_util_pagebuf_get_coords_lens(
    PFpage *page);

/**
 * @brief Get the length of pagebuf in bytes.
 *
 * @param page Page.
 * @return uint64_t the length of pagebuf.
 */
uint64_t bf_util_pagebuf_get_len(
    PFpage *page);

/**
 * @brief Get the offset value of the cell in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *     (i.e., first is 0, and second is 1).
 * @return uint64_t the offset value in bytes
 *     (e.g., first is 0, and second is 4 for the int).
 */
uint64_t bf_util_pagebuf_get_offset(
    PFpage *page,
    uint64_t offset);

/**
 * @brief Get the length of the offset values.
 *
 * @param page Page.
 * @return uint64_t The length of the offset values in bytes.
 */
uint64_t bf_util_pagebuf_get_offset_len(
    PFpage *page);

/**
 * @brief Get the length of the cell in the page. It is useful when you deal
 *      with variable-length attribute.
 *
 * @param page Page.
 * @param idx The index of the cell.
 * @return uint64_t The length of the cell in bytes.
 */
uint64_t bf_util_pagebuf_get_cell_length(
    PFpage *page,
    uint64_t idx);

/**
 * @brief Get the offset of the cell in tha page. It is useful when you deal
 *      with variable-length attribute.
 *
 * @param page Page.
 * @param idx The index of the cell.
 * @return uint64_t The length of the cell in bytes.
 */
uint64_t bf_util_pagebuf_get_cell_offset(
    PFpage *page,
    uint64_t idx);

/**
 * @brief Get the unfilled_idx of the page.
 *
 * @param page Page.
 * @return uint64_t The unfilled_idx of the page.
 */
uint64_t bf_util_pagebuf_get_unfilled_idx(
    PFpage *page);

/**
 * @brief Get the unfilled_pagebuf_offset of the page in bytes.
 *
 * @param page Page.
 * @return uint64_t The unfilled_pagebuf_offset of the page in bytes.
 */
uint64_t bf_util_pagebuf_get_unfilled_pagebuf_offset(
    PFpage *page);

/**
 * @brief Return coordinates for a cell that has given idx of a page.
 *      A user should free the return pointer.
 *
 * @param page Page.
 * @param num_of_dims the number of dimensions.
 * @param idx the idx of the cell.
 * @return coordinates for a cell in uint64_t. A user should free the return pointer.
 */
uint64_t *bf_util_get_coords_of_a_cell_of_idx_uint64(
    PFpage *page,
    uint32_t num_of_dims,
    uint64_t idx);

/**
 * @brief Return coordinates for a cell that has given idx of a page.
 *      A user should free the return pointer.
 *
 * @param page Page.
 * @param num_of_dims the number of dimensions.
 * @param idx the idx of the cell.
 * @return coordinates for a cell in int32_t. A user should free the return pointer.
 */
int32_t *bf_util_get_coords_of_a_cell_of_idx_int32(
    PFpage *page,
    uint32_t num_of_dims,
    uint64_t idx);

/**
 * @brief Set the offset-th char value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @param value The value to be set.
 */
void bf_util_pagebuf_set_char(
    PFpage *page,
    uint64_t offset,
    char value);

/**
 * @brief Set the offset-th int value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @param value The value to be set.
 */
void bf_util_pagebuf_set_int(
    PFpage *page,
    uint64_t offset,
    int value);

/**
 * @brief Set the offset-th float value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @param value The value to be set.
 */
void bf_util_pagebuf_set_float(
    PFpage *page,
    uint64_t offset,
    float value);

/**
 * @brief Set the offset-th double value in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @param value The value to be set.
 */
void bf_util_pagebuf_set_double(
    PFpage *page,
    uint64_t offset,
    double value);

/**
 * @brief Set the offset-th validity bit in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *      (i.e., first is 0, and second is 1).
 * @param value The value to be set.
 */
void bf_util_pagebuf_set_validity(
    PFpage *page,
    uint64_t offset,
    bool_t value);

/**
 * @brief Set the offset value of the cell in the page.
 *
 * @param page Page.
 * @param offset The index of the value in number
 *     (i.e., first is 0, and second is 1).
 * @param value the offset value in bytes
 *     (e.g., first is 0, and second is 4 for the int).
 */
void bf_util_pagebuf_set_offset(
    PFpage *page,
    uint64_t offset,
    uint64_t value);

/**
 * @brief Set the unfilled_idx of the page.
 *
 * @param page Page.
 * @param new_unfilled_idx new value of unfilled_idx.
 */
void bf_util_pagebuf_set_unfilled_idx(
    PFpage *page,
    uint64_t new_unfilled_idx);

/**
 * @brief Set the unfilled_pagebuf_offset of the page.
 *
 * @param page Page.
 * @param new_unfilled_pagebuf_offset new value of unfilled_pagebuf_offset.
 */
void bf_util_pagebuf_set_unfilled_pagebuf_offset(
    PFpage *page,
    uint64_t new_unfilled_pagebuf_offset);

/**
 * @brief Sort given PFpage by the coords.
 *
 * @param page Page.
 * @param order the order of the tile.
 * @param num_of_dims the number of dimensions.
 * @return int error code (0 if successful).
 */
int bf_util_sort_pfpage_by_coords(
    PFpage *page,
    tilestore_layout_t order,
    uint32_t num_of_dims);


/**************************************************
 * Some useful utils
 **************************************************/

/**
 * @brief Return index of the selected dimensions
 *
 * @param array_name Name of the array.
 * @param selected_dim List of dimension indices.
 * @param dim_len The number of dimensions.
 * @param selected_len The number of selected dimensions.
 * @param dim_selection_map (return) List of 0 or 1. 0 for not selected, 1 for selected.
 * @return int error code (0 if successful).
 */
int bf_util_get_dim_selection_map(const char *array_name,
                                      uint32_t *selected_dim,
                                      uint32_t dim_len,
                                      uint32_t selected_len,
                                      uint8_t **dim_selection_map);


/**
 * @brief Calculate ND coordinates from 1D coordinate (ROW_MAJOR).
 * Note that the caller should free the tile_extents argument.
 *
 * @param pos_1d the 1D coordinate.
 * @param pos_nd_lens the size of ND array, chunk, or tile.
 * @param dim_len The number of dimensions.
 * @param pos_nd (return) the ND coordinates
 * @return int error code (0 if successful).
 */
int bf_util_calculate_nd_from_1d_row_major(
    uint64_t pos_1d,
    uint64_t *pos_nd_lens,
    uint32_t dim_len,
    uint64_t **pos_nd);

/**
 * @brief Calculate 1D coordinate from ND coordinates (ROW_MAJOR).
 *
 * @param pos_nd the ND coordinates.
 * @param pos_nd_lens the size of ND array, chunk, or tile.
 * @param dim_len The number of dimensions.
 * @param pos_1d (return) the 1D coordinate.
 * @return int error code (0 if successful).
 */
int bf_util_calculate_1d_from_nd_row_major(
    uint64_t *pos_nd,
    uint64_t *pos_nd_lens,
    uint32_t dim_len,
    uint64_t *pos_1d);

/**
 * @brief Clear array_schema_cache.
 *
 * @return int error code (0 if successful).
 */
int bf_util_clear_array_schema_cache();

/**
 * @brief Compare two given coordinates.
 *
 * @param lhs_coords the coordinates of the lhs cell.
 * @param rhs_coords the coordinates of the rhs cell.
 * @param dim_len The number of dimensions.
 * @param order the cell order.
 * @return the compare result.
 */
int bf_util_compare_coords(
    uint64_t *lhs_coords,
    uint64_t *rhs_coords,
    uint32_t dim_len,
    tilestore_layout_t order);


/**
 * @brief Write values to the TileDB array with incremental values.
 *
 * @param array_name the array name.
 * @param dim_domains the domains for each dimensions.
 * @param num_of_dimensions the number of dimensions.
 * @param attribute_length the length of attribute.
 * @return int error code (0 if successful).
 */
int bf_util_write_array_with_incremental_values(
    const char *array_name,
    int *dim_domain,
    uint32_t num_of_dimensions,
    uint32_t attribute_length);

/**
 * @brief Write values to a sparse TileDB array with incremental values
 *          only in the cells that all of the cell indexes modulo
 *          the M(given argument) is zero.
 *
 * @param array_name the array name.
 * @param dim_domains the domains for each dimensions.
 * @param num_of_dimensions the number of dimensions.
 * @param m the given modulo.
 * @return int error code (0 if successful).
 */
int bf_util_write_sparse_array_with_incremental_values(
    const char *array_name,
    int *dim_domain,
    tilestore_datatype_t attr_type,
    uint32_t num_of_dimensions,
    uint64_t m);

/**************************************************
 * Some debuging facilities
 **************************************************/

/**
 * @brief Print the contents of PFpage.
 *
 * @param page Page.
 * @param arrayname the name of array.
 * @param num_of_dims the number of dimensions
 */
void bf_util_print_pfpage(
    PFpage *page,
    const char *arrayname,
    uint32_t num_of_dims);

/**
 * @brief Print given array (like a char buf[1024]).
 *
 * @param page Page.
 * @param attr_type the type of attribute.
 * @param data array.
 * @param data_size the length of data in bytes.
 */
void bf_util_print_array(
    tilestore_datatype_t attr_type,
    void *data,
    uint64_t data_size);

/**
 * @brief Pretty print the coordinates.
 *
 * @param coords The coordinates.
 * @param dim_len The number of dimensions.
 * @return int error code (0 if successful).
 */
int bf_util_print_coords(
    uint64_t *coords,
    uint32_t dim_len);

// fill in csr format from dense format
void bf_util_dense_to_csr(
    // Dense
    double *d_pagebuf,
    // CSR
    double *s_pagebuf, uint64_t *indptr, uint64_t *indices,
    // info
    uint64_t *tile_extents, uint64_t dim_len);

void bf_util_csr_to_dense(
    // Dense
    double *d_pagebuf,
    // CSR
    double *s_pagebuf, uint64_t *indptr, uint64_t *indices,
    // info
    uint64_t *tile_extents, uint64_t dim_len);


void* bf_util_get_pagebuf(PFpage *page);
uint64_t* bf_util_get_tile_extents(PFpage *page);

#endif