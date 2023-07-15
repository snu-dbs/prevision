#ifndef __TILESTORE_H__
#define __TILESTORE_H__

#include <stdio.h>
#include <stdint.h>

/**************************************************
 * Types
 **************************************************/

typedef enum tilestore_format_t {
    TILESTORE_DENSE,
    TILESTORE_SPARSE_CSR,
    TILESTORE_SPARSE_COO
} tilestore_format_t;

typedef enum tilestore_datatype_t {
    TILESTORE_CHAR, 
    TILESTORE_INT32, 
    TILESTORE_UINT64,
    TILESTORE_FLOAT32,
    TILESTORE_FLOAT64
} tilestore_datatype_t;

// typedef enum tilestore_nullable_t {
//     FALSE,
//     TRUE
// } tilestore_nullable_t;

typedef enum tilestore_layout_t {
    TILESTORE_ROW_MAJOR, 
    TILESTORE_COL_MAJOR
} tilestore_layout_t;

typedef enum tilestore_errcode_t {
    TILESTORE_OK,
    TILESTORE_OPEN_FAILED,
    TILESTORE_PWRITE_FAILED,     
    TILESTORE_PREAD_FAILED,
    TILESTORE_CLOSE_FAILED,         
    TILESTORE_ARRAY_ALREADY_EXISTS,  
    TILESTORE_ARRAY_NOT_EXISTS,
    TILESTORE_REMOVE_ARRAY_FAILED,
    TILESTORE_INVALID_TILE_COORDS,
    TILESTORE_TILE_NOT_INSERTED,
    TILESTORE_HASHTABLE_ERR,
    TILESTORE_UNDEFINED_ERR
} tilestore_errcode_t;

typedef enum tilestore_io_mode_t {
    TILESTORE_NORMAL,
    TILESTORE_DIRECT
} tilestore_io_mode_t;

typedef struct tilestore_array_schema_t {
    tilestore_datatype_t data_type;
    uint32_t num_of_dimensions;
    uint64_t *array_size;
    uint64_t *array_size_in_tile;
    uint64_t *unpadded_array_size;
    uint64_t *tile_extents;
    // tilestore_layout_t cell_order;
    tilestore_format_t format;
} tilestore_array_schema_t;

typedef struct tilestore_tile_metadata_t {
    tilestore_format_t tiletype;      // dense?, sparse csr?, or sparse coo?
    // tilestore_nullable_t nullable;
    size_t nbytes;              // the size of the whole tile in bytes
    size_t data_nbytes;         // the size of the tile data in bytes
    size_t coord_nbytes;        // the size of the coordinates in bytes
    size_t cxs_indptr_nbytes;   // the size of the indptr in bytes (CSR only)
    size_t nnz;                 // the number of non-zero values (sparse only)
    size_t offset;              // offset in data file
} tilestore_tile_metadata_t;

// RULES
// Related to ARRAY SIZE : uint64_t
// Related to TILE SIZE: uint64_t
// Related to dimension: uint32_t

/**************************************************
 * TileStore array create/delete functions
 **************************************************/

// related to alignment
void* aligned_512bytes_ptr(void* ptr);
size_t ceil_to_512bytes(size_t original);

/**
 * @brief Create an array.
 *
 * @param array_name the array name.
 * @param array_size the size of the array.
 * @param tile_extents the tile extent for each dimensions.
 * @param num_of_dimensions the number of dimensions.
 * @param tilestore_datatype the data type of attribute.
 * @param tilestore_format_t the format of the data. Dense or SPARSE_CSR.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t 
tilestore_create_array(
    const char *array_name,
    uint64_t *array_size,
    uint64_t *tile_extents,
    uint32_t num_of_dimensions,
    tilestore_datatype_t attribute_type,
    tilestore_format_t format);

/**
 * @brief Delete an array.
 *
 * @param array_name the array name.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t tilestore_delete_array(const char *arrayname);

/**************************************************
 * TileStore Schema Functions
 **************************************************/

/**
 * @brief Get tilestore array schema.
 *
 * @param array_name the array name.
 * @param array_schema schema of the array.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t 
tilestore_get_array_schema(
    const char *array_name,
    tilestore_array_schema_t **array_schema);

/**
 * @brief Clear tilestore array schema.
 *
 * @param array_schema schema of the array.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t 
tilestore_clear_array_schema(
    tilestore_array_schema_t *array_schema);

/**************************************************
 * TileStore array read/write functions
 **************************************************/

/**
 * @brief Read metadata of the tile from TileStore.
 * To read tile data, a user call this function first and then 
 *      call tilestore_read_dense() function with the metadata 
 *      obtained from this function.
 * 
 * (Reading empty tile) If the tile that user request is not inserted yet,
 *      then the function return TILESTORE_TILE_NOT_INSERTED.
 *
 * @param array_name the array name.
 * @param tile_coords the coordinates of a tile which user want to read.
 * @param num_of_dimensions the number of dimensions.
 * @param metadata_of_tile (return) the metadata of the tile 
 *      that's coord is tile_coords.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t 
tilestore_read_metadata_of_tile(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        tilestore_tile_metadata_t *metadata_of_tile);

/**
 * @brief Read a dense tile from TileStore array.
 * This function read data from disk to `data` pointer so that 
 *      user must allocate the data pointer before calling this function.
 * A user should obtain `metadata_of_tile ` using 
 *      tilestore_read_metadata_of_tile() function before call this function.
 * The `data` pointer must be aligned to 512 bytes memory address. 
 * The `data_size` does not required to align. 
 * 
 * @param array_name the array name.
 * @param metadata_of_tile the metadata of the tile user want to read.
 * @param data an aligned pointer that pre-allocated with data_size.
 * @param data_size the size of the data pointer.
 * @param iomode TILESTORE_DIRECT if want O_DIRECT, otherwise TILESTORE_NORMAL.
    
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t 
tilestore_read_dense(
        const char *array_name,
        tilestore_tile_metadata_t metadata_of_tile,
        void *data,
        size_t data_size,
        tilestore_io_mode_t iomode);

/**
 * @brief Write a dense tile to TileStore array.
 * 
 * The `data` pointer must be aligned to 512 bytes memory address. 
 * The `data_size` does not required to align. 
 * 
 * @param array_name the array name.
 * @param tile_coords the coordinates of a tile which user want to read.
 * @param num_of_dimensions the number of dimensions.
 * @param data an aligned pointer that contains data.
 * @param data_size the size of the data pointer.
 * @param iomode TILESTORE_DIRECT if want O_DIRECT, otherwise TILESTORE_NORMAL.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t
tilestore_write_dense(
    const char *array_name,
    uint64_t *tile_coords,
    uint32_t num_of_dimensions,
    void *data,
    size_t data_size,
    tilestore_io_mode_t iomode);

/**
 * @brief Read a sparse CSR tile from TileStore array.
 * This function read data from disk to the given arguments directly so that 
 *      user must allocate the pointers before calling this function.
 *
 * @param array_name the array name.
 * @param metadata_of_tile the metadata of the tile user want to read.
 * @param data a pointer that pre-allocated with data_size.
 * @param data_size the size of the data pointer.
 * @param coords a pointer to an array of pointers that point an array of coordinate values.
 * @param coord_sizes the sizes of an array of coordinate values.
 * @param iomode TILESTORE_DIRECT if want O_DIRECT, otherwise TILESTORE_NORMAL.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t
tilestore_read_sparse_csr(
    const char *array_name,
    tilestore_tile_metadata_t metadata_of_tile,
    void *data,
    size_t data_size,
    uint64_t **coords,
    size_t *coord_sizes,
    tilestore_io_mode_t iomode);

/**
 * @brief Write a sparse CSR tile to TileStore array.
 *
 * @param array_name the array name.
 * @param tile_coords the coordinates of a tile which user want to read.
 * @param num_of_dimensions the number of dimensions.
 * @param data a pointer that contains data.
 * @param data_size the size of the data pointer.
 * @param coords a pointer to an array of pointers that point an array of coordinate values.
 * @param coord_sizes the sizes of an array of coordinate values.
 * @param nnz the number of non-zero elements.
 * @param iomode TILESTORE_DIRECT if want O_DIRECT, otherwise TILESTORE_NORMAL.
 * @return tilestore error code (0 if successful).
 */

tilestore_errcode_t
tilestore_write_sparse_csr(
    const char *array_name,
    uint64_t *tile_coords,
    uint32_t num_of_dimensions,
    void *data,
    size_t data_size,
    uint64_t **coords,
    size_t *coord_sizes,
    tilestore_io_mode_t iomode);


/**************************************************
 * TileStore array read/write utils
 **************************************************/

/**
 * @brief Write values to the TileDB array with incremental values.
 *
 * @param array_name the array name.
 * @param dim_domains the domains for each dimensions.
 * @param num_of_dimensions the number of dimensions.
 * @param attribute_length the length of attribute.
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t
tilestore_write_array_with_incremental_values(
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
 * @return tilestore error code (0 if successful).
 */
tilestore_errcode_t
tilestore_write_sparse_array_with_incremental_values(
    const char *array_name,
    int *dim_domain,
    tilestore_datatype_t attr_type,
    uint32_t num_of_dimensions,
    uint64_t m);


tilestore_errcode_t
tilestore_compaction(const char *array_name);

// type related functions
size_t tilestore_datatype_size(tilestore_datatype_t input);

uint8_t _is_tile_coords_in_array(
        tilestore_array_schema_t *schema, uint64_t *tile_coords);

uint64_t _tile_coords_to_1D_coord(
        tilestore_array_schema_t *schema,
        uint64_t *tile_coords);


// For compatibility
tilestore_errcode_t 
tilestore_get_array_schema_v3(
        const char *array_name,
        tilestore_array_schema_t **array_schema);

tilestore_errcode_t 
tilestore_get_array_schema_v4(
        const char *array_name,
        tilestore_array_schema_t **array_schema);

tilestore_errcode_t
tilestore_write_dense_v4(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        void *data,
        size_t data_size,
        tilestore_io_mode_t iomode);

tilestore_errcode_t 
tilestore_read_dense_v4(
        const char *array_name,
        tilestore_tile_metadata_t metadata_of_tile,
        void *data,
        size_t data_size,
        tilestore_io_mode_t iomode);

tilestore_errcode_t
tilestore_write_sparse_csr_v4(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        void *data,
        size_t data_size,
        uint64_t **coords,
        size_t *coord_sizes,
        tilestore_io_mode_t iomode);

tilestore_errcode_t
tilestore_read_sparse_csr_v4(
        const char *array_name,
        tilestore_tile_metadata_t metadata_of_tile,
        void *data,
        size_t data_size,
        uint64_t **coords,
        size_t *coord_sizes,
        tilestore_io_mode_t iomode);


tilestore_errcode_t 
tilestore_read_metadata_of_tile_v4(
        const char *array_name,
        uint64_t *tile_coords,
        uint32_t num_of_dimensions,
        tilestore_tile_metadata_t *metadata_of_tile);
#endif