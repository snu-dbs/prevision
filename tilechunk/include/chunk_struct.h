#ifndef __CHUNK_STRUCT_H__
#define __CHUNK_STRUCT_H__

#include <stddef.h>
#include <stdint.h>

#include "tilestore.h"

#include "bf_struct.h"

/* Temp data structure */

typedef struct
{
    // Fixed
    size_t n_row;
    size_t n_col;
    int data_type;

    // Variable
    size_t *indptr;
    size_t *indices;
    double *data;
    size_t nnz;

} CSR_MATRIX;

/* Chunk structure definition */
typedef struct Chunk
{
    /* Fixed attribute (Never changes) */
    char *array_name;               // Name of array (given)
    char *attr_name;                // Name of attribute (internal, assume single attribute)
    tilestore_layout_t cell_order;     // Cell order of the array (internal)
    tilestore_format_t array_type; // Array type (Dense / Sparse)     
    uint32_t dim_len;               // Length of the dimension (internal)
    uint64_t **array_domains;       // Domain of the entire array (internal)
    uint64_t **chunk_domains;       // Domain of the chunk (internal)
    uint64_t *tile_extents;         // Tile extents of the current array (internal)
    uint64_t *chunk_coords;         // Coordinate of this chunk (internal)
    int64_t max_consume_cnt;        // Write prevention

    /* Variable attribute (Changed when tile changes) */
    uint64_t *tile_coords;   // Coordinate of the current tile (internal)
    uint64_t **tile_domains; // Domain of the current tile (internal)
    PFpage *curpage;         // Current tile (internal)
    uint8_t dirty;           // Whether the curpage is dirty or not. 0 if clean, 1 if dirty

    /* Only used for the result of matmul  */
    CSR_MATRIX *temp_csr;       // sparse
} Chunk;

/* Utility constants maintained within chunk layer */
#define MAX_ARRAY_NAME_LENGTH 255
#define MAX_ATTR_NAME_LENGTH 255

#endif