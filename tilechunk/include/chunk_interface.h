#ifndef __CHUNK_INTERFACE_H__
#define __CHUNK_INTERFACE_H__

#include <stddef.h>
#include <stdint.h>

#include "bf_struct.h"
#include "chunk_struct.h"
#include "chunk_iter.h" // to support backward compatibility

/* Chunk interface definitions */
int chunk_get_chunk(
    const char *array_name,
    uint64_t *chunk_size,
    uint64_t *chunk_coords,
    Chunk **chunk);

int chunk_get_chunk_custom_pos(
    const char *array_name,
    uint64_t *starting_cell_coord,
    uint64_t *chunk_size,
    Chunk **chunk);

int chunk_get_chunk_window(
    ChunkIterator *chunk_iter,
    int64_t *window_size,
    Chunk **chunk);

int chunk_get_sparse_chunk_window(
    ChunkSparseRandomOrderIterator *chunk_iter,
    int64_t *window_size,
    Chunk **chunk);

int chunk_get_cell_int(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_get_cell_float(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_get_cell_double(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_write_cell_int(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_write_cell_float(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_write_cell_double(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_get_cell_validity(
    Chunk *chunk,
    uint64_t *cell_coords_chunk);

void chunk_destroy(Chunk *chunk);

int chunk_sparse_get_cell_int(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_sparse_write_cell_int(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_sparse_get_cell_float(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_sparse_write_cell_float(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_sparse_get_cell_double(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_sparse_write_cell_double(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    void *cell_value);

int chunk_getbuf(Chunk* chunk, emptytile_template_type_t empty_template);
int chunk_unpinbuf(Chunk* chunk);

// private functions
#define NOT_FOUND -1
#define RETURN_NOT_FOUND_IF_NOT_FOUND 1
#define RETURN_LUB_IF_NOT_FOUND 2

int _chunk_replace_tile(
    Chunk *chunk,
    uint64_t *tile_coords_new);

int _chunk_coordinate_check(
    Chunk *chunk,
    int64_t *cell_coords_array);

int _chunk_process(
    Chunk *chunk,
    uint64_t *cell_coords_chunk);

uint64_t _chunk_sparse_get_idx_for_coordinates(
    Chunk *chunk,
    uint64_t *cell_coords_array,
    uint8_t return_mode);

int _chunk_sparse_get_cell_value_for_coordinates_int(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    int *value);

int _chunk_sparse_get_cell_value_for_coordinates_float(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    float *value);

int _chunk_sparse_get_cell_value_for_coordinates_double(
    Chunk *chunk,
    uint64_t *cell_coords_chunk,
    double *value);

/**************************************************
 *       Chunk Error codes definitions              *
 **************************************************/
#define CHUNK_OK 0
#define CHUNK_INVALID_CELL_COORDS (-1)
#define CHUNK_CELL_NULL_VALUE (-2)

#define CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE (-3)

/*******************************************
 *       ATTRIBUTE TYPE CODE DEFINITION    *
 *******************************************/

#define ATTR_TYPE_INT 0
#define ATTR_TYPE_FLOAT 1
#define ATTR_TYPE_DOUBLE 2

/*******************************************
 *       CONSTANT TYPE CODE DEFINITION    *
 *******************************************/

#define CONST_TYPE_INT 0
#define CONST_TYPE_FLOAT 1
#define CONST_TYPE_DOUBLE 2

/*************************************
 *      CHUNK OPERATION CODE DEFINITION    *
 **************************************/

#define CHUNK_OP_ADD 0
#define CHUNK_OP_SUB 1
#define CHUNK_OP_MUL 2
#define CHUNK_OP_DIV 3

#endif