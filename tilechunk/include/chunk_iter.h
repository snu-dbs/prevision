#ifndef __CHUNK_ITER_H__
#define __CHUNK_ITER_H__

typedef struct ChunkIterator ChunkIterator;
typedef struct ChunkSparseFitIterator ChunkSparseFitIterator;
typedef struct ChunkSparseRandomOrderIterator ChunkSparseRandomOrderIterator;
typedef struct ChunkSparseIterator ChunkSparseIterator;

#include "chunk_struct.h"
#include "chunk_interface.h"

/**************************************************
 *       DENSE Chunk Iterator                     *
 **************************************************/

struct ChunkIterator
{
    Chunk *chunk; // chunk

    uint64_t length;        // # of cells in the chunk
    uint64_t chunk_idx;     // current index of the cell in the chunk.
    uint64_t tile_idx;      // current index of the cell in the tile.
    uint64_t chunk_mod_idx; // modulo-ed chunk_idx to gain performance.

    uint64_t tile_stride;   // how much tile_idx is being increased.
    uint32_t *dim_order;    // dimension order for custom order iterator.
                            //   the value is NULL if it is not custom order iterator.
    uint32_t *original_dim; // original dimensions w.r.t. dimension idx

    uint64_t *check_list;       // index patterns that requires _chunk_process
    uint64_t check_list_length; // the length of check_list
    uint64_t check_idx;         // the current index of the check_list.
                                //      check_list[check_idx] will be compared
                                //      with chunk_idx to run _chunk_process.
    uint64_t modulo;            // to reduce the size of check_list.
                                //      chunk_idx % modulo will be evaluated.

    uint64_t *chunk_size; // pre-computed chunk size.
};

// init / free
ChunkIterator *chunk_iterator_init(Chunk *chunk);
ChunkIterator *chunk_custom_order_iterator_init(Chunk *chunk, uint32_t *dim_order);
void chunk_iterator_free(ChunkIterator *iter);

// size() / has_next() / get_next()
uint64_t chunk_iterator_size(ChunkIterator *iter);
uint8_t chunk_iterator_has_next(ChunkIterator *iter);
ChunkIterator *chunk_iterator_get_next(ChunkIterator *iter);

// read / write cell
int chunk_iterator_get_cell_int(ChunkIterator *iter);
float chunk_iterator_get_cell_float(ChunkIterator *iter);
double chunk_iterator_get_cell_double(ChunkIterator *iter);
void chunk_iterator_write_cell_int(ChunkIterator *iter, int val);
void chunk_iterator_write_cell_float(ChunkIterator *iter, float val);
void chunk_iterator_write_cell_double(ChunkIterator *iter, double val);

// WIP: fast iterator
int64_t chunk_iterator_safe_cell_count(ChunkIterator *iter);
ChunkIterator *chunk_iterator_fast_get_next(ChunkIterator *iter);

/**************************************************
 *       SPARSE Chunk Iterator                     *
 **************************************************/

/* Sparse Fit Iterator */
struct ChunkSparseFitIterator
{
    Chunk *chunk; // chunk

    uint64_t length;   // # of cells in the chunk
    uint64_t tile_idx; // current index of the cell in the tile.

    // .... lightweight!
};

// init / free
ChunkSparseFitIterator *chunk_sparse_fit_iterator_init(Chunk *chunk);
void chunk_sparse_fit_iterator_free(ChunkSparseFitIterator *iter);

// size() / has_next() / get_next()
uint64_t chunk_sparse_fit_iterator_size(ChunkSparseFitIterator *iter);
uint8_t chunk_sparse_fit_iterator_has_next(ChunkSparseFitIterator *iter);
uint8_t chunk_sparse_fit_iterator_is_eof(ChunkSparseFitIterator *iter);
ChunkSparseFitIterator *chunk_sparse_fit_iterator_get_next(ChunkSparseFitIterator *iter);

// read cell
int chunk_sparse_fit_iterator_get_cell_int(ChunkSparseFitIterator *iter);
float chunk_sparse_fit_iterator_get_cell_float(ChunkSparseFitIterator *iter);
double chunk_sparse_fit_iterator_get_cell_double(ChunkSparseFitIterator *iter);
// int32_t* chunk_sparse_fit_iterator_get_coords(ChunkSparseFitIterator *iter);
uint64_t *chunk_sparse_fit_iterator_get_coords(ChunkSparseFitIterator *iter);
uint64_t *chunk_sparse_fit_iterator_get_coords_in_chunk(ChunkSparseFitIterator *iter);
void chunk_sparse_fit_iterator_get_coords_in_chunk_no_malloc(ChunkSparseFitIterator *iter, uint64_t *coords);

/* Sparse Random Order Iterator */
struct ChunkSparseRandomOrderIterator
{
    Chunk *chunk; // chunk

    uint64_t idx_in_tile; // current index of the cell in the tile.
    uint64_t tile_length; // # of cells in the tile

    uint64_t **check_tile_coords_list;
    uint64_t check_tile_coords_length;
    uint64_t check_tile_coords_idx;
};

// init / free
ChunkSparseRandomOrderIterator *chunk_sparse_random_order_iterator_init(Chunk *chunk);
void chunk_sparse_random_order_iterator_free(ChunkSparseRandomOrderIterator *iter);

// size() / has_next() / get_next()
uint64_t chunk_sparse_random_order_iterator_size(ChunkSparseRandomOrderIterator *iter);
// uint8_t chunk_sparse_random_order_iterator_has_next(ChunkSparseRandomOrderIterator *iter);
uint8_t chunk_sparse_random_order_iterator_is_eof(ChunkSparseRandomOrderIterator *iter);
ChunkSparseRandomOrderIterator *chunk_sparse_random_order_iterator_get_next(ChunkSparseRandomOrderIterator *iter);

// read cell
int chunk_sparse_random_order_iterator_get_cell_int(ChunkSparseRandomOrderIterator *iter);
float chunk_sparse_random_order_iterator_get_cell_float(ChunkSparseRandomOrderIterator *iter);
double chunk_sparse_random_order_iterator_get_cell_double(ChunkSparseRandomOrderIterator *iter);
// int32_t* chunk_sparse_random_order_iterator_get_coords(ChunkSparseRandomOrderIterator *iter);
uint64_t *chunk_sparse_random_order_iterator_get_coords(ChunkSparseRandomOrderIterator *iter);
uint64_t *chunk_sparse_random_order_iterator_get_coords_in_chunk(ChunkSparseRandomOrderIterator *iter);

/* Sparse Iterator */
struct ChunkSparseIterator
{
    Chunk *chunk;         // chunk
    uint64_t *chunk_size; // pre-computed chunk size.
    bool_t is_eof;        // is EOF?

    // check list
    uint64_t *check_list;       // index patterns that requires
                                //      getting tile seq
    uint64_t check_list_length; // the length of check_list
    uint64_t check_idx;         // the current index of the check_list.
                                //      check_list[check_idx] will be
                                //      compared with
                                //      cell_sequence_count
                                //      to get tile sequence.
    uint64_t modulo;            // to reduce the size of check_list.
                                //      chunk_idx % modulo will be
                                //      evaluated.

    // tile infomation
    // TODO: naming...?
    uint64_t **check_tile_coords_list;   // tiles overlap the chunk
    uint64_t check_tile_coords_length;   // the # of the list above
    bool_t *is_tile_seen_list;           // is the tile seen?
    uint64_t *idx_of_cells_in_tile_list; // current index of the cell in the tile
    uint64_t *tile_length_list;          // # of cells in the tile

    // tile sequence list
    // it will be changed during getnext()
    uint64_t *tile_sequence_list; // the list of idx
                                  //      (to check_tile_coords_list) of
                                  //      tile sequence, being changed
                                  //      after init
    uint64_t tile_sequence_len;   // the length of tile sequence
    uint64_t tile_sequence_idx;   // the current idx of tile sequence

    // cell coords
    int32_t *cell_sequence_coords; // current coords of cell sequence.
                                   //      note that if the cell order is
                                   //      row major, then the last element
                                   //      (cell_sequence_coords[d-1]) will
                                   //      not be used.
                                   //      if the cell order is col major,
                                   //      the first element will not be
                                   //      used.
    uint64_t cell_sequence_count;  // how many cell_sequence_coords changed
};

// init / free
ChunkSparseIterator *chunk_sparse_iterator_init(Chunk *chunk);
void chunk_sparse_iterator_free(ChunkSparseIterator *iter);

// size() / has_next() / get_next()
uint64_t chunk_sparse_iterator_size(ChunkSparseIterator *iter);
// uint8_t chunk_sparse_iterator_has_next(ChunkSparseIterator *iter);
uint8_t chunk_sparse_iterator_is_eof(ChunkSparseIterator *iter);
ChunkSparseIterator *chunk_sparse_iterator_get_next(ChunkSparseIterator *iter);

// read cell
int chunk_sparse_iterator_get_cell_int(ChunkSparseIterator *iter);
float chunk_sparse_iterator_get_cell_float(ChunkSparseIterator *iter);
double chunk_sparse_iterator_get_cell_double(ChunkSparseIterator *iter);
uint64_t *chunk_sparse_iterator_get_coords(ChunkSparseIterator *iter);
uint64_t *chunk_sparse_iterator_get_coords_in_chunk(ChunkSparseIterator *iter);

#endif