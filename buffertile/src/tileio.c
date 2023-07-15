#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>

#include "bf.h"
#include "tileio.h"
#include "tilestore.h"

#include "bf_malloc.h"

#define ASSUMED_LEN     8       // Assumed length of variable-length cell
#define ASSUMED_NNZ     4       // assumed nnz of sparse tile

// #define BF_TILEIO_DEBUG 0

extern BFmspace *_mspace_data, *_mspace_key;

unsigned long long bf_iread_io_time, bf_iread_io_size;
unsigned long long bf_read_io_time, bf_read_io_size;
unsigned long long bf_write_io_time, bf_write_io_size;


#ifdef BF_TILEIO_DEBUG
void tileio_debug_print_data(tiledb_datatype_t attr_type, void *data, uint64_t write_data_size) {
    if (attr_type == TILEDB_INT32) {
        int32_t* typed_data = (int32_t*) data;
        uint64_t len = write_data_size / sizeof(int32_t);
        fprintf(stderr, "data_size=%lu\n", len);
        fprintf(stderr, "data=");
        for (uint64_t i = 0; i < len; i++)
            fprintf(stderr, "%d,", typed_data[i]);

    } else if (attr_type == TILEDB_UINT64) {
        uint64_t* typed_data = (uint64_t*) data;
        uint64_t len = write_data_size / sizeof(uint64_t);
        fprintf(stderr, "data_size=%lu\n", len);
        fprintf(stderr, "data=");
        for (uint64_t i = 0; i < len; i++)
            fprintf(stderr, "%lu,", typed_data[i]);

    } else if (attr_type == TILEDB_CHAR) {
        char* typed_data = (char*) data;
        uint64_t len = write_data_size / sizeof(char);
        fprintf(stderr, "data_size=%lu\n", len);
        fprintf(stderr, "data=");
        for (uint64_t i = 0; i < len; i++)
            fprintf(stderr, "%c,", typed_data[i]);

    } else {
        // Not supported yet!
    }

    fprintf(stderr, "\n");
}
#endif

int free_pfpage_content(PFpage *page) {
    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;

    if (page->pagebuf_o != BF_SHM_NULL) {
        void *pagebuf = bf_util_get_pagebuf(page);
        void *pagebuf_unaligned = 
            (void *) ((uintptr_t) pagebuf - page->_pagebuf_align_offset);
        BF_shm_free(mspace, pagebuf_unaligned);   
    }

    if (page->coords_o != BF_SHM_NULL) {
        if (page->sparse_format == PFPAGE_SPARSE_FORMAT_CSR) {   
            for (uint32_t d = 0; d < page->dim_len; d++) {
                BFshm_offset_t *coords = (BFshm_offset_t*) _BF_SHM_PTR_AUTO(mspace, page->coords_o);
                void *target = _BF_SHM_PTR_AUTO(mspace, coords[d]);
                uint16_t *_coord_align_offsets = (uint16_t*) _BF_SHM_PTR_AUTO(mspace, page->_coord_align_offsets_o);
                void *target_unaligned = (void*) ((uintptr_t) target - (uintptr_t) _coord_align_offsets[d]);

                BF_shm_free(mspace, target_unaligned);
            }

            BF_shm_free(mspace, _BF_SHM_PTR_AUTO(mspace, page->coords_o));
            BF_shm_free(mspace, _BF_SHM_PTR_AUTO(mspace, page->coords_lens_o));
            BF_shm_free(mspace, _BF_SHM_PTR_AUTO(mspace, page->_coord_align_offsets_o));
        } else if (page->sparse_format == PFPAGE_SPARSE_FORMAT_COO) {
            fprintf(stderr, "[BufferTile] Not implemented!!\n");
            assert(0);

            for (uint32_t d = 0; d < page->dim_len; d++) {
                BFshm_offset_t *coords = (BFshm_offset_t*) _BF_SHM_PTR_AUTO(mspace, page->coords_o);
                void *target = _BF_SHM_PTR_AUTO(mspace, coords[d]);
                BF_shm_free(mspace, target);
            }

            BF_shm_free(mspace, _BF_SHM_PTR_AUTO(mspace, page->coords_o));
            BF_shm_free(mspace, _BF_SHM_PTR_AUTO(mspace, page->coords_lens_o));
        }
    }

    if (page->tile_extents_o != BF_SHM_NULL) {
        BF_shm_free(mspace, _BF_SHM_PTR_AUTO(mspace, page->tile_extents_o));
    }

    page->pagebuf_o = BF_SHM_NULL;
    page->coords_o = BF_SHM_NULL;
    page->coords_lens_o = BF_SHM_NULL;
    page->tile_extents_o = BF_SHM_NULL;
    page->attrsize = 0;
    page->unfilled_pagebuf_offset = 0;
    page->unfilled_idx = 0;
    page->dim_len = 0;
    page->max_idx = 0;

    // dense mode
    page->est_density = NAN;
}

// free PFpage
int free_pfpage(PFpage *page) {
    free_pfpage_content(page);
    BF_shm_free(page->is_input ? _mspace_idata : _mspace_data, page);
    page->is_input = false;

    return 0;
}

int* get_subarray_region(
        uint64_t* coords,       // tile coordinates
        uint64_t *start_d,      // starting domain of each dimensions
        uint64_t *end_d,        // ending domain of each dimensions
        uint64_t *tsize,        // tile extent
        uint32_t dim_len) {     // dimension length

    int *subarray = malloc(sizeof(int) * dim_len * 2);
    for (uint32_t d = 0; d < dim_len; d++) {       // iterate dimensions
        // consider the tile extent is not fit into domain size.
        uint64_t coord = coords[d];
        uint64_t side = end_d[d] - start_d[d] + 1;              // dimension domain size
        uint64_t tail = side - (tsize[d] * coord);              // tail
        uint64_t size = (tsize[d] > tail) ? tail : tsize[d];    // valid (or iteratable) tile size

        subarray[(d * 2)] = (int) (coord * tsize[d] + start_d[d]);
        subarray[(d * 2) + 1] = (int) (((coord * tsize[d]) + size) + start_d[d] - 1);
    }

    return subarray;
}

// Read a tile from TileDB.
// this function use a goto lable to clean up the resources.
bf_tileio_ret_t read_from_tilestore(BFpage *free_BFpage, _array_key *key) {
    struct timeval start;
    gettimeofday(&start, NULL);

    int ret = BF_TILEIO_ERR;                            // return value
    size_t readio_size = 0;                             // stats

    bool is_input_req = strcmp(BF_SHM_KEY_PTR(key->attrname_o), "i") == 0;
    BFmspace *mspace = (is_input_req) ? _mspace_idata : _mspace_data;

    ///////////////////////////////////
    // Prepare schema
    ///////////////////////////////////
    const char* array_name = BF_SHM_KEY_PTR_CHAR(key->arrayname_o);
    uint32_t dim_len = key->dim_len;
    uint64_t *tile_coords = (uint64_t*) BF_SHM_KEY_PTR(key->dcoords_o);

    tilestore_array_schema_t *schema = NULL;
    tilestore_tile_metadata_t tile_meta;

    if (tilestore_get_array_schema(array_name, &schema) != TILESTORE_OK) {
        goto DEALLOC;
    }

    /************************
     * TileStore query
     ************************/
    uint64_t data_size, max_idx;
    void *data_unaligned = BF_SHM_NULL;     // to calculate the offset of data from malloc-ed ptr
    void *data = BF_SHM_NULL;

    BFshm_offset_t *coords = BF_SHM_NULL; 
    uint64_t *coord_sizes = BF_SHM_NULL;    
    uint16_t *coord_offsets = BF_SHM_NULL;  

    tilestore_errcode_t res = tilestore_read_metadata_of_tile(
        array_name, tile_coords, key->dim_len, &tile_meta);
    
    if (res == TILESTORE_TILE_NOT_INSERTED) {
        // empty read, so calculate manually 
        if (key->emptytile_template == BF_EMPTYTILE_SPARSE_CSR) {
            // if sparse CSR tile

            // calculate the length of row
            uint64_t row_length = schema->tile_extents[0];
            uint8_t is_edge = (schema->array_size_in_tile[0] - 1) == tile_coords[0];
            if (is_edge) {
                uint64_t remain = schema->array_size[0] % schema->tile_extents[0];
                if (remain != 0) {
                    row_length = remain;
                }
            }
            
            // coords
            uint64_t indptr_size = sizeof(uint64_t) * (row_length + 1);
            uint64_t *indptr_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(indptr_size) + 511);
            uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);

            uint64_t indices_size = sizeof(uint64_t) * ASSUMED_NNZ;
            uint64_t *indices_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(indices_size) + 511);
            uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);

            coords = BF_shm_malloc(mspace, sizeof(BFshm_offset_t) * schema->num_of_dimensions);
            coord_sizes = BF_shm_malloc(mspace, sizeof(uint64_t) * schema->num_of_dimensions);
            coord_offsets = BF_shm_malloc(mspace, sizeof(uint16_t) * schema->num_of_dimensions);

            coords[0] = _BF_SHM_OFFSET_AUTO(mspace, indptr);
            coords[1] = _BF_SHM_OFFSET_AUTO(mspace, indices); 

            coord_sizes[0] = indptr_size;
            coord_sizes[1] = indices_size;

            coord_offsets[0] = (uintptr_t) indptr - (uintptr_t) indptr_unaligned;
            coord_offsets[1] = (uintptr_t) indices - (uintptr_t) indices_unaligned;

            // data
            data_size = tilestore_datatype_size(schema->data_type) * ASSUMED_NNZ;

            data_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(data_size) + 511);
            data = aligned_512bytes_ptr(data_unaligned);

            // we don't have to query.
            // an operation is responsible for considering garbage values. 
            //      so we do not initialize it.
            max_idx = ASSUMED_NNZ;

            fprintf(stderr, "[BufferTile] create sparse empty tile\n");
        } else if (key->emptytile_template == BF_EMPTYTILE_DENSE) {
            // default is BF_EMPTYTILE_DENSE
            data_size = 1;
            for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
                uint64_t side = schema->tile_extents[d];

                uint8_t is_edge = (schema->array_size_in_tile[d] - 1) == tile_coords[d];
                if (is_edge) {
                    uint64_t remain = schema->array_size[d] % schema->tile_extents[d];
                    if (remain != 0) {
                        side = remain;
                    }
                } 

                data_size *= side;
            }
            max_idx = data_size;
            data_size *= tilestore_datatype_size(schema->data_type);
            data_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(data_size) + 511);

            data = aligned_512bytes_ptr(data_unaligned);

            // we don't have to query.
            // an operation is responsible for considering garbage values. 
            //      so we do not initialize it.

            fprintf(stderr, "[BufferTile] create dense empty tile\n");
        } else if (key->emptytile_template == BF_EMPTYTILE_NONE) {
            // Skip creating page

            // set free_BFpage even though it will be freed soon
            free_BFpage->fpage_o = BF_SHM_NULL;
            free_BFpage->count = 1;
            free_BFpage->dirty = FALSE;
            free_BFpage->key_o = BF_SHM_KEY_OFFSET(key);
            free_BFpage->is_input = is_input_req;

            ret = BF_TILEIO_NONE;
            goto DEALLOC;
        }

    } else if (res == TILESTORE_OK) {
        // normal read
        data_size = tile_meta.data_nbytes;
        data_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(data_size) + 511);

        data = aligned_512bytes_ptr(data_unaligned);

        if (tile_meta.tiletype == TILESTORE_SPARSE_CSR) {
            // malloc coords
            size_t indptr_size = tile_meta.cxs_indptr_nbytes;
            uint64_t *indptr_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(indptr_size) + 511);
            uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);

            size_t indices_size = tile_meta.coord_nbytes;
            uint64_t *indices_unaligned = BF_shm_malloc(mspace, ceil_to_512bytes(indices_size) + 511);
            uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);

            uint64_t **req_coords = malloc(sizeof(uint64_t*) * schema->num_of_dimensions);
            coord_sizes = BF_shm_malloc(mspace, sizeof(uint64_t) * schema->num_of_dimensions);

            req_coords[0] = indptr;
            req_coords[1] = indices;

            coord_sizes[0] = indptr_size;
            coord_sizes[1] = indices_size;

            // query
            if (tilestore_read_sparse_csr(
                    array_name, tile_meta, 
                    data, data_size, 
                    req_coords, coord_sizes,
                    TILESTORE_DIRECT) != TILESTORE_OK) {
                goto DEALLOC;
            }

            // bf_shm_malloc to coords
            coords = BF_shm_malloc(mspace, sizeof(BFshm_offset_t) * schema->num_of_dimensions);
            coord_offsets = BF_shm_malloc(mspace, sizeof(uint16_t) * schema->num_of_dimensions);

            coord_offsets[0] = (uintptr_t) indptr - (uintptr_t) indptr_unaligned;
            coord_offsets[1] = (uintptr_t) indices - (uintptr_t) indices_unaligned;

            coords[0] = _BF_SHM_OFFSET_AUTO(mspace, req_coords[0]);
            coords[1] = _BF_SHM_OFFSET_AUTO(mspace, req_coords[1]); 

            free(req_coords);

            max_idx = tile_meta.nnz;
        } else {
            // DENSE query
            if (tilestore_read_dense(
                    array_name, tile_meta, 
                    data, data_size, 
                    TILESTORE_DIRECT) != TILESTORE_OK) {
                goto DEALLOC;
            }

            max_idx = tile_meta.data_nbytes / tilestore_datatype_size(schema->data_type);
        }       

    } else {
        // err!!
        goto DEALLOC;
    }
    ///////////////////////////////////
    // Set PFpage
    ///////////////////////////////////

    // allocate page
    PFpage *page = BF_shm_malloc(mspace, sizeof(PFpage));

    // pagebuf start
    page->type = 0;
    page->dim_len = dim_len;
    page->max_idx = max_idx;

    page->pagebuf_o = _BF_SHM_OFFSET_AUTO(mspace, data);
    page->pagebuf_len = data_size;
    page->_pagebuf_align_offset = (uintptr_t) data - (uintptr_t) data_unaligned;
    // pagebuf end

    if (res == TILESTORE_OK || coords == BF_SHM_NULL) {
        // normal read, dense read, or temp dense read
        page->unfilled_idx = data_size / tilestore_datatype_size(schema->data_type);
        page->unfilled_pagebuf_offset = data_size;
    } else {
        // empty, sparse, or temp sparse read
        page->unfilled_idx = 0;
        page->unfilled_pagebuf_offset = 0;
    }

    if (coords != BF_SHM_NULL) {
        page->coords_o = _BF_SHM_OFFSET_AUTO(mspace, coords);
        page->coords_lens_o = _BF_SHM_OFFSET_AUTO(mspace, coord_sizes);
        page->_coord_align_offsets_o = _BF_SHM_OFFSET_AUTO(mspace, coord_offsets);
        if (page->coords_o != BF_SHM_NULL) page->sparse_format = PFPAGE_SPARSE_FORMAT_CSR;

        page->type = SPARSE_FIXED;
    } else {
        page->coords_o = BF_SHM_NULL;
        page->coords_lens_o = BF_SHM_NULL;
        page->_coord_align_offsets_o = BF_SHM_NULL;

        page->type = DENSE_FIXED;
    }

    // tile meta
    size_t tile_extents_size = sizeof(uint64_t) * dim_len;
    uint64_t *tile_extents = BF_shm_malloc(mspace, tile_extents_size);
    memcpy(tile_extents, schema->tile_extents, tile_extents_size);
    
    page->tile_extents_o = _BF_SHM_OFFSET_AUTO(mspace, tile_extents);
    page->attrsize = data_size / max_idx;
    page->is_input = is_input_req;


    // dense mode
    page->est_density = NAN;

    // set free_BFpage
    free_BFpage->fpage_o = _BF_SHM_OFFSET_AUTO(mspace, page);
    free_BFpage->count = 1;
    free_BFpage->dirty = FALSE;
    free_BFpage->key_o = BF_SHM_KEY_OFFSET(key);
    free_BFpage->is_input = is_input_req;

    ret = BF_TILEIO_OK;

DEALLOC:
    // free
    if (schema != NULL) tilestore_clear_array_schema(schema);

    // stats
    struct timeval end;                                                                                 \
    gettimeofday(&end, NULL);                                                                           \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);  \
    
    if (!(res == TILESTORE_TILE_NOT_INSERTED)) {
        readio_size += page->pagebuf_len;
        if (page->type == SPARSE_FIXED) {
            readio_size += coord_sizes[0];
            readio_size += coord_sizes[1];
        }
        
        if (is_input_req) {
            bf_iread_io_time += diff;
            bf_iread_io_size += readio_size;
        } else {
            bf_read_io_time += diff;
            bf_read_io_size += readio_size;
        } 
    }

    return ret;
}

bf_tileio_ret_t write_to_tilestore(BFpage *bf_page) {
    struct timeval start;
    gettimeofday(&start, NULL);

    int ret = BF_TILEIO_ERR;                // return value
    size_t writeio_size = 0;                // for stats

    ///////////////////////////////////
    // Prepare schema
    ///////////////////////////////////
    _array_key *key = BF_SHM_KEY_PTR_KEY(bf_page->key_o);
    const char* array_name = BF_SHM_KEY_PTR_CHAR(key->arrayname_o);
    uint64_t *tile_coords = BF_SHM_KEY_PTR_UINT64T(key->dcoords_o);
    uint32_t dim_len = key->dim_len;

    bool is_input_req = strcmp(BF_SHM_KEY_PTR(key->attrname_o), "i") == 0;
    BFmspace *mspace = is_input_req ? _mspace_idata : _mspace_data;

    /************************
     * TileDB query
     ************************/

    // seperating procedure with pagebuf type is better?
    PFpage *page = (PFpage*) _BF_SHM_PTR_AUTO(mspace, bf_page->fpage_o);

    // data variable (always be used)
    uint64_t data_size = page->unfilled_pagebuf_offset;
    void *data = _BF_SHM_PTR_AUTO(mspace, page->pagebuf_o);

    // write
    if (page->type == SPARSE_FIXED) {
        BFshm_offset_t *coords = _BF_SHM_PTR_AUTO(mspace, page->coords_o);
        uint64_t *coord_sizes = _BF_SHM_PTR_AUTO(mspace, page->coords_lens_o);

        uint64_t **req_coords = malloc(sizeof(uint64_t*) * 2);
        uint64_t *req_coord_sizes = malloc(sizeof(uint64_t) * 2);

        req_coords[0] = _BF_SHM_PTR_AUTO(mspace, coords[0]);
        req_coords[1] = _BF_SHM_PTR_AUTO(mspace, coords[1]);

        req_coord_sizes[0] = coord_sizes[0];// the size of indptr is fixed!!
        req_coord_sizes[1] = page->unfilled_idx * sizeof(uint64_t);

        if (tilestore_write_sparse_csr(
                array_name, tile_coords, dim_len,
                data, data_size,
                req_coords, req_coord_sizes,
                TILESTORE_DIRECT) != TILESTORE_OK) {
            goto DEALLOC;
        }

        free(req_coords);
        free(req_coord_sizes);

    } else if (page->type == DENSE_FIXED) {
        if (tilestore_write_dense(
                array_name, tile_coords, dim_len,
                data, data_size,
                TILESTORE_DIRECT) != TILESTORE_OK) {
            goto DEALLOC;
        }

    } else {
        assert(0);
    }

    ret = BF_TILEIO_OK;

DEALLOC:
    ///////////////////////////////////
    // Unset FPpage
    ///////////////////////////////////

    // for stats
    writeio_size += page->pagebuf_len;
    if (page->type == SPARSE_FIXED) {
        uint64_t *coord_sizes = _BF_SHM_PTR_AUTO(mspace, page->coords_lens_o);
        writeio_size += coord_sizes[0];
        writeio_size += coord_sizes[1];
    }


    if (bf_page->fpage_o != BF_SHM_NULL) {
        free_pfpage((PFpage*) _BF_SHM_PTR_AUTO(mspace, bf_page->fpage_o));
        bf_page->fpage_o = BF_SHM_NULL;
    }

    struct timeval end;                                                                                 \
    gettimeofday(&end, NULL);                                                                           \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);  \
    bf_write_io_time += diff; 
    bf_write_io_size += writeio_size;

    return ret;
}
