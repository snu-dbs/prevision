#include <fcntl.h>
#include <unistd.h>
#include <math.h>

#include "utils.h"
#include "bf.h"
#include "tileio.h"

#define CHAR_BUF_LEN 1024


/**************************************************
 * Some private functions
 **************************************************/

void _quicksort_pfpage_by_coords(PFpage *page, tilestore_layout_t order, uint32_t num_of_dims, int64_t left, int64_t right);
void _swap_with_type_size(void *data, uint64_t lhs, uint64_t rhs, uint64_t type_size);

/**************************************************
 * Storage functions
 **************************************************/

// Note that the caller should free the tile_extents argument.
int storage_util_get_tile_extents(
    const char *array_name,
    uint64_t **tile_extents,
    uint32_t *dim_len)
{

    tilestore_array_schema_t *schema;
    tilestore_get_array_schema(array_name, &schema);

    size_t _tile_extents_size = schema->num_of_dimensions * sizeof(uint64_t);
    uint64_t *_tile_extents = malloc(_tile_extents_size);
    memcpy(_tile_extents, schema->tile_extents, _tile_extents_size);

    *(tile_extents) = _tile_extents;
    *(dim_len) = schema->num_of_dimensions;

    tilestore_clear_array_schema(schema);
    return 0;
}

int storage_util_free_tile_extents(uint64_t **tile_extents, uint32_t dim_len)
{
    free(*tile_extents);
    return 0;
}

// Note that the caller should free the dcoord_lens argument.
int storage_util_get_dcoord_lens(uint64_t **dim_domains, uint64_t *tile_extents, uint32_t dim_len, uint64_t **dcoord_lens)
{
    // iterate dimensions
    *dcoord_lens = malloc(sizeof(uint64_t) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++)
    {
        (*dcoord_lens)[d] = ceil(
            (double)(dim_domains[d][1] - dim_domains[d][0] + 1) / tile_extents[d]);
    }

    return 0;
}

int storage_util_get_array_type(
    const char *array_name,
    tilestore_format_t *array_type)
{
    tilestore_array_schema_t *schema;
    tilestore_get_array_schema(array_name, &schema);

    *(array_type) = schema->format;

    tilestore_clear_array_schema(schema);
    return 0;
}

int storage_util_get_attribute_type(
    const char *array_name, const char *attr_name, tilestore_datatype_t *attr_type)
{
    tilestore_array_schema_t *schema;
    tilestore_get_array_schema(array_name, &schema);

    *(attr_type) = schema->data_type;

    tilestore_clear_array_schema(schema);
    return 0;
}

int storage_util_get_attribute_nullability(const char *array_name, const char *attr_name, uint8_t *nullable)
{
    *(nullable) = 0;
    return 0;
}

int storage_util_get_cell_order(const char *array_name, tilestore_layout_t *cell_order)
{
    *(cell_order) = TILESTORE_ROW_MAJOR;
}

int storage_util_get_cell_length(const char *array_name, char *attr_name, uint32_t *cell_len)
{
    *(cell_len) = 1;
}

int storage_util_get_attr_names(const char *array_name, char ***attr_names, uint32_t *attr_len)
{
    *(attr_len) = 1;
    *(attr_names) = malloc(sizeof(char *) * 1);
    *(attr_names)[0] = malloc(sizeof(char) * 2);

    if (strstr(array_name, TMPARR_NAME_PREFIX) != NULL) {
        char *dummyattr = "a\0";
        memcpy(*(attr_names)[0], dummyattr, sizeof(char) * 2);
    } else {
        char *dummyattr = "i\0";
        memcpy(*(attr_names)[0], dummyattr, sizeof(char) * 2);
    }

    return 0;
}

// Note that the caller should free the dim_domains argument.
int storage_util_get_unpadded_dim_domains(
    const char *array_name, uint64_t ***dim_domains, uint32_t *dim_len)
{
    tilestore_array_schema_t *schema;
    tilestore_get_array_schema(array_name, &schema);

    uint64_t **_dim_domains = malloc(sizeof(uint64_t *) * schema->num_of_dimensions);
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++)
    {
        _dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        _dim_domains[d][0] = 0;
        _dim_domains[d][1] = schema->unpadded_array_size[d] - 1;
    }

    *(dim_len) = schema->num_of_dimensions;
    *(dim_domains) = _dim_domains;

    tilestore_clear_array_schema(schema);
    return 0;
}

// Note that the caller should free the dim_domains argument.
int storage_util_get_dim_domains(const char *array_name, uint64_t ***dim_domains, uint32_t *dim_len)
{
    tilestore_array_schema_t *schema;
    tilestore_get_array_schema(array_name, &schema);

    uint64_t **_dim_domains = malloc(sizeof(uint64_t *) * schema->num_of_dimensions);
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++)
    {
        _dim_domains[d] = malloc(sizeof(uint64_t) * 2);
        _dim_domains[d][0] = 0;
        _dim_domains[d][1] = schema->array_size[d] - 1;
    }

    *(dim_len) = schema->num_of_dimensions;
    *(dim_domains) = _dim_domains;

    tilestore_clear_array_schema(schema);
    return 0;
}

// Note that the caller should free the 'selected' argument.
int storage_util_get_selected_dim_by_names(
    const char *array_name,
    const char **selected_dim_names,
    uint32_t dim_len,
    uint32_t selected_len,
    uint8_t **selected)
{
    // FIXME:
    assert(1);
}

int storage_util_delete_array(const char *arrayname)
{
    tilestore_delete_array(arrayname);
    return 0;
}

int storage_util_get_dcoords_from_ccoords(const char *array_name, const char *attr_name, int64_t *ccoords, uint64_t **dcoords)
{
    uint32_t dim_len;       // dimension length
    uint64_t **dim_domains; // domains of the dimensions
    uint64_t *tile_extents; // tile extent of each data tile.

    // get schema information
    storage_util_get_dim_domains(array_name, &dim_domains, &dim_len);
    storage_util_get_tile_extents(array_name, &tile_extents, &dim_len);

    *dcoords = malloc(dim_len * sizeof(uint64_t));
    for (uint32_t d = 0; d < dim_len; d++)
    {
        (*dcoords)[d] = (uint64_t)floor(((uint64_t)ccoords[d] - dim_domains[d][0]) / tile_extents[d]);
    }

    return 0;
}

int storage_util_create_array(
    const char *array_name,
    tilestore_format_t array_type,
    int *dim_domain,
    int *tile_extents,
    uint32_t num_of_dimensions,
    uint32_t attribute_length,
    tilestore_datatype_t attribute_type,
    uint8_t nullable)
{
    // define schema
    uint64_t *array_size = malloc(sizeof(uint64_t) * num_of_dimensions);
    uint64_t *_tile_extents = malloc(sizeof(uint64_t) * num_of_dimensions);
    for (uint32_t d = 0; d < num_of_dimensions; d++)
    {
        array_size[d] = (uint64_t)(dim_domain[(d * 2) + 1] - dim_domain[(d * 2)] + 1);
        _tile_extents[d] = (uint64_t)tile_extents[d];
    }

    // create array
    tilestore_create_array(
        array_name,
        array_size,
        _tile_extents,
        num_of_dimensions,
        attribute_type,
        array_type);

    free(array_size);
    free(_tile_extents);

    return 0;
}

int storage_util_free_selected(uint8_t **selected)
{
    free(*selected);

    return 0;
}

int storage_util_free_dim_domains(uint64_t ***dim_domains, uint32_t dim_len)
{
    for (uint32_t i = 0; i < dim_len; i++)
    {
        free((*dim_domains)[i]);
    }
    free(*dim_domains);
    return 0;
}

int storage_util_free_dcoord_lens(uint64_t **dcoord_lens)
{
    free(*dcoord_lens);
    return 0;
}

int storage_util_free_attr_names(char ***attr_names, uint32_t attr_len)
{
    for (uint32_t i = 0; i < attr_len; i++)
    {
        free((*attr_names)[i]);
    }
    free(*attr_names);
    return 0;
}

/**************************************************
 * TileDB Schema Utils
 **************************************************/

int bf_util_compare_coords(uint64_t *lhs_coords, uint64_t *rhs_coords, uint32_t dim_len, tilestore_layout_t order)
{
    if (order == TILESTORE_COL_MAJOR)
    {
        for (int64_t i = dim_len - 1; i >= 0; i--)
        {
            if (lhs_coords[i] < rhs_coords[i])
            {
                return COMP_RHS_IS_BIGGER;
            }
            else if (lhs_coords[i] > rhs_coords[i])
            {
                return COMP_LHS_IS_BIGGER;
            }
        }
    }
    else
    {
        for (int64_t i = 0; i < dim_len; i++)
        {
            if (lhs_coords[i] < rhs_coords[i])
            {
                return COMP_RHS_IS_BIGGER;
            }
            else if (lhs_coords[i] > rhs_coords[i])
            {
                return COMP_LHS_IS_BIGGER;
            }
        }
    }

    return COMP_EQUAL;
}

int bf_util_get_dim_selection_map(const char *array_name, uint32_t *selected_dim,
                                  uint32_t dim_len, uint32_t selected_len, uint8_t **dim_selection_map)
{
    *dim_selection_map = calloc(dim_len, sizeof(uint8_t));

    for (uint32_t d = 0; d < selected_len; d++)
    {
        (*dim_selection_map)[selected_dim[d]] = 1;
    }

    return 0;
}

// Note that the caller should free the tile_extents argument.
int bf_util_calculate_nd_from_1d_row_major(uint64_t pos_1d, uint64_t *pos_nd_lens, uint32_t dim_len, uint64_t **pos_nd)
{
    uint64_t dim_divisor = 1;
    uint64_t remainder_1d = pos_1d;

    uint64_t *ret_pos_nd = malloc(sizeof(uint64_t) * dim_len);

    // calculate dim_divisor
    for (int d = dim_len - 1; d > 0; d--)
    {
        dim_divisor *= pos_nd_lens[d];
    }

    // calculate coords
    for (int d = 1; d < dim_len; d++)
    {
        ret_pos_nd[d - 1] = remainder_1d / dim_divisor;
        remainder_1d = remainder_1d % dim_divisor;

        dim_divisor /= pos_nd_lens[d];
    }

    ret_pos_nd[dim_len - 1] = remainder_1d;

    (*pos_nd) = ret_pos_nd;

    return 0;
}

int bf_util_calculate_1d_from_nd_row_major(uint64_t *pos_nd, uint64_t *pos_nd_lens, uint32_t dim_len, uint64_t *pos_1d)
{
    uint64_t ret_pos_1d = 0;

    // calculate coord
    uint64_t base = 1;
    for (int d = dim_len - 1; d >= 0; d--)
    {
        ret_pos_1d += (base * pos_nd[d]);
        base *= pos_nd_lens[d];
    }

    (*pos_1d) = ret_pos_1d;

    return 0;
}

int bf_util_print_coords(uint64_t *coords, uint32_t dim_len)
{
    printf("(");
    for (uint32_t i = 0; i < dim_len; i++)
    {
        printf("%ld,", coords[i]);
    }
    printf(")");
    return 0;
}

int bf_util_write_array_with_incremental_values(
    const char *array_name,
    int *dim_domain,
    uint32_t num_of_dimensions,
    uint32_t attribute_length)
{

    assert(attribute_length == 1);

    // get schema
    tilestore_array_schema_t *schema;
    tilestore_get_array_schema(array_name, &schema);

    // get num of tiles
    size_t num_of_tiles = 1;
    for (uint32_t d = 0; d < num_of_dimensions; d++)
    {
        num_of_tiles *= schema->array_size_in_tile[d];
    }

    // iterate tiles
    for (uint64_t tile_coord_1d = 0; tile_coord_1d < num_of_tiles; tile_coord_1d++)
    {
        // calculate ND coords
        uint64_t *tile_coords;
        bf_util_calculate_nd_from_1d_row_major(
            tile_coord_1d, schema->array_size_in_tile, num_of_dimensions, &tile_coords);

        // prepare data
        uint64_t *tile_size = malloc(num_of_dimensions * sizeof(uint64_t));
        size_t num_of_elems = 1;
        for (uint32_t d = 0; d < schema->num_of_dimensions; d++)
        {
            uint64_t side = schema->tile_extents[d];

            uint8_t is_edge = (schema->array_size_in_tile[d] - 1) == tile_coords[d];
            if (is_edge)
            {
                uint64_t remain = schema->array_size[d] % schema->tile_extents[d];
                if (remain != 0)
                {
                    side = remain;
                }
            }

            tile_size[d] = side;
            num_of_elems *= side;
        }

        size_t data_size = num_of_elems * tilestore_datatype_size(schema->data_type);
        void *data_unaligned = malloc(ceil_to_512bytes(data_size) + 511);
        void *data = aligned_512bytes_ptr(data_unaligned);

        // fill data
        for (uint64_t cell_coord_1d_in_tile = 0;
             cell_coord_1d_in_tile < num_of_elems;
             cell_coord_1d_in_tile++)
        {
            // calculate ND coords in **tile**
            uint64_t *cell_coords_in_tile;
            bf_util_calculate_nd_from_1d_row_major(
                cell_coord_1d_in_tile, tile_size, num_of_dimensions, &cell_coords_in_tile);

            // calculate ND coords in **array**
            uint64_t *cell_coords = malloc(sizeof(uint64_t) * num_of_dimensions);
            for (uint32_t d = 0; d < schema->num_of_dimensions; d++)
            {
                cell_coords[d] = (tile_coords[d] * schema->tile_extents[d]) + cell_coords_in_tile[d];
            }

            // calculate value
            int value = 0;
            int base = 1;
            for (int32_t d = num_of_dimensions - 1; d >= 0; d--)
            {
                value += base * cell_coords[d];
                base *= schema->array_size[d];
            }

            // write
            switch (schema->data_type)
            {
            case TILESTORE_CHAR:
                ((char *)data)[cell_coord_1d_in_tile] = (char)value;
                break;
            case TILESTORE_UINT64:
                ((uint64_t *)data)[cell_coord_1d_in_tile] = (uint64_t)value;
                break;
            case TILESTORE_FLOAT32:
                ((float *)data)[cell_coord_1d_in_tile] = (float)value;
                break;
            case TILESTORE_FLOAT64:
                ((double *)data)[cell_coord_1d_in_tile] = (double)value;
                break;
            case TILESTORE_INT32:
            default:
                ((int *)data)[cell_coord_1d_in_tile] = (int)value;
                break;
            }

            free(cell_coords);
        }

        // tilestore query
        int res = tilestore_write_dense(
            array_name, tile_coords, num_of_dimensions,
            data, data_size, TILESTORE_NORMAL);
        assert(res == TILESTORE_OK);

        free(data_unaligned);
        free(tile_coords);
        free(tile_size);
    }

    return 0;
}

int bf_util_write_sparse_array_with_incremental_values(
    const char *array_name,
    int *dim_domain,
    tilestore_datatype_t attr_type,
    uint32_t num_of_dimensions,
    uint64_t m)
{
    assert(0);
    return 0;
}

char bf_util_pagebuf_get_char(PFpage *page, uint64_t offset)
{
    assert(0);          // DEPRECATED
    return '0';
}

char *bf_util_pagebuf_get_chars(PFpage *page, uint64_t offset)
{
    assert(0);          // DEPRECATED
    return NULL;
}

int bf_util_pagebuf_get_int(PFpage *page, uint64_t offset)
{
    assert(0);          // DEPRECATED
    return 0;
}

float bf_util_pagebuf_get_float(PFpage *page, uint64_t offset)
{
    assert(0);          // DEPRECATED
    return NAN;
}

double bf_util_pagebuf_get_double(PFpage *page, uint64_t offset)
{
    assert(0);          // DEPRECATED
    return NAN;
}

void *bf_util_pagebuf_get_coords(PFpage *page, uint64_t dim_idx)
{
    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;
    BFshm_offset_t *coords = _BF_SHM_PTR_AUTO(mspace, page->coords_o);

    return (void *) _BF_SHM_PTR_AUTO(mspace, coords[dim_idx]);
}

uint64_t *bf_util_pagebuf_get_coords_lens(PFpage *page)
{
    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;
    return (void *) _BF_SHM_PTR_AUTO(mspace, page->coords_lens_o);
}

uint64_t bf_util_pagebuf_get_len(PFpage *page)
{
    return page->pagebuf_len;
}

uint64_t *bf_util_get_coords_of_a_cell_of_idx_uint64(PFpage *page, uint32_t num_of_dims, uint64_t idx)
{
    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;
    BFshm_offset_t *page_coords = _BF_SHM_PTR_AUTO(mspace, page->coords_o);

    uint64_t *coords = malloc(sizeof(uint64_t) * num_of_dims);
    for (uint32_t d = 0; d < num_of_dims; d++)
    {
        int32_t *data = (int32_t *) _BF_SHM_PTR_AUTO(mspace, page_coords[d]);
        coords[d] = (uint64_t)data[idx];
    }

    return coords;
}

int32_t *bf_util_get_coords_of_a_cell_of_idx_int32(PFpage *page, uint32_t num_of_dims, uint64_t idx)
{
    BFmspace *mspace = page->is_input ? _mspace_idata : _mspace_data;
    BFshm_offset_t *page_coords = _BF_SHM_PTR_AUTO(mspace, page->coords_o);

    int32_t *coords = malloc(sizeof(uint64_t) * num_of_dims);
    for (uint32_t d = 0; d < num_of_dims; d++)
    {
        int32_t *data = (int32_t *) _BF_SHM_PTR_AUTO(mspace, page_coords[d]);
        coords[d] = data[idx];
    }

    return coords;
}

void bf_util_pagebuf_set_char(PFpage *page, uint64_t offset, char value)
{
    assert(0);          // DEPRECATED
}

void bf_util_pagebuf_set_int(PFpage *page, uint64_t offset, int value)
{
    assert(0);          // DEPRECATED
}

void bf_util_pagebuf_set_float(PFpage *page, uint64_t offset, float value)
{
    assert(0);          // DEPRECATED
}

void bf_util_pagebuf_set_double(PFpage *page, uint64_t offset, double value)
{
    assert(0);          // DEPRECATED
}

uint64_t bf_util_pagebuf_get_unfilled_idx(PFpage *page)
{
    return page->unfilled_idx;
}

void bf_util_pagebuf_set_unfilled_idx(PFpage *page, uint64_t new_unfilled_idx)
{
    page->unfilled_idx = new_unfilled_idx;
}

uint64_t bf_util_pagebuf_get_unfilled_pagebuf_offset(PFpage *page)
{
    return page->unfilled_pagebuf_offset;
}

void bf_util_pagebuf_set_unfilled_pagebuf_offset(PFpage *page, uint64_t new_unfilled_pagebuf_offset)
{
    page->unfilled_pagebuf_offset = new_unfilled_pagebuf_offset;
}

/**************************************************
 * Some debuging facilities
 **************************************************/

void bf_util_print_pfpage(PFpage *page, const char *arrayname, uint32_t num_of_dims)
{
    fprintf(stderr, "[page %p]\n", (void *)page);

    if (page->pagebuf_o != BF_SHM_NULL)
    {
        // TODO: Now, only support pagebuf that has fix-length size 1 attribute.
        tilestore_datatype_t type;

        fprintf(stderr, "- pagebuf\n");
        storage_util_get_attribute_type(arrayname, "a", &type);
        bf_util_print_array(type, bf_util_get_pagebuf(page), page->unfilled_pagebuf_offset);
    }
}

void bf_util_print_array(tilestore_datatype_t attr_type, void *data, uint64_t data_size)
{
    if (attr_type == TILESTORE_INT32)
    {
        int32_t *typed_data = (int32_t *)data;
        uint64_t len = data_size / sizeof(int32_t);
        fprintf(stderr, "\tdata_size=%lu\n", len);
        fprintf(stderr, "\tdata=");
        for (uint64_t i = 0; i < len; i++)
            fprintf(stderr, "%d,", typed_data[i]);
    }
    else if (attr_type == TILESTORE_CHAR)
    {
        char *typed_data = (char *)data;
        uint64_t len = data_size / sizeof(char);
        fprintf(stderr, "\tdata_size=%lu\n", len);
        fprintf(stderr, "\tdata=");
        for (uint64_t i = 0; i < len; i++)
            fprintf(stderr, "%c,", typed_data[i]);
    }
    else
    {
        // Not supported yet!
    }

    fprintf(stderr, "\n");
}

// Dense <-> CSR conversion

// fill in csr format from dense format
// TODO: Need to be tested
void bf_util_dense_to_csr(
        // Dense
        double *d_pagebuf,
        // CSR
        double *s_pagebuf, uint64_t *indptr, uint64_t *indices,
        // info
        uint64_t *tile_extents, uint64_t dim_len
        ) {

    for (uint64_t row_idx = 0; row_idx < tile_extents[0]; row_idx++) {
        for (uint64_t col_idx = 0; col_idx < tile_extents[1]; col_idx++) {
            uint64_t idx = row_idx * tile_extents[1] + col_idx;
            if (d_pagebuf[idx] != 0) {
                uint64_t csr_idx = indptr[row_idx + 1];
                indices[csr_idx] = col_idx;
                s_pagebuf[csr_idx] = d_pagebuf[idx];

                indptr[row_idx + 1]++;
            }
        }

        if (row_idx < tile_extents[0] - 1) {
            indptr[row_idx + 2] = indptr[row_idx + 1];
        }
    }
}

// fill in csr format from dense format. this function involves BF_Resizebuf().
// TODO: Need to be tested
void bf_util_dense_tile_to_csr_tile(PFpage *dense, PFpage *sparse) {
    double *d_pagebuf = bf_util_get_pagebuf(dense);
    double *s_pagebuf = bf_util_get_pagebuf(sparse);
    uint64_t *indptr = bf_util_pagebuf_get_coords(sparse, 0); 
    uint64_t *indices = bf_util_pagebuf_get_coords(sparse, 1); 
    uint64_t *tile_extents = bf_util_get_tile_extents(dense); 
    uint64_t dim_len = 2;

    uint64_t nnz = 0;
    uint64_t resize_chunk = 32;

    for (uint64_t row_idx = 0; row_idx < tile_extents[0]; row_idx++) {
        for (uint64_t col_idx = 0; col_idx < tile_extents[1]; col_idx++) {
            uint64_t idx = row_idx * tile_extents[1] + col_idx;
            if (d_pagebuf[idx] != 0) {
                uint64_t csr_idx = indptr[row_idx + 1];
                indices[csr_idx] = col_idx;
                s_pagebuf[csr_idx] = d_pagebuf[idx];

                indptr[row_idx + 1]++;
                nnz++;

                // resize
                if (nnz + 1 == sparse->max_idx) {
                    BF_ResizeBuf(sparse, nnz + resize_chunk);
                }
            }
        }

        if (row_idx < tile_extents[0] - 1) {
            indptr[row_idx + 2] = indptr[row_idx + 1];
        }
    }
}

// fill in dense format from csr format
void bf_util_csr_to_dense(
        // Dense
        double *d_pagebuf,
        // CSR
        double *s_pagebuf, uint64_t *indptr, uint64_t *indices,
        // info
        uint64_t *tile_extents, uint64_t dim_len
        ) {

    for (uint64_t row_idx = 0;
            row_idx < tile_extents[0]; 
            row_idx++) {
        for (uint64_t idx = indptr[row_idx]; 
                idx < indptr[row_idx + 1]; 
                idx++) {
            double value = s_pagebuf[idx];
            uint64_t col_idx = indices[idx];

            d_pagebuf[row_idx * tile_extents[1] + col_idx] = value;
        }
    }
}

void* bf_util_get_pagebuf(PFpage *page) {
    if (page->is_input) return BF_SHM_IDATA_PTR(page->pagebuf_o);
    else return BF_SHM_DATA_PTR(page->pagebuf_o);
}

uint64_t* bf_util_get_tile_extents(PFpage *page) {
    if (page->is_input) return BF_SHM_IDATA_PTR(page->tile_extents_o);
    else return BF_SHM_DATA_PTR(page->tile_extents_o);
}

