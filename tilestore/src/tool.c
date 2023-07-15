#include <string.h>
#include <stdlib.h>
#include <math.h>

#include "tilestore.h"

#define min(a, b) ((a > b) ? b : a)
#define max(a, b) ((a < b) ? b : a)

#define BUF_SIZE 4096

void usage() {
    fprintf(stderr, "./tilestore_tool <ARRAY_NAME> list : show schema and inserted tile list\n");
    fprintf(stderr, "\tIn <ARRAY_NAME>, \'.tilestore\' should be ommited.\n");

    fprintf(stderr, "./tilestore_tool <ARRAY_NAME> get <X> <Y> <LIMIT> : show the content of given tile {X, Y}\n");
    fprintf(stderr, "\tIn <ARRAY_NAME>, \'.tilestore\' should be ommited.\n");

    fprintf(stderr, "./tilestore_tool <ARRAY_NAME> upgrade_3_to_4 <OUTPUT> <FORMAT>: upgrade tilestore array from 3 to 4. it stores the result array having FORMAT format type in OUTPUT. \n");
    fprintf(stderr, "\tIn <ARRAY_NAME>, \'.tilestore\' should be ommited.\n");
    fprintf(stderr, "\t<FORMAT> should be DENSE or SPARSE.\n");
    
    // v5 => reserved space for schema file and data header, and fragmentation size in data header.
    fprintf(stderr, "./tilestore_tool <ARRAY_NAME> upgrade_4_to_5 <OUTPUT>: upgrade tilestore array from 4 to 5. \n");
    fprintf(stderr, "\tIn <ARRAY_NAME>, \'.tilestore\' should be ommited.\n");
}

size_t sizeof_tilestore(tilestore_datatype_t type) {
    switch (type) {
        case TILESTORE_CHAR: return 1;
        case TILESTORE_INT32: return sizeof(int32_t);
        case TILESTORE_UINT64: return sizeof(uint64_t);
        case TILESTORE_FLOAT32: return sizeof(float);
        case TILESTORE_FLOAT64: return sizeof(double);
    }
}

int list(const char* arrname) {
    // print schema and inserted tile list
    tilestore_errcode_t err;
    tilestore_array_schema_t *schema;
    if ((err = tilestore_get_array_schema(arrname, &schema)) != TILESTORE_OK) {
        return 1;
    }

    // print schema
    fprintf(stderr, "# of dim = %u\n", schema->num_of_dimensions);
    fprintf(stderr, "data type = %d\n", schema->data_type);
    fprintf(stderr, "format = %s\n", (schema->format == TILESTORE_DENSE) ? "DENSE" : "SPARSE_CSR");
    fprintf(stderr, "array size = {");
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        fprintf(stderr, "%lu,", schema->array_size[d]);
    }
    fprintf(stderr, "}\n");
    fprintf(stderr, "unpadded array size = {");
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        fprintf(stderr, "%lu,", schema->unpadded_array_size[d]);
    }
    fprintf(stderr, "}\n");
    fprintf(stderr, "tile size = {");
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        fprintf(stderr, "%lu,", schema->tile_extents[d]);
    }
    fprintf(stderr, "}\n");

    // print tile list
    uint64_t len1d = 1;
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        len1d *= schema->array_size_in_tile[d];
    }

    fprintf(stderr, "tile list:\n");
    uint64_t *tile_coords = malloc(sizeof(uint64_t) * schema->num_of_dimensions);
    for (uint64_t idx1d = 0; idx1d < len1d; idx1d++) {
        // 1-d -> n-d
        uint64_t base = idx1d;
        for (int32_t d = schema->num_of_dimensions - 1; d >= 0; d--) {
            uint64_t num_of_tiles_in_dim = (uint64_t) ceil((double) schema->array_size[d] / schema->tile_extents[d]);
            tile_coords[d] = base % num_of_tiles_in_dim; 
            base = base / num_of_tiles_in_dim;
        }

        tilestore_tile_metadata_t metadata;
        err = tilestore_read_metadata_of_tile(
            arrname, tile_coords, schema->num_of_dimensions, &metadata);
        if (err == TILESTORE_TILE_NOT_INSERTED) continue;
        if (err != TILESTORE_OK) {
            return 1;
        }
        
        fprintf(stderr, "\t{");
        for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
            fprintf(stderr, "%lu,", tile_coords[d]);
        }
        fprintf(stderr, "} ");

        if (schema->format == TILESTORE_DENSE) {
            fprintf(stderr, "nnz = %lu, offset = %lu, data_nbytes = %lu\n", 
                metadata.data_nbytes / sizeof_tilestore(schema->data_type), metadata.offset, metadata.data_nbytes);
        } else {
            fprintf(stderr, "nnz = %lu, offset = %lu, data_nbytes = %lu, coord_nbytes = %lu, cxs_indptr_nbytes = %lu\n", 
                metadata.nnz, metadata.offset, metadata.data_nbytes, metadata.coord_nbytes, metadata.cxs_indptr_nbytes);
        }
    }

    free(tile_coords);
    tilestore_clear_array_schema(schema);
}

int get(const char* arrname, uint64_t *coords, int printsize) {
    tilestore_errcode_t err;
    tilestore_tile_metadata_t metadata;
    tilestore_array_schema_t *schema;
    if ((err = tilestore_get_array_schema(arrname, &schema)) != TILESTORE_OK) {
        return 1;
    }

    err = tilestore_read_metadata_of_tile(
        arrname, coords, schema->num_of_dimensions, &metadata);
    if (err != TILESTORE_OK) {
        return 1;
    }

    if (schema->format == TILESTORE_DENSE) {
        fprintf(stderr, "type=DENSE\n");
        fprintf(stderr, "size of data=%lu\n", metadata.data_nbytes);

        fprintf(stderr, "assuming type to double\n");
        double *data = malloc(metadata.data_nbytes + 511);
        if ((err = tilestore_read_dense(arrname, metadata, data, metadata.data_nbytes, TILESTORE_NORMAL)) != TILESTORE_OK) {
            return -1;
        }

        uint64_t size = metadata.data_nbytes / sizeof_tilestore(schema->data_type);
        fprintf(stderr, "data(%lu)=[", size);
        for (uint64_t i = 0; i < min(printsize, size); i++) {
            fprintf(stderr, "%.9lf,", data[i]);
        }
        fprintf(stderr, "...,");
        for (uint64_t i = max(size - printsize, 0); i < size; i++) {
            fprintf(stderr, "%.9lf,", data[i]);
        }
        fprintf(stderr, "]\n");
        free(data);
    } else {
        fprintf(stderr, "type=SPARSE\n");
        fprintf(stderr, "size of data=%lu\n", metadata.data_nbytes);
        fprintf(stderr, "size of indptr=%lu\n", metadata.cxs_indptr_nbytes);
        fprintf(stderr, "size of indices=%lu\n", metadata.coord_nbytes);

        fprintf(stderr, "assuming type to double\n");
        double *data = malloc(metadata.data_nbytes + 511);
        uint64_t *indptr = malloc(metadata.cxs_indptr_nbytes + 511);
        uint64_t *indices = malloc(metadata.coord_nbytes + 511);
        uint64_t **coords = malloc(sizeof(uint64_t*) * 2);
        coords[0] = indptr;
        coords[1] = indices;
        uint64_t *coord_sizes = malloc(sizeof(uint64_t) * 2);
        coord_sizes[0] = metadata.cxs_indptr_nbytes;
        coord_sizes[1] = metadata.coord_nbytes;
        if ((err = tilestore_read_sparse_csr(arrname, metadata, data, metadata.data_nbytes, coords, coord_sizes, TILESTORE_NORMAL)) != TILESTORE_OK) {
            return -1;
        }

        uint64_t size = metadata.nnz;
        fprintf(stderr, "data(%lu)=[", size);
        for (uint64_t i = 0; i < min(printsize, size); i++) {
            fprintf(stderr, "%.9lf,", data[i]);
        }
        fprintf(stderr, "...,");
        for (uint64_t i = max(size - printsize, 0); i < size; i++) {
            fprintf(stderr, "%.9lf,", data[i]);
        }
        fprintf(stderr, "]\n");

        size = metadata.cxs_indptr_nbytes / sizeof(uint64_t);
        fprintf(stderr, "indptr(%lu)=[", size);
        for (uint64_t i = 0; i < min(printsize, size); i++) {
            fprintf(stderr, "%lu,", indptr[i]);
        }
        fprintf(stderr, "...,");
        for (uint64_t i = max(size - printsize, 0); i < size; i++) {
            fprintf(stderr, "%lu,", indptr[i]);
        }
        fprintf(stderr, "]\n");

        size = metadata.nnz;
        fprintf(stderr, "indices(%lu)=[", size);
        for (uint64_t i = 0; i < min(printsize, size); i++) {
            fprintf(stderr, "%lu,", indices[i]);
        }
        fprintf(stderr, "...,");
        for (uint64_t i = max(size - printsize, 0); i < size; i++) {
            fprintf(stderr, "%lu,", indices[i]);
        }
        fprintf(stderr, "]\n");

        free(coord_sizes);
        free(coords);
        free(indices);
        free(indptr);
        free(data);
    }
}

int upgrade_3_to_4(const char* input, const char* output, int is_csr) {
    // only change is that newver is storing tile format in metadata.
    // note that oldver still have format type in their tile metadata because of the ease of conversion.

    // modify new tilestore
    tilestore_array_schema_t *schema; 
    if (tilestore_get_array_schema_v3(input, &schema) != TILESTORE_OK) {
        return 1;
    }

    // create new one
    if (tilestore_create_array(
        output, schema->array_size, schema->tile_extents, schema->num_of_dimensions, schema->data_type, 
        is_csr ? TILESTORE_SPARSE_CSR : TILESTORE_DENSE) != TILESTORE_OK) {
        return 1;
    }

    // copy data and hash table
    char buf[BUF_SIZE];
    sprintf(buf, "cp -r %s.tilestore/data %s.tilestore/index* %s.tilestore", input, input, output);
    system(buf);

    return 0;
}


int upgrade_4_to_5(const char* input, const char* output) {
    // read original 
    tilestore_array_schema_t *schema; 
    if (tilestore_get_array_schema_v4(input, &schema) != TILESTORE_OK) {
        return 1;
    }

    // create new one
    if (tilestore_create_array(
        output, schema->array_size, schema->tile_extents, schema->num_of_dimensions, schema->data_type, schema->format) != TILESTORE_OK) {
        return 1;
    }

    // iterate and copy
    uint64_t len1d = 1;
    for (uint32_t d = 0; d < schema->num_of_dimensions; d++) {
        len1d *= schema->array_size_in_tile[d];
    }

    uint64_t *tile_coords = malloc(sizeof(uint64_t) * schema->num_of_dimensions);
    for (uint64_t idx1d = 0; idx1d < len1d; idx1d++) {
        // 1-d -> n-d
        fprintf(stderr, "coords={");
        uint64_t base = idx1d;
        for (int32_t d = schema->num_of_dimensions - 1; d >= 0; d--) {
            uint64_t num_of_tiles_in_dim = (uint64_t) ceil((double) schema->array_size[d] / schema->tile_extents[d]);
            tile_coords[d] = base % num_of_tiles_in_dim; 
            base = base / num_of_tiles_in_dim;

            fprintf(stderr, "%lu,", tile_coords[d]);
        }
        fprintf(stderr, "}\n");

        tilestore_tile_metadata_t metadata;
        tilestore_errcode_t err = tilestore_read_metadata_of_tile_v4(
            input, tile_coords, schema->num_of_dimensions, &metadata);
        if (err == TILESTORE_TILE_NOT_INSERTED) continue;
        if (err != TILESTORE_OK) {
            return 1;
        }
        
        if (schema->format == TILESTORE_DENSE) {
            fprintf(stderr, "\ttype=DENSE\n");
            fprintf(stderr, "\tsize of data=%lu\n", metadata.data_nbytes);

            fprintf(stderr, "\tassuming type to double\n");
            double *data = malloc(metadata.data_nbytes + 511);
            if ((err = tilestore_read_dense_v4(input, metadata, data, metadata.data_nbytes, TILESTORE_NORMAL)) != TILESTORE_OK) {
                fprintf(stderr, "\tcopy failed!!!!!!\n");
                return -1;
            }

            if ((err = tilestore_write_dense(output, tile_coords, schema->num_of_dimensions, data, metadata.data_nbytes, TILESTORE_NORMAL)) != TILESTORE_OK) {
                fprintf(stderr, "\tcopy failed!!!!!!\n");
                return -1;
            }

            fprintf(stderr, "\tcopy done\n");
            free(data);
        } else {
            fprintf(stderr, "\ttype=SPARSE\n");
            fprintf(stderr, "\tsize of data=%lu\n", metadata.data_nbytes);
            fprintf(stderr, "\tsize of indptr=%lu\n", metadata.cxs_indptr_nbytes);
            fprintf(stderr, "\tsize of indices=%lu\n", metadata.coord_nbytes);

            fprintf(stderr, "\tassuming type to double\n");
            double *data = malloc(metadata.data_nbytes + 511);
            uint64_t *indptr = malloc(metadata.cxs_indptr_nbytes + 511);
            uint64_t *indices = malloc(metadata.coord_nbytes + 511);
            uint64_t **coords = malloc(sizeof(uint64_t*) * 2);
            coords[0] = indptr;
            coords[1] = indices;
            uint64_t *coord_sizes = malloc(sizeof(uint64_t) * 2);
            coord_sizes[0] = metadata.cxs_indptr_nbytes;
            coord_sizes[1] = metadata.coord_nbytes;
            if ((err = tilestore_read_sparse_csr_v4(input, metadata, data, metadata.data_nbytes, coords, coord_sizes, TILESTORE_NORMAL)) != TILESTORE_OK) {
                fprintf(stderr, "\tcopy failed!!!!!!\n");
                return -1;
            }

            if ((err = tilestore_write_sparse_csr(output, tile_coords, schema->num_of_dimensions, data, metadata.data_nbytes, coords, coord_sizes, TILESTORE_NORMAL)) != TILESTORE_OK) {
                fprintf(stderr, "\tcopy failed!!!!!!\n");
                return -1;
            }
            
            fprintf(stderr, "\tcopy done\n");

            free(coord_sizes);
            free(coords);
            free(indices);
            free(indptr);
            free(data);
        }
    }

    free(tile_coords);

    return 0;
}

void _write_csr(char *arrname, uint64_t nnz, uint64_t *tile_extents, uint64_t *tile_coords)  {
    /////////////////////////////////
    // prepare memory space
    /////////////////////////////////
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = nnz;
    // data
    size_t data_size_unaligned = sizeof(double) * num_of_elems;
    size_t data_size = ceil_to_512bytes(data_size_unaligned);
    double *data_unaligned = malloc(data_size + 511);
    double *data = aligned_512bytes_ptr(data_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        data[i] = (double) i;
    }
    
    // coords
    // indices (column indices)
    size_t indices_size_unaligned = sizeof(uint64_t) * num_of_elems;
    size_t indices_size = ceil_to_512bytes(indices_size_unaligned);
    uint64_t *indices_unaligned = malloc(indices_size + 511);
    uint64_t *indices = aligned_512bytes_ptr(indices_unaligned);
    for (size_t i = 0; i < num_of_elems; i++) {
        indices[i] = ((uint64_t) i % tile_extents[1]);
    }

    // indptr (row ptr)
    size_t indptr_size_unaligned = sizeof(uint64_t) * (tile_extents[0] + 1);
    size_t indptr_size = ceil_to_512bytes(indptr_size_unaligned);
    uint64_t *indptr_unaligned = malloc(indptr_size + 511);
    uint64_t *indptr = aligned_512bytes_ptr(indptr_unaligned);
    
    indptr[0] = 0;
    for (size_t i = 1; i <= tile_extents[0]; i++) {
        indptr[i] = i * tile_extents[1];
    }

    // make input for tilestore using indices and indptr
    uint64_t **coords = malloc(sizeof(uint64_t*) * 2);         // no need to align
    size_t *coord_sizes = malloc(sizeof(size_t) * 2);      // no need to align
    coords[0] = indptr;         // the first dimension should be a indptr (row ptr) in CSR 
    coords[1] = indices;        // the second dimension should be a indices (column indices) in CSR
    coord_sizes[0] = indptr_size;
    coord_sizes[1] = indices_size;

    /////////////////////////////////
    // write
    /////////////////////////////////
    tilestore_write_sparse_csr(
        arrname, tile_coords, 2,
        data, data_size_unaligned,
        coords, coord_sizes,
        TILESTORE_DIRECT);

    // free
    free(data_unaligned);
    free(indptr_unaligned);
    free(indices_unaligned);
    free(coords);
    free(coord_sizes);
}

int test_compaction() {
    printf("%s started\n", __func__);


    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {100000, 100000};
    uint64_t tile_extents[] = {10000, 10000};

    // create array
    int res = tilestore_create_array(
        "compaction", array_size, tile_extents, num_of_dim, TILESTORE_FLOAT64, TILESTORE_SPARSE_CSR);
    // assert(res == TILESTORE_OK);

    // write_sparse - normal case
    uint64_t tile_coords[] = {0, 0};
    _write_csr("compaction", 1000, tile_extents, tile_coords);

    tile_coords[0] = 0; tile_coords[1] = 1;
    _write_csr("compaction", 1000, tile_extents, tile_coords);

    tile_coords[0] = 0; tile_coords[1] = 2;
    _write_csr("compaction", 1000, tile_extents, tile_coords);

    tile_coords[0] = 0; tile_coords[1] = 0;
    _write_csr("compaction", 400, tile_extents, tile_coords);

    tile_coords[0] = 0; tile_coords[1] = 1;
    _write_csr("compaction", 2000, tile_extents, tile_coords);

    tile_coords[0] = 0; tile_coords[1] = 2;
    _write_csr("compaction", 3000, tile_extents, tile_coords);

    tile_coords[0] = 0; tile_coords[1] = 2;
    _write_csr("compaction", 4000, tile_extents, tile_coords);

    return 0;
}

int compaction(char *arrname) {
    return tilestore_compaction(arrname);
}

int main(int argc, char* argv[]) {
    char *arrname = argv[1];
    char *op = argv[2];

    if (argc < 3) {
        usage();
    } else {
        if (strcmp(op, "list") == 0) return list(arrname);
        else if (strcmp(op, "get") == 0) {
            uint64_t tile_coords[] = {
                (uint64_t) atol(argv[3]),
                (uint64_t) atol(argv[4])
            };

            int printsize = 4;
            if (argc == 6) {
                printsize = atoi(argv[5]);
                if (printsize == -1) printsize = INT32_MAX;
            }

            return get(arrname, tile_coords, printsize);
        } else if (strcmp(op, "upgrade_3_to_4") == 0) {
            char *out = argv[3];
            int is_csr = strcmp(argv[4], "SPARSE") == 0;
            return upgrade_3_to_4(arrname, out, is_csr);
        } else if (strcmp(op, "upgrade_4_to_5") == 0) {
            char *out = argv[3];
            return upgrade_4_to_5(arrname, out);
        } else if (strcmp(op, "test_compaction") == 0) {
            return test_compaction();
        } else if (strcmp(op, "compaction") == 0) {
            return compaction(arrname);
        } 
    }

    return 0;
}
