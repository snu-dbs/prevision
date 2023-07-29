#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>

#include "tilestore.h"

#define BUFSIZE 1024

void csv_to_tilestore_dense_tallskinny(
        char *in_mat_path, char *out_mat_path, uint64_t row, uint64_t col, uint64_t trow, uint64_t tcol) {
    // TODO: This function only works for tall-skinny matrix.

    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {row, col};
    uint64_t tile_extents[] = {trow, tcol};

    // create array
    int res = tilestore_create_array(out_mat_path, array_size, tile_extents, num_of_dim, TILESTORE_FLOAT64, TILESTORE_DENSE);
    
    // write to the array
    // input data should be aligned to 512 byte memory address
    size_t num_of_elems = (tile_extents[0] * tile_extents[1]);
    size_t data_size_unaligned = sizeof(double) * num_of_elems;
    double *data_unaligned = malloc(ceil_to_512bytes(data_size_unaligned) + 511);
    double *data = aligned_512bytes_ptr(data_unaligned);

    // open in_mat_path
    FILE *csv = fopen(in_mat_path, "r");
    int row_chunk = (int) tile_extents[0];
    int col_size = (int) array_size[1];
    int row_idx = 0;
    while (!feof(csv)) {
        for (int col_idx = 0; col_idx < col_size; col_idx++) {
            double val;
            char nullbuf;

            fscanf(csv, "%lf", &val);
            data[((row_idx % row_chunk) * col_size) + col_idx] = val;

            if (col_idx < (col_size - 1)) {
                fscanf(csv, ",");
            } else {
                fscanf(csv, "\n");
            }
        }

        row_idx++;
        if ((row_idx % row_chunk) == 0) {
            uint64_t tile_coords[2] = {(row_idx / row_chunk) - 1, 0};
            printf("write a tile: {%lu,%lu}\n", tile_coords[0], tile_coords[1]);
            tilestore_write_dense(out_mat_path, tile_coords, num_of_dim, data, data_size_unaligned, TILESTORE_NORMAL);
        }
    }

    return;
}

void csv_to_tilestore_sparse(
        char *in_mat_path, char *out_mat_path, uint64_t row, uint64_t col, uint64_t trow, uint64_t tcol) {
    // create array
    // define schema
    uint32_t num_of_dim = 2;
    uint64_t array_size[] = {row, col};
    uint64_t tile_extents[] = {trow, tcol};

    // create array
    int res = tilestore_create_array(out_mat_path, array_size, tile_extents, num_of_dim, TILESTORE_FLOAT64, TILESTORE_SPARSE_CSR);

    uint64_t size_in_tile[] = {
        array_size[0] / tile_extents[0], 
        array_size[1] / tile_extents[1]
    };

    // TODO: not optimized method
    // for each tiles
    for (uint64_t ti_row = 0; ti_row < size_in_tile[0]; ti_row++) {
        for (uint64_t ti_col = 0; ti_col < size_in_tile[1]; ti_col++) {
            fprintf(stderr, "{%lu,%lu} started \n", ti_row, ti_col);

            // allocate memory of dense tile format
            size_t num_of_elems = (tile_extents[0] * tile_extents[1]);
            double *dense_data = calloc(num_of_elems, sizeof(double));
            
            // read csv file line-by-line and fill the dense_data
            FILE *csv = fopen(in_mat_path, "r");
            size_t nnz = 0;
            size_t __i = 0;
            while (!feof(csv)) {
                double val = 1;
                //double val;
                int32_t col, row;
                // fscanf(csv, "%d,%d,%lf\n", &row, &col, &val);
                fscanf(csv, "%d\t%d\n", &row, &col);

                uint64_t urow = (uint64_t) row - 1;
                uint64_t ucol = (uint64_t) col - 1;

                int in_tile = urow >= (ti_row * tile_extents[0]) && 
                    urow < ((ti_row + 1) * tile_extents[0]) &&
                    ucol >= (ti_col * tile_extents[1]) &&
                    ucol < ((ti_col + 1) * tile_extents[1]);
                if (in_tile) {
                    urow = urow % tile_extents[0];
                    ucol = ucol % tile_extents[1];

                    uint64_t idx = urow * tile_extents[1] + ucol;
                    dense_data[idx] = val;

                    nnz++;
                }
                // fprintf(stderr, "%d\t%d\t%lf\n", row, col, val);
                fprintf(stderr, "\r%lu", __i++);
            }
            fclose(csv);
            fprintf(stderr, " / dense filled (nnz=%lu) ", nnz);

            // allocate memory for csr format
            // uint64_t aligned_nnz_size = ceil_to_512bytes(nnz * )
            uint64_t *indptr = malloc((tile_extents[0] + 1) * sizeof(uint64_t));
            uint64_t *indices = malloc(nnz * sizeof(uint64_t));
            double *data = malloc(nnz * sizeof(double));

            // iterate over dense_data and construct CSR format
            uint64_t latest_row = 0;
            indptr[0] = 0;
            indptr[1] = 0;
            for (uint64_t idx = 0; idx < num_of_elems; idx++) {
                // skip if it is zero value
                if (dense_data[idx] == 0) continue;
                
                // get row and col
                uint64_t row = idx / tile_extents[1];
                uint64_t col = idx % tile_extents[1];
                
                // fill indptr if row of this iteration is higher than the latest row.
                while (latest_row < row) {
                    latest_row++;
                    indptr[latest_row + 1] = indptr[latest_row];
                }

                // fill the csr
                indices[indptr[row + 1]] = col;
                data[indptr[row + 1]] = dense_data[idx];
                indptr[row + 1]++;
                // fprintf(stderr, "%lu,%lu,%lf\n", row, col, dense_data[idx]);
            }

            while (latest_row < (tile_extents[0] - 1)) {
                latest_row++;
                indptr[latest_row + 1] = indptr[latest_row];
            }

            uint64_t real_nnz = indptr[tile_extents[0]];

            fprintf(stderr, "/ CSR constructed ");

            // write to tilestore
            uint64_t tile_coords[] = {ti_row, ti_col};
            uint64_t **coords = malloc(2 * sizeof(uint64_t*));
            coords[0] = indptr;
            coords[1] = indices;
            uint64_t *coord_sizes = malloc(sizeof(uint64_t) * 2);
            coord_sizes[0] = (tile_extents[0] + 1) * sizeof(uint64_t);
            coord_sizes[1] = real_nnz * sizeof(uint64_t);
            
            if (tilestore_write_sparse_csr(
                out_mat_path, tile_coords, num_of_dim, 
                data, real_nnz * sizeof(double), 
                coords, coord_sizes, TILESTORE_NORMAL) != TILESTORE_OK) {

                fprintf(stderr, "/ write failed!!!!!!!!!!!!!!!!!!!!!!!!!!! exited \n");
                return;
            }

            fprintf(stderr, "/ write is done\n");
            
            free(coord_sizes);
            free(coords);
            free(data);
            free(indices);
            free(indptr);
            free(dense_data);
        }
    }

    return;
}

int main(int argc, char* argv[])
{
    char *input = argv[1];
    char *output = argv[2];
    uint64_t row = (uint64_t) atoi(argv[4]);
    uint64_t col = (uint64_t) atoi(argv[5]);
    uint64_t trow = (uint64_t) atoi(argv[6]);
    uint64_t tcol = (uint64_t) atoi(argv[7]);
    if (strcmp(argv[3], "dense") == 0) {
        // dense
        csv_to_tilestore_dense_tallskinny(input, output, row, col, trow, tcol);
    } else {
        // sparse
        csv_to_tilestore_sparse(input, output, row, col, trow, tcol);
    }

    return 0;
}
