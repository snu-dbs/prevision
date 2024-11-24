#include <stdio.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"

/*
 * Helper (util) operations
 */


void csv_to_sparse_rowmajor(
        char *in_csvpath, 
        char delimeter, 
        bool is_weighted, 
        Array *outarr) {
    /* 
     * Fill in `outarr` from given csv file.
     * 
     * Restrictions:
     *      1. The input csv file must be sorted in row-major order.
     *      2. As much as nnz in `tilesize_row * # of columns` of spare buffer space is needed.
     *      3. Currently, only double value is acceptable.
     *      4. Currently, only `\t` delimeter is acceptable.
     *      5. Currently, only fit tile size is acceptable.
     * 
     * Method:
     *      1. Create buckets to maintain cells of tilesize[0] * arrsize[1] section. The number of buckets is tilesize[1].
     *      2. Read the section from the csv file and fill in the buckets.
     *      3. Write buckets to the Array.
     *      4. Repeat the aboves until EOF.
     */

    assert(outarr->desc.attr_type == TILESTORE_FLOAT64);
    assert(is_weighted == false && delimeter == '\t');       // weighted is not supported yet.

    // Metadata
    uint64_t arraysize[] = {
        outarr->desc.dim_domains[0][1] - outarr->desc.dim_domains[0][0] + 1,
        outarr->desc.dim_domains[1][1] - outarr->desc.dim_domains[1][0] + 1
    };
    
    uint64_t *tilesize = outarr->desc.tile_extents;
    uint64_t num_of_buckets = (uint64_t) ceil((double) arraysize[1] / tilesize[1]);
    uint64_t num_of_rowtiles = (uint64_t) ceil((double) arraysize[0] / tilesize[0]);

    // create nnz
    uint64_t *bnnz = BF_shm_malloc(_mspace_data, sizeof(uint64_t) * num_of_buckets);
    uint64_t *bi = BF_shm_malloc(_mspace_data, sizeof(uint64_t) * num_of_buckets);
    
    // csv read to memory read csv file line-by-line and fill the dense_data
    FILE *csv = fopen(in_csvpath, "r");
    long int f_lastoffset = 0, f_offset = 0;
    
    fprintf(stderr, "[LAM_EXECUTOR] csv_to_sparse_rowmajor - csv read\n");
    uint64_t __i;           // debugging
    // big loop
    for (uint64_t curr_tile_i = 0; curr_tile_i < num_of_rowtiles; curr_tile_i++) {
        // initialize
        for (uint64_t i = 0; i < num_of_buckets; i++) {
            bnnz[i] = bi[i] = 0;
        }

        // Get nnz for each buckets first.
        f_lastoffset = f_offset;
        assert(fseek(csv, f_lastoffset, SEEK_SET) == 0);
        __i = 0;
        while (!feof(csv)) {
            assert((f_offset = ftell(csv)) >= 0);
            double val;
            int32_t col, row;
            assert(fscanf(csv, "%d,%d,%lf\n", &row, &col, &val) == 3);

            uint64_t urow = (uint64_t) row;
            uint64_t ucol = (uint64_t) col;

            // break if outside of the fiber
            int in_tile_row = urow >= (curr_tile_i * tilesize[0]) && 
                urow < ((curr_tile_i + 1) * tilesize[0]);
            if (!in_tile_row) break;

            // increment nnz
            uint64_t bid = ucol / tilesize[1];
            bnnz[bid]++;

            // debugging
            if (__i++ % 1000 == 0) {
                fprintf(stderr, "\r[LAM_EXECUTOR] %lu", __i++);
            }
        }
        fprintf(stderr, "\r[LAM_EXECUTOR] csv_to_sparse_rowmajor - first csv read done.\n");

        // Now, create buckets and fill in it
        // create buckets
        uint64_t **brows = BF_shm_malloc(_mspace_data, sizeof(uint64_t*) * num_of_buckets);
        uint64_t **bcols = BF_shm_malloc(_mspace_data, sizeof(uint64_t*) * num_of_buckets);
        double **bvals = BF_shm_malloc(_mspace_data, sizeof(double*) * num_of_buckets);
        
        for (uint64_t i = 0; i < num_of_buckets; i++) {
            brows[i] = BF_shm_malloc(_mspace_data, sizeof(uint64_t) * bnnz[i]);
            bcols[i] = BF_shm_malloc(_mspace_data, sizeof(uint64_t) * bnnz[i]);
            bvals[i] = BF_shm_malloc(_mspace_data, sizeof(double) * bnnz[i]);
        }

        __i = 0;
        assert(fseek(csv, f_lastoffset, SEEK_SET) == 0);
        while (!feof(csv)) {
            double val;
            int32_t col, row;
            assert(fscanf(csv, "%d,%d,%lf\n", &row, &col, &val) == 3);

            uint64_t urow = (uint64_t) row;
            uint64_t ucol = (uint64_t) col;

            // break if outside of the fiber
            int in_tile_row = urow >= (curr_tile_i * tilesize[0]) && 
                urow < ((curr_tile_i + 1) * tilesize[0]);
            if (!in_tile_row) break;

            // fill bucket
            uint64_t bid = ucol / tilesize[1];
            brows[bid][bi[bid]] = urow;
            bcols[bid][bi[bid]] = ucol;
            bvals[bid][bi[bid]++] = val;

            // debugging
            if (__i++ % 1000 == 0) {
                fprintf(stderr, "\r[LAM_EXECUTOR] %lu", __i++);
            }
        }
        fprintf(stderr, "\r[LAM_EXECUTOR] csv_to_sparse_rowmajor - second csv read done.\n");

        for (uint64_t curr_tile_j = 0; curr_tile_j < num_of_buckets; curr_tile_j++) {
            fprintf(stderr, "[LAM_EXECUTOR] csv_to_sparse_rowmajor - attempt to write {%lu,%lu}.\n", curr_tile_i, curr_tile_j);
            
            // we don't need to write a tile if nnz is zero
            if (bnnz[curr_tile_j] == 0) continue;
            
            // allocate memory for csr format
            uint64_t *indptr = BF_shm_malloc(_mspace_data, (tilesize[0] + 1) * sizeof(uint64_t));
            uint64_t *indices = BF_shm_malloc(_mspace_data, bnnz[curr_tile_j] * sizeof(uint64_t));
            double *data = BF_shm_malloc(_mspace_data, bnnz[curr_tile_j] * sizeof(double));

            // iterate over dense_data and construct CSR format
            uint64_t latest_row = 0;
            indptr[0] = 0;
            indptr[1] = 0;
            for (uint64_t idx = 0; idx < bnnz[curr_tile_j]; idx++) {
                // get row and col
                uint64_t row = brows[curr_tile_j][idx] % tilesize[0];
                uint64_t col = bcols[curr_tile_j][idx] % tilesize[1];
                
                // fill indptr if row of this iteration is higher than the latest row.
                while (latest_row < row) {
                    latest_row++;
                    indptr[latest_row + 1] = indptr[latest_row];
                }

                // fill the csr
                indices[indptr[row + 1]] = col;
                data[indptr[row + 1]] = bvals[curr_tile_j][idx];
                indptr[row + 1]++;
            }

            while (latest_row < (tilesize[0] - 1)) {
                latest_row++;
                indptr[latest_row + 1] = indptr[latest_row];
            }

            uint64_t real_nnz = indptr[tilesize[0]];

            fprintf(stderr, "[LAM_EXECUTOR] csv_to_sparse_rowmajor - CSR constructed.\n");

            // write to tilestore
            uint64_t tile_coords[] = {curr_tile_i, curr_tile_j};
            uint64_t **coords = malloc(2 * sizeof(uint64_t*));
            coords[0] = indptr;
            coords[1] = indices;
            uint64_t *coord_sizes = malloc(sizeof(uint64_t) * 2);
            coord_sizes[0] = (tilesize[0] + 1) * sizeof(uint64_t);
            coord_sizes[1] = real_nnz * sizeof(uint64_t);
            
            if (tilestore_write_sparse_csr(
                    outarr->desc.object_name, tile_coords, 2, 
                    data, real_nnz * sizeof(double), 
                    coords, coord_sizes, TILESTORE_NORMAL) != TILESTORE_OK) {

                fprintf(stderr, "[LAM_EXECUTOR] csv_to_sparse_rowmajor - write failed!!!!!!!!!!!!!!!!!!!!!!!!!!! exit!\n");
                return;
            }

            fprintf(stderr, "[LAM_EXECUTOR] csv_to_sparse_rowmajor - write is done\n");
            
            free(coord_sizes);
            free(coords);
            BF_shm_free(_mspace_data, data);
            BF_shm_free(_mspace_data, indices);
            BF_shm_free(_mspace_data, indptr);
        }

        for (uint64_t i = 0; i < num_of_buckets; i++) {
            BF_shm_free(_mspace_data, brows[i]);
            BF_shm_free(_mspace_data, bcols[i]);
            BF_shm_free(_mspace_data, bvals[i]);
        }

        BF_shm_free(_mspace_data, brows);
        BF_shm_free(_mspace_data, bcols);
        BF_shm_free(_mspace_data, bvals);
    }

    fclose(csv);
    BF_shm_free(_mspace_data, bi);
    BF_shm_free(_mspace_data, bnnz);

    return;
}


// int main(int argc, char* argv[])
// {
//     // DENSE
//     // csv_to_tilestore_dense_tallskinny(argv[1]);
//     // return 0;

//     // csv_to_tilestore_dense_square_inmem(argv[1]);
//     // return 0;
    
//     // SPARSE
//     csv_to_tilestore_sparse_inmem(argv[1], 1468365182);
//     return 0;
// }
