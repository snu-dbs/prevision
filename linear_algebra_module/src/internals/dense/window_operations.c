#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"

void window_dense_count(Chunk *chunk, int64_t *window_size, Chunk *result_chunk)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);
    while (1)
    {
        chunk_get_chunk_window(chunk_iter, window_size, &window);
        window_iter = chunk_iterator_init(window);
        int window_count = 0;
        while (1)
        {
            window_count++;
            if (!chunk_iterator_has_next(window_iter))
            {
                break;
            }
            chunk_iterator_get_next(window_iter);
        }
        /* Write cell value to result chunk */
        chunk_iterator_write_cell_int(result_chunk_iter, window_count);
        chunk_iterator_free(window_iter);
        chunk_destroy(window);
        if (!chunk_iterator_has_next(chunk_iter))
        {
            break;
        }

        chunk_iterator_get_next(chunk_iter);
        chunk_iterator_get_next(result_chunk_iter);
    }
    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}

void window_dense_sum(Chunk *chunk, int64_t *window_size, Chunk *result_chunk, int opnd_attr_type)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        while (1)
        {

            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            int window_sum = 0;
            while (1)
            {
                window_sum += chunk_iterator_get_cell_int(window_iter);
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_int(result_chunk_iter, window_sum);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }

            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            float window_sum = 0;
            while (1)
            {
                window_sum += chunk_iterator_get_cell_float(window_iter);
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_int(result_chunk_iter, window_sum);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            double window_sum = 0;
            while (1)
            {
                window_sum += chunk_iterator_get_cell_double(window_iter);
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_int(result_chunk_iter, window_sum);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}

void window_dense_avg(Chunk *chunk, int64_t *window_size, Chunk *result_chunk, int opnd_attr_type)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            int window_sum = 0;
            int window_count = 0;
            float window_avg = 0;
            while (1)
            {
                window_sum += chunk_iterator_get_cell_int(window_iter);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = (float)window_sum / (float)window_count;
            chunk_iterator_write_cell_float(result_chunk_iter, window_avg);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            float window_sum = 0, window_avg = 0;
            int window_count = 0;
            while (1)
            {
                window_sum += chunk_iterator_get_cell_float(window_iter);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = window_sum / (float)window_count;
            chunk_iterator_write_cell_float(result_chunk_iter, window_avg);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            double window_sum = 0, window_avg = 0;
            int window_count = 0;
            while (1)
            {
                window_sum += chunk_iterator_get_cell_double(window_iter);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = window_sum / (double)window_count;
            chunk_iterator_write_cell_int(result_chunk_iter, window_avg);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}

void window_dense_max(Chunk *chunk, int64_t *window_size, Chunk *result_chunk, int opnd_attr_type)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            int window_max = chunk_iterator_get_cell_int(window_iter);
            while (1)
            {
                if (chunk_iterator_get_cell_int(window_iter) > window_max)
                {
                    window_max = chunk_iterator_get_cell_int(window_iter);
                }
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_int(result_chunk_iter, window_max);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            float window_max = chunk_iterator_get_cell_int(window_iter);
            while (1)
            {
                if (chunk_iterator_get_cell_float(window_iter) > window_max)
                {
                    window_max = chunk_iterator_get_cell_float(window_iter);
                }
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_float(result_chunk_iter, window_max);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            double window_max = chunk_iterator_get_cell_double(window_iter);
            while (1)
            {
                if (chunk_iterator_get_cell_double(window_iter) > window_max)
                {
                    window_max = chunk_iterator_get_cell_double(window_iter);
                }
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_double(result_chunk_iter, window_max);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}

void window_dense_min(Chunk *chunk, int64_t *window_size, Chunk *result_chunk, int opnd_attr_type)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            int window_min = chunk_iterator_get_cell_int(window_iter);
            while (1)
            {
                if (chunk_iterator_get_cell_int(window_iter) < window_min)
                {
                    window_min = chunk_iterator_get_cell_int(window_iter);
                }
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_int(result_chunk_iter, window_min);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            float window_min = chunk_iterator_get_cell_float(window_iter);
            while (1)
            {
                if (chunk_iterator_get_cell_float(window_iter) < window_min)
                {
                    window_min = chunk_iterator_get_cell_float(window_iter);
                }
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_float(result_chunk_iter, window_min);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            double window_min = chunk_iterator_get_cell_double(window_iter);
            while (1)
            {
                if (chunk_iterator_get_cell_double(window_iter) > window_min)
                {
                    window_min = chunk_iterator_get_cell_double(window_iter);
                }
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            chunk_iterator_write_cell_double(result_chunk_iter, window_min);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}

/* Variance operation output type:
    input : int -> output : float
    input : float -> output : float
    input : double -> output : double
*/

void window_dense_var(Chunk *chunk, int64_t *window_size, Chunk *result_chunk, int opnd_attr_type)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            int window_sum = 0;
            int window_square_sum = 0;
            int window_count = 0;
            float window_avg = 0, window_var = 0, sq_avg = 0;
            while (1)
            {
                int curr = chunk_iterator_get_cell_int(window_iter);
                window_sum += chunk_iterator_get_cell_int(window_iter);
                window_square_sum += pow(curr, 2);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = (float)window_sum / (float)window_count;
            sq_avg = (float)window_square_sum / (float)window_count;
            window_var = sqrt(sq_avg - pow(window_avg, 2));
            chunk_iterator_write_cell_float(result_chunk_iter, window_var);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            float window_sum = 0;
            float window_square_sum = 0;
            int window_count = 0;
            float window_avg = 0, window_var = 0, sq_avg = 0;
            while (1)
            {
                float curr = chunk_iterator_get_cell_float(window_iter);
                window_sum += chunk_iterator_get_cell_float(window_iter);
                window_square_sum += pow(curr, 2);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = window_sum / (float)window_count;
            sq_avg = window_square_sum / (float)window_count;
            window_var = sqrt(sq_avg - pow(window_avg, 2));
            chunk_iterator_write_cell_float(result_chunk_iter, window_var);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            double window_sum = 0;
            double window_square_sum = 0;
            int window_count = 0;
            double window_avg = 0, window_var = 0, sq_avg = 0;
            while (1)
            {
                double curr = chunk_iterator_get_cell_double(window_iter);
                window_sum += curr;
                window_square_sum += pow(chunk_iterator_get_cell_double(window_iter), 2);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = window_sum / (double)window_count;
            sq_avg = window_square_sum / (double)window_count;
            window_var = sqrt(sq_avg - pow(window_avg, 2));
            chunk_iterator_write_cell_double(result_chunk_iter, window_var);
            // window_num++;
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}

/* Std deviation operation output type:
    input : int -> output : float
    input : float -> output : float
    input : double -> output : double
*/

void window_dense_stdev(Chunk *chunk, int64_t *window_size, Chunk *result_chunk, int opnd_attr_type)
{
    ChunkIterator *chunk_iter, *result_chunk_iter;
    ChunkIterator *window_iter;
    Chunk *window;
    chunk_iter = chunk_iterator_init(chunk);
    result_chunk_iter = chunk_iterator_init(result_chunk);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            long long int window_sum = 0;
            long long int window_square_sum = 0;
            int window_count = 0;
            float window_avg = 0, window_var = 0, window_stdev = 0, sq_avg;
            while (1)
            {
                int curr = chunk_iterator_get_cell_int(window_iter);
                window_sum += curr;
                window_square_sum += pow(curr, 2);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = (float)window_sum / (float)window_count;
            sq_avg = (float)window_square_sum / (float)window_count;
            window_var = sqrt(sq_avg - pow(window_avg, 2));
            window_stdev = sqrt(window_var);
            chunk_iterator_write_cell_float(result_chunk_iter, window_stdev);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            float window_sum = 0;
            float window_square_sum = 0;
            int window_count = 0;
            float window_avg = 0, window_var = 0, window_stdev = 0, sq_avg;
            while (1)
            {
                float curr = chunk_iterator_get_cell_float(window_iter);
                window_sum += curr;
                window_square_sum += pow(curr, 2);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = window_sum / (float)window_count;
            sq_avg = window_square_sum / (float)window_count;
            window_var = sqrt(sq_avg - pow(window_avg, 2));
            window_stdev = sqrt(window_var);
            chunk_iterator_write_cell_float(result_chunk_iter, window_stdev);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        while (1)
        {
            chunk_get_chunk_window(chunk_iter, window_size, &window);
            window_iter = chunk_iterator_init(window);
            double window_sum = 0;
            double window_square_sum = 0;
            int window_count = 0;
            double window_avg = 0, window_var = 0, window_stdev = 0, sq_avg = 0;
            while (1)
            {
                double curr = chunk_iterator_get_cell_double(window_iter);
                window_sum += curr;
                window_square_sum += pow(curr, 2);
                window_count++;
                if (!chunk_iterator_has_next(window_iter))
                {
                    break;
                }
                chunk_iterator_get_next(window_iter);
            }
            /* Write cell value to result chunk */
            window_avg = window_sum / (double)window_count;
            sq_avg = window_square_sum / (double)window_count;
            window_var = sqrt(sq_avg - pow(window_avg, 2));
            window_stdev = sqrt(window_var);
            chunk_iterator_write_cell_float(result_chunk_iter, window_stdev);
            chunk_iterator_free(window_iter);
            chunk_destroy(window);
            if (!chunk_iterator_has_next(chunk_iter))
            {
                break;
            }
            chunk_iterator_get_next(chunk_iter);
            chunk_iterator_get_next(result_chunk_iter);
        }
    }

    chunk_iterator_free(chunk_iter);
    chunk_iterator_free(result_chunk_iter);
}