#ifndef __SIMULATE_H__
#define __SIMULATE_H__

#include "array_struct.h"
#include "chunk_struct.h"

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "exec_interface.h"
#include "node_interface.h"
#include "planner.h"
#include "hash.h"

void increment_consumer_cnt(Array* array, int howmuch);
void calculate_consumer_cnt(Array *array);
void hint_to_bf(Array *array, uint8_t order);
void insert_or_update_future_hash(Array *array, uint64_t idx, uint64_t *ts);
void _fill_future(Array* array, uint64_t idx, uint64_t *ts);
void _fill_future_scala(Array *array, uint64_t *ts);
void fill_future(Array* array);

#endif