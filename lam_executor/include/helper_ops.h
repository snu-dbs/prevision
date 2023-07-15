#ifndef LAM_EXECUTOR_HELPER_OPS_H
#define LAM_EXECUTOR_HELPER_OPS_H

#include "array_struct.h"

void csv_to_sparse_rowmajor(char *in_csvpath, char delimeter, bool is_weighted, Array *outarr);

#endif // LAM_EXECUTOR_HELPER_OPS_H
