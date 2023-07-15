#include <stdio.h>
#include <math.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/wait.h>

#include "bf.h"
#include "utils.h"

#include "bf_test.h"


int main()
{
    // common test cases
    util_tests();
    bf_tests();
    dense_tests();
    sparse_tests();
    shared_mem_tests();

    // uncomment only when you test bf for performance reason
    // perf_test();

    return 0;
}