#ifndef __TILEIO__H__
#define __TILEIO__H__

#include "tilestore.h"

#include "arraykey.h"
#include "bf_struct.h"

typedef enum bf_tileio_ret_t {
    BF_TILEIO_OK,
    BF_TILEIO_NONE,
    BF_TILEIO_ERR
} bf_tileio_ret_t;


int free_pfpage_content(PFpage *page);
bf_tileio_ret_t read_from_tilestore(BFpage *free_BFpage, _array_key *bq);
bf_tileio_ret_t write_to_tilestore(BFpage *page);
int free_pfpage(PFpage *page);


#endif