#ifndef __LIST__H__
#define __LIST__H__

#include "bf_struct.h"


/* list */

// functions
void BF_list_init();
void BF_list_free();
int BF_list_set_last(BFpage* dataptr);
BFpage* BF_list_evict_page_lru();
BFpage* BF_list_evict_page_mru();

void BF_list_printall();
void _BF_list_printall_skiplist();

BFpage* BF_list_remove(BFpage* target);
BFpage* BF_get_first_page(const char* arrayname);
BFpage* _BF_list_remove(BFList *list, BFpage *target);

int _BF_list_set_to_sorted_order(BFpage* dataptr);

size_t _BF_list_getbuffersize(BFList* list);
void _BF_print_size_of_data_or_idata_in_list(BFList *list);
void _BF_print_size_of_data_or_idata_in_skiplist(BFList *list);

/* skip list */
void _BF_list_init_skiplist(BFList *list);
void _BF_list_free_skiplist(BFList *list);
void _BF_list_delete_skiplist(BFpage* page);
void _BF_list_insert_skiplist(BFpage* page);
BFpage** _BF_list_search_skiplist(BFList *list, uint64_t ts);
BFpage* BF_list_evict_page_opt();
BFpage* BF_list_evict_page_lruk();

#endif
