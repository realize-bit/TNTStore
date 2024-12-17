#ifndef IN_MEMORY_SKT
#define IN_MEMORY_SKT 1

#include "indexes/tnt_centree.h"
#include "indexes/tnt_subtree.h"

//#define INDEX_TYPE "SKT"

// SKT 관련 함수 선언 추가
void skt_init(void);
tree_entry_t *skt_first_subtree(void *key, uint64_t *idx, index_entry_t *old_e);
tree_entry_t *skt_last_subtree(void *key, uint64_t *idx, index_entry_t *old_e);
tree_entry_t *skt_prev_last_subtree(void *key, uint64_t *idx, index_entry_t *old_e);
tree_entry_t *skt_subtree_get(void *key, uint64_t *idx, index_entry_t *old_e);
void skt_subtree_update_key(uint64_t old_key, uint64_t new_key);
index_entry_t *skt_index_lookup(void *item);
void skt_index_add(struct slab_callback *cb, void *item);
int skt_index_invalid(void *item);
subtree_t *skt_subtree_create(void);
void skt_list_worker_insert(subtree_t *t, uint64_t key);
void skt_subtree_split(int degree, struct slab *target_s);
void skt_print(void);
int create_skt_root_slab(void);

#endif
