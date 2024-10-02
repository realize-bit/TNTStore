#ifndef IN_MEMORY_TNT
#define IN_MEMORY_TNT 1

#include "indexes/tnt_centree.h"
#include "indexes/tnt_subtree.h"

#define INDEX_TYPE "TNT"
#define tnt_tree_lookup centree_worker_lookup
#define tnt_index_add_utree subtree_worker_lookup_utree
#define tnt_index_lookup_utree subtree_worker_lookup_utree
#define tnt_index_invalid_utree subtree_worker_invalid_utree
#define tnt_index_delete subtree_worker_delete

void btree_init(void);
struct index_entry *btree_worker_lookup(int worker_id, void *item);
void btree_worker_delete(int worker_id, void *item);
void btree_index_add(struct slab_callback *cb, void *item);

enum fsst_mode { GC, FSST };

tree_entry_t *centree_worker_lookup(void *key);

int subtree_worker_invalid_utree(subtree_t *tree, void *item);
index_entry_t *subtree_worker_lookup_utree(subtree_t *tree, void *item);
index_entry_t *subtree_worker_lookup_ukey(subtree_t *tree, uint64_t key);
int subtree_worker_delete(subtree_t *tree, void *item);

void centree_init(void);
struct tree_entry *tnt_worker_lookup(int worker_id, void *item);

uint64_t tnt_get_centree_level(void *n);
subtree_t *tnt_subtree_create(void);
void tnt_subtree_add(struct slab *s, void *tree, void *filter,
                     uint64_t tmp_key);
void tnt_subtree_delete(int worker_id, void *item);
void tnt_subtree_update_key(uint64_t old_key, uint64_t new_key);

tree_entry_t *tnt_subtree_get(void *key, uint64_t *idx, index_entry_t *old_e);
tree_entry_t *tnt_traverse_use_seq(int seq);

int tnt_get_nodes_at_level(int level, background_queue *q);

void tnt_index_add(struct slab_callback *cb, void *item);
index_entry_t *tnt_index_lookup(void *item);
int tnt_index_invalid(void *item);

struct index_scan tnt_scan(void *item, uint64_t size);
void tnt_print(void);

background_queue *bgq_get(enum fsst_mode m);
int bgq_is_empty(enum fsst_mode m);
int bgq_count(enum fsst_mode m);
void bgq_enqueue(enum fsst_mode m, centree_node n);
tree_entry_t *bgq_dequeue(enum fsst_mode m);
tree_entry_t *bgq_front(enum fsst_mode m);
centree_node bgq_front_node(enum fsst_mode m);

centree_node dequeue_specific_node(background_queue *queue,
                                   centree_node target);
tree_entry_t *get_next_node_entry(background_queue *queue, centree_node target);
centree_node get_next_node(background_queue *queue, centree_node target);

#include "indexes/filter.h"
//#define memory_index_init btree_init
//#define memory_index_add btree_index_add
//#define memory_index_add_utree btree_index_add_utree
//#define memory_index_lookup btree_worker_lookup
//#define memory_index_delete_utree btree_worker_delete_utree

#endif
