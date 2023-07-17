#ifndef IN_MEMORY_BTREE
#define IN_MEMORY_BTREE 1

#include "indexes/btree.h"

#define INDEX_TYPE "btree"
#define memory_index_init btree_init
#define memory_index_add btree_index_add
#define memory_index_lookup btree_worker_lookup
#define memory_index_delete btree_worker_delete
#define memory_index_scan btree_init_scan

void btree_init(void);
struct index_entry *btree_worker_lookup(int worker_id, void *item);
void btree_worker_delete(int worker_id, void *item);
struct index_scan btree_init_scan(void *item, size_t scan_size);
void btree_index_add(struct slab_callback *cb, void *item);
btree_t* btree_tnt_create(void);

#include "indexes/rbtree.h"
#define tnt_tree_init rbtree_init
#define tnt_tree_add rbtree_tree_add

#define tnt_tree_create btree_tnt_create
#define tnt_tree_get rbtree_worker_get
#define tnt_node_update rbtree_node_update

#define tnt_tree_lookup rbtree_worker_lookup
#define tnt_tree_update rbtree_tree_update
#define tnt_tree_delete rbtree_worker_delete
#define tnt_tree_scan rbtree_init_scan

#define tnt_index_lookup rbtree_tnt_lookup

void rbtree_init(void);
struct tree_entry *rbtree_worker_lookup(int worker_id, void *item);
void rbtree_worker_delete(int worker_id, void *item);
struct tree_scan rbtree_init_scan(void *item, size_t scan_size);

void rbtree_tree_add(struct slab_callback *cb, void *item, uint64_t tmp_key);
struct tree_entry *rbtree_worker_get(void *item);
void rbtree_node_update(uint64_t old_key, uint64_t new_key);

index_entry_t *rbtree_tnt_lookup(int worker_id, void *item);


#endif

