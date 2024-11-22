#ifndef SUBTREE_H
#define SUBTREE_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "memory-item.h"
typedef struct subtree {
  void *slab;
  void *tree;
} subtree_t;

subtree_t *subtree_create();
int subtree_find(subtree_t *t, unsigned char *k, size_t len,
                 struct index_entry *e);
int subtree_set_invalid(subtree_t *t, unsigned char *k, size_t len);
void subtree_set_slab(subtree_t *t, void *slab);
int subtree_delete(subtree_t *t, unsigned char *k, size_t len);
void subtree_insert(subtree_t *t, unsigned char *k, size_t len,
                    struct index_entry *e);

int subtree_forall_keys(subtree_t *t, void (*cb)(uint64_t h, int n, void *data),
                         void *data);
int subtree_forall_invalid(subtree_t *t, void *data, void (*cb)(void *slab, uint64_t slab_idx));
void subtree_free(subtree_t *t);

#ifdef __cplusplus
}
#endif

#endif
