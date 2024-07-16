#ifndef SUBTREE_H
#define SUBTREE_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "memory-item.h"
typedef void subtree_t;

subtree_t *subtree_create();
int subtree_find(subtree_t *t, unsigned char *k, size_t len,
                 struct index_entry *e);
int subtree_set_invalid(subtree_t *t, unsigned char *k, size_t len);
int subtree_delete(subtree_t *t, unsigned char *k, size_t len);
void subtree_insert(subtree_t *t, unsigned char *k, size_t len,
                    struct index_entry *e);
struct index_scan subtree_find_n(subtree_t *t, unsigned char *k, size_t len,
                                 size_t n);

void subtree_forall_keys(subtree_t *t, void (*cb)(uint64_t h, void *data),
                         void *data);
int subtree_forall_invalid(subtree_t *t, void (*cb)(void *data));
void subtree_free(subtree_t *t);
uint64_t subtree_next_key(subtree_t *t, unsigned char *k,
                          struct index_entry *e);
void subtree_allvalid_key(subtree_t *t, struct index_scan *res);

#ifdef __cplusplus
}
#endif

#endif
