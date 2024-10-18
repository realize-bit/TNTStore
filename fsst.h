#ifndef FSST_H
#define FSST_H 1

tree_entry_t *pick_garbage_node();
void fsst_worker_init(void);
void sleep_until_fsstq_empty(void);

#endif
