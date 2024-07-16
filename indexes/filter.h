#ifndef FILTER_H
#define FILTER_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void filter_t;

filter_t *filter_create(size_t total_items);
int filter_add(filter_t *f, unsigned char *k);
int filter_contain(filter_t *f, unsigned char *k);
void filter_delete(filter_t *f);

#ifdef __cplusplus
}
#endif

#endif
