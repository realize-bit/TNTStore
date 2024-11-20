#include "headers.h"
#include <errno.h>

static int cur = 0;
static char *vict_file_fsst = NULL;

void check_and_remove_tree(struct slab_callback *cb, void *item) {
  struct slab *s = cb->fsst_slab;
  free(cb->item);
  free(cb);

  W_LOCK(&s->tree_lock);
  if (s->nb_items || (s->max == 0 && s->min == -1)) {
    // printf("fsst %lu %lu %lu\n", s->nb_items, s->min, s->max);
    W_UNLOCK(&s->tree_lock);
    return;
  }

  // TODO::JS Remove file
  printf("free %lu %lu %lu %lu\n", s->min, s->max, s->seq, s->nb_items);
  s->min = -1;
  s->max = 0;
  subtree_free(s->subtree);
#if WITH_FILTER
  filter_delete(s->filter);
#endif
  inc_empty_tree();
  s->subtree = NULL;
  /*s->centree_node = NULL;*/

  printf("RM %lu\n", s->seq);

  if (__sync_fetch_and_or(&s->update_ref, 0) == 0 && s->read_ref == 0) {
    char path[128], spath[128];
    int len;
    sprintf(path, "/proc/self/fd/%d", s->fd);
    if ((len = readlink(path, spath, 512)) < 0) die("READLINK\n");
    spath[len] = 0;
    close(s->fd);
    /*strncpy(path, spath, len);*/
    /*snprintf(path + len, 128 - len, "-%lu", s->key);*/
    truncate(spath, 0);
    /*rename(spath, path);*/
    //unlink(spath);
    printf("REMOVED FILE\n");
  }

  W_UNLOCK(&s->tree_lock);
}

void skip_or_invalidate_index_fsst(void *slab, uint64_t slab_idx) {
  struct slab_callback *cb;
  struct slab *s = (struct slab *)slab;
  size_t page_num;
  size_t page_idx;
  char *src;

  cb = malloc(sizeof(*cb));
  cb->cb = NULL;
  cb->cb_cb = check_and_remove_tree;
  cb->slab = s;
  cb->slab_idx = GET_SIDX(slab_idx);
  cb->fsst_slab = s;
  cb->fsst_idx = GET_SIDX(slab_idx);
  R_UNLOCK(&s->tree_lock);
  cb->item = malloc(cb->slab->item_size);

  page_num = item_page_num(cb->slab, cb->slab_idx);
  page_idx =
      (cb->slab_idx % (PAGE_SIZE / cb->slab->item_size)) * cb->slab->item_size;
  src = &vict_file_fsst[(page_num * PAGE_SIZE) + page_idx];
  memcpy(cb->item, src, cb->slab->item_size);
  kv_update_async(cb);
  R_LOCK(&s->tree_lock);
  return;
}
// victim node 선택하기
// get_garbage_node();

// 그냥 직접 노드 선택하기 (뭘 기준으로?)
tree_entry_t *pick_garbage_node() { return tnt_traverse_use_seq(cur++); }

#define NODE_BATCH 128
#define HOT_BATCH 16384

static void *fsst_worker(void *pdata) {
  vict_file_fsst = aligned_alloc(PAGE_SIZE, MAX_FILE_SIZE);

  if (!vict_file_fsst) die("FSST Static Buf Error\n");

  while (1) {
    if (bgq_is_empty(FSST)) {
#if WITH_HOT
      if (bgq_is_empty(GC)) {
#endif
        goto fsst_sleep;
#if WITH_HOT
      }
#endif
    }

    if (!bgq_is_empty(FSST)) {
      for (size_t i = 0; i < NODE_BATCH; i++) {
        tree_entry_t *victim = (tree_entry_t *)bgq_dequeue(FSST);
        if (!victim) goto fsst_sleep;
        printf("FSST %lu\n", i);
        pread(victim->slab->fd, vict_file_fsst, victim->slab->size_on_disk, 0);

        R_LOCK(&victim->slab->tree_lock);
        subtree_forall_invalid(victim->slab->subtree, victim->slab, skip_or_invalidate_index_fsst);
        R_UNLOCK(&victim->slab->tree_lock);
      }
    }

#if WITH_HOT
    int j = 0;
    if (!bgq_is_empty(GC)) {
      printf("Hot Q: %d\n", bgq_count(GC));
      for (j = 0; j < HOT_BATCH; j++) {
        struct slab_callback *cb;
        char *item =(char *)bgq_dequeue(GC);

        if (!item) goto fsst_sleep;

        cb = malloc(sizeof(*cb));
        /*cb->cb = add_in_tree_for_update;*/
        cb->cb = NULL;
        cb->cb_cb = NULL;
        cb->payload = NULL;
        cb->fsst_slab = NULL;
        cb->fsst_idx = -1;
        cb->item = item;
        /*printf("cbitem %p\n", cb->item);*/
        kv_update_async(cb);
      }
    }
#endif

  fsst_sleep:
    sleep(1);
  }
}

void sleep_until_fsstq_empty(void) {
  while (!bgq_is_empty(FSST)) {
    NOP10();
  }
  return;
}

void fsst_worker_init(void) {
  pthread_t t;
  pthread_create(&t, NULL, fsst_worker, NULL);
}
