#include "headers.h"
#include <errno.h>

static void *gc_async_worker(void *pdata);

static int cur = 0;
static char *vict_file_data = NULL;
static char *vict_file_fsst = NULL;
static pthread_lock_t table_lock;
extern uint64_t nb_totals;

static pthread_cond_t gc_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t gc_mutex = PTHREAD_MUTEX_INITIALIZER;
static int inflight_update_gc = 0;

// static char *vict_file_gc = NULL;
// static char *new_data_gc = NULL;
// static char *new_index_gc = NULL;
// static unsigned int gc_off = 0;
// static unsigned int f_off = 0;
// static unsigned int gc_ioff = 0;
static struct fsst_file *table = NULL, *insert_ptr;
struct gc_context {
  struct io_context *io_ctx;
  struct fsst_file *f;
  struct index_scan *d;
  size_t max_pending_gc;
  char *vict_file;
  char *data;
  char *index;
  unsigned int gc_off;
  unsigned int gc_ioff;
  unsigned int f_off;
  uint32_t ing;
  uint32_t index_ing;
  uint32_t level;
  tree_entry_t *victim;
  // uint64_t last_key;
  uint64_t rdt;  // Latest timestamp
};
int get_fd_from_gtx(struct gc_context *gtx) { return gtx->f->fd; }

struct fsst_file *get_file_from_gtx(struct gc_context *gtx) {
  return gtx->f;
}

char *get_databuf_from_gtx(struct gc_context *gtx) { return gtx->data; }

char *get_indexbuf_from_gtx(struct gc_context *gtx) { return gtx->index; }

char *get_victbuf_from_gtx(struct gc_context *gtx) { return gtx->vict_file; }

struct io_context *get_io_context_from_gtx(struct gc_context *gtx) {
  return gtx->io_ctx;
}

static uint64_t get_prefix_for_item(char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  return *(uint64_t *)item_key;
}

void print_table(void) {
  struct fsst_file *f = table;

  while (f) {
    struct fsst_file *s = f;

    printf("(%lu, %lu: %lu-%lu) --> ", f->level, f->seq, f->smallest,
           f->largest);
    while (s->sibling) {
      s = s->sibling;
      printf("(%lu, %lu: %lu-%lu) --> ", s->level, s->seq, s->smallest,
             s->largest);
    }
    printf("\n      V ");
    printf("\n      V\n");

    f = f->child;
  }
}

static struct fsst_file *create_fsst_file(uint64_t level) {
  struct fsst_file *f = malloc(sizeof(struct fsst_file));
  if (!f) {
    die("Can't alloc fsst_file");
  }

  f->level = level;
  f->largest = 0;
  f->smallest = -1;
  f->fsst_items = 0;

  f->sibling = NULL;
  f->child = NULL;

  return f;
}

/**
 * Insert a new fsst_file node into a hierarchical structure.
 *
 * @param f Pointer to the fsst_file node to be inserted.
 */
static void insert_fsst_file(struct fsst_file *f) {
  struct fsst_file *p, *prev, *cprev;

  W_LOCK(&table_lock);
  if (!table) {
    table = f;
    W_UNLOCK(&table_lock);
    return;
  }

  prev = p = table;
  while (p && p->level > f->level) {
    prev = p;
    p = p->child;
  }

  // 이미 같은 레벨이 있으니까 옆에다가 삽입
  if (p && p->level == f->level) {
    // 같은 레벨끼리 오버랩은 존재하지 않는다.
    cprev = p;
    while (p && p->largest < f->largest) {
      cprev = p;
      p = p->sibling;
    }

    f->sibling = p;

    if (p != cprev) {
      cprev->sibling = f;
      W_UNLOCK(&table_lock);
      return;
    }

    f->child = p->child;
    p->child = NULL;
  }

  // 같은 레벨이 없다 //
  // 내가 head인 경우
  if (p == prev)
    table = f;
  else
    prev->child = f;

  // Child가 없는 경우도 자동 NULL 처리
  if (p && f->level != p->level) f->child = p;
  W_UNLOCK(&table_lock);
}

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
  btree_free(s->subtree);
#if WITH_FILTER
  filter_delete(s->filter);
#endif
  free(s->hot_bit);
  inc_empty_tree();
  s->subtree = NULL;
  /*s->centree_node = NULL;*/

  printf("RM %lu\n", s->seq);

  if (s->update_ref == 0 && s->read_ref == 0) {
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

void skip_or_invalidate_index_fsst(void *e) {
  index_entry_t *i = (index_entry_t *)e;
  struct slab_callback *cb;
  size_t page_num;
  size_t page_idx;
  char *src;

  cb = malloc(sizeof(*cb));
  cb->cb = NULL;
  cb->cb_cb = check_and_remove_tree;
  cb->slab = i->slab;
  cb->slab_idx = GET_SIDX(i->slab_idx);
  cb->fsst_slab = i->slab;
  cb->fsst_idx = GET_SIDX(i->slab_idx);
  R_UNLOCK(&i->slab->tree_lock);
  cb->item = malloc(cb->slab->item_size);

  page_num = item_page_num(cb->slab, cb->slab_idx);
  page_idx =
      (cb->slab_idx % (PAGE_SIZE / cb->slab->item_size)) * cb->slab->item_size;
  src = &vict_file_fsst[(page_num * PAGE_SIZE) + page_idx];
  memcpy(cb->item, src, cb->slab->item_size);
  kv_update_async(cb);
  R_LOCK(&i->slab->tree_lock);
  return;
}
// victim node 선택하기
// get_garbage_node();

// 그냥 직접 노드 선택하기 (뭘 기준으로?)
tree_entry_t *pick_garbage_node() { return tnt_traverse_use_seq(cur++); }

#define NODE_BATCH 8

static void *fsst_worker(void *pdata) {
  vict_file_fsst = aligned_alloc(PAGE_SIZE, 16384 * PAGE_SIZE);

  if (!vict_file_fsst) die("FSST Static Buf Error\n");

  while (1) {
    if (bgq_is_empty(FSST)) {
      goto fsst_sleep;
    }
    for (size_t i = 0; i < NODE_BATCH; i++) {
      tree_entry_t *victim = bgq_dequeue(FSST);
      if (!victim) goto fsst_sleep;
      printf("FSST %lu\n", i);
      pread(victim->slab->fd, vict_file_fsst, victim->slab->size_on_disk, 0);

      R_LOCK(&victim->slab->tree_lock);
      subtree_forall_invalid(victim->slab->subtree, skip_or_invalidate_index_fsst);
      R_UNLOCK(&victim->slab->tree_lock);
    }

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
  struct gc_context *gtx = calloc(1, sizeof(struct gc_context));
  INIT_LOCK(&table_lock, NULL);
  pthread_create(&t, NULL, fsst_worker, NULL);
  //pthread_create(&t, NULL, gc_async_worker, gtx);
}

static void gc_finalize(struct gc_context *gtx, struct slab *s) {
  insert_fsst_file(gtx->f);
  gtx->f_off = 0;
  gtx->gc_ioff = 0;
  gtx->ing = 0;
  gtx->victim = NULL;
  gtx->f = NULL;
  gtx->d->nb_entries = 0;
  gtx->index_ing = 0;
  // printf("GC Fin\n");
}

void remove_tree_for_gc(struct slab_callback *cb) {
  struct slab *s = cb->slab;
  uint64_t item_nums = cb->item_nums;
  free(cb);

  W_LOCK(&s->tree_lock);
  s->nb_items -= item_nums;
  if (s->nb_items || (s->max == 0 && s->min == -1)) {
    // printf("fsst %lu %lu %lu\n", s->nb_items, s->min, s->max);
    W_UNLOCK(&s->tree_lock);
    return;
  }

  printf("FREE by GC: %lu, min: %lu, max: %lu\n", s->key, s->min, s->max);
  s->min = -1;
  s->max = 0;

  btree_free(s->subtree);
#if WITH_FILTER
  filter_delete(s->filter);
#endif
  free(s->hot_bit);
  inc_empty_tree();
  s->subtree = NULL;
  /*s->centree_node = NULL;*/

  printf("END GC %lu\n", s->seq);
  if (s->update_ref == 0 && s->read_ref == 0) {
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
    // printf("REMOVED FILE\n");
  }

  W_UNLOCK(&s->tree_lock);
}

void gc_async_invalidate_index(struct gc_context *gtx, index_entry_t *e) {
  struct slab *s;
  uint64_t slab_idx;
  size_t page_num, page_idx, item_size;
  char *src;
  uint64_t key;
  struct fsst_index fi;
  struct slab_callback *cb;

  // 메모리에 카피하고 io 큐에 넣기
  s = e->slab;
  slab_idx = GET_SIDX(e->slab_idx);
  page_num = item_page_num(s, slab_idx);
  item_size = s->item_size;
  page_idx = (slab_idx % (PAGE_SIZE / item_size)) * item_size;
  src = &gtx->vict_file[(page_num * PAGE_SIZE) + page_idx];

  if (hot_bit_check(s, page_num)) {
    cb = malloc(sizeof(*cb));
    cb->slab = e->slab;
    cb->slab_idx = GET_SIDX(e->slab_idx);
    cb->cb = NULL;
    cb->cb_cb = check_and_remove_tree;
    cb->fsst_slab = e->slab;
    cb->fsst_idx = GET_SIDX(e->slab_idx);
    cb->item = malloc(cb->slab->item_size);

    memcpy(cb->item, src, cb->slab->item_size);
    kv_update_async(cb);

  } else {
    memcpy(gtx->data + gtx->gc_off, src, item_size);
    key = get_prefix_for_item(src);
    fi.key = key;
    fi.off = gtx->f_off;
    fi.sz = item_size;
    memcpy(gtx->index + gtx->gc_ioff, &fi, sizeof(struct fsst_index));

    gtx->gc_off += item_size;
    gtx->f_off += item_size;
    gtx->gc_ioff += sizeof(struct fsst_index);

    if (key < gtx->f->smallest) gtx->f->smallest = key;
    if (key > gtx->f->largest) gtx->f->largest = key;

    gtx->f->fsst_items++;
    __sync_fetch_and_sub(&nb_totals, 1);

    if (gtx->gc_off % PAGE_SIZE == 0) {
      cb = malloc(sizeof(*cb));
      cb->slab = e->slab;
      cb->io_cb = remove_tree_for_gc;
      cb->item_nums = PAGE_SIZE / item_size;

      write_gc_async(gtx, cb, (gtx->gc_off - 1) / PAGE_SIZE,
                     (gtx->f_off - 1) / PAGE_SIZE, 1);
      // write(insert_ptr->fd, new_data_gc, gc_off);
      // gc_off = 0;
    }
  }

  return;
}

static double calculate_cost_benefit(struct slab *s) {
  double cb = 0;
  double hot, cold, valid, inval, unused;

  R_LOCK(&s->tree_lock);
  hot = s->hot_pages * 4;
  valid = s->nb_items;
  inval = s->last_item - valid;
  cold = valid - hot;
  unused = s->nb_max_items - s->last_item;
  R_UNLOCK(&s->tree_lock);

  cb = (inval + cold + (0.2 * unused)) / (hot + cold);

  return cb;
}

static tree_entry_t *get_victim_centnode(void) {
  background_queue *q = bgq_get(GC);
  centree_node n;
  centree_node max_node;
  double max_value;

  if (q->count == 0) return NULL;

  n = bgq_front_node(GC);
  while (n && n->value.slab->min == -1) {
    n = dequeue_specific_node(q, n);
  }

  if (n == NULL)  // 모든 노드가 제거된 경우
    return NULL;

  max_node = n;
  max_value = calculate_cost_benefit(n->value.slab);

  while (1) {
    double v;
    n = get_next_node(q, n);

    if (!n) break;

    if (n->value.slab->min == -1) {
      dequeue_specific_node(q, n);
      continue;
    }
    if ((v = calculate_cost_benefit(n->value.slab)) > max_value) {
      max_node = n;
      max_value = v;
    }
    // printf("max value check %f vs %f\n", v, max_value);
  }

  dequeue_specific_node(q, max_node);
  max_node->removed = 1;
  return &max_node->value;
}

static void gc_dequeue_requests(struct gc_context *gtx) {
  // size_t retries =  0;

  // if (gtx->ing == 0 && bgq_count(GC) == 0)
  //    return;

  if (gtx->victim == NULL || gtx->f == NULL) return;

  // if (gtx->index_ing) {
  //    int size_aligned = ((gtx->gc_ioff + PAGE_SIZE - 1) / PAGE_SIZE);
  //
  //    while (gtx->index_ing < size_aligned) {
  //      write_gc_async(gtx, NULL, gtx->index_ing,
  //           ((gtx->f_off-1) / PAGE_SIZE) + 1 + gtx->index_ing, 0);
  //      gtx->index_ing++;
  //      if(io_pending(gtx->io_ctx) >= QUEUE_DEPTH)
  //        return;
  //    }
  //    gc_finalize(gtx, gtx->victim->slab);
  //    return;
  // }

  while (1) {
    index_entry_t e = {NULL, -1};
    e = gtx->d->entries[gtx->ing++];

    // 이번에 처리할 index 찾아오고, 다음 인덱스 키값 리턴해서 가지고 있기
    // 메모리 관련 처리하고 io request 보내놓기
    // gtx->last_key = btree_next_key(victim->slab->tree, (unsigned char *)next,
    // &e);
    gc_async_invalidate_index(gtx, &e);

    if (gtx->ing == gtx->d->nb_entries) {
      if (gtx->gc_off % PAGE_SIZE != 0) {
        struct slab_callback *cb = malloc(sizeof(*cb));
        cb->slab = e.slab;
        cb->io_cb = remove_tree_for_gc;
        cb->item_nums = gtx->gc_off % PAGE_SIZE;
        write_gc_async(gtx, cb, (gtx->gc_off - 1) / PAGE_SIZE,
                       (gtx->f_off - 1) / PAGE_SIZE, 1);
      }

      int size_aligned = ((gtx->gc_ioff + PAGE_SIZE - 1) / PAGE_SIZE);

      gtx->gc_off = 0;
      // bgq_dequeue(GC);
      gtx->f->ioff_start = gtx->f_off;
      gtx->f->file_size = gtx->f_off + gtx->gc_ioff;
      // gtx->f->index_buf = aligned_alloc(PAGE_SIZE, size_aligned * PAGE_SIZE);
      // gtx->f->page = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
      // ftruncate(gtx->f->fd, gtx->f->file_size);

      int ret = pwrite(gtx->f->fd, gtx->index, PAGE_SIZE * 512, gtx->f_off);
      // printf("File size: %lu, index write ret: %d\n", gtx->f->file_size, ret);
      if (ret == -1) {
        perror("");
      }

      // memcpy(gtx->f->index_buf, gtx->index, gtx->gc_ioff);

      // TODO:JS
      // while (gtx->index_ing < size_aligned) {
      //  write_gc_async(gtx, NULL, gtx->index_ing,
      //       ((gtx->f_off-1) / PAGE_SIZE) + 1 + gtx->index_ing, 0);
      //  gtx->index_ing++;
      //  if(io_pending(gtx->io_ctx) >= QUEUE_DEPTH)
      //    return;
      // }

      gc_finalize(gtx, gtx->victim->slab);
      break;
    }

    // 메모리가 가득 찾거나 펜딩이 다 찼으면
    if (io_pending(gtx->io_ctx) >= QUEUE_DEPTH ||
        gtx->gc_off >= (64 * PAGE_SIZE)) {
      if (gtx->gc_off % PAGE_SIZE != 0) {
        struct slab_callback *cb = malloc(sizeof(*cb));
        cb->slab = e.slab;
        cb->io_cb = remove_tree_for_gc;
        cb->item_nums = gtx->gc_off % PAGE_SIZE;
        write_gc_async(gtx, cb, (gtx->gc_off - 1) / PAGE_SIZE,
                       (gtx->f_off - 1) / PAGE_SIZE, 1);
      }
      gtx->gc_off = 0;
      break;
    }
  }
}

void cond_check_and_gc_wakeup(void) {
  uint64_t max_entry = (MAX_MEM - PAGE_CACHE_SIZE) / BYTE_PER_KV;
  uint64_t gc_start_trshld = max_entry * GC_START_TRSHLD;

  printf("nb_totals: %lu, start_threhold: %lu\n", nb_totals, gc_start_trshld);
  if (nb_totals <= gc_start_trshld) return;

  pthread_mutex_lock(&gc_mutex);
  pthread_cond_signal(&gc_cond);
  pthread_mutex_unlock(&gc_mutex);

  return;
}

static void *gc_async_worker(void *pdata) {
  struct gc_context *gtx = pdata;
  // size_t max_pending_callbacks = MAX_NB_PENDING_CALLBACKS_PER_WORKER;
  uint64_t max_entry = (MAX_MEM - PAGE_CACHE_SIZE) / BYTE_PER_KV;
  uint64_t gc_end_trshld = max_entry * GC_END_TRSHLD;

  /* Initialize the async io for the worker */
  gtx->max_pending_gc = 64;
  gtx->io_ctx = worker_ioengine_init(gtx->max_pending_gc);
  gtx->vict_file = aligned_alloc(PAGE_SIZE, 16384 * PAGE_SIZE);
  gtx->data = aligned_alloc(PAGE_SIZE, 64 * PAGE_SIZE);
  gtx->index = aligned_alloc(PAGE_SIZE, 512 * PAGE_SIZE);

  gtx->d = calloc(1, sizeof(struct index_scan));
  gtx->d->entries = malloc(16384 * 4 * sizeof(*gtx->d->entries));
  gtx->d->hashes = malloc(16384 * 4 * sizeof(*gtx->d->hashes));
  /* Main loop: do IOs and process enqueued requests */
  while (1) {
    size_t reads = 0;
    gtx->rdt++;

    while (io_pending(gtx->io_ctx)) {
      gc_ioengine_enqueue_ios(gtx->io_ctx);
      gc_ioengine_get_completed_ios(gtx->io_ctx);
      gc_ioengine_process_completed_ios(gtx->io_ctx);
    }

    // 현재 gc_end_trshld 보다 작으면 잠든다
    // printf("GC// nb_totlas: %lu, gc_end_trshld: %lu\n", nb_totals,
    // gc_end_trshld);
    printf("nb_totals: %lu, gc_end_threhold: %lu\n", nb_totals, gc_end_trshld);
    while (nb_totals < gc_end_trshld) {
      pthread_mutex_lock(&gc_mutex);
      pthread_cond_wait(&gc_cond, &gc_mutex);
      pthread_mutex_unlock(&gc_mutex);
    }

    while (bgq_is_empty(GC)) {
      background_queue *q = bgq_get(GC);
      centree_node n;
      tnt_get_nodes_at_level(gtx->level++, q);
      printf("level %d\n", gtx->level);
      n = bgq_front_node(GC);
      do {
        if (n->value.slab->full == 0 || n->value.slab->min == -1)
          n = dequeue_specific_node(q, n);
        else
          n = get_next_node(q, n);
      } while (n != NULL);
    }

    while (!gtx->victim) {
      struct slab *s;
      gtx->victim = get_victim_centnode();
      if (!gtx->victim) {
        // 큐에서 모든 노드가 제거됨. 큐를 새로 채워야함
        break;
      }
      s = gtx->victim->slab;
      if (s->min == -1) {
        // retry
        gtx->victim = NULL;
        continue;
      }

      R_LOCK(&s->tree_lock);
      subtree_allvalid_key(s->subtree, gtx->d);
      R_UNLOCK(&s->tree_lock);
    }

    while (!gtx->f) {
      struct slab *s;

      if (!gtx->victim) break;

      s = gtx->victim->slab;

      while (reads * 4096 < s->size_on_disk &&
             io_pending(gtx->io_ctx) < QUEUE_DEPTH) {
        read_gc_async(gtx, reads++, s->fd);
      }

      while (io_pending(gtx->io_ctx)) {
        gc_ioengine_enqueue_ios(gtx->io_ctx);
        gc_ioengine_get_completed_ios(gtx->io_ctx);
        gc_ioengine_process_completed_ios(gtx->io_ctx);
      }

      if (reads * 4096 == s->size_on_disk) {
        char path[512];
        struct fsst_file *f;
        size_t file_size = (gtx->d->nb_entries) * (1024 + 32);
        file_size = ((file_size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
        printf("Prev File size: %lu\n", file_size);
        gtx->f = f = create_fsst_file(gtx->victim->level);
        f->seq = s->seq;
        // pread(s->fd, gtx->vict_file, s->size_on_disk, 0);
        sprintf(path, FSST_PATH, f->level, f->seq);
        f->fd = open(path, O_RDWR | O_CREAT | O_DIRECT, 0777);
        fallocate(f->fd, 0, 0, file_size);
      }
    }

    gtx->level = 0;

    // printf("GC ing %d\n", gtx->ing);
    // volatile size_t pending = bgq_count(GC);
    // while(!pending && !io_pending(gtx->io_ctx)) {
    //   usleep(2);
    //   pending = bgq_count(GC);
    //}

    gc_dequeue_requests(gtx);
  }

  return NULL;
}

static struct fsst_file *find_fsst_file(uint64_t key, uint64_t llevel) {
  struct fsst_file *f = table;

  R_LOCK(&table_lock);
  while (f) {
    struct fsst_file *s = f;

    if (llevel && f->level >= llevel) {
      f = f->child;
      continue;
    }

    do {
      if (s->smallest <= key && s->largest >= key) {
        R_UNLOCK(&table_lock);
        return s;
      }
      s = s->sibling;
    } while (s);

    f = f->child;
  }
  R_UNLOCK(&table_lock);

  return NULL;
}

// static void fsst_index_lookup(void *item, uint64_t llevel) {
// }

void read_item_async_from_fsst(struct slab_callback *callback) {
  uint64_t key = get_prefix_for_item(callback->item);
  uint64_t llevel = 0;
  // 정확히 찾거나, 다 찾을 때까지 반복
  while (1) {
    struct fsst_file *f;
    struct fsst_index *fi;
    int num_indices;

    // find file
    f = find_fsst_file(key, llevel);

    if (!f) {
      printf("Can't Lookup due to no key-value %lu\n", key);
      return;
    }
    // TODO: 일단은 async 매커니즘을 거치지 않고
    //       직접 읽도록 만듬
    //  printf("Try find file %lu: (%lu, %lu: %lu-%lu)\n", key, f->level,
    //  f->seq, f->smallest, f->largest);
    num_indices = (f->file_size - f->ioff_start) / sizeof(struct fsst_index);
    int ret =
        pread(f->fd, callback->fsst_index_buf, 512 * PAGE_SIZE, f->ioff_start);
    if (ret == -1) {
      perror("");
      // printf("key: %lu, index read ret: %d, fd: %d, start: %lu, size: %lu\n",
      // key, ret, f->fd, f->ioff_start, f->file_size-f->ioff_start);
    }

    // printf("index read ret: %d\n", ret);
    fi = (struct fsst_index *)callback->fsst_index_buf;
    for (size_t i = 0; i < num_indices; i++) {
      // printf("fd: %d, What %lu/%lu (%lu, %lu)\n", f->fd, key, fi->key,
      // fi->off, fi->sz);
      if (fi->key == key) {
        // printf("MISS file: %lu, worker: %d\n", f->seq,
        // get_worker_ucb(callback));
        uint64_t off = fi->off % PAGE_SIZE;
        int ret = pread(f->fd, callback->fsst_buf, PAGE_SIZE, fi->off - off);
        memcpy(callback->item, &callback->fsst_buf[off], fi->sz);
        uint64_t key2 = get_prefix_for_item(callback->item);
        // printf("ret: %d, fd: %d, GoT %lu: %lu, %lu:, (%lu, %lu)\n", ret,
        // f->fd, key, fi->key, key2, fi->off, fi->sz);
        if (key != key2) die("The key is different with requested one\n");
        return;
      }
      fi++;
    }
    llevel = f->level;
  }
}
