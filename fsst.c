#include "headers.h"

static int cur = 0;
static char *vict_file_data = NULL;

static char *vict_file_gc = NULL;
static char *new_data_gc = NULL;
static char *new_index_gc = NULL;
static char *vict_file_fsst = NULL;
static unsigned int gc_off = 0;
static unsigned int f_off = 0;
static unsigned int gc_ioff = 0;
static pthread_lock_t table_lock;

static struct fsst_file *table = NULL, *insert_ptr;

void print_table(void) {
  struct fsst_file *f = table;

  while (f) {
    struct fsst_file *s = f;

    printf("(%lu, %lu: %lu-%lu) --> ", f->level, f->seq, f->smallest, f->largest);
    while (s->sibling) {
      s = s->sibling;
      printf("(%lu, %lu: %lu-%lu) --> ", s->level, s->seq, s->smallest, s->largest);
    }
    printf("\n      V ");
    printf("\n      V\n");

    f = f->child;
  }
}

static struct fsst_file* create_fsst_file (uint64_t level) {
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
  if (p && f->level != p->level)
    f->child = p;
  W_UNLOCK(&table_lock);
}

void check_and_remove_tree (struct slab_callback *cb, void *item) {
  struct slab *s = cb->fsst_slab;
  char path[128], spath[128];
  int len;
  free(cb->item);
  free(cb);

  W_LOCK(&s->tree_lock);
  if (s->nb_items || (s->max == 0 && s->min == -1)) {
    // printf("fsst %lu %lu %lu\n", s->nb_items, s->min, s->max);
    W_UNLOCK(&s->tree_lock);
    return;
  }

  //TODO::JS Remove file
  printf("free %lu %lu %lu %lu\n", s->min, s->max, s->seq, s->nb_items);
  s->min = -1;
  s->max = 0;
  btree_free(s->tree);
  inc_empty_tree();
  s->tree = NULL;
  s->tree_node = NULL;
  W_UNLOCK(&s->tree_lock);

  sprintf(path, "/proc/self/fd/%d", s->fd);
  if((len = readlink(path, spath, 128)) < 0)
    die("READLINK\n");
  close(s->fd);
  spath[len] = 0;
  printf("RM %s\n", spath);
  unlink(spath);
}

// 선택된 인덱스 처리
void skip_or_invalidate_index(void *e) {
   index_entry_t *i = (index_entry_t *)e;
   struct slab_callback *cb;
   size_t page_num;
   size_t page_idx;
   char *src;

  // 이미 inval이면 그냥 스킵.
  // if (TEST_INVAL(i->slab_idx))
    // return;

   // i->slab->nb_items--;
   cb = malloc(sizeof(*cb));
   cb->cb = NULL;
   cb->cb_cb = check_and_remove_tree;
   cb->slab = i->slab;
   cb->slab_idx = GET_SIDX(i->slab_idx);
   cb->fsst_slab = i->slab;
   cb->fsst_idx = GET_SIDX(i->slab_idx);
   R_UNLOCK(&i->slab->tree_lock);
  // val이면 
    // 그냥 여기서 inval set하고, 나중에 lookup 없이 add만 하는게 좋다. 
    // 그런데 일단은 그냥 여기서 update 호출 하는 것으로
  // printf("FD: %d\n",i->slab->fd);
  // TODO::JS 나중에 파일 통째로 읽어오는건 어때?
  // printf("FSST: (%lu, %lu)\n", cb->slab->key, cb->slab_idx);
  cb->item = malloc(cb->slab->item_size);

  page_num = item_page_num(cb->slab, cb->slab_idx);
  page_idx = (cb->slab_idx % (PAGE_SIZE/cb->slab->item_size)) * cb->slab->item_size;
  src = &vict_file_data[(page_num*PAGE_SIZE) + page_idx];
  // printf("%s\n", src);
  // printf("FSST %lu: %lu, %lu\n", cb->slab_idx, page_num, page_idx);
  memcpy(cb->item, src, cb->slab->item_size);
  struct item_metadata *meta = (struct item_metadata *)cb->item;
  char *item_key = &cb->item[sizeof(*meta)];
  uint64_t key = *(uint64_t*)item_key;
            // printf("FSST key: %lu\n", key);
  kv_update_async(cb);
  R_LOCK(&i->slab->tree_lock);
  return;
}

void skip_or_invalidate_index_gc(void *e) {
   index_entry_t *i = (index_entry_t *)e;
   struct slab *s;
   uint64_t slab_idx;
   size_t page_num, page_idx, item_size;
   char *src;
   struct item_metadata *meta;
   char *item_key;
   uint64_t key;
   struct fsst_index fi;

   s = i->slab;
   slab_idx = GET_SIDX(i->slab_idx);
   page_num = item_page_num(s, slab_idx);
   item_size = s->item_size;
   R_UNLOCK(&i->slab->tree_lock);

   if (gc_off + item_size > 2*PAGE_SIZE) {
     write(insert_ptr->fd, new_data_gc, gc_off);
     gc_off = 0;
   }

   page_idx = (slab_idx % (PAGE_SIZE/item_size)) * item_size;
   src = &vict_file_gc[(page_num*PAGE_SIZE) + page_idx];

   meta = (struct item_metadata *)src;
   item_key = &src[sizeof(*meta)];
   key = *(uint64_t*)item_key;

   if (key < insert_ptr->smallest)
      insert_ptr->smallest = key;
   if (key > insert_ptr->largest)
      insert_ptr->largest = key;

   memcpy(new_data_gc + gc_off, src, item_size);
   fi.key = key;
   fi.off = f_off;
   fi.sz = item_size;
   memcpy(new_index_gc + gc_ioff, &fi, sizeof(struct fsst_index));

   gc_off += item_size;
   f_off += item_size;
   gc_ioff += sizeof(struct fsst_index);
   insert_ptr->fsst_items++;

   R_LOCK(&i->slab->tree_lock);
   return;
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
  page_idx = (cb->slab_idx % (PAGE_SIZE/cb->slab->item_size)) * cb->slab->item_size;
  src = &vict_file_fsst[(page_num*PAGE_SIZE) + page_idx];
  memcpy(cb->item, src, cb->slab->item_size);
  kv_update_async(cb);
  R_LOCK(&i->slab->tree_lock);
  return;
}
// victim node 선택하기
// get_garbage_node();

// 그냥 직접 노드 선택하기 (뭘 기준으로?)
tree_entry_t *pick_garbage_node() {
  return tnt_tree_get_useq(cur++);
}

// 선택된 노드 처리
int make_fsst(void) {
  int count;
  tree_entry_t *victim = NULL;
  cur = 0;
  /*
  do {
      victim = pick_garbage_node();
      while (victim && (victim->slab->imm == 0 || victim->slab->tree == NULL))
        victim = pick_garbage_node();
      if (!victim || cur > 500)
        break;
      printf("FSST0 NB %lu seq %lu imm %u / read fd: %d, bytes: %d\n", victim->slab->nb_items, victim->slab->seq, victim->slab->imm, victim->slab->fd, count);

  } while (victim);
  cur = 0;
  */

  do {
      victim = pick_garbage_node();
      while (victim && (victim->slab->imm == 0 || victim->slab->tree == NULL))
      victim = pick_garbage_node();

      if (!victim || cur > 500)
        break;

      if (!vict_file_data)
        vict_file_data = aligned_alloc(PAGE_SIZE, victim->slab->size_on_disk);

      if (!vict_file_data)
        die("FSST Static Buf Error\n");

      count = pread(victim->slab->fd, vict_file_data, victim->slab->size_on_disk, 0);
      // printf("FSST1 NB %lu seq %lu imm %u / read fd: %d, bytes: %d\n", victim->slab->nb_items, victim->slab->seq, victim->slab->imm, victim->slab->fd, count);
      R_LOCK(&victim->slab->tree_lock);
      count = btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index);
      R_UNLOCK(&victim->slab->tree_lock);
      // printf("FSST2 NB %lu %lu %d\n", victim->slab->nb_items, victim->slab->seq, count);
  } while (victim);

  free(vict_file_data);
  vict_file_data = NULL;
  cur = 0;
  // printf("FSST1.5 COUNT %d\n", count);
  // sleep(10);
  // printf("FSST2 NB %lu %lu\n", victim->slab->nb_items, victim->slab->seq);
  return 0;
}

#define NODE_BATCH 8

static void *fsst_worker(void *pdata) {
  vict_file_fsst = aligned_alloc(PAGE_SIZE, 16384*PAGE_SIZE);

  if (!vict_file_fsst)
    die("FSST Static Buf Error\n");

  while (1) {
    if (rbq_isEmpty(FSST)) {
      goto fsst_sleep;
    }
    for (size_t i = 0; i < NODE_BATCH; i++) {
      tree_entry_t *victim = rbq_dequeue(FSST);
      if (!victim)
        goto fsst_sleep;
      printf("FSST %lu\n", i);
      pread(victim->slab->fd, vict_file_fsst, victim->slab->size_on_disk, 0);

      R_LOCK(&victim->slab->tree_lock);
      btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index_fsst);
      R_UNLOCK(&victim->slab->tree_lock);
    }

fsst_sleep:
    sleep(1);
  }
}

int make_fsst_gc(void) {
  tree_entry_t *victim;
  vict_file_gc = aligned_alloc(PAGE_SIZE, 16384*PAGE_SIZE);
  new_data_gc = aligned_alloc(PAGE_SIZE, 2*PAGE_SIZE);
  new_index_gc = aligned_alloc(PAGE_SIZE, 64*PAGE_SIZE);

  do {
      char path[512];
      char spath[512];
      int len;
      struct slab *s;
      victim = pick_garbage_node();
      while (victim && (victim->slab->imm == 0 || victim->slab->tree == NULL))
        victim = pick_garbage_node();

      if(!victim || cur > 500)
        break;

      s = victim->slab;
      printf("LV: %lu, Key: %lu\n", victim->level, victim->key);
      pread(s->fd, vict_file_gc, s->size_on_disk, 0);

      insert_ptr = create_fsst_file(victim->level);
      insert_ptr->seq = s->seq;
      sprintf(path, FSST_PATH, insert_ptr->level, insert_ptr->seq);
      insert_ptr->fd = open(path,  O_RDWR | O_CREAT | O_DIRECT, 0777);
      gc_off = 0;
      f_off = 0;
      gc_ioff = 0;

      R_LOCK(&victim->slab->tree_lock);
      btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index_gc);
      R_UNLOCK(&victim->slab->tree_lock);

      if (gc_off) {
        write(insert_ptr->fd, new_data_gc, gc_off);
        insert_ptr->ioff_start = f_off;
        insert_ptr->file_size = f_off + gc_ioff;
      }
      write(insert_ptr->fd, new_index_gc, gc_ioff);
      insert_ptr->index_buf = malloc(gc_ioff);
      insert_ptr->page = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
      memcpy(insert_ptr->index_buf, new_index_gc, gc_ioff);

      insert_fsst_file(insert_ptr);

      s->min = -1;
      s->max = 0;
      W_LOCK(&s->tree_lock);

      btree_free(s->tree);
      inc_empty_tree();
      s->tree = NULL;
      s->tree_node = NULL;
      W_UNLOCK(&s->tree_lock);

      sprintf(path, "/proc/self/fd/%d", s->fd);
      if((len = readlink(path, spath, 512)) < 0)
        die("READLINK\n");
      spath[len] = 0;
      printf("END GC %s\n", spath);
      close(s->fd);
      unlink(spath);
  } while (victim);

  return 0;
}

void sleep_until_fsstq_empty(void) {

    while (!rbq_isEmpty(FSST)) {
    	NOP10();
    }

    return;
}

static void *gc_worker(void *pdata) {
  vict_file_gc = aligned_alloc(PAGE_SIZE, 16384*PAGE_SIZE);
  new_data_gc = aligned_alloc(PAGE_SIZE, 2*PAGE_SIZE);
  new_index_gc = aligned_alloc(PAGE_SIZE, 64*PAGE_SIZE);

  if (!vict_file_gc)
    die("FSST Static Buf Error\n");

  while (1) {
    if (rbq_isEmpty(GC)) {
      goto gc_sleep;
    }
    for (size_t i = 0; i < NODE_BATCH; i++) {
      char path[512];
      char spath[512];
      int len;
      tree_entry_t *victim = rbq_dequeue(GC);
      struct slab *s;
      if (!victim)
        goto gc_sleep;
      s = victim->slab;
      printf("GC %lu\n", i);
      pread(s->fd, vict_file_gc, s->size_on_disk, 0);

      printf("LV: %lu, Key: %lu\n", victim->level, victim->key);
      insert_ptr = create_fsst_file(victim->level);
      insert_ptr->seq = s->seq;
      sprintf(path, FSST_PATH, insert_ptr->level, insert_ptr->seq);
      insert_ptr->fd = open(path,  O_RDWR | O_CREAT | O_DIRECT, 0777);
      gc_off = 0;
      f_off = 0;
      gc_ioff = 0;

      R_LOCK(&victim->slab->tree_lock);
      btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index_gc);
      R_UNLOCK(&victim->slab->tree_lock);

      if (gc_off) {
        write(insert_ptr->fd, new_data_gc, gc_off);
        insert_ptr->ioff_start = f_off;
        insert_ptr->file_size = f_off + gc_ioff;
      }
      write(insert_ptr->fd, new_index_gc, gc_ioff);
      insert_ptr->index_buf = malloc(gc_ioff);
      insert_ptr->page = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
      memcpy(insert_ptr->index_buf, new_index_gc, gc_ioff);

      insert_fsst_file(insert_ptr);

      s->min = -1;
      s->max = 0;
      W_LOCK(&s->tree_lock);

      btree_free(s->tree);
      inc_empty_tree();
      s->tree = NULL;
      s->tree_node = NULL;
      W_UNLOCK(&s->tree_lock);

      sprintf(path, "/proc/self/fd/%d", s->fd);
      if((len = readlink(path, spath, 512)) < 0)
        die("READLINK\n");
      spath[len] = 0;
      printf("END GC %s\n", spath);
      close(s->fd);
      unlink(spath);
    }

gc_sleep:
    sleep(1);
  }
}

void fsst_worker_init(void) {
  pthread_t t;
  INIT_LOCK(&table_lock, NULL);
  pthread_create(&t, NULL, fsst_worker, NULL);
  pthread_create(&t, NULL, gc_worker, NULL);

}

static struct fsst_file* find_fsst_file (uint64_t key, uint64_t llevel) {
  struct fsst_file *f = table;

  R_LOCK(&table_lock);
  while (f) {
    struct fsst_file *s = f;

    if (llevel && f->level >= llevel) {
      f = f->child;
      continue;
    }

    do {
      if (s->smallest <= key 
        && s->largest >= key) {
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
  struct item_metadata *meta = (struct item_metadata *)callback->item;
  char *item_key = &callback->item[sizeof(*meta)];
  uint64_t key = *(uint64_t*)item_key;
  uint64_t llevel = 0;
  // 정확히 찾거나, 다 찾을 때까지 반복
  while (1) {
    struct fsst_file *f;
    struct fsst_index *fi;
    int num_indices;

    // find file
    f = find_fsst_file(key, llevel);


    if (!f)
      return;
    //TODO: 일단은 async 매커니즘을 거치지 않고
    //      직접 읽도록 만듬
    // printf("Try find file %lu: (%lu, %lu: %lu-%lu)\n", key, f->level, f->seq, f->smallest, f->largest);
    num_indices = (f->file_size - f->ioff_start)
                        / sizeof(struct fsst_index);
    fi = (struct fsst_index*)f->index_buf;
    for (size_t i = 0; i < num_indices; i++) {
      if (fi->key == key) {
        uint64_t off = fi->off % PAGE_SIZE;
        pread(f->fd, f->page, PAGE_SIZE, fi->off - off);
        memcpy(callback->item, &f->page[off], fi->sz);
        struct item_metadata *meta2 = (struct item_metadata *)callback->item;
        char *item_key2 = &callback->item[sizeof(*meta2)];
        uint64_t key2 = *(uint64_t*)item_key2;
        // printf("%d %d, GoT %lu: %lu, %lu:, (%u, %u)\n", ret, f->fd, key, fi->key, key2, fi->off, fi->sz);
        return;
      }
      fi++;
    }
    llevel = f->level;
  }
}
