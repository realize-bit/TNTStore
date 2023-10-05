#include "headers.h"

static int cur = 0;
static char *vict_file_data = NULL;
static char *vict_file_gc = NULL;
static char *vict_file_fsst = NULL;

void check_and_remove_tree (struct slab_callback *cb, void *item) {
  struct slab *s = cb->fsst_slab;
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
  close(s->fd);
  s->min = -1;
  s->max = 0;
  btree_free(s->tree);
  s->tree = NULL;
  s->tree_node = NULL;
  W_UNLOCK(&s->tree_lock);
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
  src = &vict_file_gc[(page_num*PAGE_SIZE) + page_idx];
  memcpy(cb->item, src, cb->slab->item_size);
  kv_update_async(cb);
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

//static char* encode_varint32() {
//
//  return;
//}
//
//static void put_varint32() {
//
//  return;
//}
//
//void block_add(struct dna_block *b, const void *item) {
//  /*
//  Slice last_key_piece(last_key_);
//  assert(!finished_);
//  assert(counter_ <= options_->block_restart_interval);
//  assert(buffer_.empty()  // No values yet?
//         || options_->comparator->Compare(key, last_key_piece) > 0);
//  size_t shared = 0;
//  if (counter_ < options_->block_restart_interval) {
//    // See how much sharing to do with previous string
//    const size_t min_length = std::min(last_key_piece.size(), key.size());
//    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
//      shared++;
//    }
//  } else {
//    // Restart compression
//    restarts_.push_back(buffer_.size());
//    counter_ = 0;
//  }
//  const size_t non_shared = key.size() - shared;
//
//  // Add "<shared><non_shared><value_size>" to buffer_
//  PutVarint32(&buffer_, shared);
//  PutVarint32(&buffer_, non_shared);
//  PutVarint32(&buffer_, value.size());
//
//  // Add string delta to buffer_ followed by value
//  buffer_.append(key.data() + shared, non_shared);
//  buffer_.append(value.data(), value.size());
//
//  // Update state
//  last_key_.resize(shared);
//  last_key_.append(key.data() + shared, non_shared);
//  assert(Slice(last_key_) == key);
//  counter_++;
//  return;
//  */
//}
//
//int estimate_block_size() {
//
//  return 0;
//}
//
//void block_reset() {
//
//}
//
//void block_finish() {
//
//}
//
//void write_block() {
//}
//
//void write_raw_block() {
//}
//
//void table_flush(struct cell *c) {
//  assert(!c->closed);
//
//  if (c->data.size == 0) 
//    return;
//
//  assert(!c->pending_index_entry);
//
//  write_block(&c->data, &c->pending_handle);
//
//  if (ok()) {
//    c->pending_index_entry = true;
//    c->status = r->file->Flush();
//  }
//
//  /*
//  if (r->filter_block != nullptr) {
//    r->filter_block->StartBlock(r->offset);
//  }
//  */
//}
//
//int table_finish() {
//  return 0;
//}
//
//void table_add(struct cell *c, const void *item) {
//  struct item_metadata *meta;
//  char *item_key, *item_value;
//
//  assert(!c->closed);
//  if (c->num_entries > 0) {
//    //assert(c->options.comparator->Compare(key, Slice(r->last_key)) > 0);
//  }
//
//  meta = (struct item_metadata *)item;
//  item_key = (char *)&item[sizeof(*meta)];
//  item_value = (char *)&item[sizeof(*meta)];
//
//  if (c->pending_index_entry) {
//    assert(c->data.size == 0);
//    // r->options.comparator->FindShortestSeparator(&r->last_key, key);
//    std::string handle_encoding;
//    c->pending_handle.EncodeTo(&handle_encoding);
//    block_add(&c->index, c->last_key, Slice(handle_encoding));
//    c->pending_index_entry = 0;
//  }
//  /*
//  if (r->filter_block != nullptr) {
//    r->filter_block->AddKey(key);
//  }
//  */
//  c->last_key = key;
//  c->num_entries++;
//  block_add(&c->data, item);
//  int estimated_block_size = estimate_block_size();
//  if (estimated_block_size >= c->options.block_size) {
//    table_flush(c);
//  }
//
//}

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

#define NODE_BATCH 4

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
      printf("FSST %d\n", i);
      pread(victim->slab->fd, vict_file_fsst, victim->slab->size_on_disk, 0);
      R_LOCK(&victim->slab->tree_lock);
      btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index_fsst);
      R_UNLOCK(&victim->slab->tree_lock);
    }

fsst_sleep:
    sleep(1);
  }
}

static void *gc_worker(void *pdata) {
  vict_file_gc = aligned_alloc(PAGE_SIZE, 16384*PAGE_SIZE);

  if (!vict_file_gc)
    die("FSST Static Buf Error\n");

  while (1) {
    if (rbq_isEmpty(GC)) {
      goto gc_sleep;
    }
    for (size_t i = 0; i < NODE_BATCH; i++) {
      tree_entry_t *victim = rbq_dequeue(GC);
      if (!victim)
        goto gc_sleep;
      printf("GC %d\n", i);
      pread(victim->slab->fd, vict_file_gc, victim->slab->size_on_disk, 0);
      R_LOCK(&victim->slab->tree_lock);
      btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index_gc);
      R_UNLOCK(&victim->slab->tree_lock);
    }

gc_sleep:
    sleep(1);
  }
}

void fsst_worker_init(void) {
  pthread_t t;
  pthread_create(&t, NULL, fsst_worker, NULL);
  // pthread_create(&t, NULL, gc_worker, NULL);

}
