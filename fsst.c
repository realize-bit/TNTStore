#include "headers.h"

static int cur = 0;
static char *vict_file_data = NULL;

static void check_and_remove_tree (struct slab_callback *cb, void *item) {
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
      printf("FSST1 NB %lu seq %lu imm %u / read fd: %d, bytes: %d\n", victim->slab->nb_items, victim->slab->seq, victim->slab->imm, victim->slab->fd, count);
      R_LOCK(&victim->slab->tree_lock);
      count = btree_forall_invalid(victim->slab->tree, skip_or_invalidate_index);
      R_UNLOCK(&victim->slab->tree_lock);
      printf("FSST2 NB %lu %lu %d\n", victim->slab->nb_items, victim->slab->seq, count);
  } while (victim);

  free(vict_file_data);
  vict_file_data = NULL;
  cur = 0;
  // printf("FSST1.5 COUNT %d\n", count);
  // sleep(10);
  // printf("FSST2 NB %lu %lu\n", victim->slab->nb_items, victim->slab->seq);
  return 0;
}

