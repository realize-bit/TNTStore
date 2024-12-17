#include "headers.h"
#include "indexes/skiplist.h"
#include "indexes/tnt_subtree.h"

extern int print;
extern int load;

typedef struct skt_node {
  skiplist_node snode;
  subtree_t *tree;
  uint64_t key;
} skt_node_t;

static __thread index_entry_t tmp_entry;
static __thread tree_entry_t tmp_tree_entry;
static skiplist_raw skt_list;
static skt_node_t *skt_list_head; // for -inf tree

int _cmp_skt_node(skiplist_node *a, skiplist_node *b, void *aux) {
  skt_node_t *aa, *bb;
  aa = _get_entry(a, skt_node_t, snode);
  bb = _get_entry(b, skt_node_t, snode);
  if (aa->key < bb->key) {
    return -1;
  } else if (aa->key == bb->key) {
    return 0;
  } else {
    return 1;
  }
}

static uint64_t get_prefix_for_item(char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  return *(uint64_t *)item_key;
}

/* ========================================= */
// Worker for using indexes
static skt_node_t* _skt_get_prev_node(uint64_t key) {
  skt_node_t query;
  skiplist_node *n;
  query.key = key;
  n = skiplist_prev(&skt_list, &query.snode);
  return n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
}

void skt_list_worker_insert(subtree_t *t, uint64_t key) {
  skt_node_t *n = (skt_node_t *)malloc(sizeof(skt_node_t));
  memset(&n->snode, 0, sizeof(skiplist_node));
  n->tree = t;
  n->key = key;
  skiplist_insert(&skt_list, &n->snode);
}

void skt_subtree_split(int degree, struct slab *target_s) {
  uint64_t new_key;
  uint64_t new_level;
  degree = degree - 1;
  uint64_t new_key_list[degree];
  skt_node_t *closest_node;

  R_LOCK(&target_s->tree_lock);
  new_level = ((subtree_t *)target_s->subtree)->depth+1;
  for (size_t i = 0; i < degree; i++) {
    new_key_list[i] =
        target_s->min + (target_s->max - target_s->min) / (degree + 1) * (i + 1);
    printf("new_key_list[%lu]: %lu\n", i, new_key_list[i]);
  }
  R_UNLOCK(&target_s->tree_lock);
  
  closest_node = _skt_get_prev_node(new_key_list[0]);

  for (int i = degree-1; i >= 0; i--) {
    struct slab *new_slab =
        create_slab(NULL, new_level, new_key_list[i], 0, NULL);
    subtree_t *new_tree = subtree_create();
    subtree_set_slab(new_tree, new_slab);
    subtree_set_next(new_tree, closest_node->tree);
    new_tree->depth = closest_node->tree->depth + 1;
    new_slab->subtree = new_tree;
    skt_list_worker_insert(new_tree, new_key_list[i]);
  }

  struct slab *new_head_slab =
      create_slab(NULL, new_level, 0, 0, NULL);
  subtree_t *new_head_tree = subtree_create();
  subtree_set_slab(new_head_tree, new_head_slab);
  subtree_set_next(new_head_tree, closest_node->tree);
  new_head_tree->depth = closest_node->tree->depth + 1;
  new_head_slab->subtree = new_head_tree;
  closest_node->tree = new_head_tree;
}

/* ========================================= */
// SKT

tree_entry_t *skt_first_subtree(void *key, uint64_t *idx, index_entry_t *old_e) {
  skt_node_t query;
  skiplist_node *n;
  skt_node_t *closest_node;

  query.key = (uint64_t)key;
  n = skiplist_find_smaller_or_equal(&skt_list, &query.snode);
  closest_node = n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
  struct slab *s = closest_node->tree->slab;

  tmp_tree_entry.slab = s;
  tmp_tree_entry.key = key;
  tmp_tree_entry.seq = s->seq;
  tmp_tree_entry.level = closest_node->tree->depth;

  return &tmp_tree_entry;
}

tree_entry_t *skt_last_subtree(void *key, uint64_t *idx, index_entry_t *old_e) {
  skt_node_t query;
  skiplist_node *n;
  subtree_t *t;

  query.key = (uint64_t)key;
  n = skiplist_find_greater_or_equal(&skt_list, &query.snode);
  skt_node_t *closest_node = n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
  t = closest_node->tree;

  while (t->next) {
    t = t->next;
  }

  tmp_tree_entry.slab = t->slab;
  tmp_tree_entry.key = key;
  tmp_tree_entry.seq = ((struct slab *)t->slab)->seq;
  tmp_tree_entry.level = t->depth;

  return &tmp_tree_entry;
}

tree_entry_t *skt_prev_last_subtree(void *key, uint64_t *idx, index_entry_t *old_e) {
  skt_node_t query;
  skiplist_node *n;
  subtree_t *t, *prev = NULL;

  query.key = (uint64_t)key;
  n = skiplist_find_greater_or_equal(&skt_list, &query.snode);
  skt_node_t *closest_node = n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
  t = closest_node->tree;

  // 만약 next가 없으면 현재 tree를 return
  if (!t->next) {
    tmp_tree_entry.slab = t->slab;
    tmp_tree_entry.key = key;
    tmp_tree_entry.seq = ((struct slab *)t->slab)->seq;
    tmp_tree_entry.level = t->depth;
    return &tmp_tree_entry;
  }

  // 마지막 직전 subtree를 찾음
  while (t->next) {
    prev = t;
    t = t->next;
  }

  tmp_tree_entry.slab = prev->slab;
  tmp_tree_entry.key = key;
  tmp_tree_entry.seq = ((struct slab *)prev->slab)->seq;
  tmp_tree_entry.level = prev->depth;

  return &tmp_tree_entry;
}

tree_entry_t *skt_subtree_get(void *key, uint64_t *idx, index_entry_t *old_e) {
  skt_node_t query;
  skiplist_node *n;
  skt_node_t *closest_node;
  size_t max;
  
  query.key = (uint64_t)key;

  n = skiplist_find_smaller_or_equal(&skt_list, &query.snode);
  closest_node = n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
  struct slab *s = closest_node->tree->slab;
  if (old_e && s == old_e->slab) {
    *idx = -1;
    goto out;
  }

  W_LOCK(&s->tree_lock);
  *idx = s->last_item++;
  s->nb_items++;
  if (s->last_item == s->nb_max_items) {
    ftruncate(s->fd, s->size_on_disk*2);
    s->nb_max_items *= 2;
  }
  max = s->nb_max_items;
  W_UNLOCK(&s->tree_lock);

  if (*idx == (max*0.75)) {
    skt_subtree_split(2, s);
  }

out:
  __sync_fetch_and_add(&s->update_ref, 1);
  tmp_tree_entry.slab = s;
  tmp_tree_entry.key = key;
  tmp_tree_entry.seq = s->seq;
  tmp_tree_entry.level = closest_node->tree->depth;

  return &tmp_tree_entry;
}

void skt_subtree_update_key(uint64_t old_key, uint64_t new_key) {
}

index_entry_t *skt_index_lookup(void *item) {
  skt_node_t query;
  skiplist_node *n;
  skt_node_t *closest_node;
  subtree_t *t;

  uint64_t key = get_prefix_for_item(item);

  query.key = (uint64_t)key;

  n = skiplist_find_smaller_or_equal(&skt_list, &query.snode);
  closest_node = n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
  t = closest_node->tree;
  int res;
  int count = 0;
  while (t != NULL) {
    struct slab *s = t->slab;
    R_LOCK(&s->tree_lock);
    count++;
    res = subtree_find(t, (unsigned char *)&key, sizeof(key), &tmp_entry);
    if (res) {
      R_UNLOCK(&s->tree_lock);
      return &tmp_entry;
    }
    t = t->next;
    R_UNLOCK(&s->tree_lock);
  }

  return NULL;
}

void skt_index_add(struct slab_callback *cb, void *item) {
  index_entry_t new_entry;
  new_entry.slab = cb->slab;
  new_entry.slab_idx = cb->slab_idx;
  subtree_worker_insert(cb->slab->subtree, item, &new_entry);
}

int skt_index_invalid(void *item) {
  skt_node_t query;
  skiplist_node *n;
  skt_node_t *closest_node;
  subtree_t *t;
  int count = 0;

  uint64_t key = get_prefix_for_item(item);
  query.key = (uint64_t)key;

  n = skiplist_find_smaller_or_equal(&skt_list, &query.snode);
  closest_node = n ? _get_entry(n, skt_node_t, snode) : skt_list_head;
  t = closest_node->tree->next;

  while (t != NULL) {
    struct slab *s = t->slab;
    R_LOCK(&s->tree_lock);
    if (subtree_worker_invalid_utree(t, item)) {
      // TODO: 이것만 atm이면 문제 생길 수 있음
      __sync_fetch_and_sub(&s->nb_items, 1);
      count++;
      R_UNLOCK(&s->tree_lock);
      return 1;
    }
    t = t->next;
    R_UNLOCK(&s->tree_lock);
  }

  return count;
}

subtree_t *skt_subtree_create(void) { return subtree_create(); }

void skt_init(void) {
  skiplist_init(&skt_list, _cmp_skt_node);
  skt_list_head = (skt_node_t *)malloc(sizeof(skt_node_t));
  skt_list_head->tree = subtree_create();
  skt_list_head->tree->depth = 0;
}

int create_skt_root_slab(void) {
  struct slab *new_slab;
  subtree_t *new_tree;
  skt_init();

  new_slab = create_slab(NULL, 0, 0, 0, NULL);
  new_slab->subtree = skt_list_head->tree;
  subtree_set_slab(skt_list_head->tree, new_slab);

  return 1;
}

void skt_print(void) {
  // skiplist 노드 정보 출력
  printf("\n=== SKT 구조 출력 ===\n");
  printf("Skiplist 구조:\n");
  skiplist_node *curr = &skt_list.head;
  int level = 0;
  
  // skt_list_head의 subtree들 먼저 출력
  printf("SKT List Head의 Subtree 정보:\n");
  subtree_t *head_tree = skt_list_head->tree;
  while (head_tree != NULL) {
    printf("    깊이: %lu\n", (unsigned long)head_tree->depth);
    if (head_tree->slab) {
      printf("    Slab 정보 - 아이템 수: %lu, 최소키: %lu, 최대키: %lu\n",
             ((struct slab *)head_tree->slab)->nb_items,
             ((struct slab *)head_tree->slab)->min,
             ((struct slab *)head_tree->slab)->max);
    }
    head_tree = head_tree->next;
  }
  printf("\n");

  // skt_list의 나머지 노드들 출력
  curr = skiplist_begin(&skt_list);
  while (curr) {
    skt_node_t *skt_node = _get_entry(curr, skt_node_t, snode);
    printf("Level %d - Key: %lu\n", skt_node->tree->depth, skt_node->key);
    
    // subtree 정보 출력
    printf("  Subtree 정보:\n");
    subtree_t *tree = skt_node->tree;
    while (tree != NULL) {
      printf("    깊이: %lu\n", (unsigned long)tree->depth);
      if (tree->slab) {
        printf("    Slab 정보 - 아이템 수: %lu, 최소키: %lu, 최대키: %lu\n",
               ((struct slab *)tree->slab)->nb_items,
               ((struct slab *)tree->slab)->min,
               ((struct slab *)tree->slab)->max);
      }
      tree = tree->next;
    }
    curr = skiplist_next(&skt_list, curr);
    printf("\n");
  }
  printf("=== SKT 구조 출력 완료 ===\n\n");
}

void skt_flush_batched_load(void) {
  tree_entry_t *victim = NULL;
  struct slab *s = NULL;
  
  // skt_list_head의 subtree들 먼저 처리
  subtree_t *head_tree = skt_list_head->tree;
  while (head_tree != NULL) {
    s = head_tree->slab;
    if (s && s->nb_batched != 0) {
      for (int i=0; i < s->nb_batched; i++) {
        struct slab_callback *cb = s->batched_callbacks[i];
        kv_add_async_no_lookup(cb, cb->slab, cb->slab_idx);
        s->batched_callbacks[i] = NULL;
      }
      s->nb_batched = 0;
      if (!s->batched_callbacks)
        free(s->batched_callbacks);
    }
    head_tree = head_tree->next;
  }

  // skt_list의 나머지 노드들 처리
  skiplist_node *curr = skiplist_begin(&skt_list);
  while (curr) {
    skt_node_t *skt_node = _get_entry(curr, skt_node_t, snode);
    subtree_t *tree = skt_node->tree;
    
    while (tree != NULL) {
      s = tree->slab;
      if (s && s->nb_batched != 0) {
        for (int i=0; i < s->nb_batched; i++) {
          struct slab_callback *cb = s->batched_callbacks[i];
          kv_add_async_no_lookup(cb, cb->slab, cb->slab_idx);
          s->batched_callbacks[i] = NULL;
        }
        s->nb_batched = 0;
        if (!s->batched_callbacks)
          free(s->batched_callbacks);
      }
      tree = tree->next;
    }
    curr = skiplist_next(&skt_list, curr);
  }
}


