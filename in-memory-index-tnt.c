#include "headers.h"
#include "indexes/tnt_centree.h"
#include "indexes/tnt_subtree.h"

extern int print;
extern int load;

static background_queue *gc_queue, *fsst_queue;
static pthread_lock_t gc_lock;
static pthread_lock_t fsst_lock;

static __thread index_entry_t tmp_entry;

static uint64_t get_prefix_for_item(char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  return *(uint64_t *)item_key;
}

background_queue *bgq_get(enum fsst_mode m) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  return queue;
}

int bgq_is_empty(enum fsst_mode m) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  pthread_lock_t lock = m == GC ? gc_lock : fsst_lock;
  int count;
  R_LOCK(&lock);
  count = queue->count;
  R_UNLOCK(&lock);
  return count == 0;
}

int bgq_count(enum fsst_mode m) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  pthread_lock_t lock = m == GC ? gc_lock : fsst_lock;
  int count;

  R_LOCK(&lock);
  count = queue->count;
  R_UNLOCK(&lock);
  return count;
}

void bgq_enqueue(enum fsst_mode m, centree_node n) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  pthread_lock_t lock = m == GC ? gc_lock : fsst_lock;
  bgq_node *new = (bgq_node *)malloc(sizeof(bgq_node));
  new->data = n;
  new->next = NULL;
  printf("mode: %d, enqueu: %lu\n", m, n->value.slab->seq);

  W_LOCK(&lock);
  if (is_empty(queue)) {
    queue->front = new;
  } else {
    queue->rear->next = new;
  }
  queue->rear = new;
  queue->count++;
  W_UNLOCK(&lock);
}

tree_entry_t *bgq_dequeue(enum fsst_mode m) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  pthread_lock_t lock = m == GC ? gc_lock : fsst_lock;
  centree_node data;
  bgq_node *ptr;
  W_LOCK(&lock);
  if (is_empty(queue)) {
    W_UNLOCK(&lock);
    return 0;
  }
  ptr = queue->front;
  data = ptr->data;
  queue->front = ptr->next;
  queue->count--;
  W_UNLOCK(&lock);
  free(ptr);

  return &data->value;
}

tree_entry_t *bgq_front(enum fsst_mode m) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  pthread_lock_t lock = m == GC ? gc_lock : fsst_lock;
  centree_node data;
  bgq_node *ptr;
  R_LOCK(&lock);
  if (is_empty(queue)) {
    W_UNLOCK(&lock);
    return 0;
  }
  ptr = queue->front;
  data = ptr->data;
  R_UNLOCK(&lock);

  return &data->value;
}

centree_node bgq_front_node(enum fsst_mode m) {
  background_queue *queue = m == GC ? gc_queue : fsst_queue;
  pthread_lock_t lock = m == GC ? gc_lock : fsst_lock;
  centree_node data;
  bgq_node *ptr;
  R_LOCK(&lock);
  if (is_empty(queue)) {
    W_UNLOCK(&lock);
    return 0;
  }
  ptr = queue->front;
  data = ptr->data;
  R_UNLOCK(&lock);

  return data;
}

centree_node dequeue_specific_node(background_queue *queue,
                                   centree_node target) {
  if (queue->front == NULL) return NULL;

  bgq_node *current = queue->front;
  bgq_node *prev = NULL;

  while (current != NULL) {
    centree_node ret;
    if (current->data == target) {
      if (prev == NULL) {
        // target node is the front node
        queue->front = current->next;
        if (queue->front == NULL) {
          queue->rear = NULL;
        }
      } else {
        prev->next = current->next;
        if (current->next == NULL) {
          queue->rear = prev;
        }
      }
      if (current->next == NULL)
        ret = NULL;
      else
        ret = current->next->data;
      free(current);
      queue->count--;
      return ret;
    }
    prev = current;
    current = current->next;
  }

  return NULL;
}

tree_entry_t *get_next_node_entry(background_queue *queue,
                                  centree_node target) {
  bgq_node *current = queue->front;

  while (current != NULL) {
    if (current->data == target) {
      if (current->next != NULL) {
        return &current->next->data->value;
      } else {
        return NULL;
      }
    }
    current = current->next;
  }

  return NULL;
}

centree_node get_next_node(background_queue *queue, centree_node target) {
  bgq_node *current = queue->front;

  while (current != NULL) {
    if (current->data == target) {
      if (current->next != NULL) {
        return current->next->data;
      } else {
        return NULL;
      }
    }
    current = current->next;
  }

  return NULL;
}

/* ========================================= */
// Worker for using indexes

static centree centree_root;
static pthread_lock_t centree_root_lock;

tree_entry_t *centree_worker_lookup(void *key) {
  return centree_lookup(centree_root, key, tnt_pointer_cmp);
}

void centree_worker_insert(int worker_id, void *item, tree_entry_t *e) {
  centree_node n;
  W_LOCK(&centree_root_lock);
  n = centree_insert(centree_root, (void *)e->key, e, tnt_pointer_cmp);
  W_UNLOCK(&centree_root_lock);
  e->slab->centree_node = (void *)n;
}

// void tnt_subtree_delete(int worker_id, void *item) {
//    W_LOCK(&centree_root_lock);
//    centree_delete(centree_root, (void*)get_prefix_for_item(item),
//    tnt_pointer_cmp); W_UNLOCK(&centree_root_lock);
// }
//
index_entry_t *subtree_worker_lookup_utree(subtree_t *tree, void *item) {
  uint64_t hash = get_prefix_for_item(item);
  // printf("# \tLookup Debug hash 1: %lu\n", hash);
  int res =
      subtree_find(tree, (unsigned char *)&hash, sizeof(hash), &tmp_entry);
  if (res)
    return &tmp_entry;
  else
    return NULL;
}

index_entry_t *subtree_worker_lookup_ukey(subtree_t *tree, uint64_t key) {
  int res = subtree_find(tree, (unsigned char *)&key, sizeof(key), &tmp_entry);
  if (res)
    return &tmp_entry;
  else
    return NULL;
}

uint64_t tnt_get_centree_level(void *n) {
  return ((centree_node)n)->value.level;
}

void tnt_subtree_add(struct slab *s, void *tree, void *filter,
                     uint64_t tmp_key) {
  tree_entry_t e = {
      .seq = s->seq,
      .key = tmp_key,
      .slab = s,
  };
  s->subtree = tree;
  s->filter = filter;
  centree_worker_insert(0, NULL, &e);
}

void subtree_worker_insert(subtree_t *tree, void *item, index_entry_t *e) {
  uint64_t hash = get_prefix_for_item(item);
  subtree_insert(tree, (unsigned char *)&hash, sizeof(hash), e);
}

int subtree_worker_delete(subtree_t *tree, void *item) {
  uint64_t hash = get_prefix_for_item(item);
  return subtree_delete(tree, (unsigned char *)&(hash), sizeof(hash));
}

int subtree_worker_invalid_utree(subtree_t *tree, void *item) {
  uint64_t hash = get_prefix_for_item(item);
  return subtree_set_invalid(tree, (unsigned char *)&hash, sizeof(hash));
}

/* ========================================= */
// TNT

tree_entry_t *tnt_parent_subtree_get(void *centnode) {
  centree_node p = ((centree_node)centnode)->parent;
  return p ? &p->value : NULL ;
}

tree_entry_t *tnt_subtree_get(void *key, uint64_t *idx, index_entry_t *old_e) {
  centree t = centree_root;
  centree_node n = t->root, prev;

  R_LOCK(&centree_root_lock);
  while (1) {
    struct slab *s = n->value.slab;
    int comp_result;

    R_LOCK(&s->tree_lock);
    if (s->full == 0) {
      R_UNLOCK(&s->tree_lock);
      if (old_e && s == old_e->slab) {
        s->update_ref++;
        // IN-PLACE UPDATE
        *idx = -1;
        break;
      }
      W_LOCK(&s->tree_lock);
      if (s->last_item >= s->nb_max_items) {
        W_UNLOCK(&s->tree_lock);
        continue;
      }
      assert(s->last_item < s->nb_max_items);
      s->update_ref++;
      *idx = s->last_item++;
      s->nb_items++;
      if (s->last_item == s->nb_max_items) s->full = 1;
      W_UNLOCK(&s->tree_lock);
      break;
    } else {
      assert(s->last_item == s->nb_max_items);
    }
    R_UNLOCK(&s->tree_lock);
    prev = n;
    do {
      R_LOCK(&s->tree_lock);
      comp_result = tnt_pointer_cmp((void *)key, prev->key);
      if (comp_result <= 0) {
        n = prev->left;
      } else {
        assert(comp_result > 0);
        n = prev->right;
      }
      R_UNLOCK(&s->tree_lock);
      if (!n) {
        int cur_nb_elements = t->nb_elements;
        R_UNLOCK(&centree_root_lock);
        while (1) {
          if (cur_nb_elements >= t->nb_elements) {  // Queue is full, wait
            NOP10();
            if (!PINNING) usleep(2);
          } else {
            break;
          }
        }
        R_LOCK(&centree_root_lock);
      }
    } while (!n);
  }
  R_UNLOCK(&centree_root_lock);
  if (n)
    return &n->value;
  else
    return NULL;
}

tree_entry_t *tnt_traverse_use_seq(int seq) {
  tree_entry_t *t;
  R_LOCK(&centree_root_lock);
  t = centree_traverse_useq(centree_root, seq);
  R_UNLOCK(&centree_root_lock);
  return t;
}

int tnt_get_nodes_at_level(int level, background_queue *q) {
  int current_level = 0;
  int count = 0;
  background_queue queue;

  init_queue(&queue);
  enqueue_centnode(&queue, centree_root->root);
  while (!is_empty(&queue)) {
    int level_size = queue.count;
    if (current_level == level) {
      while (level_size-- > 0) {
        centree_node node = dequeue_centnode(&queue);
        enqueue_centnode(q, node);
        count++;
      }
      break;
    }

    while (level_size-- > 0) {
      centree_node node = dequeue_centnode(&queue);
      if (node->left != NULL) enqueue_centnode(&queue, node->left);
      if (node->right != NULL) enqueue_centnode(&queue, node->right);
    }
    current_level++;
  }

  return count;
}

void tnt_subtree_update_key(uint64_t old_key, uint64_t new_key) {
  centree t = centree_root;
  centree_node n = t->root;
  R_LOCK(&centree_root_lock);

  while (n != NULL) {
    struct slab *s = n->value.slab;
    int comp_result;
    W_LOCK(&s->tree_lock);
    comp_result = tnt_pointer_cmp((void *)old_key, n->key);
    if (comp_result == 0) {
      n->key = (void *)new_key;
      W_UNLOCK(&s->tree_lock);
      R_UNLOCK(&centree_root_lock);
      return;
    } else if (comp_result < 0) {
      n = n->left;
    } else {
      assert(comp_result > 0);
      n = n->right;
    }
    W_UNLOCK(&s->tree_lock);
  }
  R_UNLOCK(&centree_root_lock);
}

index_entry_t *tnt_index_lookup(void *item) {
  centree t = centree_root;
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  centree_node n = t->root;
  index_entry_t *e = NULL, *tmp = NULL;

  // Leaf node까지 내려가는 과정
  R_LOCK(&centree_root_lock);
  while (n != NULL) {
    int comp_result = tnt_pointer_cmp((void *)key, n->key);
    if (comp_result <= 0) {
      if (n->left == NULL) break;  // Leaf node에 도달
      n = n->left;
    } else {
      assert(comp_result > 0);
      if (n->right == NULL) break;  // Leaf node에 도달
      n = n->right;
    }
  }
  R_UNLOCK(&centree_root_lock);

  // Leaf node에서 upward 탐색
  while (n != NULL) {
    struct slab *s = n->value.slab;
    R_LOCK(&s->tree_lock);
    if (s->min != -1) {
      if (filter_contain(s->filter, (unsigned char *)&key)) {
        tmp = subtree_worker_lookup_utree(s->subtree, item);
        if (tmp) {
          //  && !TEST_INVAL(tmp->slab_idx)
          //  이 조건을 지웠는데, update의 lock을 최소화하기 위해서
          //  1. 지운다. 2. 추가한다 이 과정에서 1-2 사이에 있는 중일 수 있음
          e = tmp;
          R_UNLOCK(&s->tree_lock);
          break;
        }
      }
    }
    R_UNLOCK(&s->tree_lock);

    // 부모 노드로 이동
    n = n->parent;  // parent 필드를 추가하고, 부모 노드로 이동
  }

  if (e) return e;
  return NULL;
}

// index_entry_t *tnt_index_lookup(void *item) {
//    centree t = centree_root;
//    struct item_metadata *meta = (struct item_metadata *)item;
//    char *item_key = &item[sizeof(*meta)];
//    uint64_t key = *(uint64_t*)item_key;
//    centree_node n = t->root;
//    index_entry_t *e = NULL, *tmp = NULL;
//
//    R_LOCK(&centree_root_lock);
//    while (n != NULL) {
//       struct slab *s = n->value.slab;
//       int comp_result;
//
//       R_LOCK(&s->tree_lock);
//       if (s->min != -1) {
//         if (filter_contain(s->filter, (unsigned char *)&key)) {
//          tmp = subtree_worker_lookup_utree(s->tree, item);
//          if (tmp && !TEST_INVAL(tmp->slab_idx)) {
//             e = tmp;
//             R_UNLOCK(&s->tree_lock);
//             break;
//          }
//         }
//       }
//       comp_result = tnt_pointer_cmp((void*)key, n->key);
//       R_UNLOCK(&s->tree_lock);
//
//       if (comp_result <= 0) {
//          n = n->left;
//       } else {
//          assert(comp_result > 0);
//          n = n->right;
//       }
//    }
//    R_UNLOCK(&centree_root_lock);
//    if (e)
//       return e;
//    return NULL;
// }

tree_scan_res_t tnt_scan(void *item, uint64_t size) {
  struct index_scan scan_res;
  centree t = centree_root;

  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t root_key = *(uint64_t *)item_key;
  int count = 0;

  scan_res.entries = malloc(size * sizeof(*scan_res.entries));
  scan_res.hashes = malloc(size * sizeof(*scan_res.hashes));
  scan_res.nb_entries = 0;
  // TODO::JS:: 나중에 BTREE의 스캔 구조를 이용하도록 수정
  // 지금은 그냥 무조건 하나씩 찾음
  R_LOCK(&centree_root_lock);
  while (count < size) {
    centree_node n = t->root;
    index_entry_t *e = NULL, *tmp = NULL;
    uint64_t key = root_key + count;
    while (n != NULL) {
      struct slab *s = n->value.slab;
      int comp_result;
      R_LOCK(&s->tree_lock);
      if (s->min != -1 && filter_contain(s->filter, (unsigned char *)&key)) {
        tmp = subtree_worker_lookup_ukey(s->subtree, key);
        if (tmp) {
          e = tmp;
        }
      }
      comp_result = tnt_pointer_cmp((void *)key, n->key);
      R_UNLOCK(&s->tree_lock);

      if (comp_result <= 0) {
        n = n->left;
      } else {
        assert(comp_result > 0);
        n = n->right;
      }
    }
    R_UNLOCK(&centree_root_lock);

    if (e) {
      scan_res.entries[scan_res.nb_entries] = *e;
      scan_res.hashes[scan_res.nb_entries] = key;
      scan_res.nb_entries++;
    }
    count++;
  }

  return scan_res;
}
void tnt_index_add(struct slab_callback *cb, void *item) {
  index_entry_t new_entry;
  new_entry.slab = cb->slab;
  new_entry.slab_idx = cb->slab_idx;
  subtree_worker_insert(cb->slab->subtree, item, &new_entry);
}

int tnt_index_invalid(void *item) {
  centree t = centree_root;
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  centree_node n = t->root;
  int count = 0;

  R_LOCK(&centree_root_lock);
  while (n != NULL) {
    struct slab *s = n->value.slab;
    int comp_result;

    R_LOCK(&s->tree_lock);
    if (s->min != -1 && filter_contain(s->filter, (unsigned char *)&key)) {
      if (subtree_worker_invalid_utree(s->subtree, item)) {
        __sync_fetch_and_sub(&s->nb_items, 1);
        count++;
        R_UNLOCK(&s->tree_lock);
        R_UNLOCK(&centree_root_lock);
        return 1;
      }
    }
    comp_result = tnt_pointer_cmp((void *)key, n->key);

    R_UNLOCK(&s->tree_lock);

    if (comp_result <= 0) {
      n = n->left;
    } else {
      assert(comp_result > 0);
      n = n->right;
    }
  }
  R_UNLOCK(&centree_root_lock);

  return count;
}

subtree_t *tnt_subtree_create(void) { return subtree_create(); }

void centree_init(void) {
  centree_root = malloc(sizeof(*centree_root));
  centree_root = centree_create();
  gc_queue = malloc(sizeof(background_queue));
  fsst_queue = malloc(sizeof(background_queue));
  init_queue(gc_queue);
  init_queue(fsst_queue);
  INIT_LOCK(&centree_root_lock, NULL);
  INIT_LOCK(&gc_lock, NULL);
  INIT_LOCK(&fsst_lock, NULL);
}

/* ========================================= */
// ETC

void tnt_print(void) {
  R_LOCK(&centree_root_lock);
  centree_print(centree_root);
  R_UNLOCK(&centree_root_lock);
}

void inc_empty_tree() {
  __sync_fetch_and_add(&centree_root->empty_elements, 1);
}

int get_number_of_subtree() {
  int n;
  R_LOCK(&centree_root_lock);
  n = centree_root->nb_elements - centree_root->empty_elements;
  R_UNLOCK(&centree_root_lock);
  return n;
}
