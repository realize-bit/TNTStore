#include "headers.h"
#include "indexes/rbtree.h"

extern int print;
extern int load;

static uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

/* In memory RB-Tree */
static rbtree trees_location;
static pthread_lock_t trees_location_lock;
tree_entry_t *rbtree_worker_lookup(int worker_id, void *item) {
   return rbtree_lookup(trees_location, (void*)get_prefix_for_item(item), pointer_cmp);
}
void rbtree_worker_insert(int worker_id, void *item, tree_entry_t *e) {
   W_LOCK(&trees_location_lock);
   rbtree_insert(trees_location, (void*)e->key, e, pointer_cmp);
   W_UNLOCK(&trees_location_lock);
}
void rbtree_worker_delete(int worker_id, void *item) {
   W_LOCK(&trees_location_lock);
   rbtree_delete(trees_location, (void*)get_prefix_for_item(item), pointer_cmp);
   W_UNLOCK(&trees_location_lock);
}

void rbtree_tree_add(struct slab *s, void *tree, void *filter, uint64_t tmp_key) {
   tree_entry_t e = {
         .seq = s->seq,
         .key = tmp_key,
         .slab = s,
   };
   s->tree = tree;
   s->filter = filter;
   rbtree_worker_insert(0, NULL, &e);
}

tree_entry_t *rbtree_worker_get(void *key, uint64_t *idx, index_entry_t * old_e) {
   rbtree t = trees_location;
   rbtree_node n = t->root, prev;
   index_entry_t *e = NULL, *tmp = NULL;
   R_LOCK(&trees_location_lock);
   // t = rbtree_closest_lookup(trees_location, key, pointer_cmp);
   while (1) {
      struct slab *s = n->value.slab;
      int comp_result;

      W_LOCK(&s->tree_lock);
      if (s->imm == 0) {
        if (old_e && s == old_e->slab) {
          *idx = -1;
          W_UNLOCK(&s->tree_lock);
          break;
        }
        assert(s->last_item < s->nb_max_items);
        *idx = s->last_item++;
        s->nb_items++;
        if(s->last_item == s->nb_max_items) 
          s->imm = 1;
        W_UNLOCK(&s->tree_lock);
        break;
      } else {
        assert(s->last_item == s->nb_max_items);
      }
      W_UNLOCK(&s->tree_lock);
      prev = n;
      do {
        R_LOCK(&s->tree_lock);
        comp_result = pointer_cmp((void*)key, prev->key);
        R_UNLOCK(&s->tree_lock);
        if (comp_result <= 0) {
         // n->imm = 1;
          n = prev->left;
        } else {
          assert(comp_result > 0);
          n = prev->right;
        }
        if (!n) {
            R_UNLOCK(&trees_location_lock);
            NOP10();
            R_LOCK(&trees_location_lock);
        }
      } while (!n);
   }
   R_UNLOCK(&trees_location_lock);
   if (n)
    return &n->value;
   else
    return NULL;
}

tree_entry_t *rbtree_worker_get_useq(int seq) {
   tree_entry_t *t;
   R_LOCK(&trees_location_lock);
   t = rbtree_traverse_useq(trees_location, seq);
   R_UNLOCK(&trees_location_lock);
   return t;
}

void rbtree_node_update(uint64_t old_key, uint64_t new_key) {
   rbtree t = trees_location;
   rbtree_node n = t->root;
   index_entry_t *e = NULL, *tmp = NULL;
   R_LOCK(&trees_location_lock);
   // rbtree_n_update(trees_location, (void*)old_key, (void*)new_key, pointer_cmp);
   while (n != NULL) {
      struct slab *s = n->value.slab;
      int comp_result;
      W_LOCK(&s->tree_lock);
      comp_result = pointer_cmp((void*)old_key, n->key);
      if (comp_result == 0) {
         // n->imm = 1;
         n->key = (void*)new_key;
         W_UNLOCK(&s->tree_lock);
         R_UNLOCK(&trees_location_lock);
         return;
      } else if (comp_result < 0) {
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
      W_UNLOCK(&s->tree_lock);
   }
   R_UNLOCK(&trees_location_lock);
}

index_entry_t *rbtree_tnt_lookup(void *item) {
   rbtree t = trees_location;
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;
   rbtree_node n = t->root;
   index_entry_t *e = NULL, *tmp = NULL;
   int count = 0;

   R_LOCK(&trees_location_lock);
   while (n != NULL) {
      struct slab *s = n->value.slab;
      int comp_result;
      // if (key >= s->min && key <= s->max && s->seq >= cur_seq) {
      // TODO::JS::나중에 필터를 없애버리고, 없으면 아예 찾지 않도록 수정해야함
      R_LOCK(&s->tree_lock);
      // printf("filter: %p, (%lu)\n", s->filter, (key>>16)%2);
      if (s->min != -1 && 
        filter_contain(s->filter, (unsigned char *)&key)) {
         count++;
         tmp = btree_worker_lookup_utree(s->tree, item);
         if (tmp) {
          e = tmp;
         } 
         // int test =filter_contain((filter_t *)s->filter, (unsigned char *)&key);
         // if (load == 0 && tmp && !test)
          // printf("WHATDU2 %d %p\n", test, (filter_t *)s->filter);
      }
      comp_result = pointer_cmp((void*)key, n->key);
      R_UNLOCK(&s->tree_lock);
      if (comp_result <= 0) {
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
   }
   R_UNLOCK(&trees_location_lock);
   if (e)
      return e;
   // if(load == 0)
    // printf("NULL count: %d\n", count);
   return NULL;
}

tree_scan_res_t rbtree_tnt_scan(void *item, uint64_t size) {
   struct index_scan scan_res;
   rbtree t = trees_location;

   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   uint64_t root_key = *(uint64_t*)item_key;

   int count = 0;

   scan_res.entries = malloc(size * sizeof(*scan_res.entries));
   scan_res.hashes = malloc(size * sizeof(*scan_res.hashes));
   scan_res.nb_entries = 0;
   // TODO::JS:: 나중에 BTREE의 스캔 구조를 이용하도록 수정
   // 지금은 그냥 무조건 하나씩 찾음
   R_LOCK(&trees_location_lock);
   while (count < size) {
     rbtree_node n = t->root;
     index_entry_t *e = NULL, *tmp = NULL;
     uint64_t key = root_key + count;
     while (n != NULL) {
        struct slab *s = n->value.slab;
        int comp_result;
        R_LOCK(&s->tree_lock);
        if (s->min != -1 && 
          filter_contain(s->filter, (unsigned char *)&key)) {
           tmp = btree_worker_lookup_ukey(s->tree, key);
           if (tmp) {
            e = tmp;
           } 
        }
        comp_result = pointer_cmp((void*)key, n->key);
        R_UNLOCK(&s->tree_lock);

        if (comp_result <= 0) {
           n = n->left;
        } else {
           assert(comp_result > 0);
           n = n->right;
        }
     }
    R_UNLOCK(&trees_location_lock);

     if (e) {
      scan_res.entries[scan_res.nb_entries] = *e; 
      scan_res.hashes[scan_res.nb_entries] = key; 
      scan_res.nb_entries++;
     } /*else {
     if (print)
      printf("CANT BE FOUND %lu\n", key);
     }*/
     count++;
   }

   return scan_res;
}

int rbtree_tnt_invalid(void *item) {
   rbtree t = trees_location;
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;
   rbtree_node n = t->root;
   index_entry_t *e = NULL, *tmp = NULL;
   int count = 0;

   R_LOCK(&trees_location_lock);
   while (n != NULL) {
      struct slab *s = n->value.slab;
      int comp_result;

      W_LOCK(&s->tree_lock);
      if (s->min != -1 && 
        filter_contain(s->filter, (unsigned char *)&key)) {
         if(btree_worker_invalid_utree(s->tree, item)) {
            s->nb_items--;
            count++;
         }
      }
      comp_result = pointer_cmp((void*)key, n->key);
      W_UNLOCK(&s->tree_lock);

      if (comp_result <= 0) {
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
   }
   R_UNLOCK(&trees_location_lock);

   return count;
}

void rbtree_worker_print(void) {
  R_LOCK(&trees_location_lock);
  rbtree_print(trees_location);
  R_UNLOCK(&trees_location_lock);
}

/*
 * Returns up to scan_size keys >= item.key.
 * If item is not in the database, this will still return up to scan_size keys > item.key.
 */
struct tree_scan rbtree_init_scan(void *item, size_t scan_size) {
   size_t nb_workers = get_nb_workers();

   struct rbtree_scan_tmp *res = malloc(nb_workers * sizeof(*res));
   for(size_t w = 0; w < nb_workers; w++) {
      R_LOCK(&trees_location_lock);
      res[w] = rbtree_lookup_n(trees_location, (void*)get_prefix_for_item(item), scan_size, pointer_cmp);
      R_UNLOCK(&trees_location_lock);
   }

   struct tree_scan scan_res;
   scan_res.entries = malloc(scan_size * sizeof(*scan_res.entries));
   scan_res.hashes = malloc(scan_size * sizeof(*scan_res.hashes));
   scan_res.nb_entries = 0;

   size_t *positions = calloc(nb_workers, sizeof(*positions));
   while(scan_res.nb_entries < scan_size) {
      size_t min_worker = nb_workers;
      struct rbtree_node_t *min = NULL;
      for(size_t w = 0; w < nb_workers; w++) {
         if(res[w].nb_entries <= positions[w]) {
            continue; // no more item to read in that rbtree
         } else {
            struct rbtree_node_t *current = &res[w].entries[positions[w]];
            if(!min || pointer_cmp(min->key, current->key) > 0) {
               min = current;
               min_worker = w;
            }
         }
      }
      if(min_worker == nb_workers)
         break; // no worker has any scannable item left
      positions[min_worker]++;
      scan_res.entries[scan_res.nb_entries] = min->value;
      scan_res.hashes[scan_res.nb_entries] = (uint64_t)min->key;
      scan_res.nb_entries++;
   }
   for(size_t w = 0; w < nb_workers; w++) {
      free(res[w].entries);
   }
   free(res);
   free(positions);
   return scan_res;
}

void rbtree_init(void) {
   trees_location = malloc(sizeof(*trees_location));
   trees_location = rbtree_create();
   INIT_LOCK(&trees_location_lock, NULL);
}
