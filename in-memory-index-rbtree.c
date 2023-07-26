#include "headers.h"
#include "indexes/rbtree.h"

extern int print;

static uint64_t get_prefix_for_item(char *item) {
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   return *(uint64_t*)item_key;
}

/* In memory RB-Tree */
static rbtree *trees_location;
static pthread_spinlock_t *trees_location_lock;
tree_entry_t *rbtree_worker_lookup(int worker_id, void *item) {
   return rbtree_lookup(trees_location[worker_id], (void*)get_prefix_for_item(item), pointer_cmp);
}
void rbtree_worker_insert(int worker_id, void *item, tree_entry_t *e) {
   pthread_spin_lock(&trees_location_lock[worker_id]);
   rbtree_insert(trees_location[worker_id], (void*)e->key, e, pointer_cmp);
   pthread_spin_unlock(&trees_location_lock[worker_id]);
}
void rbtree_worker_delete(int worker_id, void *item) {
   pthread_spin_lock(&trees_location_lock[worker_id]);
   rbtree_delete(trees_location[worker_id], (void*)get_prefix_for_item(item), pointer_cmp);
   pthread_spin_unlock(&trees_location_lock[worker_id]);
}

void rbtree_tree_add(struct slab_callback *cb, void *tree, void *filter, uint64_t tmp_key) {
   tree_entry_t e = {
         .seq = cb->slab->seq,
         .key = tmp_key,
         .slab = cb->slab,
   };
   cb->slab->tree = tree;
   cb->slab->filter = filter;
   rbtree_worker_insert(0, NULL, &e);
}

tree_entry_t *rbtree_worker_get(void *key) {
   return rbtree_closest_lookup(trees_location[0], key, pointer_cmp);
}

tree_entry_t *rbtree_worker_get_useq(int seq) {
   return rbtree_traverse_useq(trees_location[0], seq);
}

void rbtree_node_update(uint64_t old_key, uint64_t new_key) {
  rbtree_n_update(trees_location[0], (void*)old_key, (void*)new_key, pointer_cmp);
}

index_entry_t *rbtree_tnt_lookup(void *item) {
   rbtree t = trees_location[0];
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;
   rbtree_node n = t->root;
   index_entry_t *e = NULL, *tmp = NULL;
   int count = 0;
   // uint64_t cur_seq = 0;

   // uint64_t elapsed;
   // struct timeval st, et;

   //if (print) {
   //   gettimeofday(&st,NULL);
   //}

   while (n != NULL) {
      struct slab *s = n->value.slab;
      int comp_result;
      // if (key >= s->min && key <= s->max && s->seq >= cur_seq) {
      if (filter_contain(s->filter, (unsigned char *)&key)) {
         count++;
        // printf("(%s,%d) FIND in Filter %d\n", __FUNCTION__ , __LINE__, count);
      // } else
        // printf("(%s,%d) NONE in Filter %d\n", __FUNCTION__ , __LINE__, count);
         // printf("LLL %lu: %lu, %lu // %lu-%lu\n", s->seq,
             // (uint64_t)s->key, key, s->min, s->max);
         tmp = btree_worker_lookup_utree(s->tree, item);
         // if (print)
          // printf("(%s,%d) [%6luus] %d CHECK \n", __FUNCTION__ , __LINE__, elapsed, count);
         if (tmp) {
          e = tmp;
          // cur_seq = s->seq;
          // printf("(%s,%d) FIND in Btree %d\n", __FUNCTION__ , __LINE__, count);
         } 
          // else
          // printf("(%s,%d) NONE in Btree %d\n", __FUNCTION__ , __LINE__, count);
         // if (tmp) {
           // if (print) {
           //  gettimeofday(&et,NULL);
            // elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec) + 1;
            // printf("(%s,%d) [%6luus] %d FIND \n", __FUNCTION__ , __LINE__, elapsed, count);
           // }
           // return e; 
         // }
      }
      comp_result = pointer_cmp((void*)key, n->key);
      //if (comp_result == 0) {
      //   t->last_visited_node = n;
      //   n = NULL;
      //} else 
      if (comp_result <= 0) {
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
   }
  // if (print && e) {
  //   gettimeofday(&et,NULL);
  //   elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec) + 1;
    // printf("(%s,%d) [%6luus] %d NONE \n", __FUNCTION__ , __LINE__, elapsed, count);
  // }
    // printf("(%s,%d) %d \n", __FUNCTION__ , __LINE__, count);
   if (e)
      return e;
   return NULL;
   // return lookup_tnt_index(trees_location[0], (void*)key, pointer_cmp);
}

int rbtree_tnt_invalid(void *item) {
   rbtree t = trees_location[0];
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;
   rbtree_node n = t->root;
   index_entry_t *e = NULL, *tmp = NULL;
   int count = 0;

   while (n != NULL) {
      struct slab *s = n->value.slab;
      int comp_result;
      if (key >= s->min && key <= s->max) {
         if(btree_worker_invalid_utree(s->tree, item)) {
            s->nb_items--;
            count++;
         }
      }
      comp_result = pointer_cmp((void*)key, n->key);
      if (comp_result <= 0) {
         n = n->left;
      } else {
         assert(comp_result > 0);
         n = n->right;
      }
   }

   return count;
}

void rbtree_worker_print(void) {
  rbtree_print(trees_location[0]);
}

/*
 * Returns up to scan_size keys >= item.key.
 * If item is not in the database, this will still return up to scan_size keys > item.key.
 */
struct tree_scan rbtree_init_scan(void *item, size_t scan_size) {
   size_t nb_workers = get_nb_workers();

   struct rbtree_scan_tmp *res = malloc(nb_workers * sizeof(*res));
   for(size_t w = 0; w < nb_workers; w++) {
      pthread_spin_lock(&trees_location_lock[w]);
      res[w] = rbtree_lookup_n(trees_location[w], (void*)get_prefix_for_item(item), scan_size, pointer_cmp);
      pthread_spin_unlock(&trees_location_lock[w]);
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
   trees_location = malloc(get_nb_workers() * sizeof(*trees_location));
   trees_location_lock = malloc(get_nb_workers() * sizeof(*trees_location_lock));
   for(size_t w = 0; w < get_nb_workers() ; w++) {
      trees_location[w] = rbtree_create();
      pthread_spin_init(&trees_location_lock[w], PTHREAD_PROCESS_PRIVATE);
   }
}
