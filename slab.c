#include "headers.h"
#include "utils.h"
#include "items.h"
#include "slab.h"
#include "ioengine.h"
#include "pagecache.h"
#include "slabworker.h"

extern int print;
extern int load;

/*
 * A slab is a file containing 1 or more items of a given size.
 * The size of items is in slab->item_size.
 *
 * Format is [ [size_t rdt1, size_t key_size1, size_t value_size1][key1][value1][maybe some empty space]     [rdt2, key_size2, value_size2][key2]etc. ]
 *
 * When an idem is deleted its key_size becomes -1. value_size is then equal to a next free idx in the slab.
 * That way, when we reuse an empty spot, we know where the next one is.
 *
 *
 * This whole file assumes that when a file is newly created, then all the data is equal to 0. This should be true on Linux.
 */

static int create_sequence = 0;

/*
 * Where is my item in the slab?
 */
off_t item_page_num(struct slab *s, size_t idx) {
   size_t items_per_page = PAGE_SIZE/s->item_size;
   return idx / items_per_page;
}
static off_t item_in_page_offset(struct slab *s, size_t idx) {
   size_t items_per_page = PAGE_SIZE/s->item_size;
   return (idx % items_per_page)*s->item_size;
}

/*
 * When first loading a slab from disk we need to rebuild the in memory tree, these functions do that.
 */
// TODO::JS::지금은 사용 안함. 앞으로도 사용 안하면 제거
void add_existing_item(struct slab *s, size_t idx, void *_item, struct slab_callback *callback) {
   struct item_metadata *item = _item;
   if(item->key_size == -1) { // Removed item
      add_item_in_free_list_recovery(s, idx, item);
      if(idx > s->last_item)
         s->last_item = idx;
   } else if(item->key_size != 0) {
      s->nb_items++;
      if(idx > s->last_item)
         s->last_item = idx;
      if(item->rdt > get_rdt(s->ctx)) // Remember the maximum timestamp existing in the DB
         set_rdt(s->ctx, item->rdt);
      if(callback) { // Call the user callback if it exists
         callback->slab_idx = idx;
         callback->cb(callback, item);
      }
   } else {
      //printf("Empty item on page #%lu idx %lu\n", page_num, idx);
   }
}

// TODO::JS::지금은 사용 안함. 앞으로도 사용 안하면 제거
void process_existing_chunk(int slab_worker_id, struct slab *s, size_t nb_files, size_t file_idx, char *data, size_t start, size_t length, struct slab_callback *callback) {
   static __thread declare_periodic_count;
   size_t nb_items_per_page = PAGE_SIZE / s->item_size;
   size_t nb_pages = length / PAGE_SIZE;
   for(size_t p = 0; p < nb_pages; p++) {
      size_t page_num = ((start + p*PAGE_SIZE) / PAGE_SIZE); // Physical page to virtual page
      size_t base_idx = page_num*nb_items_per_page*nb_files + file_idx*nb_items_per_page;
      size_t current = p*PAGE_SIZE;
      for(size_t i = 0; i < nb_items_per_page; i++) {
         add_existing_item(s, base_idx, &data[current], callback);
         base_idx++;
         current += s->item_size;
         periodic_count(1000, "[SLAB WORKER %d] Init - Recovered %lu items, %lu free spots", slab_worker_id, s->nb_items, s->nb_free_items);
      }
   }
}

#define GRANULARITY_REBUILD (2*1024*1024) // We rebuild 2MB by 2MB
void rebuild_index(int slab_worker_id, struct slab *s, struct slab_callback *callback) {
   char *cached_data = aligned_alloc(PAGE_SIZE, GRANULARITY_REBUILD);

   int fd = s->fd;
   size_t start = 0, end;
   while(1) {
      end = start + GRANULARITY_REBUILD;
      if(end > s->size_on_disk)
         end = s->size_on_disk;
      if( ((end - start) % PAGE_SIZE) != 0)
         end = end - (end % PAGE_SIZE);
      if( ((end - start) % PAGE_SIZE) != 0)
         die("File size is wrong (%%PAGE_SIZE!=0)\n");
      if(end == start)
         break;
      int r = pread(fd, cached_data, end - start, start);
      if(r != end - start)
         perr("pread failed! Read %d instead of %lu (offset %lu)\n", r, end-start, start);
      process_existing_chunk(slab_worker_id, s, 1, 0, cached_data, start, end-start, callback);
      start = end;

   }
   free(cached_data);
   s->last_item++;
   rebuild_free_list(s);
}




/*
 * Create a slab: a file that only contains items of a given size.
 * @callback is a callback that will be called on all previously existing items of the slab if it is restored from disk.
 */
struct slab* create_slab(struct slab_context *ctx, int slab_worker_id, uint64_t key) {
   struct stat sb;
   char path[512];
   struct slab *s = calloc(1, sizeof(*s));

   size_t disk = slab_worker_id / (get_nb_workers()/get_nb_disks());
   sprintf(path, PATH, disk, slab_worker_id, 0LU, key);
   s->fd = open(path,  O_RDWR | O_CREAT | O_DIRECT, 0777);
   if(s->fd == -1)
      perr("Cannot allocate slab %s", path);

   fstat(s->fd, &sb);
   s->size_on_disk = sb.st_size;

   if(s->size_on_disk < 16384*PAGE_SIZE) {
      fallocate(s->fd, 0, 0, 16384*PAGE_SIZE);
      s->size_on_disk = 16384*PAGE_SIZE;
   }

    __sync_add_and_fetch(&create_sequence, 1);
   size_t nb_items_per_page = PAGE_SIZE / 1024;
   s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
   // TODO::JS::구조체 수정
   s->nb_items = 0;
   s->item_size = 1024;
   s->nb_free_items = 0;
   s->last_item = 0;
   s->ctx = ctx;

   s->min = -1;
   s->max = 0;
   s->key = key;
   s->seq = create_sequence;
   s->imm = 0;
   s->update_ref = 0;

   if (load)
    s->batched_callbacks = calloc(32, sizeof(*s->batched_callbacks));
   s->batch_idx = 0;
   s->nb_batched = 0;
   INIT_LOCK(&s->tree_lock, NULL);
   /*
   // Read the first page and rebuild the index if the file contains data
   struct item_metadata *meta = read_item(s, 0);
   if(meta->key_size != 0) { // if the key_size is not 0 then then file has been written before
      callback->slab = s;
      rebuild_index(slab_worker_id, s, callback);
   }
   */

   return s;
}

/*
 * Double the size of a slab on disk
 */
struct slab* resize_slab(struct slab *s) {
   if(s->size_on_disk < 10000000000LU) {
      s->size_on_disk *= 2;
      if(fallocate(s->fd, 0, 0, s->size_on_disk))
         perr("Cannot resize slab (item size %lu) new size %lu\n", s->item_size, s->size_on_disk);
      s->nb_max_items *= 2;
   } else {
      size_t nb_items_per_page = PAGE_SIZE / s->item_size;
      s->size_on_disk += 10000000000LU;
      if(fallocate(s->fd, 0, 0, s->size_on_disk))
         perr("Cannot resize slab (item size %lu) new size %lu\n", s->item_size, s->size_on_disk);
      s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
   }
   return s;
}

static int show_rbtree(void *key, void *value) {
 printf("key: %lu\n", key);
 return 0;
}

void create_root_slab() {
   tnt_tree_init();
   struct slab_callback *cb = malloc(sizeof(*cb));
   // page_cache_init(ctx->pagecache);
   cb->cb = NULL;
   cb->slab = create_slab(NULL, 0, 0);
   tnt_tree_add(cb->slab, tnt_tree_create(), filter_create(200000), 0);
   free(cb);
}

struct slab* close_and_create_slab(struct slab *s) {
   uint64_t new_key;

   //TODO::JS:: add_in_tree 쪽으로 옮겨야함
   R_LOCK(&s->tree_lock);
    new_key = (s->min + s->max)/2;
   R_UNLOCK(&s->tree_lock);

   // if(s->key != new_key)
     tnt_node_update(s->key, new_key);
   printf("NNN: %lu, %lu // %lu-%lu\n", 
          (uint64_t)s->key, (uint64_t)new_key, 
          s->min, s->max);

   W_LOCK(&s->tree_lock);
   s->key = new_key;
   W_UNLOCK(&s->tree_lock);

   // if (s->nb_batched != 0) {
   //   printf("Possible? %d\n", s->nb_batched);
   //   for (int i=0; i < s->nb_batched; i++) {
   //     update_item_async(s->batched_callbacks[i]);
   //     s->batched_callbacks[i] = NULL;
   //   }
   //   s->nb_batched = 0;
   // }

   tnt_tree_add(create_slab(NULL, 0, new_key-1), tnt_tree_create(), (void*)filter_create(200000), new_key-1);
   tnt_tree_add(create_slab(NULL, 0, new_key+1), tnt_tree_create(), (void*)filter_create(200000), new_key+1);
   // cb->slab =  tnt_tree_get((void*)key)->slab;
   // printf("NNN4: %lu %lu // %lu-%lu\n", 
   //        key, (uint64_t)cb->slab->key, 
   //        cb->slab->min, cb->slab->max);
   // tnt_print(show_rbtree);
   // s->key = new_key;
   return s;
}





/*
 * Synchronous read item
 */
void *read_item(struct slab *s, size_t idx) {
   size_t page_num = item_page_num(s, idx);
   char *disk_data = safe_pread(s->fd, page_num*PAGE_SIZE);
   return &disk_data[item_in_page_offset(s, idx)];
}

/*
 * Asynchronous read
 * - read_item_async creates a callback for the ioengine and queues the io request
 * - read_item_async_cb is called when io is completed (might be synchronous if page is cached)
 * If nothing happen it is because do_io is not called.
 */
void read_item_async_cb(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;
   off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);
   if(callback->cb) 
      callback->cb(callback, &disk_page[in_page_offset]);
}

void read_item_async(struct slab_callback *callback) {
   callback->io_cb = read_item_async_cb;
   read_page_async(callback);
}

void scan_item_async(struct slab_callback *callback) {
   callback->io_cb = read_item_async_cb;
   read_file_async(callback);
}

/*
 * Asynchronous update item:
 * - First read the page where the item is staying
 * - Once the page is in page cache, write it
 * - Then send the order to flush it.
 */
void update_item_async_cb2(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;
   off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);
   unsigned char cbcb = callback->cb_cb ? 1 : 0;


   struct item_metadata *meta = (struct item_metadata *)callback->item;
   char *item_key = &callback->item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;

   void *item = &disk_page[in_page_offset];
   struct item_metadata *meta2 = (struct item_metadata *)item;
   char *item_key2 = &item[sizeof(*meta2)];
   uint64_t key2 = *(uint64_t*)item_key2;
   if (key != key2)
    printf("DIFF key1: %lu, key2: %lu, key/idx: %lu/%lu\n", key, key2, callback->slab->seq, callback->slab_idx);

   if(callback->cb)
      callback->cb(callback, &disk_page[in_page_offset]);
   if(cbcb)
      callback->cb_cb(callback, &disk_page[in_page_offset]);

}

void update_item_async_cb1(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;

   struct slab *s = callback->slab;
   size_t idx = callback->slab_idx;
   void *item = callback->item;
   struct item_metadata *meta = item;
   off_t offset_in_page = item_in_page_offset(s, idx);
   struct item_metadata *old_meta = (void*)(&disk_page[offset_in_page]);
   // struct slab_context *ctx = get_slab_context(item);
   struct slab_context *ctx = callback->ctx;

   /* UPDATE도 ADD 취급이다.
   if(callback->action == UPDATE) {
      size_t new_key_size = meta->key_size;
      size_t old_key_size = old_meta->key_size;
      if(new_key_size != old_key_size) {
         die("Updating an item, but key size changed! Likely this is because 2 keys have the same prefix in the index and we got confused because they have the same prefix. TODO: make the index more robust by detecting that 2 keys have the same prefix and transforming the prefix -> slab_idx to prefix -> [ { full key 1, slab_idx1 }, { full key 2, slab_idx2 } ]\n");
      }

      char *new_key = &disk_page[offset_in_page + sizeof(*meta)];
      char *old_key = &(((char*)old_meta)[sizeof(*meta)]);
      if(memcmp(new_key, old_key, new_key_size))
         die("Updating an item, but key mismatch! Likely this is because 2 keys have the same prefix in the index. TODO: make the index more robust by detecting that 2 keys have the same prefix and transforming the prefix -> slab_idx to prefix -> [ { full key 1, slab_idx1 }, { full key 2, slab_idx2 } ]\n");
   }
   */

   meta->rdt = get_rdt(ctx);
   if(meta->key_size == -1)
      memcpy(&disk_page[offset_in_page], meta, sizeof(*meta));
   else if(get_item_size(item) > s->item_size)
      die("Trying to write an item that is too big for its slab\n");
   else
      memcpy(&disk_page[offset_in_page], item, get_item_size(item));

   /*
   char *item_key = &callback->item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;
   off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);

   void *item2 = &disk_page[in_page_offset];
   struct item_metadata *meta2 = (struct item_metadata *)item;
   char *item_key2 = &item2[sizeof(*meta)];
   uint64_t key2 = *(uint64_t*)item_key2;
   // if (key != key2)
    // printf("DIFFFF key1: %lu, key2: %lu, %lu/%lu\n", key, key2, s->seq, idx);
   */

   callback->io_cb = update_item_async_cb2;
   write_page_async(callback);
}

void update_item_async(struct slab_callback *callback) {
   callback->io_cb = update_item_async_cb1;
   read_page_async(callback);
}

/*
 * Add an item is just like updating, but we need to find a suitable page first!
 * get_free_item_idx returns lru_entry == NULL if no page with empty spot exist.
 * If a page with an empty spot exists, we have to scan it to find a suitable spot.
 */
void add_item_async_cb1(struct slab_callback *callback) {
   struct slab *s = callback->slab;
   struct lru *lru_entry = callback->lru_entry;

   // if (callback->slab_idx != -1)
      // goto skip;

   R_LOCK(&s->tree_lock);
   if(lru_entry == NULL) { // no free page, append
      // TODO::JS::일단 아이템 사이즈 고정시킴
      // assert(s->last_item < s->nb_max_items);
      // callback->slab_idx = s->last_item-1;
      // if (load) 
         // s->batched_callbacks[s->nb_batched++] = callback;

      if(callback->slab_idx+1 == s->nb_max_items && 
        callback->slab_idx != callback->fsst_idx) {
         assert(s->imm == 1);
         assert(s->last_item == s->nb_max_items);
         // s->imm = 1;
         R_UNLOCK(&s->tree_lock);
         s = close_and_create_slab(s);
         R_LOCK(&s->tree_lock);
      }
      // printf("%lu %lu\n", s->nb_items, s->nb_max_items);
      // assert(s->last_item < s->nb_max_items);
   } else { // reuse a free spot. Don't forget to add the linked tombstone in the freelist.
      char *disk_page = callback->lru_entry->page;
      off_t in_page_offset = item_in_page_offset(callback->slab, callback->slab_idx);
      add_son_in_freelist(callback->slab, callback->slab_idx, (void*)(&disk_page[in_page_offset]));
   }
   
   //JS::그냥 실험 빨리빨리 해보기 위해 로딩 성능 높여주려고 임의로 추가
   /*
   if (load) {
     if (s->nb_batched == 32) {
      for (int i=0; i<32; i++) {
        pthread_spin_unlock(&s->tree_lock);
        update_item_async(s->batched_callbacks[i]);
        pthread_spin_lock(&s->tree_lock);
        s->batched_callbacks[i] = NULL;
      }
      s->nb_batched = 0;
      if (s->last_item == s->nb_max_items)
        free(s->batched_callbacks);
     }
     pthread_spin_unlock(&s->tree_lock);
     return;
   }
   */
   R_UNLOCK(&s->tree_lock);
   // if (print)
   //  printf("ADD ITEM (%lu, %lu)\n", s->key, callback->slab_idx);

// skip:
   // if (callback->ctx != get_slab_context_uidx(callback->slab_idx)) {
    // increase_processed(callback->ctx);
   kv_add_async_no_lookup(callback, callback->slab, callback->slab_idx);
    // return;
   // }

   // update_item_async(callback);
   // increase_processed(callback->ctx);
}

void add_item_async(struct slab_callback *callback) {
   callback->io_cb = add_item_async_cb1;
   get_free_item_idx(callback);
}

void add_in_tree_for_update(struct slab_callback *cb, void *item) {
   struct slab *s = cb->slab;
   unsigned char enqueue = 0;
   struct item_metadata *meta = (struct item_metadata *)item;
   char *item_key = &item[sizeof(*meta)];
   uint64_t key = *(uint64_t*)item_key;
   int alrdy = 0;

   // tnt_index_invalid(cb->item);
   // W_LOCK(&cb->fsst_slab->tree_lock);
   // if(btree_worker_invalid_utree(cb->fsst_slab->tree, item)) {
      // cb->fsst_slab->nb_items--;
   // }
   // W_UNLOCK(&cb->fsst_slab->tree_lock);
   if(tnt_index_invalid(cb->item) == 0) {
     if (cb->fsst_slab)
      printf("UPDATE NULL %lu seq/idx %lu/%lu fsst %lu/%lu\n", key, cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq, cb->fsst_idx);
     else 
      printf("UPDATE NULL %lu seq/idx %lu/%lu fsst NULL\n", key, cb->slab->seq, cb->slab_idx);
    }
   // else
     // printf("UPDATE COMP\n");

   W_LOCK(&s->tree_lock);
   alrdy = memory_index_delete_utree(cb->slab->tree, item);
   // W_LOCK(&s->tree_lock);
   // printf("ADD %lu -> (%lu, %lu)\n", key, s->key, cb->slab_idx);
   // W_LOCK(&s->tree_lock);
   memory_index_add_utree(cb, item);

   if(!alrdy) {
    // s->nb_items++;
    if (filter_add((filter_t *)s->filter, (unsigned char*)&key) == 0) {
      printf("Fail adding to filter %p %lu seq/idx %lu/%lu fsst %lu/%lu\n", s->filter, key, cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq, cb->fsst_idx);
    } else if (!filter_contain(s->filter, (unsigned char *)&key)) {
        printf("FIFIFIFIF UPUP\n");
    }
   } 
  //else {
    // 1. imm!=0인 곳에 데이터가 있어서, 거기를 업데이트 하는 상황
    // 2. imm!=1인 곳에 데이터가 있는데, 두 리퀘스트가 동시에 업데이트 하려는 상황
    // 1은 idx=-1이므로 여기에 도달하지 않는다.
    // 2는 여기에 도달함
    // 2의 상황이 좀 애매한데, 두 리퀘스트를 A, B라고 할 때,
    //    (1) A완벽 수행 B 수행
    //    (2) A수행 도중 B 수행이면?
    // (1)과 (2)모두 여기 도달 가능함. 왜냐면 tnt_index_invalid는 인덱스 삭제가 아닌
    // invalid 표시이기 때문에, (1)도 memory_index_delete_utree에서 삭제됨
    // 지금까지 포착하기로는 다 (1)임. 그런데 (2)의 케이스라면 nb_items가 줄어들지 않는 문제가
    // 발생 가능해서 이걸 수정이 필요하다. 발생하긴 하겠지? TODO::JS

     //if (cb->fsst_slab)
     // printf("DoubleReq %lu seq/idx %lu/%lu fsst %lu/%lu\n", key, cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq, cb->fsst_idx);
     //else 
     // printf("DoubleReq %lu seq/idx %lu/%lu fsst NULL\n", key, cb->slab->seq, cb->slab_idx);

  //}

   s->update_ref--;
   if (key < s->min) {
      // printf("Min: %lu -> %lu\n", s->min, key);
      s->min = key;
   }
   if (key > s->max) {
      // printf("Max: %lu -> %lu\n", s->max, key);
      s->max = key;
   }
   if ((s->max - s->min) > (s->nb_max_items * 10)
       && s->imm && !s->update_ref 
       && !((rbtree_node)s->tree_node)->imm) {
      enqueue = 1;
      ((rbtree_node)s->tree_node)->imm = 1;
   }
   // if (s->last_item == s->nb_max_items)
      // s->imm = 1;
   W_UNLOCK(&s->tree_lock);
   if (enqueue)
      rbq_enqueue(FSST, s->tree_node);

   if (cb->cb_cb == add_in_tree_for_update) {
    // printf("CBCB\n");
    free(cb->item);
    free(cb);
   }
}

void remove_and_add_item_async(struct slab_callback *callback) {
   callback->io_cb = add_item_async_cb1;
   if (callback->slab_idx != -1) {
      if (!callback->cb) // making fsst by using cb_cb
       callback->cb = add_in_tree_for_update;
      else {
       callback->cb_cb = callback->cb; // computes_stat
       callback->cb = add_in_tree_for_update;
      }
   }  else  {
      //FSST과정에서는 불가능 해야 하는(호출되면 안되는) 상황
      // In-place update라 트리 수정이 불필요.
      // 그냥 원래 위치에 데이터만 업데이트
     callback->slab_idx = callback->fsst_idx;
      // printf("UPDATE TREE\n");
  }
  // else
  //     printf("KEEP TREE (%lu, %lu)\n", callback->slab->key, callback->slab_idx);
   get_free_item_idx(callback);
}


/*
 * Remove an item
 */
void remove_item_by_idx_async_cb1(struct slab_callback *callback) {
   char *disk_page = callback->lru_entry->page;

   struct slab *s = callback->slab;
   size_t idx = callback->slab_idx;

   off_t offset_in_page = item_in_page_offset(s, idx);
   struct slab_context *ctx = get_slab_context(callback->item);

   struct item_metadata *meta = (void*)&disk_page[offset_in_page];
   if(meta->key_size == -1) { // already removed
      if(callback->cb)
         callback->cb(callback, &disk_page[offset_in_page]);
      return;
   }

   meta->rdt = get_rdt(ctx);
   meta->key_size = -1;

   s->nb_items--;
   add_item_in_free_list(s, idx, meta);

   callback->io_cb = update_item_async_cb2;
   write_page_async(callback);
}


void remove_item_async(struct slab_callback *callback) {
   callback->io_cb = remove_item_by_idx_async_cb1;
   read_page_async(callback);
}
