#include "headers.h"
#include "utils.h"
#include "items.h"
#include "slab.h"
#include "ioengine.h"
#include "pagecache.h"
#include "slabworker.h"

extern int print;
extern int load;
extern uint64_t nb_totals;

/*
 * A slab is a file containing 1 or more items of a given size.
 * The size of items is in slab->item_size.
 *
 * Format is [ [size_t rdt1, size_t key_size1, size_t
 * value_size1][key1][value1][maybe some empty space]     [rdt2, key_size2,
 * value_size2][key2]etc. ]
 *
 * When an idem is deleted its key_size becomes -1. value_size is then equal to
 * a next free idx in the slab. That way, when we reuse an empty spot, we know
 * where the next one is.
 *
 *
 * This whole file assumes that when a file is newly created, then all the data
 * is equal to 0. This should be true on Linux.
 */

static int create_sequence = 0;

/*
 * Where is my item in the slab?
 */
off_t item_page_num(struct slab *s, size_t idx) {
  size_t items_per_page = PAGE_SIZE / s->item_size;
  return idx / items_per_page;
}
static off_t item_in_page_offset(struct slab *s, size_t idx) {
  size_t items_per_page = PAGE_SIZE / s->item_size;
  return (idx % items_per_page) * s->item_size;
}

/*
 * When first loading a slab from disk we need to rebuild the in memory tree,
 * these functions do that.
 */
// TODO::JS::지금은 사용 안함. 앞으로도 사용 안하면 제거
/*
void add_existing_item(struct slab *s, size_t idx, void *_item, struct
slab_callback *callback) { struct item_metadata *item = _item; if(item->key_size
== -1) { // Removed item add_item_in_free_list_recovery(s, idx, item); if(idx >
s->last_item) s->last_item = idx; } else if(item->key_size != 0) {
      s->nb_items++;
      if(idx > s->last_item)
         s->last_item = idx;
      if(item->rdt > get_rdt(s->ctx)) // Remember the maximum timestamp existing
in the DB set_rdt(s->ctx, item->rdt); if(callback) { // Call the user callback
if it exists callback->slab_idx = idx; callback->cb(callback, item);
      }
   } else {
      //printf("Empty item on page #%lu idx %lu\n", page_num, idx);
   }
}

void process_existing_chunk(int slab_worker_id, struct slab *s, size_t nb_files,
size_t file_idx, char *data, size_t start, size_t length, struct slab_callback
*callback) { static __thread declare_periodic_count; size_t nb_items_per_page =
PAGE_SIZE / s->item_size; size_t nb_pages = length / PAGE_SIZE; for(size_t p =
0; p < nb_pages; p++) { size_t page_num = ((start + p*PAGE_SIZE) / PAGE_SIZE);
// Physical page to virtual page size_t base_idx =
page_num*nb_items_per_page*nb_files + file_idx*nb_items_per_page; size_t current
= p*PAGE_SIZE; for(size_t i = 0; i < nb_items_per_page; i++) {
         add_existing_item(s, base_idx, &data[current], callback);
         base_idx++;
         current += s->item_size;
      }
   }
}

#define GRANULARITY_REBUILD (2*1024*1024) // We rebuild 2MB by 2MB
void rebuild_index(int slab_worker_id, struct slab *s, struct slab_callback
*callback) { char *cached_data = aligned_alloc(PAGE_SIZE, GRANULARITY_REBUILD);

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
         perr("pread failed! Read %d instead of %lu (offset %lu)\n", r,
end-start, start); process_existing_chunk(slab_worker_id, s, 1, 0, cached_data,
start, end-start, callback); start = end;

   }
   free(cached_data);
   s->last_item++;
   rebuild_free_list(s);
}
*/

/*
 * Create a slab: a file that only contains items of a given size.
 * @callback is a callback that will be called on all previously existing items
 * of the slab if it is restored from disk.
 */
struct slab *create_slab(struct slab_context *ctx, int slab_worker_id,
                         uint64_t key) {
  struct stat sb;
  char path[512];
  struct slab *s = calloc(1, sizeof(*s));
  int cur_seq = -1;

  size_t disk = slab_worker_id / (get_nb_workers() / get_nb_disks());
  sprintf(path, PATH, disk, slab_worker_id, 0LU, key);
  s->fd = open(path, O_RDWR | O_CREAT | O_DIRECT, 0777);
  if (s->fd == -1) perr("Cannot allocate slab %s", path);

  fstat(s->fd, &sb);
  s->size_on_disk = sb.st_size;

  if (s->size_on_disk < 16384 * PAGE_SIZE) {
    fallocate(s->fd, 0, 0, 16384 * PAGE_SIZE);
    s->size_on_disk = 16384 * PAGE_SIZE;
  }

  cur_seq = __sync_add_and_fetch(&create_sequence, 1);
  size_t nb_items_per_page = PAGE_SIZE / 1024;
  s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
  // TODO::JS::구조체 수정
  s->nb_items = 0;
  s->item_size = 1024;
  s->last_item = 0;
  s->ctx = ctx;

  s->min = -1;
  s->max = 0;
  s->key = key;
  s->seq = cur_seq;
  s->full = 0;
  s->update_ref = 0;
  s->read_ref = 0;
  s->hot_pages = 0;

  INIT_LOCK(&s->tree_lock, NULL);
  s->hot_bit = calloc((s->size_on_disk / PAGE_SIZE / 8), sizeof(char));

  return s;
}

/*
 * Double the size of a slab on disk
 */
struct slab *resize_slab(struct slab *s) {
  if (s->size_on_disk < 10000000000LU) {
    s->size_on_disk *= 2;
    if (fallocate(s->fd, 0, 0, s->size_on_disk))
      perr("Cannot resize slab (item size %lu) new size %lu\n", s->item_size,
           s->size_on_disk);
    s->nb_max_items *= 2;
  } else {
    size_t nb_items_per_page = PAGE_SIZE / s->item_size;
    s->size_on_disk += 10000000000LU;
    if (fallocate(s->fd, 0, 0, s->size_on_disk))
      perr("Cannot resize slab (item size %lu) new size %lu\n", s->item_size,
           s->size_on_disk);
    s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
  }
  return s;
}

static int show_rbtree(void *key, void *value) {
  printf("key: %lu\n", key);
  return 0;
}

void create_root_slab() {
  centree_init();
  struct slab_callback *cb = malloc(sizeof(*cb));
  cb->cb = NULL;
  cb->slab = create_slab(NULL, 0, 0);
  tnt_subtree_add(cb->slab, tnt_subtree_create(), filter_create(200000), 0);
  free(cb);
}

struct slab *close_and_create_slab(struct slab *s) {
  uint64_t new_key;

  // TODO::JS:: add_in_tree 쪽으로 옮겨야함
  R_LOCK(&s->tree_lock);
  new_key = (s->min + s->max) / 2;
  R_UNLOCK(&s->tree_lock);

  // if(s->key != new_key)
  tnt_subtree_update_key(s->key, new_key);
  printf("NNN: %lu, %lu // %lu-%lu\n", (uint64_t)s->key, (uint64_t)new_key,
         s->min, s->max);

  W_LOCK(&s->tree_lock);
  s->key = new_key;
  W_UNLOCK(&s->tree_lock);

  tnt_subtree_add(create_slab(NULL, 0, new_key - 1), tnt_subtree_create(),
                  (void *)filter_create(200000), new_key - 1);
  tnt_subtree_add(create_slab(NULL, 0, new_key + 1), tnt_subtree_create(),
                  (void *)filter_create(200000), new_key + 1);

  return s;
}

/*
 * Synchronous read item
 */
void *read_item(struct slab *s, size_t idx) {
  size_t page_num = item_page_num(s, idx);
  char *disk_data = safe_pread(s->fd, page_num * PAGE_SIZE);
  return &disk_data[item_in_page_offset(s, idx)];
}

/*
 * Asynchronous read
 * - read_item_async creates a callback for the ioengine and queues the io
 * request
 * - read_item_async_cb is called when io is completed (might be synchronous if
 * page is cached) If nothing happen it is because do_io is not called.
 */
void read_item_async_cb(struct slab_callback *callback) {
  char *disk_page = callback->lru_entry->page;
  off_t in_page_offset =
      item_in_page_offset(callback->slab, callback->slab_idx);
  struct slab *s = callback->slab;
  uint64_t prev;

  prev = __sync_fetch_and_sub(&s->read_ref, 1);

  R_LOCK(&s->tree_lock);
  if (s->min == -1 && prev == 1) {
    char path[128], spath[128];
    int len;
    sprintf(path, "/proc/self/fd/%d", s->fd);
    if ((len = readlink(path, spath, 512)) < 0) die("READLINK\n");
    spath[len] = 0;
    close(s->fd);
    unlink(spath);
    printf("REMOVED FILE\n");
  }
  R_UNLOCK(&s->tree_lock);

  if (callback->cb) callback->cb(callback, &disk_page[in_page_offset]);
}

void read_item_async(struct slab_callback *callback) {
  callback->io_cb = read_item_async_cb;
  read_page_async(callback);
}

void scan_item_async(struct slab_callback *callback) {
  callback->io_cb = read_item_async_cb;
  // read_file_async(callback);
}

/*
 * Asynchronous update item:
 * - First read the page where the item is staying
 * - Once the page is in page cache, write it
 * - Then send the order to flush it.
 */
void update_item_async_cb2(struct slab_callback *callback) {
  char *disk_page = callback->lru_entry->page;
  off_t in_page_offset =
      item_in_page_offset(callback->slab, callback->slab_idx);
  unsigned char cbcb = callback->cb_cb ? 1 : 0;

  struct item_metadata *meta = (struct item_metadata *)callback->item;
  char *item_key = &callback->item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;

  void *item = &disk_page[in_page_offset];
  struct item_metadata *meta2 = (struct item_metadata *)item;
  char *item_key2 = &item[sizeof(*meta2)];
  uint64_t key2 = *(uint64_t *)item_key2;
  if (key != key2)
    die("DIFF key1: %lu, key2: %lu, key/idx: %lu/%lu\n", key, key2,
        callback->slab->seq, callback->slab_idx);

  if (callback->cb) callback->cb(callback, &disk_page[in_page_offset]);
  if (cbcb) callback->cb_cb(callback, &disk_page[in_page_offset]);
}

void update_item_async_cb1(struct slab_callback *callback) {
  char *disk_page = callback->lru_entry->page;

  struct slab *s = callback->slab;
  size_t idx = callback->slab_idx;
  void *item = callback->item;
  struct item_metadata *meta = item;
  off_t offset_in_page = item_in_page_offset(s, idx);
  struct item_metadata *old_meta = (void *)(&disk_page[offset_in_page]);
  struct slab_context *ctx = callback->ctx;

  meta->rdt = get_rdt(ctx);
  if (meta->key_size == -1)
    memcpy(&disk_page[offset_in_page], meta, sizeof(*meta));
  else if (get_item_size(item) > s->item_size)
    die("Trying to write an item that is too big for its slab\n");
  else
    memcpy(&disk_page[offset_in_page], item, get_item_size(item));

#ifdef DEBUG
    /*
    char *item_key = &callback->item[sizeof(*meta)];
    uint64_t key = *(uint64_t*)item_key;
    off_t in_page_offset = item_in_page_offset(callback->slab,
    callback->slab_idx);

    void *item2 = &disk_page[in_page_offset];
    struct item_metadata *meta2 = (struct item_metadata *)item;
    char *item_key2 = &item2[sizeof(*meta)];
    uint64_t key2 = *(uint64_t*)item_key2;
    if (key != key2)
     printf("DIFFFF key1: %lu, key2: %lu, %lu/%lu\n", key, key2, s->seq, idx);
    */
#endif

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
 * If a page with an empty spot exists, we have to scan it to find a suitable
 * spot.
 */
void add_item_async_cb1(struct slab_callback *callback) {
  struct slab *s = callback->slab;
  struct lru *lru_entry = callback->lru_entry;

  R_LOCK(&s->tree_lock);
  if (lru_entry == NULL) {  // no free page, append
    // TODO::JS::일단 아이템 사이즈 고정시킴
    if (callback->slab_idx + 1 == s->nb_max_items &&
        callback->slab_idx != callback->fsst_idx) {
      assert(s->full == 1);
      assert(s->last_item == s->nb_max_items);
      // s->imm = 1;
      R_UNLOCK(&s->tree_lock);
      s = close_and_create_slab(s);
      R_LOCK(&s->tree_lock);
    }
  } else {  // reuse a free spot. Don't forget to add the linked tombstone in
            // the freelist.
    die("LRU entry != NULL\n");
  }

  R_UNLOCK(&s->tree_lock);

  kv_add_async_no_lookup(callback, callback->slab, callback->slab_idx);
}

void add_item_async(struct slab_callback *callback) {
  if (callback->cb != add_in_tree) {
    if (callback->cb == NULL) {
      printf("testestest WHAT?\n");
      callback->cb = add_in_tree;
    } else {
      callback->cb_cb = callback->cb;  // computes_stat
      callback->cb = add_in_tree;
    }
  }
  callback->io_cb = add_item_async_cb1;
  callback->lru_entry = NULL;
  callback->io_cb(callback);
}

void add_in_tree_for_update(struct slab_callback *cb, void *item) {
  struct slab *s = cb->slab;
  struct slab *old_s = cb->fsst_slab;
  unsigned char enqueue = 0;
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  int removed = 0, alrdy = 0;

  R_LOCK(&old_s->tree_lock);
  if (old_s->min != -1)
    removed = tnt_index_invalid_utree(old_s->tree, cb->item);
  R_UNLOCK(&old_s->tree_lock);

  if (!removed) {
    // 동시에 수정하는 누군가가 자기가 지워야 하는 것을 이미 지운 것.
    //  그 친구가 새로 추가한 것을 지워야함
    removed = tnt_index_invalid(cb->item);
    if (!removed)
      printf("UPDATE NULL %lu seq/idx %lu/%lu fsst %lu/%lu\n", key,
             cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq, cb->fsst_idx);
  } else {
    __sync_fetch_and_sub(&old_s->nb_items, 1);
  }

  W_LOCK(&s->tree_lock);
  alrdy = tnt_index_delete(cb->slab->tree, item);
  tnt_index_add(cb, item);

  if (!alrdy) {
    __sync_fetch_and_add(&nb_totals, 1);
    // s->nb_items++;
    if (filter_add((filter_t *)s->filter, (unsigned char *)&key) == 0) {
      printf("Fail adding to filter %p %lu seq/idx %lu/%lu fsst %lu/%lu\n",
             s->filter, key, cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq,
             cb->fsst_idx);
    } else if (!filter_contain(s->filter, (unsigned char *)&key)) {
      printf("FIFIFIFIF UPUP\n");
    }
  }

  if (key < s->min) s->min = key;
  if (key > s->max) s->max = key;

  s->update_ref--;

  if ((s->max - s->min) > (s->nb_max_items * 10) && s->full && !s->update_ref &&
      !((centree_node)s->tree_node)->removed) {
    enqueue = 1;
    ((centree_node)s->tree_node)->removed = 1;
  }

  if (s->min == -1 && s->update_ref == 0) {
    char path[128], spath[128];
    int len;
    sprintf(path, "/proc/self/fd/%d", s->fd);
    if ((len = readlink(path, spath, 512)) < 0) die("READLINK\n");
    spath[len] = 0;
    close(s->fd);
    unlink(spath);
    printf("REMOVED FILE\n");
  }

  W_UNLOCK(&s->tree_lock);

  if (enqueue) bgq_enqueue(FSST, s->tree_node);

  if (cb->cb_cb == add_in_tree_for_update) {
    free(cb->item);
    free(cb);
  }
}

void remove_and_add_item_async(struct slab_callback *callback) {
  callback->io_cb = add_item_async_cb1;
  if (callback->slab_idx != -1) {
    if (!callback->cb)  // making fsst by using cb_cb
      callback->cb = add_in_tree_for_update;
    else {
      callback->cb_cb = callback->cb;  // computes_stat
      callback->cb = add_in_tree_for_update;
    }
  } else {
    // FSST과정에서는 불가능 해야 하는(호출되면 안되는) 상황
    //  In-place update라 트리 수정이 불필요.
    //  그냥 원래 위치에 데이터만 업데이트
    callback->slab_idx = callback->fsst_idx;
  }
  callback->lru_entry = NULL;
  callback->io_cb(callback);
}
