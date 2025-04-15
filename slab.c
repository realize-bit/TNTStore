#include "headers.h"
#include "utils.h"
#include "items.h"
#include "slab.h"
#include "ioengine.h"
#include "pagecache.h"
#include "slabworker.h"
#include <errno.h>

extern int print;
extern int load;
extern uint64_t nb_totals;
extern int rc_thr;

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
 * Create a slab: a file that only contains items of a given size.
 * @callback is a callback that will be called on all previously existing items
 * of the slab if it is restored from disk.
 */
struct slab *create_slab(struct slab_context *ctx, uint64_t level,
                         uint64_t key, int rebuild, char *name) {
  struct stat sb;
  char path[512];
  struct slab *s = calloc(1, sizeof(*s));
  uint64_t cur_seq = -1;
  int flag = O_RDWR | O_DIRECT;

  // not rebuild
  if (!rebuild)
    flag = flag | O_CREAT;

  //size_t disk = slab_worker_id / (get_nb_workers() / get_nb_disks());
  cur_seq = __sync_add_and_fetch(&create_sequence, 1);
  if (rebuild)
    sprintf(path, "/scratch0/kvell/%s", name);
  else
    sprintf(path, PATH, 0LU, cur_seq, level, key);

  s->fd = open(path, flag, 0777);

  if (s->fd == -1) {
      perr("Cannot allocate slab %s", path);
  } 

  fstat(s->fd, &sb);
  s->size_on_disk = sb.st_size;
  if (!rebuild && s->size_on_disk < MAX_FILE_SIZE) {
    fallocate(s->fd, 0, 0, MAX_FILE_SIZE);
    s->size_on_disk = MAX_FILE_SIZE;
  }


  size_t nb_items_per_page = PAGE_SIZE / KV_SIZE;
  s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
  // TODO::JS::구조체 수정
  s->nb_items = 0;
  s->item_size = KV_SIZE;
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

   if (load)
    s->batched_callbacks = calloc(NUM_LOAD_BATCH, sizeof(struct slab_callback *));
   else
    s->batched_callbacks = NULL;
   s->nb_batched = 0;

  INIT_LOCK(&s->tree_lock, NULL);

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

int rebuild_slabs(int filenum, struct dirent **file_list) {
  int ret = 0;
  for (int i = 0; i < filenum; i++) {
    if (file_list[i]->d_type == DT_REG) {  // If it's a regular file
      char *filename = file_list[i]->d_name;
      uint64_t level, key, old_key, seq;
      int keynum;
      if ((keynum = sscanf(filename, "slab-%lu-%lu-%lu-%lu", &seq, &level, &old_key, &key)) >= 2) {
        struct slab *s;
        s = create_slab(NULL, level, old_key, 1, filename);
        // Process the slab file
        //process_slab_file(level, key);
#if WITH_FILTER
        tnt_subtree_add(s, tnt_subtree_create(), 
                  (void *)filter_create(200000), old_key);
#else
        tnt_subtree_add(s, tnt_subtree_create(), 
                  NULL, old_key);
#endif
        if (keynum == 4) {
          tnt_subtree_update_key(s->key, key);
          s->key = key;
        }
        ret++;
      } else {
        free(file_list[i]);
        file_list[i] = NULL;
      }
    }
    else {
      free(file_list[i]);
      file_list[i] = NULL;
    }
    //free(file_list[i]);
  }
  return ret;
}

static int root_exists() {
    const char *dir_path = "/scratch0/kvell";
    const char *prefix = "slab-1-0-";
    
    DIR *dir = opendir(dir_path);
    if (dir == NULL) {
        perror("Unable to open directory");
        return -1;  // 디렉터리를 열 수 없는 경우
    }
    
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        // 파일 이름이 "slab-0-"로 시작하는지 확인
        if (strncmp(entry->d_name, prefix, strlen(prefix)) == 0) {
            closedir(dir);
            return 1;  // 해당 파일을 찾은 경우
        }
    }
    
    closedir(dir);
    return 0;  // 해당 파일을 찾지 못한 경우
}

int create_root_slab() {
  struct slab *s;
  centree_init();

  // need to rebuild
  if (root_exists())
    return 0;

  s = create_slab(NULL, 0, 0, 0, NULL);

#if WITH_FILTER
  tnt_subtree_add(s, tnt_subtree_create(), filter_create(200000), 0);
#else
  tnt_subtree_add(s, tnt_subtree_create(), NULL, 0);
#endif
  return 1;
}

struct slab *close_and_create_slab(struct slab *s) {
  uint64_t new_key;
  uint64_t new_level;

  // TODO::JS:: add_in_tree 쪽으로 옮겨야함
  R_LOCK(&s->tree_lock);
  //new_key = (s->min + s->max) / 2;
  new_key = s->min + (s->max - s->min) / 2;
  new_level = tnt_get_centree_level(s->centree_node)+1;
  R_UNLOCK(&s->tree_lock);

  // if(s->key != new_key)
  tnt_subtree_update_key(s->key, new_key);
  printf("NNN: %lu, %lu // %lu-%lu\n", (uint64_t)s->key, (uint64_t)new_key,
         s->min, s->max);

  W_LOCK(&s->tree_lock);
  s->key = new_key;
  W_UNLOCK(&s->tree_lock);
  char path[128], spath[128];
  int len;
  sprintf(path, "/proc/self/fd/%d", s->fd);
  if ((len = readlink(path, spath, 512)) < 0) {
    perr("Can't find file\n");
  }
  spath[len] = 0;
  strncpy(path, spath, len);
  snprintf(path + len, 128 - len, "-%lu", s->key);
  rename(spath, path);

#if WITH_FILTER
  tnt_subtree_add(create_slab(NULL, new_level, new_key - 1, 0, NULL), tnt_subtree_create(),
                  (void *)filter_create(200000), new_key - 1);
  tnt_subtree_add(create_slab(NULL, new_level, new_key + 1, 0, NULL), tnt_subtree_create(),
                  (void *)filter_create(200000), new_key + 1);
#else
  tnt_subtree_add(create_slab(NULL, new_level, new_key - 1, 0, NULL), tnt_subtree_create(),
                  NULL, new_key - 1);
  tnt_subtree_add(create_slab(NULL, new_level, new_key + 1, 0, NULL), tnt_subtree_create(),
                  NULL, new_key + 1);
  //__sync_fetch_and_add(&t->nb_elements, 2);
  add_number_of_subtree(2);
#endif

  //tnt_print();
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
  uint64_t cur;

  cur = __sync_sub_and_fetch(&s->read_ref, 1);

  R_LOCK(&s->tree_lock);
  if (s->min == -1 && 
    !__sync_fetch_and_or(&s->update_ref, 0)
    && cur == 0) {
    char path[128], spath[128];
    int len;
    sprintf(path, "/proc/self/fd/%d", s->fd);
    if ((len = readlink(path, spath, 512)) < 0) {
	    // 아마 sub 하고 R_LOCK 기다리는 동안
	    // check and remove 쪽에서 W_LOCK 잡고
	    // read_ref == 0 테스트 통과해서
	    // 지우는 듯
	    printf("already removed\n");
	    goto skip;
    }
    spath[len] = 0;
    close(s->fd);
    /*strncpy(path, spath, len);*/
    /*snprintf(path + len, 128 - len, "-%lu", s->key);*/
    truncate(spath, 0);
    /*rename(spath, path);*/
    //unlink(spath);
    // printf("REMOVED FILE\n");
  }
skip:
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

  if(callback->cb != add_in_tree_for_update 
    && callback->cb != add_in_tree)
    __sync_fetch_and_sub(&callback->slab->update_ref, 1);

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
  struct slab_context *ctx = callback->ctx;

  meta->rdt = get_rdt(ctx);
  if (meta->key_size == -1)
    memcpy(&disk_page[offset_in_page], meta, sizeof(*meta));
  else if (get_item_size(item) > s->item_size)
    die("Trying to write an item that is too big for its slab\n");
  else
    memcpy(&disk_page[offset_in_page], item, get_item_size(item));

#if DEBUG
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
  if (load) {
    W_LOCK(&s->tree_lock);
    s->batched_callbacks[s->nb_batched++] = callback;
    if (s->nb_batched == NUM_LOAD_BATCH) {
      struct slab_callback *batched_callbacks_copy[NUM_LOAD_BATCH];
      memcpy(batched_callbacks_copy, s->batched_callbacks, NUM_LOAD_BATCH * sizeof(struct slab_callback *));
      s->nb_batched = 0;
      if (s->full == 1)
        free(s->batched_callbacks);
      W_UNLOCK(&s->tree_lock);

      for (int i=0; i<NUM_LOAD_BATCH; i++) {
        struct slab_callback *cb = batched_callbacks_copy[i];

        if (cb == NULL)
          continue;

        kv_add_async_no_lookup(cb, cb->slab, cb->slab_idx);
      }
      /*printf("FLUSH BATCh: %lu\n", s->seq);*/
    } else {
      W_UNLOCK(&s->tree_lock);
    }
    return;
  }

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
  uint64_t old_idx = cb->fsst_idx;
  unsigned char enqueue = 0;
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  int removed = 0, alrdy = 0;
  uint64_t cur;
  index_entry_t *e = NULL;

  // 앞에서 걸러져서 in-place는 애초에 여기 안온다
  //if (old_s == s) {
  //  goto skip;
  //}

  //while(!removed) {

  //  R_LOCK(&old_s->tree_lock);
  //  if (old_s == s) {
  //    R_UNLOCK(&old_s->tree_lock);
  //    // 이미 최신게 들어있으므로 업데이트 안함
  //    // 두 케이스 다 원래 두 자리 잡아놨는데 하나만 유효하므로 하나 뺌
  //    if (old_idx > cb->slab_idx) {
  //      __sync_fetch_and_sub(&s->nb_items, 1);
  //      //printf("UPCASE: 1\n");
  //      goto skip;
  //    } else if (old_idx == cb->slab_idx) {
  //      goto skip;
  //    }
  //    // s에 delete 후 add
  //    //printf("UPCASE: 2\n");
  //    break;
  //  } else if (old_s->seq > s->seq) {
  //    R_UNLOCK(&old_s->tree_lock);
  //    // 이미 최신게 들어있으므로 업데이트 안함
  //    //printf("UPCASE: 3\n");
  //    __sync_fetch_and_sub(&s->nb_items, 1);
  //    goto skip;
  //  }
  //
  //  if (old_s->min != -1)
  //    removed = tnt_index_invalid_utree(old_s->subtree, cb->item);
  //  R_UNLOCK(&old_s->tree_lock);

  //  if (!removed) {
  //    do {
  //      e = tnt_index_lookup(cb->item);
  //    } while(e->slab == old_s);

  //    old_s = e->slab;
  //    old_idx = e->slab_idx;
  //    // 동시에 수정하는 누군가가 자기가 지워야 하는 것을 이미 지운 것.
  //    //  그 친구가 새로 추가한 것을 지워야함
  //    //removed = tnt_index_invalid(cb->item);
  //    //if (!removed)
  //    //  printf("UPDATE NULL %lu seq/idx %lu/%lu fsst %lu/%lu\n", key,
  //    //         cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq, cb->fsst_idx);
  //    //printf("UPCASE: RETRY\n");
  //  } else {
  //    //printf("UPCASE: 5\n");
  //    __sync_fetch_and_sub(&old_s->nb_items, 1);
  //  }
  //}

  W_LOCK(&s->tree_lock);
    // CASE 2에서 여러 쓰레드가 여기 도달 가능.
    // 두 쓰레드들 중 가장 최신의 애가 먼저 lock 잡고 추가했다면
    // 그 다음 쓰레드는 스킵해야함.
  e = tnt_index_lookup_utree(s->subtree, item);
  if (e) {
    if (e->slab_idx > cb->slab_idx) {
      __sync_fetch_and_sub(&s->nb_items, 1);
      // printf("UPCASE: Edge 1\n");
      //printf("UPCASE: Final Skip\n");
      W_UNLOCK(&s->tree_lock);
      goto skip;
    } 
    // printf("UPCASE: Edge 2\n");
    alrdy = tnt_index_delete(s->subtree, item);
  }

  tnt_index_add(cb, item);

  if (!alrdy) {
    __sync_fetch_and_add(&nb_totals, 1);
    // s->nb_items++;
#if WITH_FILTER
    if (filter_add((filter_t *)s->filter, (unsigned char *)&key) == 0) {
      printf("Fail adding to filter %p %lu seq/idx %lu/%lu fsst %lu/%lu\n",
             s->filter, key, cb->slab->seq, cb->slab_idx, cb->fsst_slab->seq,
             cb->fsst_idx);
    } else if (!filter_contain(s->filter, (unsigned char *)&key)) {
      printf("FIFIFIFIF UPUP\n");
    }
#endif
  } else {
    __sync_fetch_and_sub(&s->nb_items, 1);
    W_UNLOCK(&s->tree_lock);
    goto skip;
  }

  if (key < s->min) s->min = key;
  if (key > s->max) s->max = key;

  W_UNLOCK(&s->tree_lock);

  R_LOCK(&old_s->tree_lock);
  if (old_s->min == -1)
    removed = 1;
  else {
    removed = tnt_index_invalid_utree(old_s->subtree, cb->item);
    if (removed)
      __sync_fetch_and_sub(&old_s->nb_items, 1);
  }
  R_UNLOCK(&old_s->tree_lock);


  if (!removed) {
      // printf("UPCASE: Edge 3\n");
      printf("UPCASE: Edge\n");
  }

skip:
  __sync_fetch_and_sub(&s->update_ref, 1);

  R_LOCK(&s->tree_lock);

  if (s->full && s->seq < rc_thr &&
    !__sync_fetch_and_or(&s->update_ref, 0) &&
      !((centree_node)s->centree_node)->removed) {
    enqueue = 1;
    ((centree_node)s->centree_node)->removed = 1;
  }

  cur = __sync_fetch_and_add(&s->read_ref, 0);

  if (s->min == -1 && 
    __sync_fetch_and_or(&s->update_ref, 0) == 0 
    && cur == 0) {
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
    //printf("REMOVED FILE\n");
  }
  R_UNLOCK(&s->tree_lock);


#if WITH_RC
  if (enqueue) bgq_enqueue(FSST, s->centree_node);
#endif

  if (cb->cb_cb == add_in_tree_for_update) {
    free(cb->item);
    free(cb);
  }
}

void remove_and_add_item_async(struct slab_callback *callback) {
  callback->io_cb = add_item_async_cb1;
  if (callback->slab_idx != -1) {
    if (!callback->cb){  // making fsst by using cb_cb
      callback->cb = add_in_tree_for_update;
    }
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
