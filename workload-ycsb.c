/*
 * YCSB Workload
 */

#include "headers.h"
#include "workload-common.h"

static char *_create_unique_item_ycsb(uint64_t uid) {
   size_t item_size = 1024;
   //size_t item_size = sizeof(struct item_metadata) + 2*sizeof(size_t);
   return create_unique_item(item_size, uid);
}

static char *create_unique_item_ycsb(uint64_t uid, uint64_t max_uid) {
   return _create_unique_item_ycsb(uid);
}

/* Is the current request a get or a put? */
static int random_get_put(int test) {
   long random = uniform_next() % 100;
   switch(test) {
      case 0: // A
         return random >= 50;
      case 1: // B
         return random >= 95;
      case 2: // C
         return 0;
      case 3: // E
         return random >= 95;
   }
   die("Not a valid test\n");
}

/* YCSB A (or D), B, C */
static void _launch_ycsb(int test, int nb_requests, int zipfian) {
   declare_periodic_count;
   for(size_t i = 0; i < nb_requests; i++) {
      struct slab_callback *cb = bench_cb();
      if(zipfian)
         cb->item = _create_unique_item_ycsb(zipf_next());
      else
         cb->item = _create_unique_item_ycsb(uniform_next());
      if(random_get_put(test)) { // In these tests we update with a given probability
         kv_update_async(cb);
      } else { // or we read
         kv_read_async(cb);
      }
      periodic_count(1000, "YCSB Load Injector (%lu%%)", i*100LU/nb_requests);

      if (i==(nb_requests/2)) 
         cond_check_and_gc_wakeup();
   }
}

static int compare (const void *a, const void *b)
{
  index_entry_t *x = (index_entry_t *)a,
       *y = (index_entry_t *)b;
    
  /* if ->num not equal, sort by ->num */
  if (x->slab->fd != y->slab->fd)
    return (x->slab->fd > y->slab->fd) - (x->slab->fd < y->slab->fd);
    
  /* otherwise, sort by val */
  return (x->slab_idx > y->slab_idx) - (x->slab_idx < y->slab_idx);
}
/* YCSB E */
static void _launch_ycsb_e(int test, int nb_requests, int zipfian) {
   declare_periodic_count;
   random_gen_t rand_next = zipfian?zipf_next:uniform_next;
   /*
   for(size_t i = 0; i < nb_requests; i++) {
      if(random_get_put(test)) { // In this test we update with a given probability
         struct slab_callback *cb = bench_cb();
         cb->item = _create_unique_item_ycsb(rand_next());
         kv_update_async(cb);
      } else {  // or we scan
         char *item = _create_unique_item_ycsb(rand_next());
         tree_scan_res_t scan_res = kv_init_scan(item, uniform_next()%99+1);
         free(item);
         for(size_t j = 0; j < scan_res.nb_entries; j++) {
            struct slab_callback *cb = bench_cb();
            cb->item = _create_unique_item_ycsb(scan_res.hashes[j]);
            kv_read_async_no_lookup(cb, scan_res.entries[j].slab, scan_res.entries[j].slab_idx);
         }
         free(scan_res.hashes);
         free(scan_res.entries);
      }
      periodic_count(1000, "YCSB Load Injector (scans) (%lu%%)", i*100LU/nb_requests);
   }
  */

  for(size_t i = 0; i < nb_requests; i++) {
      if(random_get_put(test)) { // In this test we update with a given probability
         struct slab_callback *cb = bench_cb();
         cb->item = _create_unique_item_ycsb(rand_next());
         kv_update_async(cb);
      } else {  // or we scan
         // 2. key와 size를 가지고 트리에서 slab과 idx들을 가져온다.
         char *item = _create_unique_item_ycsb(rand_next());
         tree_scan_res_t scan_res = tnt_scan(item, uniform_next()%99+1);
         free(item);
         // 2. slab과 idx를 기준으로 정렬한다.
         // printf("BEFORE: %ld\n", scan_res.nb_entries);
         // for(int i=0; i < scan_res.nb_entries; i++) {
         //  printf("(%d, %d, %lu, %lu) ", i, scan_res.entries[i].slab->fd, scan_res.entries[i].slab_idx, scan_res.hashes[i]);
         // }
         qsort(scan_res.entries, scan_res.nb_entries, sizeof(struct index_entry), compare);
         // printf("AFTER: %ld\n", scan_res.nb_entries);
         // for(int i=0; i < scan_res.nb_entries; i++) {
         //  printf("(%d, %d, %lu) ", i, scan_res.entries[i].slab->fd, scan_res.entries[i].slab_idx);
         //
         // }
         // printf("\n");

         // 3. 최대한 연속된 I/O가 되도록 쪼개어 보낸다. 
         // (128KB 안에 들어오면 연속된 I/O라고 판단하고 이어 붙인다.)
         for(size_t j = 0; j < scan_res.nb_entries;) {
            struct slab_callback *cb = bench_cb();
            uint64_t count = 1;
            cb->item = _create_unique_item_ycsb(scan_res.hashes[j]);
            /*
            for(size_t k = j; k < scan_res.nb_entries-1; k++) {
              if (scan_res.entries[k].slab != scan_res.entries[k+1].slab
              || ((scan_res.entries[k].slab_idx + 4096)
                  < scan_res.entries[k+1].slab_idx)) {
                break;
              }
              count++;
            }
            */
            kv_read_async_no_lookup(cb, scan_res.entries[j].slab, scan_res.entries[j].slab_idx, count);
            j += count;
         }
         free(scan_res.hashes);
         free(scan_res.entries);
      }
      periodic_count(1000, "YCSB Load Injector (scans) (%lu%%)", i*100LU/nb_requests);
   }
}

/* Generic interface */
static void launch_ycsb(struct workload *w, bench_t b) {
   switch(b) {
      case ycsb_a_uniform:
         return _launch_ycsb(0, w->nb_requests_per_thread, 0);
      case ycsb_b_uniform:
         return _launch_ycsb(1, w->nb_requests_per_thread, 0);
      case ycsb_c_uniform:
         return _launch_ycsb(2, w->nb_requests_per_thread, 0);
      case ycsb_e_uniform:
         return _launch_ycsb_e(3, w->nb_requests_per_thread, 0);
      case ycsb_a_zipfian:
         return _launch_ycsb(0, w->nb_requests_per_thread, 1);
      case ycsb_b_zipfian:
         return _launch_ycsb(1, w->nb_requests_per_thread, 1);
      case ycsb_c_zipfian:
         return _launch_ycsb(2, w->nb_requests_per_thread, 1);
      case ycsb_e_zipfian:
         return _launch_ycsb_e(3, w->nb_requests_per_thread, 1);
      default:
         die("Unsupported workload\n");
   }
}

/* Pretty printing */
static const char *name_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
         return "YCSB A - Uniform";
      case ycsb_b_uniform:
         return "YCSB B - Uniform";
      case ycsb_c_uniform:
         return "YCSB C - Uniform";
      case ycsb_e_uniform:
         return "YCSB E - Uniform";
      case ycsb_a_zipfian:
         return "YCSB A - Zipf";
      case ycsb_b_zipfian:
         return "YCSB B - Zipf";
      case ycsb_c_zipfian:
         return "YCSB C - Zipf";
      case ycsb_e_zipfian:
         return "YCSB E - Zipf";
      default:
         return "??";
   }
}

static int handles_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
      case ycsb_b_uniform:
      case ycsb_c_uniform:
      case ycsb_e_uniform:
      case ycsb_a_zipfian:
      case ycsb_b_zipfian:
      case ycsb_c_zipfian:
      case ycsb_e_zipfian:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_ycsb(void) {
   return "YCSB";
}

struct workload_api YCSB = {
   .handles = handles_ycsb,
   .launch = launch_ycsb,
   .api_name = api_name_ycsb,
   .name = name_ycsb,
   .create_unique_item = create_unique_item_ycsb,
};
