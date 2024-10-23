#include "headers.h"
#include "workload-common.h"
#include "db_bench.h"
#include <math.h>

typedef struct {
  int64_t keyrange_start;
  int64_t keyrange_access;
  int64_t keyrange_keys;
} KeyrangeUnit;

static void *gen_exp;
//static int64_t keyrange_rand_max_ = 0;
//static int64_t keyrange_size_ = 0;
//static int64_t keyrange_num_ = 0;
//static KeyrangeUnit *keyrange_set_ = NULL;

static void init_dbbench(struct workload *w, bench_t b) {
  switch (b) {
    case dbbench_prefix_random:
    case dbbench_prefix_dist:
      gen_exp = init_exp_prefix(w->nb_items_in_db, 30, 14.18, -2.917, 0.0164,
                                -0.08082);
      init_rand();
      return;
    case dbbench_all_random:
    case dbbench_all_dist:
    default:
      return;
  }
}

static char *_create_unique_item_dbbench(uint64_t uid) {
  size_t item_size = 1024;
  // size_t item_size = sizeof(struct item_metadata) + 2*sizeof(size_t);
  return create_unique_item(item_size, uid);
}

static char *create_unique_item_dbbench(uint64_t uid, uint64_t max_uid) {
  return _create_unique_item_dbbench(uid);
}

/* Is the current request a get or a put? */
/*
static int random_get_put(int test) {
  long random = uniform_next() % 100;
  switch (test) {
    case 0:  // A
      return random >= 50;
    case 1:  // B
      return random >= 95;
    case 2:  // C
      return 0;
    case 3:  // E
      return random >= 95;
  }
  die("Not a valid test\n");
}
*/

/* YCSB A (or D), B, C */
static void _launch_dbbench(int total, int nb_requests, double dist_a,
                            double dist_b, int range_num, double range_a,
                            double range_b, double range_c, double range_d) {
  declare_periodic_count;
  unsigned char prefix_model = 0, random_model = 0;
  //void *rand_var;

  if (range_a != 0.0 || range_b != 0.0 || range_c != 0.0 || range_d != 0.0) {
    prefix_model = 1;
  }

  if (dist_a == 0 || dist_b == 0) random_model = 1;

  // create_rand(rand_var);
  for (size_t i = 0; i < nb_requests; i++) {
    uint64_t ini_rand, rand_v, key_rand, key_seed;
    double u;
    struct slab_callback *cb = bench_cb();

    // ini_rand = GetRandomKey(rand_var, total);
    ini_rand = rand_next() % total;
    rand_v = ini_rand % total;
    u = (double)(rand_v) / total;
    if (prefix_model) {
      key_rand = prefix_get_key(gen_exp, ini_rand, dist_a, dist_b);
    } else if (random_model) {
      key_rand = ini_rand;
    } else {
      key_seed = (int64_t)ceil(pow((u / dist_a), (1 / dist_b)));
      key_rand = (int64_t)(rand_next_seed(key_seed)) % total;
    }
    // printf("KEY %ld\n", key_rand);

    cb->item = _create_unique_item_dbbench(key_rand);

    // if(random_get_put(test)) { // In these tests we update with a given
    // probability
    //    kv_update_async(cb);
    // } else { // or we read
    //    kv_read_async(cb);
    // }
    kv_read_async(cb);
    periodic_count(1000, "DB_BENCH Load Injector (%lu%%)",
                   i * 100LU / nb_requests);
  }
}

/* Generic interface */
static void launch_dbbench(struct workload *w, bench_t b) {
  switch (b) {
    case dbbench_all_random:
      return _launch_dbbench(w->nb_items_in_db, w->nb_requests_per_thread, 0, 0,
                             1, 0, 0, 0, 0);
    case dbbench_all_dist:
      return _launch_dbbench(w->nb_items_in_db, w->nb_requests_per_thread,
                             0.002312, 0.3467, 1, 0, 0, 0, 0);
    case dbbench_prefix_random:
      return _launch_dbbench(w->nb_items_in_db, w->nb_requests_per_thread, 0, 0,
                             30, 14.18, -2.917, 0.0164, -0.08082);
    case dbbench_prefix_dist:
      return _launch_dbbench(w->nb_items_in_db, w->nb_requests_per_thread,
                             0.002312, 0.3467, 30, 14.18, -2.917, 0.0164,
                             -0.08082);
    default:
      die("Unsupported workload\n");
  }
}

/* Pretty printing */
static const char *name_dbbench(bench_t w) {
  switch (w) {
    case dbbench_all_random:
      return "DB_BENCH - All Random";
    case dbbench_all_dist:
      return "DB_BENCH - All Dist";
    case dbbench_prefix_random:
      return "DB_BENCH - Prefix Random";
    case dbbench_prefix_dist:
      return "DB_BENCH - Prefix Dist";
    default:
      return "??";
  }
}

static int handles_dbbench(bench_t w) {
  switch (w) {
    case dbbench_all_random:
    case dbbench_all_dist:
    case dbbench_prefix_random:
    case dbbench_prefix_dist:
      return 1;
    default:
      return 0;
  }
}

static const char *api_name_dbbench(void) { return "DBBENCH"; }

struct workload_api DBBENCH = {
    .init = init_dbbench,
    .handles = handles_dbbench,
    .launch = launch_dbbench,
    .api_name = api_name_dbbench,
    .name = name_dbbench,
    .create_unique_item = create_unique_item_dbbench,
};
