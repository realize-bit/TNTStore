#include "headers.h"
#include <math.h>

int print = 0;
int load = 1;
extern int cache_hit;
extern int try_fsst;
int rc_thr = 1;

int main(int argc, char **argv) {
  int nb_disks, nb_workers_per_disk, nb_distributors_per_disk;
  declare_timer;

  /* Definition of the workload, if changed you need to erase the DB before
   * relaunching */
  struct workload w = {
      //.api = &YCSB,
      .api = &DBBENCH,
      .nb_items_in_db = 100000000LU,
      .nb_load_injectors = 4,
  };

  /* Parsing of the options */
  if (argc < 4)
    die("Usage: ./main <nb disks> <nb workers per disk> <nb "
        "distributors>\n\tData is stored in %s\n",
        PATH);
  nb_disks = atoi(argv[1]);
  nb_workers_per_disk = atoi(argv[2]);
  nb_distributors_per_disk = atoi(argv[3]);

  /* Pretty printing useful info */
  printf("# Configuration:\n");
  printf("# \tPage cache size: %lu GB\n", PAGE_CACHE_SIZE / 1024 / 1024 / 1024);
  printf("# \tWorkers: %d working on %d disks\n",
         nb_disks * nb_workers_per_disk, nb_disks);
  printf(
      "# \tIO configuration: %d queue depth (capped: %s, extra waiting: %s)\n",
      QUEUE_DEPTH, NEVER_EXCEED_QUEUE_DEPTH ? "yes" : "no",
      WAIT_A_BIT_FOR_MORE_IOS ? "yes" : "no");
  printf("# \tQueue configuration: %d maximum pending callbaks per worker\n",
         MAX_NB_PENDING_CALLBACKS_PER_WORKER);
  printf("# \tDatastructures: %d (memory index) %d (pagecache)\n", MEMORY_INDEX,
         PAGECACHE_INDEX);
  printf("# \tThread pinning: %s\n", PINNING ? "yes" : "no");
  printf("# \tBench: %s (%lu elements)\n", w.api->api_name(), w.nb_items_in_db);

  //rc_thr = log2((w.nb_items_in_db / (MAX_FILE_SIZE / KV_SIZE) )) * 0.7;
  rc_thr = (w.nb_items_in_db / (MAX_FILE_SIZE / KV_SIZE) ) * 0.05;
  printf("RC THR: %d\n", rc_thr);

  /* Initialization of random library */
  start_timer {
    printf(
        "Initializing random number generator (Zipf) -- this might take a "
        "while for large databases...\n");
    init_zipf_generator(
        0, w.nb_items_in_db - 1); /* This takes about 3s... not sure why, but
                                     this is legacy code :/ */
  }
  stop_timer("Initializing random number generator (Zipf)");

#ifdef REALKEY_FILE_PATH
  start_timer {
    printf(
      "Loading real world keys from file -- this might take a while for large key sets...\n");
    load_real_keys(w.nb_items_in_db); // OSM 파일에서 키를 불러옵니다.
  }
  stop_timer("Loading keys from real world file");
#endif


  /* Recover database */
  start_timer {
    slab_workers_init(nb_disks, nb_workers_per_disk, nb_distributors_per_disk);
  }
  stop_timer("Init found %lu elements", get_database_size());


  /* Add missing items if any */
  //system("cat /proc/vmstat | grep psw");
  repopulate_db(&w);
  load = 0;

  start_timer {
    flush_batched_load();
  }
  stop_timer("Remaining batch loading");

  // flush_batched_load();

  print = 1;
  cache_hit = 0;

  /* Launch benchs */
  //system("cat /proc/vmstat | grep psw");
  bench_t workload, workloads[] = {
			SELECTED_BENCH,
                    };
  // sleep(5);
  // make_fsst();
  // sleep(5);
  // cache_hit = 0;

  //start_timer {
  //  tnt_rebalancing();
  //}
  //stop_timer("Rebalancing operations");

  //fsst_worker_init();
  //start_timer {
  //  sleep_until_fsstq_empty();
  //}
  //stop_timer("Reinsertion operations");

  foreach (workload, workloads) {
    if (workload == ycsb_e_uniform || workload == ycsb_e_zipfian) {
      w.nb_requests =
          2000000LU;  // requests for YCSB E are longer (scans) so we do less
    } else if (workload == dbbench_all_random || workload == dbbench_all_dist ||
               workload == dbbench_prefix_random ||
               workload == dbbench_prefix_dist) {
      w.nb_requests = 100000000LU;
    } else {
      w.nb_requests = 100000000LU;
    }
    run_workload(&w, workload);
    printf("lookup hit: %d\n", cache_hit);
    printf("try_fsst: %d\n", try_fsst);
    cache_hit = 0;
  }
  tnt_print();

  /*
  foreach(workload, workloads) {
      if(workload == ycsb_e_uniform || workload == ycsb_e_zipfian) {
         w.nb_requests = 2000000LU; // requests for YCSB E are longer (scans) so
  we do less } else { w.nb_requests = 100000000LU;
      }
      // w.nb_requests = 20000000LU;
      //w.nb_requests = 100LU;
      run_workload(&w, workload);
      printf("lookup hit: %d\n", cache_hit);
      cache_hit = 0;
   }
   return 0;
  */
}
