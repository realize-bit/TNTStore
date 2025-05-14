/*
 * 테스트 코드 (매개변수 적용, YCSB 스타일 랜덤 조회/업데이트, 레이턴시 측정)
 * Usage: ./test <total_requests> <shuffle_range> <workload_type>
 *   total_requests: 수행할 총 연산(request) 수
 *   shuffle_range : 키 셔플 시 범위 크기 (초기 데이터 삽입 후)
 *   workload_type : A, B, C 중 하나 (업데이트 확률 결정)
 */

#include "headers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <pthread.h>

static pthread_barrier_t barrier;
uint64_t *keys;

// 전역 설정
size_t TOTAL_REQS = 1000000;
size_t RANGE_SIZE = 1000000;
char WORKLOAD = 'A';
int UPDATE_THRESHOLD = 50; // A:50%, B:5%, C:0%
static int create_sequence = 0;
int load = 1;
int rc_thr = 1;

// 레이턴시 저장
static size_t read_count = 0;
static size_t update_count = 0;

typedef struct {
  int thread_id;
  size_t start_idx, end_idx;        // 이 쓰레드가 처리할 키 구간
  double *read_lat, *upd_lat;       // 쓰레드별 레이턴시 배열
  size_t read_cnt, upd_cnt;
} thread_data_t;

// 비교 함수
static int cmp_double(const void *a, const void *b) {
  double da = *(double*)a;
  double db = *(double*)b;
  return (da < db) ? -1 : (da > db);
}

// 통계 출력
static void print_latency(const char *label, double *arr, size_t n) {
  if (n == 0) {
    printf("%s: no samples\n", label);
    return;
  }
  qsort(arr, n, sizeof(double), cmp_double);
  double sum = 0;
  for (size_t i = 0; i < n; i++) sum += arr[i];
  double mean = sum / n;
  double p50 = arr[(size_t)(0.50 * (n - 1))];
  double p99 = arr[(size_t)(0.99 * (n - 1))];
  double p999 = arr[(size_t)(0.999 * (n - 1))];
  double p9999 = arr[(size_t)(0.9999 * (n - 1))];
  printf("%s latency (ns): mean=%.2f, p50=%.2f, p99=%.2f, p99.9=%.2f, p99.99=%.2f\n",
         label, mean, p50, p99, p999, p9999);
}

static struct slab *create_mem_slab(uint64_t level, uint64_t key) {
  struct slab *s = calloc(1, sizeof(*s));
  uint64_t cur_seq = __sync_add_and_fetch(&create_sequence, 1);
  size_t nb_items_per_page = PAGE_SIZE / KV_SIZE;

  s->nb_items = 0;
  s->size_on_disk = MAX_FILE_SIZE;
  s->nb_max_items = s->size_on_disk / PAGE_SIZE * nb_items_per_page;
  s->item_size = KV_SIZE;
  s->min = UINT64_MAX;
  s->max = 0;
  s->key = key;
  s->seq = cur_seq;
  INIT_LOCK(&s->tree_lock, NULL);
  return s;
}

static struct slab *close_and_create_slab(struct slab *s) {
  uint64_t new_key;
  uint64_t new_level;
  R_LOCK(&s->tree_lock);
  //new_key = (s->min + s->max) / 2;
  new_key = s->min + (s->max - s->min) / 2;
  new_level = tnt_get_centree_level(s->centree_node)+1;
  R_UNLOCK(&s->tree_lock);

  tnt_subtree_update_key(s->key, new_key);
  printf("mem_create: %lu, %lu // %lu-%lu\n", (uint64_t)s->key, (uint64_t)new_key,
         s->min, s->max);
  W_LOCK(&s->tree_lock);
  s->key = new_key;
  W_UNLOCK(&s->tree_lock);
  struct slab *left = create_mem_slab(new_level, new_key - 1);
  struct slab *right = create_mem_slab(new_level, new_key + 1);
  tnt_subtree_add(left, tnt_subtree_create(), NULL, new_key - 1);
  tnt_subtree_add(right, tnt_subtree_create(), NULL, new_key + 1);
  // reset item counters for continuing inserts
  s->nb_items = 0;
  s->min = UINT64_MAX;
  s->max = 0;
  return s;
}

// 초기 삽입용 함수
static void add_to_tree(struct slab_callback *cb, char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  uint64_t idx;
  struct tree_entry *tree = tnt_subtree_get((void *)key, &idx, NULL);
  struct slab *s = tree->slab;
  cb->slab = s;

  if ((idx + 1) == s->nb_max_items) {
    close_and_create_slab(s);
  }
  W_LOCK(&s->tree_lock);
  tnt_index_add(cb, item);
  W_UNLOCK(&s->tree_lock);
  if (key < s->min) s->min = key;
  if (key > s->max) s->max = key;
}

// 업데이트 및 레이턴시 측정: subtree_get과 index_add+min/max를 각각 측정 후 합산하여 반환
double add_to_tree_for_update_timed(struct slab_callback *cb, char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  uint64_t idx;
  struct timespec start0, end0, start1, end1;

  // subtree_get 호출 시간 측정
  clock_gettime(CLOCK_MONOTONIC, &start0);
  struct tree_entry *tree = tnt_subtree_get((void *)key, &idx, NULL);
  clock_gettime(CLOCK_MONOTONIC, &end0);

  struct slab *s = tree->slab;
  cb->slab = s;
  if ((idx + 1) == s->nb_max_items) {
    close_and_create_slab(s);
  }

  // index_add 및 min/max 갱신 시간 측정
  clock_gettime(CLOCK_MONOTONIC, &start1);
  if (idx!=-1) {
    W_LOCK(&s->tree_lock);
    tnt_index_add(cb, item);
    W_UNLOCK(&s->tree_lock);
    if (key < s->min) s->min = key;
    if (key > s->max) s->max = key;
  }
  clock_gettime(CLOCK_MONOTONIC, &end1);

  double ns0 = (end0.tv_sec - start0.tv_sec) * 1e9 + (end0.tv_nsec - start0.tv_nsec);
  double ns1 = (end1.tv_sec - start1.tv_sec) * 1e9 + (end1.tv_nsec - start1.tv_nsec);
  return ns0 + ns1;
}

// 아이템 생성
static char* create_test_item(uint64_t key) {
  struct item_metadata *meta;
  char *item = malloc(KV_SIZE);
  meta = (struct item_metadata *)item;
  meta->key_size = sizeof(uint64_t);
  meta->value_size = KV_SIZE - sizeof(uint64_t) - sizeof(*meta);
  *(uint64_t *)(item + sizeof(*meta)) = key;
  memset(item + sizeof(*meta) + sizeof(uint64_t), 0, meta->value_size);
  return item;
}

// 업데이트 결정
static int should_update() {
  return rand() % 100 < UPDATE_THRESHOLD;
}

// 범위 단위 셔플
static void shuffle_test(uint64_t *keys, size_t size, size_t range_size) {
  if (range_size == 0) {
    fprintf(stderr, "Range size cannot be zero.\n");
    exit(EXIT_FAILURE);
  }
  size_t num_ranges = (size + range_size - 1) / range_size;
  size_t *range_indices = malloc(num_ranges * sizeof(size_t));
  for (size_t i = 0; i < num_ranges; i++) range_indices[i] = i;
  for (size_t i = num_ranges - 1; i > 0; i--) {
    size_t j = rand() % (i + 1);
    size_t tmp = range_indices[i]; range_indices[i] = range_indices[j]; range_indices[j] = tmp;
  }
  uint64_t *shuffled = malloc(size * sizeof(uint64_t));
  size_t idx = 0;
  for (size_t ri = 0; ri < num_ranges; ri++) {
    size_t start = range_indices[ri] * range_size;
    size_t end = start + range_size < size ? start + range_size : size;
    for (size_t k = start; k < end; k++) shuffled[idx++] = keys[k];
  }
  memcpy(keys, shuffled, size * sizeof(uint64_t));
  free(range_indices); free(shuffled);
}

void *worker(void *arg) {
  thread_data_t *td = (thread_data_t*)arg;

  // 1) 초기 삽입 파트
  for (size_t i = td->start_idx; i < td->end_idx; i++) {
    char *item = create_test_item(keys[i]);
    struct slab_callback cb = {/*...*/};
    cb.slab_idx = i;
    cb.item     = item;
    add_to_tree(&cb, item);
    free(item);
  }

  // 모든 쓰레드가 초기 삽입 끝날 때까지 대기
  pthread_barrier_wait(&barrier);

  // 2) YCSB 스타일 연산 파트
  for (size_t i = td->start_idx; i < td->end_idx; i++) {
    uint64_t key = (rand() % TOTAL_REQS) + 1;
    char *item = create_test_item(key);
    struct slab_callback cb = {/*...*/};
    cb.slab_idx = 0;
    cb.item     = item;

    if (should_update()) {
      double lat = add_to_tree_for_update_timed(&cb, item);
      td->upd_lat[td->upd_cnt++] = lat;
    } else {
      struct timespec ts, te;
      clock_gettime(CLOCK_MONOTONIC, &ts);
      index_entry_t *e = tnt_index_lookup(item);
      (void)e;
      clock_gettime(CLOCK_MONOTONIC, &te);
      double lat = (te.tv_sec - ts.tv_sec) * 1e9 + (te.tv_nsec - ts.tv_nsec);
      td->read_lat[td->read_cnt++] = lat;
    }
    free(item);
  }

  return NULL;
}

// ------------------------------------------------------------
// 리밸런싱 후 YCSB 연산만 수행하는 함수
void *phase2_worker(void *arg) {
  thread_data_t *td = (thread_data_t*)arg;

  // YCSB 스타일 연산 및 레이턴시 측정
  for (size_t i = td->start_idx; i < td->end_idx; i++) {
    uint64_t key = (rand() % TOTAL_REQS) + 1;
    char *item = create_test_item(key);
    struct slab_callback cb = { .slab_idx = 0, .item = item };

    if (should_update()) {
      double lat = add_to_tree_for_update_timed(&cb, item);
      td->upd_lat[td->upd_cnt++] = lat;
    } else {
      struct timespec ts, te;
      clock_gettime(CLOCK_MONOTONIC, &ts);
      (void)tnt_index_lookup(item);
      clock_gettime(CLOCK_MONOTONIC, &te);
      td->read_lat[td->read_cnt++] =
        (te.tv_sec - ts.tv_sec) * 1e9 + (te.tv_nsec - ts.tv_nsec);
    }

    free(item);
  }

  return NULL;
}

int main(int argc, char *argv[]) {
  if (argc != 5) {
    fprintf(stderr, "Usage: %s <total_requests> <shuffle_range> <workload_type (A/B/C)> <num_threads>\n", argv[0]);
    return EXIT_FAILURE;
  }
  TOTAL_REQS = strtoull(argv[1], NULL, 10);
  RANGE_SIZE = strtoull(argv[2], NULL, 10);
  WORKLOAD = toupper((unsigned char)argv[3][0]);
  int num_threads = atoi(argv[4]);

  if (num_threads <= 0) {
    fprintf(stderr, "Invalid number of threads: %d\n", num_threads);
    return EXIT_FAILURE;
  }

  switch (WORKLOAD) {
    case 'A': UPDATE_THRESHOLD = 50; break;
    case 'B': UPDATE_THRESHOLD = 5;  break;
    case 'C': UPDATE_THRESHOLD = 0;  break;
    default:
      fprintf(stderr, "Invalid workload type '%c'. Use A, B, or C.\n", WORKLOAD);
      return EXIT_FAILURE;
  }
  srand((unsigned)time(NULL));

  // 레이턴시 배열 할당

  if (!create_root_slab()) {
    fprintf(stderr, "Failed to initialize root slab.\n");
    return EXIT_FAILURE;
  }

  printf(
    "Root slab initialized. TOTAL_REQS=%zu, RANGE_SIZE=%zu, WORKLOAD=%c (%%upd=%d), NUM_THREADS=%d\n",
    TOTAL_REQS,
    RANGE_SIZE,
    WORKLOAD,
    UPDATE_THRESHOLD,
    num_threads
  );

  // 2) barrier 초기화
  pthread_barrier_init(&barrier, NULL, num_threads);

  keys = malloc(TOTAL_REQS * sizeof(uint64_t));
  for (size_t i = 0; i < TOTAL_REQS; i++) keys[i] = i + 1;
  shuffle_test(keys, TOTAL_REQS, RANGE_SIZE);

  // 3) 쓰레드용 데이터 할당
  thread_data_t *tds = calloc(num_threads, sizeof(thread_data_t));
  pthread_t *tids    = calloc(num_threads, sizeof(pthread_t));
  size_t chunk = (TOTAL_REQS + num_threads - 1) / num_threads;
  for (int t = 0; t < num_threads; t++) {
    tds[t].thread_id = t;
    tds[t].start_idx = t * chunk;
    tds[t].end_idx   = (t+1)*chunk < TOTAL_REQS ? (t+1)*chunk : TOTAL_REQS;
    tds[t].read_lat  = malloc(chunk * sizeof(double));
    tds[t].upd_lat   = malloc(chunk * sizeof(double));
    tds[t].read_cnt = tds[t].upd_cnt = 0;
    pthread_create(&tids[t], NULL, worker, &tds[t]);
  }

  // 4) 쓰레드 종료 대기
  for (int t = 0; t < num_threads; t++) {
    pthread_join(tids[t], NULL);
  }

  // 5) 모든 쓰레드별 레이턴시 합산
  size_t total_reads = 0, total_upds = 0;
  for (int t = 0; t < num_threads; t++) {
    total_reads += tds[t].read_cnt;
    total_upds  += tds[t].upd_cnt;
  }
  // 합쳐진 배열에 복사
  double *all_reads = malloc(total_reads * sizeof(double));
  double *all_upds  = malloc(total_upds  * sizeof(double));
  size_t ri = 0, ui = 0;
  for (int t = 0; t < num_threads; t++) {
    memcpy(all_reads + ri, tds[t].read_lat,  tds[t].read_cnt * sizeof(double));
    memcpy(all_upds  + ui, tds[t].upd_lat,   tds[t].upd_cnt  * sizeof(double));
    ri += tds[t].read_cnt;
    ui += tds[t].upd_cnt;
  }


  /*
  // 초기 삽입
  uint64_t *keys = malloc(TOTAL_REQS * sizeof(uint64_t));
  for (size_t i = 0; i < TOTAL_REQS; i++) keys[i] = i + 1;
  shuffle_test(keys, TOTAL_REQS, RANGE_SIZE);
  struct slab_callback cb;
  for (size_t i = 0; i < TOTAL_REQS; i++) {
    char *item = create_test_item(keys[i]);
    cb.slab_idx = i;
    cb.item = item;
    add_to_tree(&cb, item);
    free(item);
  }
  free(keys);

  // YCSB 스타일 연산 및 레이턴시 측정
  printf("--- Workload Phase: %zu operations ---\n", TOTAL_REQS);
  for (size_t i = 0; i < TOTAL_REQS; i++) {
    uint64_t key = (rand() % TOTAL_REQS) + 1;
    char *item = create_test_item(key);
    cb.slab_idx = 0;
    cb.item = item;
    if (should_update()) {
      double lat = add_to_tree_for_update_timed(&cb, item);
      update_latencies[update_count++] = lat;
    } else {
      struct timespec ts, te;
      clock_gettime(CLOCK_MONOTONIC, &ts);
      index_entry_t *e = tnt_index_lookup(item);
      (void)e;
      clock_gettime(CLOCK_MONOTONIC, &te);
      double lat = (te.tv_sec - ts.tv_sec) * 1e9 + (te.tv_nsec - ts.tv_nsec);
      read_latencies[read_count++] = lat;
    }
    free(item);
  }
  print_latency("Read", read_latencies, read_count);
  print_latency("Update", update_latencies, update_count);
  */

  // 결과 통계
  print_latency("Read",   all_reads, total_reads);
  print_latency("Update", all_upds,  total_upds);

  //tnt_print();
  printf("---------------------------------------------\n");

  struct timespec rb_start, rb_end;
  clock_gettime(CLOCK_MONOTONIC, &rb_start);
  tnt_rebalancing();
  clock_gettime(CLOCK_MONOTONIC, &rb_end);
  long rb_ns = (rb_end.tv_sec - rb_start.tv_sec) * 1e9
    + (rb_end.tv_nsec - rb_start.tv_nsec);
  printf("Rebalancing time: %ld ns\n", rb_ns);

  for (int t = 0; t < num_threads; t++) {
    tds[t].read_cnt = tds[t].upd_cnt = 0;
    memset(tds[t].read_lat, 0, chunk * sizeof(double));
    memset(tds[t].upd_lat,  0, chunk * sizeof(double));
  }

  // Phase3 실행: phase2_worker 사용
  for (int t = 0; t < num_threads; t++) {
    // 기존 tids, tds 그대로 재사용
    pthread_create(&tids[t], NULL, phase2_worker, &tds[t]);
  }


  // Phase3 스레드 종료 대기
  for (int t = 0; t < num_threads; t++) {
    pthread_join(tids[t], NULL);
  }
  ri = ui = 0;
  for (int t = 0; t < num_threads; t++) {
    memcpy(all_reads + ri, tds[t].read_lat,  tds[t].read_cnt * sizeof(double));
    memcpy(all_upds  + ui, tds[t].upd_lat,   tds[t].upd_cnt  * sizeof(double));
    ri += tds[t].read_cnt;
    ui += tds[t].upd_cnt;
  }
  print_latency("Post-rebalance Read",   all_reads, total_reads);
  print_latency("Post-rebalance Update", all_upds,  total_upds);
  //tnt_print();

  free(all_reads); free(all_upds);
  for (int t = 0; t < num_threads; t++) {
    free(tds[t].read_lat);
    free(tds[t].upd_lat);
  }
  pthread_barrier_destroy(&barrier);
  free(tds); free(tids);
  return 0;
}

