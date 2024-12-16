#include "headers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 테스트 데이터 크기
#define TEST_SIZE 10000000
#define KV_SIZE 16
#define MAX_FILE_SIZE (4 * 1024 * 1024) // 4MB
int load = 0;
int rc_thr = 1;

// 테스트용 데이터 생성
char* create_test_item(uint64_t key, size_t item_size) {
  struct item_metadata *meta;
  char *item = malloc(item_size);
  meta = (struct item_metadata *)item;
  meta->key_size = sizeof(uint64_t);
  meta->value_size = item_size - sizeof(uint64_t) - sizeof(*meta);
  *(uint64_t *)(item + sizeof(*meta)) = key;
  memset(item + sizeof(*meta) + sizeof(uint64_t), 0, meta->value_size); // 초기화
  return item;
}

void add_to_tree(struct slab_callback *cb, char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  uint64_t idx;
  struct tree_entry *tree = tnt_subtree_get((void *)key, &idx, NULL);
  struct slab *s = tree->slab;
  cb->slab = s;

  if (s->nb_items >= s->nb_max_items) {
    close_and_create_slab(s);
  }

  tnt_index_add(cb, item); // 데이터 추가
  if (key < s->min) s->min = key;
  if (key > s->max) s->max = key;

}

void add_to_tree_for_update(struct slab_callback *cb, char *item) {
  struct item_metadata *meta = (struct item_metadata *)item;
  char *item_key = &item[sizeof(*meta)];
  uint64_t key = *(uint64_t *)item_key;
  uint64_t idx;
  struct tree_entry *tree = tnt_subtree_get((void *)key, &idx, NULL);
  struct slab *s = tree->slab;
  cb->slab = s;

  if (s->nb_items >= s->nb_max_items) {
    close_and_create_slab(s);
  }

  tnt_index_add(cb, item); // 데이터 추가
  if (key < s->min) s->min = key;
  if (key > s->max) s->max = key;

}

// 키 배열을 랜덤하게 섞는 함수
//void shuffle_keys(uint64_t *keys, size_t size) {
//  for (size_t i = size - 1; i > 0; i--) {
//    size_t j = rand() % (i + 1);
//    uint64_t temp = keys[i];
//    keys[i] = keys[j];
//    keys[j] = temp;
//  }
//}
void shuffle_ranges(uint64_t *keys, size_t size, size_t range_size) {
  if (range_size == 0) {
    printf("Range size cannot be zero.\n");
    return;
  }

  // 범위의 개수 계산
  size_t num_ranges = (size + range_size - 1) / range_size;

  // 범위 인덱스를 섞기 위한 배열 생성
  size_t *range_indices = (size_t *)malloc(num_ranges * sizeof(size_t));
  for (size_t i = 0; i < num_ranges; i++) {
    range_indices[i] = i;
  }

  // Fisher-Yates Shuffle로 범위 인덱스 섞기
  for (size_t i = num_ranges - 1; i > 0; i--) {
    size_t j = rand() % (i + 1);
    size_t temp = range_indices[i];
    range_indices[i] = range_indices[j];
    range_indices[j] = temp;
  }

  // 새로운 배열에 섞인 범위 데이터 저장
  uint64_t *shuffled_keys = (uint64_t *)malloc(size * sizeof(uint64_t));
  size_t index = 0;
  for (size_t i = 0; i < num_ranges; i++) {
    size_t start = range_indices[i] * range_size;
    size_t end = start + range_size < size ? start + range_size : size;
    for (size_t j = start; j < end; j++) {
      shuffled_keys[index++] = keys[j];
    }
  }

  // 원래 배열에 섞인 결과 복사
  for (size_t i = 0; i < size; i++) {
    keys[i] = shuffled_keys[i];
  }

  free(range_indices);
  free(shuffled_keys);
}

int main() {
  // 초기화
  if (!create_root_slab()) {
    fprintf(stderr, "Failed to initialize root slab.\n");
    return EXIT_FAILURE;
  }
  printf("Root slab initialized.\n");

  struct slab_callback cb;
  uint64_t *keys = malloc(TEST_SIZE * sizeof(uint64_t));
  if (!keys) {
    fprintf(stderr, "Failed to allocate memory for keys.\n");
    return EXIT_FAILURE;
  }
  for (uint64_t i = 0; i < TEST_SIZE; i++) {
    keys[i] = i + 1;
  }
  // 키 랜덤하게 섞기
  srand(time(NULL));
  shuffle_ranges(keys, TEST_SIZE, 1000000);


  // 테스트 데이터 생성 및 추가
  for (uint64_t i = 1; i <= TEST_SIZE; i++) {
    char *item = create_test_item(keys[i], 1024); // 16바이트 값 크기

    cb.slab_idx = i;
    cb.item = item;

    // 데이터 추가
    add_to_tree(&cb, item);
    //printf("Added key: %lu to tree.\n", i);
    free(item);
  }
  free(keys);

  // 테스트 데이터 조회
  for (uint64_t i = 1; i <= TEST_SIZE; i++) {
    char *item = create_test_item(i, 1024);
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start); // 시작 시간 측정
    index_entry_t *entry = tnt_index_lookup(item);
    clock_gettime(CLOCK_MONOTONIC, &end);   // 종료 시간 측정

    long elapsed_time = (end.tv_sec - start.tv_sec) * 1e9 + 
      (end.tv_nsec - start.tv_nsec); // 밀리초 단위로 계산

    if (entry) {
      printf(" Try. Lookup key %lu: Found. Time: %ld ns\n", i, elapsed_time);
    } else {
      printf("Lookup key %lu: Not found. Time: %ld ns\n", i, elapsed_time);
    }

    free(item);
  }
  tnt_print();

  //// 테스트 데이터 업데이트
  //for (uint64_t i = 1; i <= TEST_SIZE; i++) {
  //  char *item = create_test_item(i, 16);
  //  index_entry_t *entry = tnt_index_lookup(item);

  //  if (entry) {
  //    entry->slab_idx += 1000; // slab_idx 값 갱신
  //    tnt_index_add(&cb, item); // 업데이트된 항목 추가
  //    printf("Updated key %lu: New slab_idx %lu.\n", i, entry->slab_idx);
  //  }

  //  free(item);
  //}

  //// 업데이트 확인
  //for (uint64_t i = 1; i <= TEST_SIZE; i++) {
  //  char *item = create_test_item(i, 16);
  //  index_entry_t *entry = tnt_index_lookup(item);

  //  if (entry) {
  //    printf("Post-update key %lu: slab_idx %lu.\n", i, entry->slab_idx);
  //  }

  //  free(item);
  //}


  return 0;
}

