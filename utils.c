#include "headers.h"
#include "utils.h"

static uint64_t freq = 0;
static uint64_t get_cpu_freq(void) {
  if (freq) return freq;

  FILE *fd;
  float freqf = 0;
  char *line = NULL;
  size_t len = 0;

  fd = fopen("/proc/cpuinfo", "r");
  if (!fd) {
    fprintf(stderr, "failed to get cpu frequency\n");
    perror(NULL);
    return freq;
  }

  while (getline(&line, &len, fd) != EOF) {
    if (sscanf(line, "cpu MHz\t: %f", &freqf) == 1) {
      freqf = freqf * 1000000UL;
      freq = (uint64_t)freqf;
      break;
    }
  }

  fclose(fd);
  return freq;
}

uint64_t cycles_to_us(uint64_t cycles) {
  return cycles * 1000000LU / get_cpu_freq();
}

void shuffle_ranges(size_t *array, size_t n, size_t range_size) {
  if (range_size == 0) {
    printf("Range size cannot be zero.\n");
    return;
  }

  size_t num_ranges = (n + range_size - 1) / range_size;

  // 범위 단위 Fisher-Yates Shuffle
  for (size_t i = 0; i < num_ranges - 1; i++) {
    size_t j = i + rand() / (RAND_MAX / (num_ranges - i) + 1);

    // i번째 범위와 j번째 범위를 swap (range_size만큼)
    size_t start_i = i * range_size;
    size_t start_j = j * range_size;

    // 실제 범위 끝이 n을 넘지 않도록 조정
    size_t end_i = start_i + range_size < n ? start_i + range_size : n;
    size_t end_j = start_j + range_size < n ? start_j + range_size : n;

    size_t len_i = end_i - start_i;
    size_t len_j = end_j - start_j;
    size_t len = len_i < len_j ? len_i : len_j;

    // 두 범위의 최소 길이만큼 swap
    for (size_t k = 0; k < len; k++) {
      size_t temp = array[start_i + k];
      array[start_i + k] = array[start_j + k];
      array[start_j + k] = temp;
    }
  }
}

void shuffle(size_t *array, size_t n) {
  if (n > 1) {
    size_t i;
    for (i = 0; i < n - 1; i++) {
      size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
      size_t t = array[j];
      array[j] = array[i];
      array[i] = t;
    }
  }
}

void pin_me_on(int core) {
  if (!PINNING) return;

  printf("CORE: %d\n", core);
  cpu_set_t cpuset;
  pthread_t thread = pthread_self();

  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);

  int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  if (s != 0) die("Cannot pin thread on core %d\n", core);
}
