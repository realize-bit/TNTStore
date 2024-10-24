#ifndef DBBENCH_H
#define DBBENCH_H

#include "headers.h"

#ifdef __cplusplus
extern "C" {
#endif
int64_t GetRandomKey(void *rand, int64_t num);
int64_t ParetoCdfInversion(double u, double theta, double k, double sigma);
int64_t PowerCdfInversion(double u, double a, double b);

void *init_exp_prefix(int64_t total_keys, int range_num, double prefix_a,
                      double prefix_b, double prefix_c, double prefix_d);
void *init_query(double mix_get_ratio, double mix_put_ratio, double mix_scan_ratio);
int64_t prefix_get_key(void *gen_exp, int64_t ini_rand, double key_dist_a,
                       double key_dist_b);
int query_get_type(void *query, int64_t rand_num);

void init_rand(void);
void *create_rand_seed(uint64_t s);
uint64_t rand_next_seed(uint64_t key_seed);
uint64_t rand_next(void);
#ifdef __cplusplus
}
#endif

#endif
