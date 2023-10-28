#ifndef HASH_TRIE_JOIN_H_
#define HASH_TRIE_JOIN_H_

#include "benchmark_setup.h"

#ifdef BENCHMARK_ALGOS
  typedef struct
  {
    uint64_t partitioning;
    uint64_t build;
    uint64_t probe;
  } algo_times_t;

  void init_algo_times();
  void print_algo_times(int query_nr, int num_iterations);
  void write_algo_times(int query_nr, int num_iterations);
#endif // BENCHMARK_ALGOS

void join(join_t *join, json_t *benchmark_json, const int num_iterations, 
    const int benchmark_nr, const char* sql_options, const bool is_validation);

#endif  // HASH_TRIE_JOIN_H_
