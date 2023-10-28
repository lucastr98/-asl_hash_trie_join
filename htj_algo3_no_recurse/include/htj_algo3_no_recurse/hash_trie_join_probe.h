#ifndef HASH_TRIE_JOIN_PROBE_H_ 
#define HASH_TRIE_JOIN_PROBE_H_

#include "hash_trie.h"
#include "benchmark_setup.h"

#define MAX_PART_IT 64

static inline bool contains(__int64_t *mask, int i)
{
  return (*mask >> i) & 1;
}

static inline void set(__int64_t *mask, int i)
{
  *mask |= (1 << i);
}

void fill_trie(hash_trie_t *trie, relation_t *relation, int *total_num_attributes);
void get_valid_tuples(int j, const int m, const int num_att, 
    const hash_trie_t *hash_trie, __int64_t mask, 
    char *join_atts[], tuple_t **result_tuples, size_t *head_index);
    
void enumerate(int i, int n, int num_att, hash_trie_t *relations, int m, 
    tuple_t **result, size_t *head_index);

#ifdef BENCHMARK_SUBFUNCS
  typedef struct
  {
    uint64_t select_participants;
    uint64_t find_hashes;
    uint64_t check_and_produce_result;
    uint64_t up_down;
  } probe_subfunc_times_t;

  void init_probe_subfunc_times();
  void print_probe_subfunc_times(int query_nr, int num_iterations);
  void write_probe_subfunc_times(int query_nr, int num_iterations);
#endif // SUBFUNC_BENCHMARKS

#endif // HASH_TRIE_JOIN_PROBE_H_ 