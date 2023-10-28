#ifndef HASH_TRIE_H_
#define HASH_TRIE_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <limits.h>
#include <math.h>

// #define DEBUG
#define BENCHMARK_PER_QUERY
#define BENCHMARK_SUBFUNCS
// #define BENCHMARK_ALGOS

typedef struct Tuple tuple_t;
typedef struct JoinTuple join_tuple_t;
typedef struct TrieNode trie_node_t;

// actual tuple holding the data in strings
struct Tuple
{
  size_t nFields;
  size_t nTuples;
  char **values;
};

// tuple holding only the join attributes
// assumes that join attributes are integers
struct JoinTuple
{
  size_t nFields;
  size_t nTuples;
  size_t *next;
  size_t *values;
};

typedef struct
{
  size_t nFields;
  size_t nTuples;
  size_t *values;
  size_t *next;
  int *join_numbers;
  size_t lvl;
  __uint64_t **shifts;
  size_t *node_indices;
  size_t **bucket_indices;
  __uint64_t ***hashes;
  size_t ***children;
} hash_trie_t;

typedef struct
{
  int num_join_attributes;
  int *join_numbers;
  int *join_att_indices;
  tuple_t *tuple_list;
  join_tuple_t *join_tuple_list;
} relation_t;

typedef struct
{
  int total_num_join_attributes;
  int num_relations;
  relation_t *relations;
} join_t;

typedef struct
{
  int num_benchmarks;
  join_t *benchmarks;
} benchmarks_t;

static inline uint64_t node_size(__uint64_t shift)
{
  return (UINT64_MAX >> (shift)) + 1;
}

void up(hash_trie_t *trie);
// move iterator to 1st bucket in the child node of current bucket
void down(hash_trie_t *trie);
// move iterator to next occupied bucket within current node, return false if no
// further occupied buckets exist
bool next(hash_trie_t *trie);
// move iterator to bucket with specified hash
bool lookup(hash_trie_t *trie, __uint64_t hash);

// printing
void list_to_csv(size_t *tuples, join_t *join, int benchmark_nr, size_t head_index);
void print_list(join_tuple_t *tuples, size_t head_index);
// void print_trie(hash_trie_t *M);

// destroy
void destroy_join_tuples(join_tuple_t *list);
void destroy_tuples(join_t *join);
void destroy_tries(hash_trie_t *tries, size_t **num_nodes_per_level_vec, 
    int num_relations);

#endif // HASH_TRIE_H_
