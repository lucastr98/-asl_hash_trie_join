#ifndef BENCHMARK_SETUP_H_ 
#define BENCHMARK_SETUP_H_

#include "hash_trie.h"
#include <jansson.h>

typedef struct
{
  int num_attributes;                 // total number of attributes
  int num_join_attributes;            // number of attributes that are joined over
  int *join_numbers;                  // stores join numbers
  int *tuple_indices;                 // stores idx into the tuple for every join nr
  tuple_t *tuple_list;
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

json_t *get_benchmarks_json_object();
benchmarks_t *get_benchmarks();
void setup_tuples(join_t *benchmark, json_t *benchmark_json, 
    const char *sql_options, const bool is_validation);
// void setup_tuples(join_t *benchmark, const char *sql_options, 
//     const int input_size);
// benchmarks_t *setup_data(const char *sql_options, const int input_size);
void free_benchmarks(benchmarks_t *benchmarks);

#endif // BENCHMARK_SETUP_H_
