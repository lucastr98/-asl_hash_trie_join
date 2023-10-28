#include "benchmark_setup.h"
#include "hash_trie.h"
#include "hash_trie_join_build.h"
#include "hash_trie_join_probe.h"
#include "hash_trie_join.h"
#include "partitioning.h"
#include <string.h>
#include <stdlib.h>
#include <time.h>

#ifdef BENCHMARK_ALGOS
  algo_times_t algo_times;

  void init_algo_times()
  {
    algo_times.partitioning = 0;
    algo_times.build = 0;
    algo_times.probe = 0;
  }

  void print_algo_times(int query_nr, int num_iterations)
  {
    if (query_nr == -1)
    {
      printf("All benchmarks BUILD time: %lu\n", 
          algo_times.build / num_iterations);
      printf("All benchmarks PROBE time: %lu\n",
          algo_times.probe / num_iterations);
    }
    else
    {
      printf("Benchmark %u BUILD time: %lu\n", query_nr,
          algo_times.build / num_iterations);
      printf("Benchmark %u PROBE time: %lu\n", query_nr,
          algo_times.probe / num_iterations);
    }
  }

  void write_algo_times(int query_nr, int num_iterations)
  {
    if (query_nr == -1)
    {
      char filename[100];
      sprintf(filename, "../../test/times/algo_times_all.txt");

      // Open the file for writing.
      FILE *file = fopen(filename, "w");
      if (file == NULL)
      {
        perror("Error opening file");
        exit(1);
      }

      fprintf(file, "%lu\n", algo_times.partitioning / num_iterations);
      fprintf(file, "%lu\n", algo_times.build / num_iterations);
      fprintf(file, "%lu\n", algo_times.probe / num_iterations);
    }
    else
    {
      char filename[100];
      sprintf(filename, "../../test/times/algo_times_%u.txt", query_nr);

      // Open the file for writing.
      FILE *file = fopen(filename, "w");
      if (file == NULL)
      {
        perror("Error opening file");
        exit(1);
      }
      fprintf(file, "%lu\n", algo_times.partitioning / num_iterations);
      fprintf(file, "%lu\n", algo_times.build / num_iterations);
      fprintf(file, "%lu\n", algo_times.probe / num_iterations);
    }
  }
#endif  // BENCHMARK_ALGOS

void join(join_t *join, json_t *benchmark_json, const int num_iterations, 
    const int benchmark_nr, const char* sql_options, const bool is_validation)
{
  srand(time(NULL));
  setup_tuples(join, benchmark_json, sql_options, is_validation);

  // copy all join_tuple_t's from join
  join_tuple_t *join_tuples = malloc(sizeof(join_tuple_t) * join->num_relations);
  for (int i = 0; i < join->num_relations; i++) 
  {
    size_t nTuples = join->relations[i].tuple_list->nTuples;
    size_t values_size = nTuples * join->relations[i].num_join_attributes * sizeof(size_t);
    size_t next_size = nTuples * sizeof(size_t);
    join_tuples[i].values = malloc(values_size);
    join_tuples[i].next = malloc(next_size);
    join_tuples[i].nFields = join->relations[i].num_join_attributes;
    join_tuples[i].nTuples = nTuples;
    memcpy(join_tuples[i].values, join->relations[i].join_tuple_list->values, values_size);
    memcpy(join_tuples[i].next, join->relations[i].join_tuple_list->next, next_size);
  }
  for (int iteration = 0; iteration < num_iterations; ++iteration)
  {
    // copy join tuples from join_tuples array
    for (int i = 0; i < join->num_relations; i++) 
    {
      size_t nTuples = join_tuples[i].nTuples;
      size_t next_size = nTuples * sizeof(size_t);
      size_t values_size = nTuples * join->relations[i].num_join_attributes * sizeof(size_t);
      memcpy(join->relations[i].join_tuple_list->values, join_tuples[i].values, values_size);
      memcpy(join->relations[i].join_tuple_list->next, join_tuples[i].next, next_size);
      join->relations[i].join_tuple_list->nFields = join_tuples[i].nFields;
      join->relations[i].join_tuple_list->nTuples = join_tuples[i].nTuples;
    }

    hash_trie_t *hash_tries = (hash_trie_t *)malloc(
        join->num_relations * sizeof(hash_trie_t));
    int total_num_attributes = 0;
    long long max_num_tuples = 0;
    for (int j = 0; j < join->num_relations; j++)
    {
      hash_tries[j].num_join_attributes = join->relations[j].num_join_attributes;
      hash_tries[j].stack = createStack(hash_tries[j].num_join_attributes);
      hash_tries[j].join_numbers = join->relations[j].join_numbers;
      hash_tries[j].join_tuple_list = join->relations[j].join_tuple_list;
      total_num_attributes += join->relations[j].tuple_list->nFields;
      max_num_tuples = (max_num_tuples > join->relations[j].tuple_list->nTuples) ? 
          max_num_tuples : join->relations[j].tuple_list->nTuples;

      #ifdef BENCHMARK_ALGOS
      uint64_t start_partitioning = start_tsc();
      #endif  // BENCHMARK_ALGOS
      partition_tuples(&(join->relations[j]));
      #ifdef BENCHMARK_ALGOS
        algo_times.partitioning += stop_tsc(start_partitioning);
      #endif  // BENCHMARK_ALGOS

      #ifdef BENCHMARK_ALGOS
        uint64_t start_build = start_tsc();
      #endif  // BENCHMARK_ALGOS
      hash_tries[j].it = build(join->relations[j].join_tuple_list, 0,
                              &(join->relations[j].num_join_attributes));
      #ifdef BENCHMARK_ALGOS
        algo_times.build += stop_tsc(start_build);
      #endif  // BENCHMARK_ALGOS
    }
    size_t head_index = 0;
    size_t *result = malloc(join->num_relations * (max_num_tuples + 1) * sizeof(size_t));
    #ifdef BENCHMARK_ALGOS
      uint64_t start_probe = start_tsc();
    #endif  // BENCHMARK_ALGOS
    enumerate(join->total_num_join_attributes, join->num_relations, 
        hash_tries, &result, &head_index);
    if (head_index != 0)
      --head_index;
    #ifdef BENCHMARK_ALGOS
      algo_times.probe += stop_tsc(start_probe);
    #endif  // BENCHMARK_ALGOS
    
    if (is_validation)
      list_to_csv(result, join, benchmark_nr, head_index);
    free(result);
    destroy_tries(hash_tries, join->num_relations);

    // print progress
    fflush(stdout);
    printf("\33[2K \r%.0f%% ", ((double)(iteration+1))/num_iterations * 100);
  }
  // clear progress print
  printf("\33[2K \r");
  for (int i = 0; i < join->num_relations; i++) {
    destroy_join_tuples(join->relations[i].join_tuple_list);
    free(join_tuples[i].values);
    free(join_tuples[i].next);
  }
  free(join_tuples);
  destroy_tuples(join);
}
