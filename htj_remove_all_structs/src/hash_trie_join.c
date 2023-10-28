#include "benchmark_setup.h"
#include "hash_trie.h"
#include "hash_trie_join_build.h"
#include "hash_trie_join_probe.h"
#include "hash_trie_join.h"
#include <string.h>

#ifdef BENCHMARK_ALGOS
  algo_times_t algo_times;

  void init_algo_times()
  {
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

      fprintf(file, "%lu\n", algo_times.build / num_iterations);
      fprintf(file, "%lu\n", algo_times.probe / num_iterations);
    }
  }
#endif  // BENCHMARK_ALGOS

void join(join_t *join, json_t *benchmark_json, const int num_iterations, 
    const int benchmark_nr, const char* sql_options, const bool is_validation)
{
  setup_tuples(join, benchmark_json, sql_options, is_validation);

  // copy all join_tuple_t's from join
  join_tuple_t *join_tuples = malloc(sizeof(join_tuple_t) * join->num_relations);
  for (int i = 0; i < join->num_relations; i++) {
    size_t nTuples = join->relations[i].tuple_list->nTuples;
    size_t values_size = nTuples * join->relations[i].num_join_attributes * 
                         sizeof(size_t);
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
    for (int i = 0; i < join->num_relations; i++) {
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
    size_t **num_nodes_per_lvl_vec = malloc(join->num_relations * sizeof(size_t *));
    int total_num_attributes = 0;
    long long max_num_tuples = 0;
    for (int j = 0; j < join->num_relations; j++)
    {
      // get input tuples
      hash_tries[j].join_numbers = join->relations[j].join_numbers;
      hash_tries[j].nFields = join->relations[j].join_tuple_list->nFields;
      hash_tries[j].nTuples = join->relations[j].join_tuple_list->nTuples;
      hash_tries[j].values = join->relations[j].join_tuple_list->values;
      hash_tries[j].next = join->relations[j].join_tuple_list->next;
      hash_tries[j].lvl = 0;

      // initialize what is already known in the hash trie
      hash_tries[j].node_indices = 
          calloc(hash_tries[j].nFields, sizeof(size_t));
      hash_tries[j].shifts = malloc(hash_tries[j].nFields * sizeof(size_t *));
      hash_tries[j].bucket_indices = 
          malloc(hash_tries[j].nFields * sizeof(size_t *));
      hash_tries[j].hashes = malloc(hash_tries[j].nFields * sizeof(size_t **));
      hash_tries[j].children = malloc(hash_tries[j].nFields * sizeof(size_t **));

      // get total number of join attributes for the result and an estimate of 
      // the maximum number of result tuples
      total_num_attributes += hash_tries[j].nFields;
      max_num_tuples = (max_num_tuples > hash_tries[j].nTuples) ? 
          max_num_tuples : hash_tries[j].nTuples;

      #ifdef BENCHMARK_ALGOS
        uint64_t start_build = start_tsc();
      #endif  // BENCHMARK_ALGOS
      num_nodes_per_lvl_vec[j] = build(0, &hash_tries[j]);
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
    destroy_tries(hash_tries, num_nodes_per_lvl_vec, join->num_relations);

    // print progress
    fflush(stdout);
    printf("\33[2K \r%.0f%% ", ((double)(iteration+1))/num_iterations * 100);
  }
  //clear progress print
  printf("\33[2K \r");
  for (int i = 0; i < join->num_relations; i++) {
    destroy_join_tuples(join->relations[i].join_tuple_list);
    free(join_tuples[i].values);
    free(join_tuples[i].next);
  }
  free(join_tuples);
  destroy_tuples(join);
}
