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
  tuple_t **tuples = malloc(join->num_relations*sizeof(tuple_t *));
  for (int i = 0; i < join->num_relations; i++) {
    size_t nFields = join->relations[i].num_attributes;
    tuple_t *join_head = join->relations[i].tuple_list;
    tuple_t *head = malloc(sizeof(*head) + nFields * sizeof(*head->values));
    memcpy(head, join_head, sizeof(*head) + nFields * sizeof(*head->values));

    tuple_t *tmp = head;
    tuple_t *join_tmp = join_head->next;
    while (join_tmp != NULL) {
      tuple_t *tup = malloc(sizeof(*tup) + nFields * sizeof(*tup->values));
      memcpy(tup, join_tmp, sizeof(*tup) + nFields * sizeof(*tup->values));
      tmp->next = tup;
      tmp = tup;
      join_tmp = join_tmp->next;
    }
    tuples[i] = head;
  }

  for (int iteration = 0; iteration < num_iterations; ++iteration)
  {
    for (int i = 0; i < join->num_relations; i++) 
    {
      size_t nFields = join->relations[i].num_attributes;
      tuple_t *join_head = tuples[i];
      tuple_t *head = malloc(sizeof(*head) + nFields * sizeof(*head->values));
      memcpy(head, join_head, sizeof(*head) + nFields * sizeof(*head->values));

      tuple_t *tmp = head;
      tuple_t *join_tmp = join_head->next;
      while (join_tmp != NULL) {
        tuple_t *tup = malloc(sizeof(*tup) + nFields * sizeof(*tup->values));
        memcpy(tup, join_tmp, sizeof(*tup) + nFields * sizeof(*tup->values));
        tmp->next = tup;
        tmp = tup;
        join_tmp = join_tmp->next;
      }
      join->relations[i].tuple_list = head;
    }

    hash_trie_t *hash_tries = (hash_trie_t *)malloc(
        join->num_relations * sizeof(hash_trie_t));
    int total_num_attributes = 0;
    for (int j = 0; j < join->num_relations; j++)
    {
      hash_tries[j].num_attributes = join->relations[j].num_attributes;
      hash_tries[j].num_join_attributes = join->relations[j].num_join_attributes;
      hash_tries[j].stack = createStack(hash_tries[j].num_join_attributes);
      hash_tries[j].join_numbers = join->relations[j].join_numbers;
      hash_tries[j].tuple_indices = join->relations[j].tuple_indices;
      hash_tries[j].result_idx = total_num_attributes;
      total_num_attributes += join->relations[j].num_attributes;

      #ifdef BENCHMARK_ALGOS
        uint64_t start_build = start_tsc();
      #endif  // BENCHMARK_ALGOS
      hash_tries[j].it = build(0, join->relations[j].tuple_list,
                              &(join->relations[j].num_join_attributes),
                              join->relations[j].tuple_indices);
      #ifdef BENCHMARK_ALGOS
        algo_times.build += stop_tsc(start_build);
      #endif  // BENCHMARK_ALGOS
    }

    tuple_t *result = (tuple_t *)malloc(sizeof(tuple_t) +
        total_num_attributes * sizeof(char *));
    result->next = NULL;
    #ifdef BENCHMARK_ALGOS
      uint64_t start_probe = start_tsc();
    #endif  // BENCHMARK_ALGOS
    enumerate(0, join->total_num_join_attributes, total_num_attributes, 
        hash_tries, join->num_relations, &result);
    #ifdef BENCHMARK_ALGOS
      algo_times.probe += stop_tsc(start_probe);
    #endif  // BENCHMARK_ALGOS

    if (is_validation)
      list_to_csv(result->next, benchmark_nr, total_num_attributes);
    destroy_result_list(result);
    destroy_tries(hash_tries, join->num_relations);

    // print progress
    fflush(stdout);
    printf("\33[2K \r%.0f%% ", ((double)(iteration+1))/num_iterations *100);
  }
  //clear progress print
  printf("\33[2K \r");

  for (int i = 0; i < join->num_relations; i++) {
    // destroy_result_list(tuples[i]);
    size_t nFields = join->relations[i].num_attributes;
    destroy_list(tuples[i], nFields);
  }
  free(tuples);
}
