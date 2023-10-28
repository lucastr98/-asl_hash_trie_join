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

  tuple_t *tuples = malloc(join->num_relations*sizeof(tuple_t));
  for(int i = 0; i < join->num_relations; i++) {
    size_t nTuples = join->relations[i].tuple_list->nTuples;
    size_t nFields = join->relations[i].tuple_list->nFields;

    tuples[i].values = malloc(nTuples*nFields*sizeof(char *));
    tuples[i].next = malloc(nTuples*sizeof(size_t));
    tuples[i].nFields = nFields;
    tuples[i].nTuples = nTuples;
    for (int j = 0; j < nFields*nTuples; j++)
    {
      size_t len = strlen(join->relations[i].tuple_list->values[j]);
      tuples[i].values[j] = malloc((len+1) * sizeof(char));
      strcpy(tuples[i].values[j], join->relations[i].tuple_list->values[j]);
    }
    memcpy(tuples[i].next, join->relations[i].tuple_list->next, nTuples*sizeof(size_t));
  }

  for (int iteration = 0; iteration < num_iterations; ++iteration)
  {
    for(int i = 0; i < join->num_relations; i++) 
    {
      size_t nTuples = tuples[i].nTuples;
      size_t nFields = tuples[i].nFields;
      for (int j = 0; j < nFields*nTuples; j++)
      {
        size_t len = strlen(tuples[i].values[j]);
        strcpy(join->relations[i].tuple_list->values[j], tuples[i].values[j]);
      }
      memcpy(join->relations[i].tuple_list->next, tuples[i].next, nTuples*sizeof(size_t));
    }
    hash_trie_t *hash_tries = (hash_trie_t *)malloc(
        join->num_relations * sizeof(hash_trie_t));
    int total_num_attributes = 0;
    long long max_num_tuples = 0;
    for (int j = 0; j < join->num_relations; j++)
    {
      hash_tries[j].num_attributes = join->relations[j].num_attributes;
      hash_tries[j].num_join_attributes = join->relations[j].num_join_attributes;
      hash_tries[j].stack = createStack(hash_tries[j].num_join_attributes);
      hash_tries[j].join_numbers = join->relations[j].join_numbers;
      hash_tries[j].tuple_indices = join->relations[j].tuple_indices;
      hash_tries[j].result_idx = total_num_attributes;
      hash_tries[j].tuple_list = join->relations[j].tuple_list;
      total_num_attributes += join->relations[j].num_attributes;
      max_num_tuples = (max_num_tuples > join->relations[j].tuple_list->nTuples) ? 
          max_num_tuples : join->relations[j].tuple_list->nTuples;
      #ifdef BENCHMARK_ALGOS
        uint64_t start_build = start_tsc();
      #endif  // BENCHMARK_ALGOS
      hash_tries[j].it = build(0, join->relations[j].tuple_list,0,
                              &(join->relations[j].num_join_attributes),
                              join->relations[j].tuple_indices);
      // print_trie(&hash_tries[j]);
      #ifdef BENCHMARK_ALGOS
        algo_times.build += stop_tsc(start_build);
      #endif  // BENCHMARK_ALGOS
    }
    // print_trie(&(hash_tries[0]));
    tuple_t *result = (tuple_t *)malloc(sizeof(tuple_t));
    result->next = calloc(max_num_tuples+1, sizeof(size_t));
    // ARBITRARY MAXIMUM ON THE NUMBER OF TUPLES!
    result->values = 
        malloc(total_num_attributes * (max_num_tuples+1) * sizeof(char *));
    result->nFields = total_num_attributes;
    result->nTuples = 0;
    size_t head_index = 0;
    result->next[head_index] = -1;

    if(result->values == NULL) {
      printf("Cannot allocate to result->values\n");
      exit(1);
    }
    
    #ifdef BENCHMARK_ALGOS
      uint64_t start_probe = start_tsc();
    #endif  // BENCHMARK_ALGOS
    enumerate(0, join->total_num_join_attributes, total_num_attributes, 
        hash_tries, join->num_relations, &result, &head_index);
    
    //UGLY IM SORRY
    if (head_index != 0) {
      result->next[head_index-1] = -1;
    }
    //WILL TRY TO FIX

    #ifdef BENCHMARK_ALGOS
      algo_times.probe += stop_tsc(start_probe);
    #endif  // BENCHMARK_ALGOS
    
    if (is_validation)
      list_to_csv(result, benchmark_nr, 0);
    destroy_result_list(result);
    destroy_tries(hash_tries, join->num_relations);
    //print progress
    fflush(stdout);
    printf("\33[2K \r%.0f%% ", ((double)(iteration+1))/num_iterations *100);
  }

  for(int i = 0; i < join->num_relations; i++) 
  {

    destroy_tuples(join->relations[i].tuple_list);
    size_t nFields = tuples[i].nFields;
    size_t nTuples = tuples[i].nTuples;
    for(int j = 0; j < nFields*nTuples; j++) 
    {
      free(tuples[i].values[j]);
    }
    free(tuples[i].values);
    free(tuples[i].next);
  }
  free(tuples);

  //clear progress print
  printf("\33[2K \r");
}
