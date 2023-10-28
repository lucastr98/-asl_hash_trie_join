#include "tsc_x86.h"
#include "hash_trie.h"
#include "hash_trie_join_build.h"
#include "hash_trie_join_probe.h"
#include "hash_trie_join.h"
#include "benchmark_setup.h"
#include "validation.h"
#include "timing.h"

int main(int argc, char **argv)
{
  system("[ -n \"$(ls -A ../../test/times/)\" ] && exec rm ../../test/times/*;");
  
  const char *options;
  if (argc > 1)
    options = argv[1];
  else
    options = "host=localhost dbname=tpch_db user=tpch_user password=tpch_user";

  // get benchmarking data
  json_t *benchmarks_json = get_benchmarks_json_object();
  benchmarks_t *benchmarks = get_benchmarks(benchmarks_json);
  join_t *joins = benchmarks->benchmarks;

  // perform validation
  validate_hashtries(benchmarks, benchmarks_json, options);
    
  //get timing params
  timing_params_t *timing_params = get_timing_params();
  #ifndef BENCHMARK_PER_QUERY
    #ifdef BENCHMARK_ALGOS
      init_algo_times();
    #endif  // BENCHMARK_ALGOS
    #ifdef BENCHMARK_SUBFUNCS
      init_build_subfunc_times();
      init_probe_subfunc_times();
    #endif  // BENCHMARK_SUBFUNCS
  #endif  // BENCHMARK_PER_QUERY
  for (int i = 0; i < benchmarks->num_benchmarks; i++)
  {
    #ifdef BENCHMARK_PER_QUERY
      #ifdef BENCHMARK_ALGOS
        init_algo_times();
      #endif  // BENCHMARK_ALGOS
      #ifdef BENCHMARK_SUBFUNCS
        init_build_subfunc_times();
        init_probe_subfunc_times();
      #endif  // BENCHMARK_SUBFUNCS
    #endif  // BENCHMARK_PER_QUERY

    join(&joins[i], json_array_get(benchmarks_json, i), 
        timing_params->num_iterations, i, options, false);

    #ifdef BENCHMARK_PER_QUERY
      #ifdef BENCHMARK_ALGOS
        write_algo_times(i, timing_params->num_iterations);
      #endif  // BENCHMARK_ALGOS
      #ifdef BENCHMARK_SUBFUNCS
        write_build_subfunc_times(i, timing_params->num_iterations);
        write_probe_subfunc_times(i, timing_params->num_iterations);
      #endif  // BENCHMARK_SUBFUNCS
    #endif  // BENCHMARK_PER_QUERY
  }

  #ifndef BENCHMARK_PER_QUERY
    #ifdef BENCHMARK_ALGOS
      write_algo_times(-1, timing_params->num_iterations);
    #endif  // BENCHMARK_ALGOS
    #ifdef BENCHMARK_SUBFUNCS
      write_build_subfunc_times(-1, timing_params->num_iterations);
      write_probe_subfunc_times(-1, timing_params->num_iterations);
    #endif  // BENCHMARK_SUBFUNCS
  #endif  // BENCHMARK_PER_QUERY

  printf("\33[2K \rFinished\n");
  free_benchmarks(benchmarks);
  free(timing_params);
}
