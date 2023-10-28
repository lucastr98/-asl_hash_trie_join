#include "hash_trie_join_probe.h"
#include "tsc_x86.h"
#include <string.h>

#ifdef DEBUG
  #define DEBUG_PRINT printf
#else
  #define DEBUG_PRINT
#endif  // DEBUG

#ifdef BENCHMARK_SUBFUNCS
  probe_subfunc_times_t probe_subfunc_times;

  void init_probe_subfunc_times()
  {
    probe_subfunc_times.select_participants = 0;
    probe_subfunc_times.find_hashes = 0;
    probe_subfunc_times.check_and_produce_result = 0;
    probe_subfunc_times.up_down = 0;
  }

  void print_probe_subfunc_times(int query_nr, int num_iterations)
  {
    if (query_nr == -1)
    {
      printf("All benchmarks SELECT PARTICIPANTS time:      %lu\n",
          probe_subfunc_times.select_participants / num_iterations);
      printf("All benchmarks FIND HASHES time:              %lu\n",
          probe_subfunc_times.find_hashes / num_iterations);
      printf("All benchmarks CHECK AND PRODUCE RESULT time: %lu\n",
          probe_subfunc_times.check_and_produce_result / num_iterations);
      printf("All benchmarks UP DOWN time:                  %lu\n",
          probe_subfunc_times.up_down / num_iterations);
    }
    else
    {
      printf("Benchmark %u SELECT PARTICIPANTS time:      %lu\n", 
          query_nr, probe_subfunc_times.select_participants / num_iterations);
      printf("Benchmark %u FIND HASHES time:              %lu\n", 
          query_nr, probe_subfunc_times.find_hashes / num_iterations);
      printf("Benchmark %u CHECK AND PRODUCE RESULT time: %lu\n", 
          query_nr, probe_subfunc_times.check_and_produce_result / num_iterations);
      printf("Benchmark %u UP DOWN time:                  %lu\n", 
          query_nr, probe_subfunc_times.up_down / num_iterations);
    }
  }

  void write_probe_subfunc_times(int query_nr, int num_iterations)
  {
    if (query_nr == -1)
    {
      char filename[100];
      sprintf(filename, "../../test/times/probe_subfunc_times_all.txt");

      // Open the file for writing.
      FILE *file = fopen(filename, "w");
      if (file == NULL)
      {
        perror("Error opening file");
        exit(1);
      }

      fprintf(file, "%lu\n", 
          probe_subfunc_times.select_participants / num_iterations);
      fprintf(file, "%lu\n", 
          probe_subfunc_times.find_hashes / num_iterations);
      fprintf(file, "%lu\n", 
          probe_subfunc_times.check_and_produce_result / num_iterations);
      fprintf(file, "%lu\n", 
          probe_subfunc_times.up_down / num_iterations);
    }
    else
    {
      char filename[100];
      sprintf(filename, "../../test/times/probe_subfunc_times_%u.txt", query_nr);

      // Open the file for writing.
      FILE *file = fopen(filename, "w");
      if (file == NULL)
      {
        perror("Error opening file");
        exit(1);
      }
      
      fprintf(file, "%lu\n", 
          probe_subfunc_times.select_participants / num_iterations);
      fprintf(file, "%lu\n", 
          probe_subfunc_times.find_hashes / num_iterations);
      fprintf(file, "%lu\n", 
          probe_subfunc_times.check_and_produce_result / num_iterations);
      fprintf(file, "%lu\n", 
          probe_subfunc_times.up_down / num_iterations);
    }
  }
#endif  // BENCHMARK_SUBFUNCS

// m: number of relations
// hash_tries: vector of hash tries of all relations
// mask: mask with the i-th bit indicating whether the i-th join attribute 
//       already came up 
// join_atts: stores the current value of the i-th join_attribute
// result_tuples: pointer to (bcs passed from hash_trie_join) an array of size_t 
//                holding indices to the tuples of each relation participating 
//                in a result tuple
// head_idx: index into result array
void get_valid_tuples(const int m, const hash_trie_t *hash_trie, __int64_t mask, 
    size_t *join_atts, size_t **result_tuples, size_t *head_index)
{
  int j = 0;    // idx into relation that is currently considered
  size_t *current_tuples = malloc(m * sizeof(size_t));
  hash_trie_t *relation = (hash_trie_t *)&hash_trie[j];
  size_t last_lvl = relation->nFields - 1;
  size_t prev_node_idx = relation->node_indices[last_lvl];
  size_t prev_bucket_idx = relation->bucket_indices[last_lvl][prev_node_idx];
  current_tuples[j] = relation->children[last_lvl][prev_node_idx][prev_bucket_idx];
  while (true)
  {
    bool recurse = false;
    (*result_tuples)[(*head_index) * m + j] = current_tuples[j];

    for (int i = 0; i < relation->nFields; ++i)
    {
      int a = relation->join_numbers[i];
      if (contains(&mask, a))
      {
        // previous hash_trie already had this attribute
        if (join_atts[a] == relation->values[relation->nFields * 
            current_tuples[j] + i])
        {
          // this is an attribute that can be joined over
          recurse = true;
        }
        else
        {
          // join is empty (break without setting recurse)
          recurse = false;
          break;
        }
      }
      else
      {
        // first hash_trie that contains this attribute
        set(&mask, a);
        join_atts[a] = relation->values[
            relation->nFields * current_tuples[j] + i];
      }
    }

    if (recurse && (j == m - 1))
    {
      // store
      memcpy(&(*result_tuples)[((*head_index) + 1) * m],
          &(*result_tuples)[(*head_index) * m],
          sizeof(size_t) * (m - 1));
      *head_index = *head_index + 1;
    }
    else if ((recurse && (j != m - 1)) || (!recurse && j == 0))
    {
      // recurse
      relation = (hash_trie_t *)&hash_trie[++j];
      last_lvl = relation->nFields - 1;
      prev_node_idx = relation->node_indices[last_lvl];
      prev_bucket_idx = relation->bucket_indices[last_lvl][prev_node_idx];
      current_tuples[j] = relation->children[last_lvl][prev_node_idx][prev_bucket_idx];
      continue;
    }

    // next
    current_tuples[j] = relation->next[current_tuples[j]];
    while (current_tuples[j] == -1)
    {
      if (--j == -1)
      {
        free(current_tuples);
        return;
      }
      relation = (hash_trie_t *)&hash_trie[j];
      current_tuples[j] = relation->next[current_tuples[j]];
    }
  }
  free(current_tuples);
}

void enumerate_cleanup(int n, int ***I_join, int **I_scan, 
    int **num_relations_per_join_nr)
{
  for (int join_nr = 0; join_nr < n; ++join_nr)
    free((*I_join)[join_nr]);
  free(*I_join);
  free(*I_scan);
  free(*num_relations_per_join_nr);
}

// n: total number of join attributes
// m: number of relations
// relations: array of all relations represented as a hash trie
// result: points to first element of linked list holding result tuples
// head_idx: index into result array
void enumerate(int n, int m, hash_trie_t *relations, 
    size_t **result, size_t *head_index)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  // precompute I_join that stores relation indices for every join_nr
  int *num_relations_per_join_nr = malloc(n * sizeof(int));
  int **I_join = malloc(n * sizeof(int *));
  for (int join_nr = 0; join_nr < n; ++join_nr)
  {
    I_join[join_nr] = malloc(m * sizeof(int));
    num_relations_per_join_nr[join_nr] = 0;
  }
  for (int relation_nr = 0; relation_nr < m; ++relation_nr)
  {
    for (int join_nr_idx = 0; 
         join_nr_idx < relations[relation_nr].nFields; 
         ++join_nr_idx)
    {
      int current_join_nr = relations[relation_nr].join_numbers[join_nr_idx];
      I_join[current_join_nr][num_relations_per_join_nr[current_join_nr]++] = 
          relation_nr;
    }
  }
  for (int join_nr = 0; join_nr < n; ++join_nr)
    I_join[join_nr] = realloc(I_join[join_nr], 
        num_relations_per_join_nr[join_nr] * sizeof(int));

  // setup I_scan vector that stores nr of current scan relation for each join_nr
  int *I_scan = malloc(n * sizeof(int));

  #ifdef BENCHMARK_SUBFUNCS
    probe_subfunc_times.select_participants += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS

  // big fat while loop
  int i = 0;
  while (true)
  {
    for (; i < n; ++i)
    {
      // update I_scan
      #ifdef BENCHMARK_SUBFUNCS
        uint64_t start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      int min_node_size = INT_MAX;
      for (int get_relation_idx = 0; 
          get_relation_idx < num_relations_per_join_nr[i];
          ++get_relation_idx)
      {
        int relation_idx = I_join[i][get_relation_idx];
        hash_trie_t *relation = &relations[relation_idx];
        size_t relation_lvl = relation->lvl;
        uint64_t current_node_size = node_size(
            relation->shifts[relation_lvl][relation->node_indices[relation_lvl]]);
        if (current_node_size < min_node_size)
        {
          I_scan[i] = relation_idx;
          min_node_size = current_node_size;
        }
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.select_participants += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS

      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      bool skip = false;
      for (int get_relation_idx = 0; 
           get_relation_idx < num_relations_per_join_nr[i];
           ++get_relation_idx)
      {
        int relation_idx = I_join[i][get_relation_idx];
        if (relation_idx == I_scan[i])
          continue;

        hash_trie_t *relation = &relations[relation_idx];
        hash_trie_t *scan_relation = &relations[I_scan[i]];
        size_t scan_relation_lvl = scan_relation->lvl;
        size_t scan_node_idx = scan_relation->node_indices[scan_relation_lvl];
        size_t scan_bucket_idx = scan_relation->bucket_indices[scan_relation_lvl][scan_node_idx];
        __uint64_t hash = scan_relation->hashes[scan_relation_lvl][scan_node_idx][scan_bucket_idx];
        if (!lookup(relation, hash))
        {
          // lookup not successful should go to next in scan relation
          skip = true;
          break;
        }
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.find_hashes += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS

      // if skip then go to next in I_scan and then continue
      if (skip)
      {
        if (next(&relations[I_scan[i]]))
        {
          // decrement i to continue with the previous i in the next for loop iteration
          --i;
          continue;
        }
        else if (i == 0)
        {
          enumerate_cleanup(n, &I_join, &I_scan, &num_relations_per_join_nr);
          return;
        }
        else
          break;
      }

      // down
      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      for (int get_relation_idx = 0; 
           get_relation_idx < num_relations_per_join_nr[i];
           ++get_relation_idx)
      {
        down(&relations[I_join[i][get_relation_idx]]);
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.up_down += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS
    }

    // check and produce result
    if (i == n)
    {
      __int64_t mask = 0;
      size_t *join_atts = calloc(n, sizeof(size_t));
      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      get_valid_tuples(m, relations, mask, join_atts, result, head_index);
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.check_and_produce_result += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS
      free(join_atts);
    }


    // up
    for (--i; i >= 0; --i)
    {
      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      for (int get_relation_idx = 0; 
           get_relation_idx < num_relations_per_join_nr[i];
           ++get_relation_idx)
      {
        int relation_idx = I_join[i][get_relation_idx];
        up(&relations[relation_idx]);
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.up_down += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS
      
      if (next(&relations[I_scan[i]]))
        break;
      else if (i == 0)
      {
        enumerate_cleanup(n, &I_join, &I_scan, &num_relations_per_join_nr);
        return;
      }
    }
  }

  // free
  enumerate_cleanup(n, &I_join, &I_scan, &num_relations_per_join_nr);
}
