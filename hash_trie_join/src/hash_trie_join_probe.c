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

void fill_trie(hash_trie_t *trie, relation_t *relation, int *total_num_attributes)
{
  trie->num_attributes = relation->num_attributes;
  trie->num_join_attributes = relation->num_join_attributes;
  trie->stack = createStack(trie->num_join_attributes);
  trie->join_numbers = relation->join_numbers;
  trie->tuple_indices = relation->tuple_indices;
  trie->result_idx = (*total_num_attributes);
  (*total_num_attributes) += relation->num_attributes;
}

bool bin_search(hash_trie_t *relation, int i)
{
  int low = 0;
  int high = relation->num_join_attributes - 1;

  while (low <= high)
  {
    int mid = (low + high) / 2;
    if (relation->join_numbers[mid] == i)
    {
      return true;
    }
    else if (relation->join_numbers[mid] < i)
    {
      low = mid + 1;
    }
    else
    {
      high = mid - 1;
    }
  }

  return false;
}

void get_valid_tuples(int j, const int m, const int num_att, 
    const hash_trie_t *hash_trie, __int64_t mask, 
    char *join_atts[], tuple_t **result_tuples)
{
  hash_trie_t *relation = (hash_trie_t *)&hash_trie[j];
  tuple_t *current_tuple = relation->it.list;
  while (current_tuple != NULL)
  {
    bool recurse = false;
    memcpy((*result_tuples)->values + relation->result_idx, current_tuple->values, 
        relation->num_attributes * sizeof(char *));
    
    for (int i = 0; i < relation->num_join_attributes; ++i)
    {
      int a = relation->join_numbers[i];
      if (contains(&mask, a))
      {
        // previous hash_trie already had this attribute
        if (strcmp(join_atts[a], 
            current_tuple->values[relation->tuple_indices[i]]) == 0)
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
        join_atts[a] = current_tuple->values[relation->tuple_indices[i]];
      }
    }

    if (recurse)
    {
      if (j == m - 1)
      {
        tuple_t *new_tuple = (tuple_t *)malloc(sizeof(tuple_t) + num_att * sizeof(char *));
        memcpy(new_tuple->values, (*result_tuples)->values, 
            num_att * sizeof(char *));
        new_tuple->next = (*result_tuples);
        (*result_tuples) = new_tuple;
      }
      else
      {
        get_valid_tuples(j + 1, m, num_att, hash_trie, mask, join_atts, result_tuples);
      }
    }
    else 
    {
      // first hash_trie -> always recurse
      if (j == 0)
        get_valid_tuples(j + 1, m, num_att, hash_trie, mask, join_atts, result_tuples);
    }
    current_tuple = current_tuple->next;
  }
}

// i: attribute currently considered
// n: total number of join attributes
// num_att: total number of attributes (for result tuple)
// relations: array of all relations represented as a hash trie
// m: number of relations
// result: points to first element of linked list holding result tuples
void enumerate(int i, int n, int num_att, hash_trie_t *relations, int m, 
    tuple_t **result)
{
  if (i < n)
  {
    #ifdef BENCHMARK_SUBFUNCS
      uint64_t start = start_tsc();
    #endif  // BENCHMARK_SUBFUNCS
    // find all participating iterators, store smallest
    int I_join[MAX_PART_IT] = {-1};
    int I_scan, min_node_size = INT_MAX, join_idx = 0;
    for (int j = 0; j < m; ++j)
    {
      hash_trie_t* current_relation = &relations[j];
      if (bin_search(current_relation, i))
      {
        I_join[join_idx++] = j;
        if (node_size(current_relation->it.node) < min_node_size)
        {
          I_scan = j;
          min_node_size = node_size(current_relation->it.node);
        }
        current_relation->it.node->idx = 0;
      }
    }
    DEBUG_PRINT("i = %u, #joiners = %u, I_scan = %u\n", i, join_idx, I_scan);

    // iterate over hashes in smallest hash table
    hash_trie_t *scan_relation = &relations[I_scan];
    if (scan_relation->it.node->buckets[scan_relation->it.node->idx].ptr.list == 
        NULL && !next(scan_relation)) {
      // make use of short-circuit evaluation and go to first valid element in
      // hash table if exists, otherwise return
      return;
    }
    #ifdef BENCHMARK_SUBFUNCS
      probe_subfunc_times.select_participants += stop_tsc(start);
    #endif  // BENCHMARK_SUBFUNCS

    do
    {
      // find hash in remaining hash tables
      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      bool skip = false;
      for (int j = 0; j < join_idx; ++j)
      {
        if (I_join[j] == I_scan)
          continue;
        if (!lookup(&relations[I_join[j]], 
            &scan_relation->it.node->buckets[scan_relation->it.node->idx].hash))
        {
          DEBUG_PRINT("skip: %p, join: %p\n", 
              scan_relation->it.node, relations[I_join[j]].it.node);
          skip = true;
          break;
        }
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.find_hashes += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS

      if (skip)
        continue;

      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      // move to the next trie level
      for (int j = 0; j < join_idx; ++j)
      {
        DEBUG_PRINT("down: attribute = %u, relation = %u\n", i, I_join[j]);
        down(&relations[I_join[j]]);
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.up_down += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS

      // recursively enumerate matching tuples
      enumerate(i + 1, n, num_att, relations, m, result);

      #ifdef BENCHMARK_SUBFUNCS
        start = start_tsc();
      #endif  // BENCHMARK_SUBFUNCS
      // move back to the current trie level
      for (int j = 0; j < join_idx; ++j)
      {
        DEBUG_PRINT("up: attribute = %u, relation = %u\n", i, I_join[j]);
        up(&relations[I_join[j]]);
      }
      #ifdef BENCHMARK_SUBFUNCS
        probe_subfunc_times.up_down += stop_tsc(start);
      #endif  // BENCHMARK_SUBFUNCS
    } while (next(scan_relation));
  }
  else
  {
    // really implement some sort of a cartesian product here
    __int64_t mask = 0;
    char *join_atts[n];
    #ifdef BENCHMARK_SUBFUNCS
      uint64_t start = start_tsc();
    #endif  // BENCHMARK_SUBFUNCS
    get_valid_tuples(0, m, num_att, relations, mask, join_atts, result);
    #ifdef BENCHMARK_SUBFUNCS
      probe_subfunc_times.check_and_produce_result += stop_tsc(start);
    #endif  // BENCHMARK_SUBFUNCS
  }
}
