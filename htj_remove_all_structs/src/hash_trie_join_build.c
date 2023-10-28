#include "hash_trie_join_build.h"
#include <string.h>

#ifdef BENCHMARK_SUBFUNCS
  build_subfunc_times_t build_subfunc_times;

  void init_build_subfunc_times()
  {
    build_subfunc_times.get_list_length = 0;
    build_subfunc_times.allocate_hash_table = 0;
    build_subfunc_times.hash = 0;
    build_subfunc_times.lookup_bucket = 0;
  }

  void print_build_subfunc_times(int query_nr, int num_iterations)
  {
    if (query_nr == -1)
    {
      printf("All benchmarks GET LIST LENGTH time:     %lu\n",
          build_subfunc_times.get_list_length / num_iterations);
      printf("All benchmarks ALLOCATE HASH TABLE time: %lu\n",
          build_subfunc_times.allocate_hash_table / num_iterations);
      printf("All benchmarks HASH time:                %lu\n",
          build_subfunc_times.hash / num_iterations);
      printf("All benchmarks LOOKUP BUCKET time:       %lu\n",
          build_subfunc_times.lookup_bucket / num_iterations);
    }
    else
    {
      printf("Benchmark %u GET LIST LENGTH time:     %lu\n", 
          query_nr, build_subfunc_times.get_list_length / num_iterations);
      printf("Benchmark %u ALLOCATE HASH TABLE time: %lu\n", 
          query_nr, build_subfunc_times.allocate_hash_table / num_iterations);
      printf("Benchmark %u HASH time:                %lu\n", 
          query_nr, build_subfunc_times.hash / num_iterations);
      printf("Benchmark %u LOOKUP BUCKET time:       %lu\n",
          query_nr, build_subfunc_times.lookup_bucket / num_iterations);
    }
  }

  void write_build_subfunc_times(int query_nr, int num_iterations)
  {
    if (query_nr == -1)
    {
      char filename[100];
      sprintf(filename, "../../test/times/build_subfunc_times_all.txt");

      // Open the file for writing.
      FILE *file = fopen(filename, "w");
      if (file == NULL)
      {
        perror("Error opening file");
        exit(1);
      }

      fprintf(file, "%lu\n", 
          build_subfunc_times.get_list_length / num_iterations);
      fprintf(file, "%lu\n", 
          build_subfunc_times.allocate_hash_table / num_iterations);
      fprintf(file, "%lu\n", 
          build_subfunc_times.hash/ num_iterations);
      fprintf(file, "%lu\n", 
          build_subfunc_times.lookup_bucket / num_iterations);
    }
    else
    {
      char filename[100];
      sprintf(filename, "../../test/times/build_subfunc_times_%u.txt", query_nr);

      // Open the file for writing.
      FILE *file = fopen(filename, "w");
      if (file == NULL)
      {
        perror("Error opening file");
        exit(1);
      }

      fprintf(file, "%lu\n", 
          build_subfunc_times.get_list_length / num_iterations);
      fprintf(file, "%lu\n", 
          build_subfunc_times.allocate_hash_table / num_iterations);
      fprintf(file, "%lu\n", 
          build_subfunc_times.hash/ num_iterations);
      fprintf(file, "%lu\n", 
          build_subfunc_times.lookup_bucket / num_iterations);
    }
  }
#endif  // BENCHMARK_SUBFUNCS

size_t get_list_length(size_t *next, size_t head_index)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  size_t n = 0;
  size_t head = head_index;
  while (head != -1)
  {
    n++;
    head = next[head];
  }
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.get_list_length += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS
  return n;
}

// pretty f'in sketch if you ask me
uint64_t hash(void *key, int len)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  uint64_t retVal[2];
  MurmurHash3_x86_128(key, len, SEED, retVal);
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.hash = stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS
  return retVal[0];
}

uint64_t lookupBucket(int lvl, size_t node_idx, hash_trie_t *trie, uint64_t hash)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  uint64_t n_size = node_size(trie->shifts[lvl][node_idx]);
  uint64_t bucket_index = hash >> (trie->shifts[lvl][node_idx]);
  if (trie->children[lvl][node_idx][bucket_index] == -1)
  {
    trie->hashes[lvl][node_idx][bucket_index] = hash;
  }

  // linear probing
  while (hash != trie->hashes[lvl][node_idx][bucket_index])
  {
    // increment bucket mod node size
    bucket_index = (bucket_index + 1) % n_size;

    // if bucket is empty set hash (breaks the loop)
    if (trie->children[lvl][node_idx][bucket_index] == -1)
    {
      trie->hashes[lvl][node_idx][bucket_index] = hash;
    }
  }
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.lookup_bucket += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS
  return bucket_index;
}

size_t *build(size_t head_index, hash_trie_t *trie)
{
  size_t *next = trie->next;
  size_t *values = trie->values;
  size_t nFields = trie->nFields;

  // current node level
  int i = 0;
  size_t *num_nodes_per_lvl = calloc(nFields, sizeof(size_t));
  num_nodes_per_lvl[i] = 1;

  // create root node
  // next two lines equal pow(2, ceil(log2(1.25 * get_list_length(L, head_index))));
  int p = 64 - __builtin_clzll(1.25 * get_list_length(next, head_index));
  size_t num_buckets = (1 << p);
  if (i < nFields - 1)
    num_nodes_per_lvl[i + 1] += num_buckets;

  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  // lvl 0 will have only one node, lvl 1 will have num_buckets nodes
  trie->shifts[0] = malloc(sizeof(__uint64_t));
  trie->shifts[0][0] = 64 - p;

  // in all nodes the first bucket is currently considered
  trie->bucket_indices[0] = calloc(1, sizeof(size_t));

  // allocate the memory for lvl 0 hashes and children indices (indicating node
  // or tuple nr) where we have one node and num_buckets buckets
  trie->hashes[0] = malloc(sizeof(__uint64_t *));
  trie->hashes[0][0] = malloc(num_buckets * sizeof(__uint64_t));
  trie->children[0] = malloc(sizeof(size_t *));
  trie->children[0][0] = malloc(num_buckets * sizeof(size_t));
  for (int j = 0; j < num_buckets; ++j)
    trie->children[0][0][j] = -1;
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.allocate_hash_table += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS

  // set return val as root node
  size_t head = head_index;
  while (head != -1)
  {
    // pop next tuple from L
    size_t current = head;
    head = next[head];
    next[current] = -1;
    size_t tuple_bucket_idx = lookupBucket(i, 0, trie, 
        hash(&values[current * nFields + i], sizeof(size_t)));

    // extract linked list stored in B
    next[current] = trie->children[i][0][tuple_bucket_idx];
    trie->children[i][0][tuple_bucket_idx] = current;
  }

  for (i = 1; i < nFields; ++i)
  {
    // allocate memory for i
    trie->shifts[i] = calloc(num_nodes_per_lvl[i], sizeof(__uint64_t));
    trie->bucket_indices[i] = calloc(num_nodes_per_lvl[i], sizeof(size_t));
    trie->hashes[i] = malloc(num_nodes_per_lvl[i] * sizeof(__uint64_t *));
    trie->children[i] = malloc(num_nodes_per_lvl[i] * sizeof(size_t *));

    // now fill memory for i by allocating hash table and filling buckets
    for (int prev_node_nr = 0; prev_node_nr < num_nodes_per_lvl[i - 1]; 
        ++prev_node_nr)
    {
      size_t node_nr = -1;
      size_t prev_node_sz = node_size(trie->shifts[i - 1][prev_node_nr]);
      for (int prev_bucket_nr = 0; prev_bucket_nr < prev_node_sz; 
          ++prev_bucket_nr)
      {
        // skip empty buckets
        if (trie->children[i - 1][prev_node_nr][prev_bucket_nr] == -1)
          continue;
        
        // increment node number
        ++node_nr;
        
        // get size of child node of current bucket
        head_index = trie->children[i - 1][prev_node_nr][prev_bucket_nr];
        p = 64 - __builtin_clzll(1.25 * get_list_length(next, head_index));
        num_buckets = (1 << p);

        // if not on the last join attribute lvl update number of possible buckets
        if (i < nFields - 1)
          num_nodes_per_lvl[i + 1] += num_buckets;
        
        // allocate hash table
        #ifdef BENCHMARK_SUBFUNCS
          uint64_t start = start_tsc();
        #endif  // BENCHMARK_SUBFUNCS
        trie->shifts[i][node_nr] = 64 - p;
        trie->hashes[i][node_nr] = malloc(num_buckets * sizeof(__uint64_t));
        trie->children[i][node_nr] = malloc(num_buckets * sizeof(size_t));
        for (int j = 0; j < num_buckets; ++j)
          trie->children[i][node_nr][j] = -1;
        #ifdef BENCHMARK_SUBFUNCS
          build_subfunc_times.allocate_hash_table += stop_tsc(start);
        #endif  // BENCHMARK_SUBFUNCS

        // fill hash table
        size_t head = head_index;
        while (head != -1)
        {
          // pop next tuple from L
          size_t current = head;
          head = next[head];
          next[current] = -1;
          size_t tuple_bucket_idx = lookupBucket(i, node_nr, trie, 
              hash(&values[current * nFields + i], sizeof(size_t)));

          // extract linked list stored in B
          next[current] = trie->children[i][node_nr][tuple_bucket_idx];
          trie->children[i][node_nr][tuple_bucket_idx] = current;
          trie->children[i - 1][prev_node_nr][prev_bucket_nr] = node_nr;
        }
      }
    }
  }
  return num_nodes_per_lvl;
}

// recursive helper function to destroy
// void destroy_rec(bucket_child_t M, int level, const int *N)
// {
//   if (level == *N)
//   {
//     return;
//   }
//   trie_node_t *node = M.node;
//   for (uint64_t i = 0; i < node_size(node); i++)
//   {
//     if (node->buckets[i].ptr.list == -1)
//     {
//       continue;
//     }
//     destroy_rec(node->buckets[i].ptr, level + 1, N);
//   }
//   free(node->buckets);
//   free(node);
// }

// void trie_destructor(trie_node_t *M, const int *N)
// {
//   bucket_child_t x;
//   x.node = M;
//   destroy_rec(x, 0, N);
// }
