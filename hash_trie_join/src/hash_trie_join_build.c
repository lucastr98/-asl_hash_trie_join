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

int get_list_length(tuple_t *node)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  int n = 0;
  tuple_t *head = node;
  while (head != NULL)
  {
    n++;
    head = head->next;
  }
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.get_list_length += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS
  return n;
}

trie_node_t *allocateHashTable(size_t pow2_p)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  trie_node_t *hash_table = (trie_node_t *)malloc(sizeof(trie_node_t));
  size_t p = ceil(log2(pow2_p));

  hash_table->buckets = (trie_bucket_t *)malloc(sizeof(trie_bucket_t) * pow2_p);
  for (int i = 0; i < pow2_p; i++)
  {
    hash_table->buckets[i].hash = 0;
    hash_table->buckets[i].ptr.list = NULL;
  }
  // this could be in the build function as well
  // TODO change!!
  hash_table->shift = 64 - p;
  hash_table->idx = 0;
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.allocate_hash_table += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS
  return hash_table;
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

trie_bucket_t *lookupBucket(trie_node_t *M, uint64_t hash)
{
  #ifdef BENCHMARK_SUBFUNCS
    uint64_t start = start_tsc();
  #endif  // BENCHMARK_SUBFUNCS
  uint64_t n_size = node_size(M);
  uint64_t initial_index = hash >> (M->shift);
  uint64_t bucket_index = initial_index;
  trie_bucket_t *B = &(M->buckets[bucket_index]);
  if (B->ptr.list == NULL)
  {
    B->hash = hash;
  }
  // linear probing
  while (hash != B->hash)
  {
    // increment bucket mod node size
    bucket_index = (bucket_index + 1) % n_size;
    B = &(M->buckets[bucket_index]);

    // sanity check. Pretty sure we set the size so that it isn't possible to fill
    if (initial_index == bucket_index)
    {
      fprintf(stderr, "node filled");
      return NULL;
    }

    // if bucket is empty set hash (breaks the loop)
    if (B->ptr.list == NULL)
    {
      B->hash = hash;
    }
  }
  #ifdef BENCHMARK_SUBFUNCS
    build_subfunc_times.lookup_bucket += stop_tsc(start);
  #endif  // BENCHMARK_SUBFUNCS
  return B;
}

bucket_child_t build(int i, tuple_t *L, const int *N, const int *attributes)
{
  bucket_child_t trie_node;
  if (i < *N)
  {
    size_t num_buckets = (size_t)pow(2, ceil(log2(1.25 * get_list_length(L))));
    trie_node_t *M = allocateHashTable(num_buckets);
    tuple_t *head = L;
    while (head != NULL)
    {
      // pop next tuple from L
      tuple_t *tuple = head;
      head = head->next;
      tuple->next = NULL;
      trie_bucket_t *B = lookupBucket(M, hash(tuple->values[attributes[i]],
                                              sizeof(char) * strlen(tuple->values[attributes[i]])));
      // extract linked list stored in B
      tuple_t *list = B->ptr.list;
      tuple->next = list;
      B->ptr.list = tuple;
    }
    trie_bucket_t *buckets = M->buckets;

    for (int j = 0; j < num_buckets; j++)
    {
      trie_bucket_t bucket = buckets[j];
      if (bucket.ptr.list != NULL)
      {
        tuple_t *L_next = bucket.ptr.list;
        int i_next = i + 1;
        bucket_child_t M_next = build(i_next, L_next, N, attributes);
        M->buckets[j].ptr = M_next;
      }
    }
    trie_node.node = M;
  }
  else
  {
    trie_node.list = L;
  }
  return trie_node;
}

/* DESTRUCTOR */
void destroy_list(tuple_t *head, size_t array_len)
{
  tuple_t *input_it = head;
  tuple_t *input_it2 = input_it;
  while (input_it != NULL)
  {
    input_it = input_it->next;
    for (int i = 0; i < array_len; i++)
    {
      free(input_it2->values[i]);
    }
    free(input_it2);
    input_it2 = input_it;
  }
}

void destroy_result_list(tuple_t *head)
{
  tuple_t *input_it = head;
  tuple_t *input_it2 = input_it;
  while (input_it != NULL)
  {
    input_it = input_it->next;
    free(input_it2);
    input_it2 = input_it;
  }
}

// recursive helper function to destroy
void destroy_rec(bucket_child_t M, int level, const int *N)
{
  if (level == *N)
  {
    destroy_result_list(M.list);
    return;
  }
  trie_node_t *node = M.node;
  for (uint64_t i = 0; i < node_size(node); i++)
  {
    if (node->buckets[i].ptr.node == NULL)
    {
      continue;
    }
    destroy_rec(node->buckets[i].ptr, level + 1, N);
  }
  free(node->buckets);
  free(node);
}

void trie_destructor(trie_node_t *M, const int *N)
{
  bucket_child_t x;
  x.node = M;
  destroy_rec(x, 0, N);
}
