#include "MurmurHash3.h"
#include "hash_trie.h"
#include "hash_trie_join_build.h"

void up(hash_trie_t *trie)
{
  --trie->lvl;
}

void down(hash_trie_t *trie)
{
  size_t lvl = trie->lvl;
  size_t node_idx = trie->node_indices[lvl];
  size_t bucket_idx = trie->bucket_indices[lvl][node_idx];
  trie->node_indices[lvl + 1] = trie->children[lvl][node_idx][bucket_idx];
  ++trie->lvl;
}

bool next(hash_trie_t *trie)
{
  size_t lvl = trie->lvl;
  size_t node_idx = trie->node_indices[lvl];
  uint64_t n_size = node_size(trie->shifts[lvl][node_idx]);
  size_t idx_before = trie->bucket_indices[lvl][node_idx];
  while (++trie->bucket_indices[lvl][node_idx] < n_size)
  {
    if (trie->children[lvl][node_idx][trie->bucket_indices[lvl][node_idx]] != -1)
    {
      return true;
    }
  }
  trie->bucket_indices[lvl][node_idx] = idx_before;
  return false;
}

bool lookup(hash_trie_t *trie, __uint64_t hash)
{
  size_t lvl = trie->lvl;
  size_t node_idx = trie->node_indices[lvl];
  uint64_t n_size = node_size(trie->shifts[lvl][node_idx]);
  uint64_t bucket_idx = hash >> (trie->shifts[lvl][node_idx]);
  while (trie->children[lvl][node_idx][bucket_idx] != -1)
  {
    if (trie->hashes[lvl][node_idx][bucket_idx] == hash)
    {
      trie->bucket_indices[lvl][node_idx] = bucket_idx;
      return true;
    }
    bucket_idx = (bucket_idx + 1) % n_size;
  }
  return false;
}

void list_to_csv(size_t *result, join_t *join, int benchmark_nr, size_t head_index)
{
  char filename[100];
  sprintf(filename, "../../test/results/result_%u.csv", benchmark_nr);

  // Open the file for writing.
  FILE *file = fopen(filename, "w");
  if (file == NULL)
  {
    perror("Error opening file");
    exit(1);
  }
  // if (tuples->values[0] == NULL) {
  if (head_index == 0) {
    fclose(file);
    return;
  }

  size_t head = head_index;
  int m = join->num_relations;
  while (head != -1)
  {
    for (int i = 0; i < join->num_relations; ++i)
    {
      tuple_t *current_tuple_list = join->relations[i].tuple_list;
      join_tuple_t *current_join_tuple_list = join->relations[i].join_tuple_list;
      int subtracter = 0;
      for (int j = 0; j < current_tuple_list->nFields + 
          current_join_tuple_list->nFields; ++j)
      {
        if ((i == 0) && (j == 0))
        {
          if (join->relations[i].join_att_indices[j] != -1)
          {
            fprintf(file, "%ld", current_join_tuple_list->values[
                result[head * m] * current_join_tuple_list->nFields +
                join->relations[i].join_att_indices[j]]);
            ++subtracter;
          }
          else
          {
            fprintf(file, "%s", current_tuple_list->values[
                result[head * m] * current_tuple_list->nFields]);
          }
          continue;
        }
        
        if (join->relations[i].join_att_indices[j] != -1)
        {
          fprintf(file, "|%ld", current_join_tuple_list->values[
              result[head * m + i] * current_join_tuple_list->nFields + 
              join->relations[i].join_att_indices[j]]);
          ++subtracter;
        }
        else
        {
          fprintf(file, "|%s", current_tuple_list->values[
              result[head * m + i] * current_tuple_list->nFields + j - subtracter]);
        }
      }
    }
    fprintf(file, "\n");
    --head;
  }

  // Close the file.
  fclose(file);
}

void print_list(join_tuple_t *join_tuples, size_t head_index)
{  
  if (join_tuples == NULL)
  {
    printf("[]\n");
    return;
  }
  size_t head = head_index;
  if (head == -1)
  {
    return;
  }

  size_t nFields = join_tuples->nFields;
  size_t *values = join_tuples->values;
  size_t *next = join_tuples->next;

  for (int i = 0; i < nFields; i++)
  {
    // printf("%-20.20lu ", values[head*nFields+i]);
    printf("%lu ", values[head*nFields+i]);
  }
  printf("\n");
  head = next[head];
  while (head != -1)
  {
    printf("we have a linked list!\n");
    for (int i = 0; i < nFields; i++)
    {
      // printf("%-20.20lu ", values[head*nFields + i]);
      printf("%lu ", values[head*nFields+i]);
    }
    head = next[head];
  }
  printf("\n");
}

// void print_trie_rec(bucket_child_t M, int level, int N, join_tuple_t *join_tuple_list)
// {
//   printf("Level %d: (%p)\n", level, M.node);
//   if (level == N)
//   {
//     printf("\tList length: ");
//     print_list(join_tuple_list, M.list);
//     return;
//   }
//   trie_node_t *node = M.node;
//   printf("\t| node id: %d | ", node->idx);
//   printf("node shift: %lu | ", node->shift);
//   printf("node size: %lu | \n", node_size(node));
//   // for (uint64_t i = 0; i < node_size(node); i++)
//   // {
//   //   if (node->buckets[i].ptr.list == -1)
//   //   {
//   //     continue;
//   //   }
//   //   printf("\t| hash: %lu, pointer %p |\n", node->buckets[i].hash,
//   //          node->buckets[i].ptr.node);
//   // }
//   printf("\n");
//   for (uint64_t i = 0; i < node_size(node); i++)
//   {
//     if (node->buckets[i].ptr.list == -1)
//     {
//       continue;
//     }
//     print_trie_rec(node->buckets[i].ptr, level + 1, N, join_tuple_list);
//   }
// }

// void print_trie(hash_trie_t *trie) {
//   print_trie_rec(trie->it, 0, trie->num_join_attributes, trie->join_tuple_list);
// }

void destroy_join_tuples(join_tuple_t *tuples) {
  free(tuples->values);
  free(tuples->next);
  free(tuples);
}

void destroy_tuples(join_t *join) {
  for (int i = 0; i < join->num_relations; ++i)
  {
    for (int j = 0; j < join->relations[i].tuple_list->nFields * 
        join->relations[i].tuple_list->nTuples; ++j)
    {
      free(join->relations[i].tuple_list->values[j]);
    }
    free(join->relations[i].tuple_list->values);
    free(join->relations[i].tuple_list);
  }
}

void destroy_tries(hash_trie_t *tries, size_t **num_nodes_per_level_vec, 
    int num_relations)
{
  for (int i = 0; i < num_relations; ++i)
  {
    for (int lvl = 0; lvl < tries[i].nFields; ++lvl)
    {
      for (int node = 0; node < num_nodes_per_level_vec[i][lvl]; ++node)
      {
        if (tries[i].shifts[lvl][node] != 0)
        {
          free(tries[i].hashes[lvl][node]);
          free(tries[i].children[lvl][node]);
        }
      }
      free(tries[i].shifts[lvl]);
      free(tries[i].bucket_indices[lvl]);
      free(tries[i].hashes[lvl]);
      free(tries[i].children[lvl]);
    }
    // free(tries[i].values);
    // free(tries[i].next);
    free(tries[i].node_indices);
    free(tries[i].shifts);
    free(tries[i].bucket_indices);
    free(tries[i].hashes);
    free(tries[i].children);
    free(num_nodes_per_level_vec[i]);
  }
  free(tries);
  free(num_nodes_per_level_vec);
}
