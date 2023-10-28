#include "MurmurHash3.h"
#include "hash_trie.h"
#include "hash_trie_join_build.h"

stack_t *createStack(int num_attributes)
{
  stack_t *s = malloc(sizeof(stack_t) + num_attributes * sizeof(trie_node_t *));
  s->top = -1;
  return s;
}

void push(stack_t *s, trie_node_t *new_item)
{
  s->items[++s->top] = new_item;
}

trie_node_t *pop(stack_t *s)
{
  return s->items[s->top--];
}

void up(hash_trie_t *hash_trie)
{
  hash_trie->it.node = pop(hash_trie->stack);
}

void down(hash_trie_t *hash_trie)
{
  push(hash_trie->stack, hash_trie->it.node);
  hash_trie->it = hash_trie->it.node->buckets[hash_trie->it.node->idx].ptr;
}

bool next(hash_trie_t *hash_trie)
{
  int idx_before = hash_trie->it.node->idx;
  while (++hash_trie->it.node->idx < node_size(hash_trie->it.node))
  {
    if (hash_trie->it.node->buckets[hash_trie->it.node->idx].ptr.list != -1)
    {
      return true;
    }
  }
  hash_trie->it.node->idx = idx_before;
  return false;
}

bool lookup(hash_trie_t *hash_trie, const __uint64_t *hash)
{
  uint64_t init_idx = *hash >> (hash_trie->it.node->shift);
  trie_node_t *node = hash_trie->it.node;
  node->idx = init_idx;
  while (node->buckets[node->idx].ptr.list != -1)
  {
    if (node->buckets[node->idx].hash == *hash)
      return true;
    node->idx = (node->idx + 1) % node_size(node);
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

void print_trie_rec(bucket_child_t M, int level, int N, join_tuple_t *join_tuple_list)
{
  printf("Level %d: (%p)\n", level, M.node);
  if (level == N)
  {
    printf("\tList length: ");
    print_list(join_tuple_list, M.list);
    return;
  }
  trie_node_t *node = M.node;
  printf("\t| node id: %d | ", node->idx);
  printf("node shift: %lu | ", node->shift);
  printf("node size: %lu | \n", node_size(node));
  // for (uint64_t i = 0; i < node_size(node); i++)
  // {
  //   if (node->buckets[i].ptr.list == -1)
  //   {
  //     continue;
  //   }
  //   printf("\t| hash: %lu, pointer %p |\n", node->buckets[i].hash,
  //          node->buckets[i].ptr.node);
  // }
  printf("\n");
  for (uint64_t i = 0; i < node_size(node); i++)
  {
    if (node->buckets[i].ptr.list == -1)
    {
      continue;
    }
    print_trie_rec(node->buckets[i].ptr, level + 1, N, join_tuple_list);
  }
}

void print_trie(hash_trie_t *trie) {
  print_trie_rec(trie->it, 0, trie->num_join_attributes, trie->join_tuple_list);
}

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

void destroy_tries(hash_trie_t *tries, int num_relations)
{
  for (int i = 0; i < num_relations; ++i)
  {
    // destroy_join_tuples(tries[i].join_tuple_list);
    trie_destructor(tries[i].it.node, &tries[i].num_join_attributes);
    free(tries[i].stack);
  }
  free(tries);
}
