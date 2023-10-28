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
  while (++hash_trie->it.node->idx < node_size(hash_trie->it.node))
  {
    if (hash_trie->it.node->buckets[hash_trie->it.node->idx].ptr.list != -1)
      return true;
  }
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

void list_to_csv(tuple_t *tuples, int benchmark_nr, size_t head_index)
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
  if(tuples->values[0] == NULL) {
    fclose(file);
    return;
  }
  size_t *next = tuples->next;
  char **values = tuples->values;
  size_t head = head_index;
  size_t nFields = tuples->nFields;
  size_t nTuples = tuples->nTuples;
  // Traverse the linked list and write each tuple's values to the file.
  while (head != -1)
  {
    // printf("%s\n", tuples->values[j*nFields]);
    fprintf(file, "%s", tuples->values[head*nFields]);
    for (int i = 1; i < nFields; ++i)
    {
      fprintf(file, "|%s", tuples->values[head*nFields + i]);
    }

    fprintf(file, "\n");
    head = next[head];
    // printf("%lu\n", head);
  }

  // Close the file.
  fclose(file);
}

void print_list(tuple_t *tuples, size_t head_index)
{  
  if (tuples == NULL)
  {
    printf("[]\n");
    return;
  }
  size_t head = head_index;
  if (head == -1)
  {
    return;
  }

  size_t nFields = tuples->nFields;
  char **values = tuples->values;
  size_t *next = tuples->next;


  for (int i = 0; i < nFields; i++)
  {
    printf("%-20.20s ", values[head*nFields+i]);
  }
  printf("\n");
  head = next[head];
  while (head != -1)
  {
    for (int i = 0; i < nFields; i++)
    {
      printf("%-20.20s ", values[head*nFields + i]);
    }
    head = next[head];
  }
  printf("\n");
}

void print_trie_rec(bucket_child_t M, int level, int N, tuple_t *tuple_list)
{
  printf("Level %d: (%p)\n", level, M.node);
  if (level == N)
  {
    printf("\tList length: ");
    print_list(tuple_list, M.list);
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
    print_trie_rec(node->buckets[i].ptr, level + 1, N, tuple_list);
  }
}

void print_trie(hash_trie_t *trie) {
  print_trie_rec(trie->it, 0, trie->num_join_attributes, trie->tuple_list);
}


void destroy_result_list(tuple_t *tuples)
{
  free(tuples->values);
  free(tuples->next);
  free(tuples);
}

void destroy_tuples(tuple_t *tuples) {
  size_t nFields = tuples->nFields;
  size_t nTuples = tuples->nTuples;
  for(int i = 0; i < nFields*nTuples; i++) {
    free(tuples->values[i]);
  }
  free(tuples->values);
  free(tuples->next);
  free(tuples);
}

void destroy_tries(hash_trie_t *tries, int num_relations)
{
  for (int i = 0; i < num_relations; ++i)
  {
    // destroy_tuples(tries[i].tuple_list);
    trie_destructor(tries[i].it.node, &tries[i].num_join_attributes);
    free(tries[i].stack);
  }
  free(tries);
}
