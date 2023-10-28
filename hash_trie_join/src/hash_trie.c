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
    if (hash_trie->it.node->buckets[hash_trie->it.node->idx].ptr.list != NULL)
      return true;
  }
  return false;
}

bool lookup(hash_trie_t *hash_trie, const __uint64_t *hash)
{
  uint64_t init_idx = *hash >> (hash_trie->it.node->shift);
  trie_node_t *node = hash_trie->it.node;
  node->idx = init_idx;
  while (node->buckets[node->idx].ptr.list != NULL)
  {
    if (node->buckets[node->idx].hash == *hash)
      return true;
    node->idx = (node->idx + 1) % node_size(node);
  }
  return false;
}

void list_to_csv(tuple_t *node, int benchmark_nr, int N)
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

  // Traverse the linked list and write each tuple's values to the file.
  while (node != NULL)
  {
    fprintf(file, "%s", node->values[0]);
    for (int i = 1; i < N; ++i)
    {
      fprintf(file, "|%s", node->values[i]);
    }
    fprintf(file, "\n");
    node = node->next;
  }

  // Close the file.
  fclose(file);
}

void print_list(tuple_t *node, int N)
{
  if (node == NULL)
  {
    printf("[]\n");
    return;
  }
  tuple_t *head = node;
  if (head == NULL)
  {
    return;
  }
  for (int i = 0; i < N; i++)
  {
    printf("%-20.20s ", head->values[i]);
  }
  printf("\n");
  head = head->next;
  while (head != NULL)
  {
    for (int i = 0; i < N; i++)
    {
      printf("%-20.20s ", head->values[i]);
    }
    head = head->next;
  }
  printf("\n");
}

void print_trie(bucket_child_t M, int level, int N)
{
  printf("Level %d: (%p)\n", level, M.node);

  if (level == N)
  {
    printf("\tList: ");
    print_list(M.list, N);
    return;
  }
  trie_node_t *node = M.node;
  printf("\t| node id: %d | ", node->idx);
  printf("node shift: %lu | ", node->shift);
  printf("node size: %lu | \n", node_size(node));
  for (uint64_t i = 0; i < node_size(node); i++)
  {
    if (node->buckets[i].ptr.node == NULL)
    {
      continue;
    }
    printf("\t| hash: %lu, pointer %p |\n", node->buckets[i].hash,
           node->buckets[i].ptr.node);
  }
  printf("\n");
  for (uint64_t i = 0; i < node_size(node); i++)
  {
    if (node->buckets[i].ptr.node == NULL)
    {
      continue;
    }
    print_trie(node->buckets[i].ptr, level + 1, N);
  }
}

void destroy_tries(hash_trie_t *tries, int num_relations)
{
  for (int i = 0; i < num_relations; ++i)
  {
    trie_destructor(tries[i].it.node, &tries[i].num_join_attributes);
    free(tries[i].stack);
  }
  free(tries);
}
