#include "partitioning.h"
#include "hash_trie_join_build.h"
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>



void swap_join(uint32_t arr[], int i, int j) {
    uint32_t temp = arr[i];
    arr[i] = arr[j];
    arr[j] = temp;
}

void swap_tup(char *arr[], int i, int j) {
    char* temp = arr[i];
    arr[i] = arr[j];
    arr[j] = temp;
}

int partition(uint32_t arr[], char* tup_arr[], int low, int high, int nFields, int tupFields) {

    int r = rand()%(high-low+1);      // Returns a pseudo-random integer between 0 and RAND_MAX.
    r += low;
    uint64_t pivot = hash(&arr[r * nFields], 4);
    int i = low - 1;
    for (int j = low; j <= high - 1; j++) {
        if (hash(&arr[j * nFields], 4) < pivot) {
            i++;
            for (int k = 0; k < nFields; k++) {
                swap_join(arr, i * nFields + k, j * nFields + k);
            }
            for(int k = 0; k < tupFields; k++) {
                swap_tup(tup_arr, i * tupFields + k, j * tupFields + k);
            }
        }
    }

    for (int k = 0; k < nFields; k++) {
        swap_join(arr, (i + 1) * nFields + k, high * nFields + k);
    }
    for (int k = 0; k < tupFields; k++) {
        swap_tup(tup_arr, (i + 1) * tupFields + k, high * tupFields + k);
    }

    return (i + 1);
}

// basically just a quicksort, but it only goes down num_partitions levels
void partition_tuples(relation_t *relation)
{
    uint32_t *join_arr = relation->join_tuple_list->values;
    size_t joinFields = relation->join_tuple_list->nFields;
    size_t nTuples = relation->join_tuple_list->nTuples;
    char **tup_arr = relation->tuple_list->values;
    size_t tupFields = relation->tuple_list->nFields;
    int high, low;
    high = nTuples - 1;
    low = 0;

    int *stack = malloc((high - low + 1) * sizeof(int));
    int top = -1;

    stack[++top] = low;
    stack[++top] = high;
    int i = 0;
    while (top >= 0) {
        high = stack[top--];
        low = stack[top--];
        // if((high-low) <= 32768) continue;
        int pi = partition(join_arr, tup_arr, low, high, joinFields, tupFields);
        if (pi - 1 > low) {
            stack[++top] = low;
            stack[++top] = pi - 1;
        }
        if (pi + 1 < high) {
            stack[++top] = pi + 1;
            stack[++top] = high;
        }
    }
    free(stack);
}
