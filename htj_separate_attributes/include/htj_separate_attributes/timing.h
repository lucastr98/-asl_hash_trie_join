#ifndef TIMING_H_
#define TIMING_H_

#include <stdint.h>

typedef struct
{
  int num_iterations;
} timing_params_t;

timing_params_t *get_timing_params();

#endif  // TIMING_H_
