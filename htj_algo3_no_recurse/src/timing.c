#include "timing.h"
#include <jansson.h>

timing_params_t *get_timing_params()
{
  // open JSON file and read content
  FILE *fp;
  char *buffer;
  long file_size;
  fp = fopen("../../test/timing_params.json", "rb");
  if (!fp)
  {
    fprintf(stderr, "Error opening file\n");
    return NULL;
  }
  fseek(fp, 0L, SEEK_END);
  file_size = ftell(fp);
  rewind(fp);
  buffer = (char *)malloc(file_size + 1);
  if (!buffer)
  {
    fprintf(stderr, "Error allocating memory\n");
    fclose(fp);
    return NULL;
  }
  fread(buffer, 1, file_size, fp);
  fclose(fp);
  buffer[file_size] = '\0';

  // load content into json_t object
  json_error_t error;
  json_t *json = json_loads(buffer, 0, &error);
  if (!json)
  {
    fprintf(stderr, "Error parsing JSON: %s\n", error.text);
    return NULL;
  }
  free(buffer);

  timing_params_t *timing_params = 
      (timing_params_t *)malloc(sizeof(timing_params_t));
  timing_params->num_iterations = json_integer_value(
      json_object_get(json, "num_iterations"));
  printf("Doing %d timing iterations\n", timing_params->num_iterations);

  return timing_params;
}
