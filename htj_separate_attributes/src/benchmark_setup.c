#include "benchmark_setup.h"
#include <libpq-fe.h>
#include <string.h>

static void exit_nicely(PGconn *conn)
{
  PQfinish(conn);
  exit(1);
}

json_t *get_benchmarks_json_object()
{
  // open JSON file and read content
  FILE *fp;
  char *buffer;
  long file_size;
  fp = fopen("../../test/benchmarks.json", "rb");
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

  // get the benchmarks array
  json_t *benchmarks = json_object_get(json, "benchmarks");
  if (!json_is_array(benchmarks))
  {
    fprintf(stderr, "Error: benchmarks is not an array\n");
    json_decref(json);
    return NULL;
  }

  return benchmarks;
}

benchmarks_t *get_benchmarks(json_t *benchmarks_json)
{
  benchmarks_t *benchmarks = malloc(sizeof(benchmarks_t));
  benchmarks->num_benchmarks = json_array_size(benchmarks_json);
  benchmarks->benchmarks = 
      malloc(json_array_size(benchmarks_json) * sizeof(join_t));

  // loop over the benchmarks
  for (int i = 0; i < benchmarks->num_benchmarks; i++)
  {
    json_t *benchmark = json_array_get(benchmarks_json, i);
    json_t *relations = json_object_get(benchmark, "relations");
    json_t *all_join_attributes = json_object_get(benchmark, "join_attributes");

    benchmarks->benchmarks[i].num_relations = json_array_size(relations);
    benchmarks->benchmarks[i].relations =
        malloc(benchmarks->benchmarks[i].num_relations * sizeof(relation_t));
    benchmarks->benchmarks[i].total_num_join_attributes = json_array_size(
        json_array_get(all_join_attributes, 0));

    // fill relations except for the tuple list
    for (int j = 0; j < benchmarks->benchmarks[i].num_relations; ++j)
    {
      json_t *join_attributes = json_array_get(all_join_attributes, j);
      int max_num_join_attributes = json_array_size(join_attributes);
      benchmarks->benchmarks[i].relations[j].num_join_attributes = 0;
      benchmarks->benchmarks[i].relations[j].join_numbers =
          malloc(max_num_join_attributes * sizeof(int));
      for (int k = 0; k < max_num_join_attributes; ++k)
      {
        const char *join_attribute =
            json_string_value(json_array_get(join_attributes, k));
        if (strlen(join_attribute) == 0)
          continue;

        benchmarks->benchmarks[i].relations[j].join_numbers[
            benchmarks->benchmarks[i].relations[j].num_join_attributes++] = k;
      }
      benchmarks->benchmarks[i].relations[j].join_numbers = realloc(
          benchmarks->benchmarks[i].relations[j].join_numbers,
          benchmarks->benchmarks[i].relations[j].num_join_attributes * sizeof(int));
    }
  }
  return benchmarks;
}


//selects * from validation view if input size is <= 0
void setup_tuples(join_t *benchmark, json_t *benchmark_json, 
    const char *sql_options, const bool is_validation)
{
  // something like "host=localhost dbname=tpch_db user=user password=password"
  PGconn *conn;
  PGresult *res;
  int nFields, nTuples, nJoinFields;

  conn = PQconnectdb(sql_options);
  if (PQstatus(conn) != CONNECTION_OK)
  {
    fprintf(stderr, "Connection to database failed: %s",
            PQerrorMessage(conn));
    exit_nicely(conn);
  }
  /* Start a transaction block */
  res = PQexec(conn, "BEGIN");
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
    PQclear(res);
    exit_nicely(conn);
  }
  PQclear(res);

  for (int j = 0; j < benchmark->num_relations; ++j)
  {
    // get relation
    char query[100];
    if (is_validation) 
    {
      sprintf(query, "SELECT * FROM validate_%s;",
          json_string_value(json_array_get(json_object_get(
          benchmark_json, "relations"), j)));
    }
    else 
    {
      sprintf(query, "SELECT * FROM %s ORDER BY ctid LIMIT %lld;",
          json_string_value(json_array_get(json_object_get(
          benchmark_json, "relations"), j)), json_integer_value(json_array_get(
          json_object_get(benchmark_json, "sizes"), j)));
    }
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
      fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
      PQclear(res);
      exit_nicely(conn);
    }

    json_t *join_attributes = json_array_get(json_object_get(
        benchmark_json, "join_attributes"), j);
    int max_num_join_attributes = json_array_size(join_attributes); 
    int idx = 0;
    int *join_attribute_nrs = 
        malloc(benchmark->relations[j].num_join_attributes * sizeof(int));
    for (int k = 0; k < max_num_join_attributes; ++k)
    {
      const char *join_attribute = json_string_value(json_array_get(
          join_attributes, k));
      if (strlen(join_attribute) == 0)
        continue;

      join_attribute_nrs[idx++] = PQfnumber(res, join_attribute);
    }

    nTuples = PQntuples(res);
    nFields = PQnfields(res);
    nJoinFields = benchmark->relations[j].num_join_attributes;
    tuple_t *tuples = malloc(sizeof(tuple_t));
    tuples->values = malloc(nTuples * (nFields - nJoinFields) * sizeof(char *));
    tuples->nFields = nFields - nJoinFields;
    tuples->nTuples = nTuples;
    join_tuple_t *join_tuples = malloc(sizeof(join_tuple_t));
    join_tuples->values = malloc(nTuples * nJoinFields * sizeof(size_t));
    join_tuples->next = malloc(nTuples * sizeof(size_t));
    join_tuples->nFields = nJoinFields;
    join_tuples->nTuples = nTuples;
    int *join_indices = malloc(nFields * sizeof(int));
    for (int l = 0; l < nFields; ++l)
    {
      join_indices[l] = -1;
      for (int join_idx = 0; join_idx < nJoinFields; ++join_idx) 
      {
        if (l == join_attribute_nrs[join_idx])
        {
          join_indices[l] = join_idx;
          break;
        }
      }
    }
    for (int k = 0; k < nTuples; k++)
    {
      int subtracter = 0;
      for (int l = 0; l < nFields; l++)
      {
        char *s = PQgetvalue(res, k, l);
        if (join_indices[l] == -1)
        {
          size_t len = strlen(s);
          tuples->values[tuples->nFields * k + l - subtracter] = 
              malloc((len + 1) * sizeof(char));
          strcpy(tuples->values[tuples->nFields * k + l - subtracter], s);
        }
        else
        {
          join_tuples->values[join_tuples->nFields * k + join_indices[l]] = 
              (size_t)strtoul(s, NULL, 10); 
          ++subtracter;
        }
      }
      join_tuples->next[k] = k+1;
    }
    // free(join_indices);
    free(join_attribute_nrs);
    join_tuples->next[nTuples-1] = -1;
    benchmark->relations[j].tuple_list = tuples;
    benchmark->relations[j].join_tuple_list = join_tuples;
    benchmark->relations[j].join_att_indices = join_indices;

    PQclear(res);
  }
  /* end the transaction */
  res = PQexec(conn, "END");
  PQclear(res);
  /* close the connection to the database and cleanup */
  PQfinish(conn);
}

void free_benchmarks(benchmarks_t *benchmarks)
{
  join_t *joins = benchmarks->benchmarks;
  for (int i = 0; i < benchmarks->num_benchmarks; i++)
  {
    for (int j = 0; j < joins[i].num_relations; j++)
    {
      free(joins[i].relations[j].join_numbers);
      free(joins[i].relations[j].join_att_indices);
    }
    free(joins[i].relations);
  }
  free(joins);
  free(benchmarks);
}
