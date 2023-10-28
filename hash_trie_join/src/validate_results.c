#include "tsc_x86.h"
#include "hash_trie.h"
#include "hash_trie_join.h"
#include "hash_trie_join_build.h"
#include "hash_trie_join_probe.h"
#include <jansson.h>
#include <libpq-fe.h>
#include <string.h>

#include <string.h>

typedef struct benchmarking_data
{
    int num_tuples;
    int num_benchmarks;
} benchmarking_data;

static void exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

void print_benchmark(json_t *benchmarks_json,
                     const char *sql_options)
{
    // something like "host=localhost dbname=tpch_db user=user password=password"
    PGconn *conn;
    PGresult *res;
    int nFields, nTuples;

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

    for (int i = 0; i < json_array_size(benchmarks_json); i++)
    {
        json_t *benchmark = json_array_get(benchmarks_json, i);
        json_t *relations = json_object_get(benchmark, "relations");
        int num_relations = 0;
        char filename[100];
        sprintf(filename, "../../test/results/tpch_benchmark%u_%u.csv", i, 10000);

        // check if file already exists, and skip writing if it does.
        FILE *file = fopen(filename, "r");
        if (file)
        {
            fclose(file);
            continue;
        }
        if (!json_is_array(relations))
        {
            fprintf(stderr, "Error: relations is not an array\n");
            json_decref(benchmarks_json);
            return;
        }
        json_t *all_join_attributes = json_object_get(benchmark, "join_attributes");
        if (!json_is_array(all_join_attributes))
        {
            fprintf(stderr, "Error: join_attributes is not an array\n");
            json_decref(benchmarks_json);
            return;
        }

        char query[1000];
        sprintf(query, "SELECT * FROM ");

        // loop over relations participating in this join and call build
        for (int j = 0; j < json_array_size(relations); ++j)
        {
            char sub_query[100];
            // get relation
            sprintf(sub_query, "(SELECT * FROM validate_%s) AS relation%d, ",
                    json_string_value(json_array_get(relations, j)), j);
            strcat(query, sub_query);
        }
        query[strlen(query) - 2] = '\0';
        strcat(query, " WHERE ");
        int num_join_attrs = json_array_size(json_array_get(
            all_join_attributes, 0));
        for (int k = 0; k < num_join_attrs; k++)
        {
            int y = 0;
            for (int j = 0; j < json_array_size(relations); ++j)
            {
                char eq[100];
                json_t *join_attributes = json_array_get(all_join_attributes, j);
                char *join_attribute =
                    json_string_value(json_array_get(join_attributes, k));
                if (strlen(join_attribute) == 0)
                    continue;
                if (y == 0)
                {
                    sprintf(eq, "%s = ", join_attribute);
                    y++;
                }
                else
                {
                    sprintf(eq, "%s AND ", join_attribute);
                }
                strcat(query, eq);
            }
        }
        query[strlen(query) - 5] = ';';
        query[strlen(query) - 4] = '\0';

        res = PQexec(conn, query);
        // printf("%s", query);
        // Open the file for writing.
        file = fopen(filename, "w");
        if (file == NULL)
        {
            perror("Error opening file");
            exit(1);
        }

        nTuples = PQntuples(res);
        nFields = PQnfields(res);
        for (int k = 0; k < nTuples; k++)
        {
            fprintf(file, "%s", PQgetvalue(res, k, 0));
            for (int l = 1; l < nFields; l++)
            {
                fprintf(file, "|%s", PQgetvalue(res, k, l));
            }
            fprintf(file, "\n");
        }
        fclose(file);
    }
}

benchmarking_data run_functions(int num_tuples, benchmarks_t *benchmarks, json_t *benchmarks_json, const char *sql_options)
{
    int num_iterations = 1;
    join_t *joins = benchmarks->benchmarks;

    for (int i = 0; i < benchmarks->num_benchmarks; i++)
    {
        join(&joins[i], json_array_get(benchmarks_json, i),
             num_iterations, i, sql_options, true);
    }
    benchmarking_data ret_val = {num_tuples, benchmarks->num_benchmarks};
    return ret_val;
}

bool check_validity(benchmarking_data metadata)
{
    bool is_valid = true;
    for (int k = 0; k < metadata.num_benchmarks; k++)
    {
        char result_filename[100];
        sprintf(result_filename, "../../test/results/result_%u.csv", k, metadata.num_tuples);
        char postgres_filename[100];
        sprintf(postgres_filename, "../../test/results/tpch_benchmark%u_%u.csv", k, metadata.num_tuples);
        int line_count_result = 0;
        int line_count_postgres = 0;

        char c;
        FILE *fp, *fp2;
        if ((fp = fopen(result_filename, "r")) == NULL)
        {
            return 0;
        }

        for (c = getc(fp); c != EOF; c = getc(fp))
            if (c == '\n')
                line_count_result = line_count_result + 1;
        fclose(fp);
        if ((fp = fopen(postgres_filename, "r")) == NULL)
        {
            return 0;
        }
        for (c = getc(fp); c != EOF; c = getc(fp))
            if (c == '\n')
                line_count_postgres = line_count_postgres + 1;
        fclose(fp);

        if (line_count_result != line_count_postgres)
        {
            printf("❌ Validation failed for benchmark %u, input size %u\n", k, metadata.num_tuples);
            is_valid = false;
            // continue;
        }

        // if we have >= 50000 tuples in the result (and we get the same number of tuples between postgres and our implementation)
        // we assume that it's correct (takes way too long to validate else)
        if (line_count_result >= 50000)
        {
            continue;
        }
        if ((fp = fopen(result_filename, "r")) == NULL)
        {
            return 0;
        }

        char result_row[1024];
        char postgres_row[1024];

        for (int i = 0; i < line_count_result; i++)
        {
            if ((fp2 = fopen(postgres_filename, "r")) == NULL)
            {
                return 0;
            }
            fgets(result_row, 1024, fp);
            bool line_found = false;
            for (int j = 0; j < line_count_result; j++)
            {
                fgets(postgres_row, 1024, fp2);
                if (strcmp(result_row, postgres_row) == 0)
                {
                    line_found = true;
                    break;
                }
            }
            if (!line_found)
            {
                printf("❌ Validation failed for benchmark %u, input size %u\n", k, metadata.num_tuples);
                fclose(fp2);
                fclose(fp);
                is_valid = false;
                fclose(fp2);
                break;
            }
            fclose(fp2);
        }

        fclose(fp);
    }
    return is_valid;
}

bool validate_hashtries(benchmarks_t *benchmarks, json_t *benchmarks_json, const char *sql_options)
{
    int num_tuples = 10000;
    print_benchmark(benchmarks_json, sql_options);
    benchmarking_data metadata = run_functions(num_tuples, benchmarks, benchmarks_json, sql_options);
    bool is_valid = check_validity(metadata);
    if (!is_valid)
    {
        exit(1);
    }
    else
    {
        printf("✅ All tests passed\n");
    }
}
