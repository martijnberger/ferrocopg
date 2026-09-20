/* Diagnostic only: the same prepared binary int4 as layer_probe.rs. */
#define _POSIX_C_SOURCE 200809L
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now(void) {
    struct timespec stamp;
    if (clock_gettime(CLOCK_MONOTONIC, &stamp)) {
        perror("clock_gettime");
        exit(1);
    }
    return stamp.tv_sec + stamp.tv_nsec / 1e9;
}

static const char *sql = "select 42::int4";
static int parameterized = 0, prepared = 1, binary = 1;

static void query(PGconn *conn) {
    const char value[] = {0, 41};
    const char *values[] = {value};
    const int lengths[] = {2}, formats[] = {1};
    const Oid types[] = {21};
    PGresult *result = prepared
        ? PQexecPrepared(conn, "layer_probe", parameterized, values, lengths, formats, binary)
        : PQexecParams(conn, sql, 1, types, values, lengths, formats, binary);
    const char *expected = binary ? "\0\0\0*" : "42";
    int expected_length = binary ? 4 : 2;
    if (!result || PQresultStatus(result) != PGRES_TUPLES_OK ||
        PQntuples(result) != 1 || PQnfields(result) != 1 ||
        PQgetisnull(result, 0, 0) || PQgetlength(result, 0, 0) != expected_length ||
        PQftype(result, 0) != 23 || PQfformat(result, 0) != binary ||
        memcmp(PQgetvalue(result, 0, 0), expected, expected_length)) {
        fprintf(stderr, "incorrect query result: %s\n", PQerrorMessage(conn));
        PQclear(result);
        PQfinish(conn);
        exit(1);
    }
    PQclear(result);
}

int main(int argc, char **argv) {
    if ((argc != 3 && argc != 5) || !getenv("PHASE5_DSN")) {
        fprintf(stderr, "usage: PHASE5_DSN=... libpq_probe iterations samples [constant|prepared|unprepared binary|text]\n");
        return 1;
    }
    int iterations = atoi(argv[1]), samples = atoi(argv[2]);
    if (iterations <= 0 || samples < 3) return 1;
    const char *mode = argc == 5 ? argv[3] : "constant";
    const char *format = argc == 5 ? argv[4] : "binary";
    if ((strcmp(mode, "constant") && strcmp(mode, "prepared") && strcmp(mode, "unprepared")) ||
        (strcmp(format, "binary") && strcmp(format, "text"))) return 1;
    parameterized = strcmp(mode, "constant") != 0;
    prepared = strcmp(mode, "unprepared") != 0;
    binary = strcmp(format, "binary") == 0;
    if (parameterized) sql = "select $1::int + 1";
    PGconn *conn = PQconnectdb(getenv("PHASE5_DSN"));
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    const Oid types[] = {21};
    PGresult *statement = prepared ? PQprepare(conn, "layer_probe", sql, parameterized, types) : NULL;
    if (prepared && (!statement || PQresultStatus(statement) != PGRES_COMMAND_OK)) {
        fprintf(stderr, "prepare failed: %s\n", PQerrorMessage(conn));
        PQclear(statement);
        PQfinish(conn);
        return 1;
    }
    PQclear(statement);
    for (int i = 0; i < 1000; ++i) query(conn);
    printf("{\"backend\":\"libpq\",\"libpq\":%d,\"iterations\":%d,"
           "\"warmup\":1000,\"wall_us\":[", PQlibVersion(), iterations);
    for (int sample = 0; sample < samples; ++sample) {
        double start = now();
        for (int i = 0; i < iterations; ++i) query(conn);
        printf("%s%.9f", sample ? "," : "", (now() - start) * 1e6 / iterations);
    }
    printf("],\"case\":\"%s\",\"result_format\":\"%s\",\"parameter_oids\":%s}\n",
           mode, format, parameterized ? "[21]" : "[]");
    PQfinish(conn);
    return 0;
}
