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

static void query(PGconn *conn) {
    PGresult *result = PQexecPrepared(conn, "layer_probe", 0, NULL, NULL, NULL, 1);
    const unsigned char expected[] = {0, 0, 0, 42};
    if (!result || PQresultStatus(result) != PGRES_TUPLES_OK ||
        PQntuples(result) != 1 || PQnfields(result) != 1 ||
        PQgetisnull(result, 0, 0) || PQgetlength(result, 0, 0) != 4 ||
        memcmp(PQgetvalue(result, 0, 0), expected, 4)) {
        fprintf(stderr, "incorrect query result: %s\n", PQerrorMessage(conn));
        PQclear(result);
        PQfinish(conn);
        exit(1);
    }
    PQclear(result);
}

int main(int argc, char **argv) {
    if (argc != 3 || !getenv("PHASE5_DSN")) {
        fprintf(stderr, "usage: PHASE5_DSN=... libpq_probe iterations samples\n");
        return 1;
    }
    int iterations = atoi(argv[1]), samples = atoi(argv[2]);
    if (iterations <= 0 || samples < 3) return 1;
    PGconn *conn = PQconnectdb(getenv("PHASE5_DSN"));
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    PGresult *prepared = PQprepare(conn, "layer_probe", "select 42::int4", 0, NULL);
    if (!prepared || PQresultStatus(prepared) != PGRES_COMMAND_OK) {
        fprintf(stderr, "prepare failed: %s\n", PQerrorMessage(conn));
        PQclear(prepared);
        PQfinish(conn);
        return 1;
    }
    PQclear(prepared);
    for (int i = 0; i < 1000; ++i) query(conn);
    printf("{\"backend\":\"libpq\",\"libpq\":%d,\"iterations\":%d,"
           "\"warmup\":1000,\"wall_us\":[", PQlibVersion(), iterations);
    for (int sample = 0; sample < samples; ++sample) {
        double start = now();
        for (int i = 0; i < iterations; ++i) query(conn);
        printf("%s%.9f", sample ? "," : "", (now() - start) * 1e6 / iterations);
    }
    puts("]}");
    PQfinish(conn);
    return 0;
}
