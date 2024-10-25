#ifndef ENDPT_VISITS_H
#define ENDPT_VISITS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include "../endpoints.h"
#define ERROR_MESSAGE_SIZE 256
struct VisitBatchResult {
    bool success;
    long long last_timestamp;
    int records_processed;
    int records_inserted;
    int records_failed;
    char error_message[ERROR_MESSAGE_SIZE];
    int total_count;
};

int process_visit_record(MYSQL *conn, struct json_object *record);
int process_visits_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct VisitBatchResult *result);
bool verify_visits_batch(MYSQL *conn, long long last_timestamp, struct json_object *original_data);

#endif