#ifndef VISITS_H
#define VISITS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include "../endpoints.h"

#include <stdbool.h>
#define ERROR_MESSAGE_SIZE 256
struct VisitRealizationTaskBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};

bool log_batch_status(const struct VisitRealizationTaskBatchResult *result);
int process_visitrealizationtask_record(MYSQL *conn, struct json_object *record);
int process_visitrealizationtasks_batch(MYSQL *conn, const struct Endpoint *endpoint,
                                        struct json_object *batch,
                                        struct VisitRealizationTaskBatchResult *result);
bool verify_visitrealizationtasks_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif