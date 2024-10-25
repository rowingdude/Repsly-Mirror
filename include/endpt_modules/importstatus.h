#ifndef IMPORTSTATUS_H
#define IMPORTSTATUS_H
#include "../endpoints.h"

#include <mysql/mysql.h>
#include <json-c/json.h>
#include <stdbool.h>
#include <time.h>
#define ERROR_MESSAGE_SIZE 1024

struct ImportStatusBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[1024];
};

bool log_batch_status(const struct ImportStatusBatchResult *result);
int process_import_warning(MYSQL *conn, long long importjobid, struct json_object *warning);
int process_import_error(MYSQL *conn, long long importjobid, struct json_object *error);
int process_import_status_record(MYSQL *conn, struct json_object *record);
int process_import_status_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ImportStatusBatchResult *result);
bool verify_import_status_batch(MYSQL *conn, int last_id, struct json_object *original_data);
#endif // IMPORTSTATUS_H
