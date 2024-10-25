// include/endpt_modules/retailaudits.h
#ifndef ENDPT_RETAILAUDITS_H
#define ENDPT_RETAILAUDITS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include "endpoints.h"

struct RetailAuditBatchResult {
    bool success;
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    char error_message[256];
    int total_count;
};

int process_retailaudit_record(MYSQL *conn, struct json_object *record);
int process_retailaudit_items(MYSQL *conn, int retailauditid, struct json_object *items);
int process_retailaudit_customfields(MYSQL *conn, int retailauditid, struct json_object *customfields);
int process_retailaudits_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct RetailAuditBatchResult *result);
bool verify_retailaudits_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif