#ifndef ENDPT_DOCUMENTTYPES_H
#define ENDPT_DOCUMENTTYPES_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include "../endpoints.h"

#define ERROR_MESSAGE_SIZE 256

struct DocumentTypeBatchResult {
    bool success;
    int records_processed;
    int records_inserted;
    int records_failed;
    char error_message[ERROR_MESSAGE_SIZE];
    int total_count;
};

int process_documenttype_record(MYSQL *conn, struct json_object *record);
int process_documenttype_statuses(MYSQL *conn, int documenttypeid, struct json_object *statuses);
int process_documenttype_pricelists(MYSQL *conn, int documenttypeid, struct json_object *pricelists);
int process_documenttypes_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct DocumentTypeBatchResult *result);
bool verify_documenttypes_batch(MYSQL *conn, struct json_object *original_data);

#endif