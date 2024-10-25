#ifndef ENDPT_PURCHASEORDERS_H
#define ENDPT_PURCHASEORDERS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include "../endpoints.h"
#define ERROR_MESSAGE_SIZE 256
struct PurchaseOrderBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};

int process_purchaseorder_record(MYSQL *conn, struct json_object *record);
int process_purchaseorder_items(MYSQL *conn, int purchaseorderid, struct json_object *items);
int process_purchaseorder_customattributes(MYSQL *conn, int purchaseorderid, struct json_object *customattributes);
int process_purchaseorders_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct PurchaseOrderBatchResult *result);
bool verify_purchaseorders_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif