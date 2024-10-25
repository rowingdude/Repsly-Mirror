#ifndef ENDPT_MODULES_PRODUCTS_H
#define ENDPT_MODULES_PRODUCTS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include <stdbool.h>
#include "../endpoints.h"

#define ERROR_MESSAGE_SIZE 1024
struct ProductBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};


bool log_batch_status(const struct ProductBatchResult *result);
int process_product_record(MYSQL *conn, struct json_object *record);
int process_product_packagingcodes(MYSQL *conn, const char *productcode, struct json_object *packagingcodes);
int process_products_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ProductBatchResult *result);
bool verify_products_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif 