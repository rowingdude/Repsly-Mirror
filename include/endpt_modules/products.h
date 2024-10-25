#ifndef ENDPT_MODULES_PRODUCTS_H
#define ENDPT_MODULES_PRODUCTS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include <stdbool.h>
#include "../endpoints.h"

#define ERROR_MESSAGE_SIZE 256
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

struct Endpoint {
    char name[256];
    char url[512];
};

bool log_batch_status(const struct ProductBatchResult *result);
int process_product_record(MYSQL *conn, struct json_object *record);
int process_product_packagingcodes(MYSQL *conn, const char *productcode, struct json_object *packagingcodes);
int process_products_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ProductBatchResult *result);

#endif 