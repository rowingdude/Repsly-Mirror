#ifndef ENDPT_MODULES_PRODUCTS_H
#define ENDPT_MODULES_PRODUCTS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include <stdbool.h>

// Struct to store results of product batch processing
struct ProductBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[256];
};

// Struct to hold endpoint-related metadata
struct Endpoint {
    char name[256];
    char url[512];
};

// Logs batch processing status to a log file, returning success status
bool log_batch_status(const struct ProductBatchResult *result);

// Processes a single product record and upserts it into the database
int process_product_record(MYSQL *conn, struct json_object *record);

// Processes and inserts packaging codes associated with a specific product
int process_product_packagingcodes(MYSQL *conn, const char *productcode, struct json_object *packagingcodes);

// Processes a batch of products, inserting each product and recording results
int process_products_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ProductBatchResult *result);

#endif 