#ifndef ENDPT_CLIENTS_H
#define ENDPT_CLIENTS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include <stdbool.h>
#include "endpoints.h"

#define ERROR_MSG_SIZE 256

struct ClientBatchResult {
    long long last_timestamp;          // Last processed timestamp
    int records_processed;             // Total records processed
    int records_inserted;              // Total records inserted
    int records_failed;                // Total records failed
    int total_count;                   // Total records in the batch
    bool success;                      // Success flag
    char error_message[ERROR_MSG_SIZE]; // Error message if failed
};

typedef enum {
    SUCCESS,
    INSERT_FAILURE,
    RECORD_FAILURE,
    VERIFICATION_FAILURE,
    UNKNOWN_ERROR
} ProcessStatus;

int process_client_record(MYSQL *conn, struct json_object *record);
int process_client_custom_fields(MYSQL *conn, int clientid, struct json_object *custom_fields);
int process_client_price_lists(MYSQL *conn, int clientid, struct json_object *price_lists);
ProcessStatus process_clients_batch(MYSQL *conn, const struct Endpoint *endpoint __attribute__((unused)), struct json_object *batch, struct ClientBatchResult *result);
bool verify_clients_batch(MYSQL *conn, long long last_timestamp, struct json_object *original_data);
bool log_batch_status(const struct ClientBatchResult *result);

#endif
