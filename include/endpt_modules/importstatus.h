// importstatus.h

#ifndef IMPORTSTATUS_H
#define IMPORTSTATUS_H

#include <mysql/mysql.h>
#include <json-c/json.h>
#include <stdbool.h>
#include <time.h>
#define ERROR_MESSAGE_SIZE 256
// Define ImportStatusBatchResult to store the result of the batch processing
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

// Function to log batch status in a log file
bool log_batch_status(const struct ImportStatusBatchResult *result);

// Function to process import warnings by inserting into the import_warnings table
int process_import_warning(MYSQL *conn, long long importjobid, struct json_object *warning);

// Function to process import errors by inserting into the import_errors table
int process_import_error(MYSQL *conn, long long importjobid, struct json_object *error);

// Function to process individual import status records and insert into import_status table
int process_import_status_record(MYSQL *conn, struct json_object *record);

// Function to process a batch of import status records and populate the result
int process_import_status_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ImportStatusBatchResult *result);

#endif // IMPORTSTATUS_H
