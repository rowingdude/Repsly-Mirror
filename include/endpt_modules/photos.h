// src/endpt_modules/photos.h

#ifndef PHOTOS_H
#define PHOTOS_H

#include <stdbool.h>
#include <mysql/mysql.h>
#include <json-c/json.h>

// Constants
#define ERROR_MESSAGE_SIZE 256

// Struct Definitions

// Structure to store the result of photo batch processing
struct PhotoBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[MAX_ERROR_MESSAGE_LENGTH];
};

// Structure representing an endpoint (used in batch processing)
struct Endpoint {
    char endpoint_url[512];
    char auth_token[256];
};

// Function Prototypes

// Logs the batch status to a log file
bool log_batch_status(const struct PhotoBatchResult *result);

// Processes a single photo record, binding JSON fields to MySQL parameters
int process_photo_record(MYSQL *conn, struct json_object *record);

// Processes a batch of photos and records the results in the PhotoBatchResult struct
int process_photos_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                         struct json_object *batch, struct PhotoBatchResult *result);

// Verifies the processed batch against the original data for accuracy
bool verify_photos_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif // PHOTOS_H
