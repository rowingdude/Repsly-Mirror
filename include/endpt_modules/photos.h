// src/endpt_modules/photos.h

#ifndef PHOTOS_H
#define PHOTOS_H

#include <stdbool.h>
#include <mysql/mysql.h>
#include <json-c/json.h>
#include "../endpoints.h"

#define ERROR_MESSAGE_SIZE 256

struct PhotoBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};


bool log_batch_status(const struct PhotoBatchResult *result);
int process_photo_record(MYSQL *conn, struct json_object *record);
int process_photos_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                         struct json_object *batch, struct PhotoBatchResult *result);
bool verify_photos_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif // PHOTOS_H
