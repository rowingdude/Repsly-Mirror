#ifndef ENDPT_MODULES_REPRESENTATIVES_H
#define ENDPT_MODULES_REPRESENTATIVES_H

#include <mysql/mysql.h>
#include <stdbool.h>
#include <json-c/json.h>
#include "../endpoints.h"

#define ERROR_MESSAGE_SIZE 256

struct RepresentativeBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};


int process_representative_record(MYSQL *conn, struct json_object *record);
int process_representatives_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                                 struct json_object *batch, 
                                 struct RepresentativeBatchResult *result);
bool verify_representatives_batch(MYSQL *conn, int last_id, struct json_object *original_data);



#endif

