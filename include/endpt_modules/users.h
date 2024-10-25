#ifndef ENDPT_MODULES_USERS_H
#define ENDPT_MODULES_USERS_H

#include <mysql/mysql.h>
#include <stdbool.h>
#include <json-c/json.h>
#define ERROR_MESSAGE_SIZE 256
int process_user_record(MYSQL *conn, struct json_object *record);
int process_users_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                       struct json_object *batch, struct UserBatchResult *result);
bool verify_users_batch(MYSQL *conn, int last_id, struct json_object *original_data);

struct UserBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};

#endif 