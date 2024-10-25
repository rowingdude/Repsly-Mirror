#ifndef ENDPT_MODULES_DAILYWORKINGTIME_H
#define ENDPT_MODULES_DAILYWORKINGTIME_H

#include <mysql/mysql.h>
#include <stdbool.h>
#include <json-c/json.h>

#define ERROR_MESSAGE_SIZE 256

int process_dailyworkingtime_record(MYSQL *conn, struct json_object *record);
int process_dailyworkingtimes_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                                   struct json_object *batch, 
                                   struct DailyWorkingTimeBatchResult *result);
bool verify_dailyworkingtimes_batch(MYSQL *conn, int last_id, struct json_object *original_data);

struct DailyWorkingTimeBatchResult {
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

