#include "endpt_modules/visitrealizationtasks.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>

static bool log_batch_status(const struct VisitRealizationTaskBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("visitrealizationtasks_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_visitrealizationtask_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_visitrealizationtask(?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    char scheduleid[36] = {0};
    char entityid[36] = {0};
    char tasktype[20] = {0};
    char tasknote[1024] = {0};
    bool completed = false;

    // NULL indicators
    bool null_indicators[5] = {0};

    struct json_object *temp;

    // Extract fields from JSON
    if (json_object_object_get_ex(record, "ScheduleID", &temp))
        strncpy(scheduleid, json_object_get_string(temp), sizeof(scheduleid) - 1);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "EntityID", &temp))
        strncpy(entityid, json_object_get_string(temp), sizeof(entityid) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "TaskType", &temp))
        strncpy(tasktype, json_object_get_string(temp), sizeof(tasktype) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "TaskNote", &temp))
        strncpy(tasknote, json_object_get_string(temp), sizeof(tasknote) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "Completed", &temp))
        completed = json_object_get_boolean(temp);
    else
        null_indicators[4] = 1;

    // Bind parameters
    MYSQL_BIND bind[5];
    memset(bind, 0, sizeof(bind));

    // ScheduleID
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = scheduleid;
    bind[0].buffer_length = sizeof(scheduleid);
    bind[0].is_null = &null_indicators[0];

    // EntityID
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = entityid;
    bind[1].buffer_length = sizeof(entityid);
    bind[1].is_null = &null_indicators[1];

    // TaskType
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = tasktype;
    bind[2].buffer_length = sizeof(tasktype);
    bind[2].is_null = &null_indicators[2];

    // TaskNote
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = tasknote;
    bind[3].buffer_length = sizeof(tasknote);
    bind[3].is_null = &null_indicators[3];

    // Completed
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = &completed;
    bind[4].is_null = &null_indicators[4];

    if (mysql_stmt_bind_param(stmt, bind)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);
    return 0;
}

int process_visitrealizationtasks_batch(MYSQL *conn, const struct Endpoint *endpoint,
                                       struct json_object *batch,
                                       struct VisitRealizationTaskBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct VisitRealizationTaskBatchResult));
    
    // Extract metadata
    struct json_object *meta;
    if (json_object_object_get_ex(batch, "MetaCollectionResult", &meta)) {
        struct json_object *count_obj, *firstid_obj, *lastid_obj;
        
        if (json_object_object_get_ex(meta, "TotalCount", &count_obj))
            result->total_count = json_object_get_int(count_obj);
        
        if (json_object_object_get_ex(meta, "FirstID", &firstid_obj))
            result->first_id = json_object_get_int(firstid_obj);
            
        if (json_object_object_get_ex(meta, "LastID", &lastid_obj))
            result->last_id = json_object_get_int(lastid_obj);
    }

    // Process records
    struct json_object *tasks;
    if (!json_object_object_get_ex(batch, "VisitRealizationTasks", &tasks)) {
        snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                "No VisitRealizationTasks array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(tasks);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(tasks, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_visitrealizationtask_record(conn, record) == 0) {
            if (mysql_query(conn, "COMMIT")) {
                snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                        "Failed to commit transaction: %s", mysql_error(conn));
                mysql_query(conn, "ROLLBACK");
                return -1;
            }
            result->records_inserted++;
        } else {
            mysql_query(conn, "ROLLBACK");
            result->records_failed++;
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Failed to process visit realization task record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing visit realization tasks: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_visitrealizationtasks_batch(conn, result->last_id, tasks)) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_visitrealizationtasks_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT vrt.scheduleid, vrt.entityid, vrt.tasktype, "
                       "LENGTH(vrt.tasknote) as note_length, vrt.completed "
                       "FROM visitrealizationtasks vrt "
                       "ORDER BY vrt.scheduleid, vrt.entityid DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[5];
    char db_scheduleid[36];
    char db_entityid[36];
    char db_tasktype[20];
    long long db_note_length;
    bool db_completed;
    unsigned long db_scheduleid_length;
    unsigned long db_entityid_length;
    unsigned long db_tasktype_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_STRING;
    bind_result[0].buffer = db_scheduleid;
    bind_result[0].buffer_length = sizeof(db_scheduleid);
    bind_result[0].length = &db_scheduleid_length;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_entityid;
    bind_result[1].buffer_length = sizeof(db_entityid);
    bind_result[1].length = &db_entityid_length;

    bind_result[2].buffer_type = MYSQL_TYPE_STRING;
    bind_result[2].buffer = db_tasktype;
    bind_result[2].buffer_length = sizeof(db_tasktype);
    bind_result[2].length = &db_tasktype_length;

    bind_result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[3].buffer = &db_note_length;

    bind_result[4].buffer_type = MYSQL_TYPE_TINY;
    bind_result[4].buffer = &db_completed;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *task = json_object_array_get_idx(original_data, i);
            struct json_object *scheduleid_obj, *entityid_obj, *tasktype_obj, 
                             *tasknote_obj, *completed_obj;
            
            if (json_object_object_get_ex(task, "ScheduleID", &scheduleid_obj) &&
                json_object_object_get_ex(task, "EntityID", &entityid_obj) &&
                json_object_object_get_ex(task, "TaskType", &tasktype_obj) &&
                json_object_object_get_ex(task, "Completed", &completed_obj)) {
                
                const char *json_scheduleid = json_object_get_string(scheduleid_obj);
                const char *json_entityid = json_object_get_string(entityid_obj);
                const char *json_tasktype = json_object_get_string(tasktype_obj);
                bool json_completed = json_object_get_boolean(completed_obj);

                if (strncmp(db_scheduleid, json_scheduleid, db_scheduleid_length) == 0 &&
                    strncmp(db_entityid, json_entityid, db_entityid_length) == 0 &&
                    strncmp(db_tasktype, json_tasktype, db_tasktype_length) == 0 &&
                    db_completed == json_completed) {
                    
                    if (json_object_object_get_ex(task, "TaskNote", &tasknote_obj)) {
                        const char *json_note = json_object_get_string(tasknote_obj);
                        if ((json_note && strlen(json_note) != db_note_length)) {
                            verification_passed = false;
                            break;
                        }
                    }
                    found = true;
                }
            }
        }
        
        if (!found) {
            verification_passed = false;
            break;
        }
    }

    mysql_stmt_close(stmt);
    return verification_passed;
}