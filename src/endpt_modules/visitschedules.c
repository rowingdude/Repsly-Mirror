#include "endpt_modules/visitschedules.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>

bool log_batch_status(const struct VisitScheduleBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("visitschedules_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_visitschedule_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_visitschedule(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    char scheduledateandtime[20] = {0};
    char clientcode[50] = {0};
    char representativecode[20] = {0};
    char representativename[80] = {0};
    char clientname[255] = {0};
    char streetaddress[255] = {0};
    char zip[20] = {0};
    char zipext[20] = {0};
    char city[255] = {0};
    char state[255] = {0};
    char country[255] = {0};
    char territory[80] = {0};
    char visitnote[1024] = {0};
    char duedate[20] = {0};
    int metacollectiontotalcount = 0;

    // NULL indicators
    bool null_indicators[15] = {0};

    struct json_object *temp;

    // Extract fields from JSON
    if (json_object_object_get_ex(record, "ScheduleDateAndTime", &temp))
        strncpy(scheduledateandtime, json_object_get_string(temp), sizeof(scheduledateandtime) - 1);
    else
        null_indicators[0] = 1;

    // ... continue extracting other fields ...

    // Bind parameters
    MYSQL_BIND bind[15];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = scheduledateandtime;
    bind[0].buffer_length = sizeof(scheduledateandtime);
    bind[0].is_null = &null_indicators[0];

    // ... continue binding other fields ...

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

int process_visitschedules_batch(MYSQL *conn, const struct Endpoint *endpoint,
                                struct json_object *batch,
                                struct VisitScheduleBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct VisitScheduleBatchResult));
    
    // Extract metadata
    struct json_object *meta;
    if (json_object_object_get_ex(batch, "MetaCollectionResult", &meta)) {
        struct json_object *count_obj;
        
        if (json_object_object_get_ex(meta, "TotalCount", &count_obj))
            result->total_count = json_object_get_int(count_obj);
    }

    // Process records
    struct json_object *schedules;
    if (!json_object_object_get_ex(batch, "VisitSchedules", &schedules)) {
        snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                "No VisitSchedules array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(schedules);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(schedules, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_visitschedule_record(conn, record) == 0) {
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
                    "Failed to process visit schedule record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing visit schedules: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_visitschedules_batch(conn, result->last_id, schedules)) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_visitschedules_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT vs.scheduledateandtime, vs.clientcode, vs.representativecode, "
                       "COUNT(vs.visitnote) as note_count "
                       "FROM visitschedules vs "
                       "GROUP BY vs.scheduledateandtime, vs.clientcode, vs.representativecode "
                       "ORDER BY vs.scheduledateandtime DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[4];
    char db_scheduledateandtime[20];
    char db_clientcode[50];
    char db_representativecode[20];
    long long db_note_count;
    unsigned long db_scheduledateandtime_length;
    unsigned long db_clientcode_length;
    unsigned long db_representativecode_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_STRING;
    bind_result[0].buffer = db_scheduledateandtime;
    bind_result[0].buffer_length = sizeof(db_scheduledateandtime);
    bind_result[0].length = &db_scheduledateandtime_length;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_clientcode;
    bind_result[1].buffer_length = sizeof(db_clientcode);
    bind_result[1].length = &db_clientcode_length;

    bind_result[2].buffer_type = MYSQL_TYPE_STRING;
    bind_result[2].buffer = db_representativecode;
    bind_result[2].buffer_length = sizeof(db_representativecode);
    bind_result[2].length = &db_representativecode_length;

    bind_result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[3].buffer = &db_note_count;

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
            struct json_object *schedule = json_object_array_get_idx(original_data, i);
            struct json_object *datetime_obj, *clientcode_obj, *repcode_obj, *note_obj;
            
            if (json_object_object_get_ex(schedule, "ScheduleDateAndTime", &datetime_obj) &&
                json_object_object_get_ex(schedule, "ClientCode", &clientcode_obj) &&
                json_object_object_get_ex(schedule, "RepresentativeCode", &repcode_obj)) {
                
                const char *json_datetime = json_object_get_string(datetime_obj);
                const char *json_clientcode = json_object_get_string(clientcode_obj);
                const char *json_repcode = json_object_get_string(repcode_obj);

                if (strncmp(db_scheduledateandtime, json_datetime, db_scheduledateandtime_length) == 0 &&
                    strncmp(db_clientcode, json_clientcode, db_clientcode_length) == 0 &&
                    strncmp(db_representativecode, json_repcode, db_representativecode_length) == 0) {
                    
                    if (json_object_object_get_ex(schedule, "VisitNote", &note_obj)) {
                        const char *json_note = json_object_get_string(note_obj);
                        if ((json_note && strlen(json_note) > 0 && db_note_count == 0) ||
                            (!json_note && db_note_count > 0)) {
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