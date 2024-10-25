#include "endpt_modules/visitrealizations.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>

static bool log_batch_status(const struct VisitRealizationBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("visitrealizations_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_visitrealization_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_visitrealization(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    char scheduleid[36] = {0};
    char projectid[36] = {0};
    char employeeid[36] = {0};
    char employeecode[20] = {0};
    char placeid[36] = {0};
    char placecode[20] = {0};
    char modifiedutc[20] = {0};
    char timezone[255] = {0};
    char schedulenote[1024] = {0};
    char status[20] = {0};
    char datetimestart[20] = {0};
    char datetimestartutc[20] = {0};
    char datetimeend[20] = {0};
    char datetimeendutc[20] = {0};
    char plandatetimestart[20] = {0};
    char plandatetimestartutc[20] = {0};
    char plandatetimeend[20] = {0};
    char plandatetimeendutc[20] = {0};
    int metacollectiontotalcount = 0;

    // NULL indicators
    bool null_indicators[19] = {0};

    struct json_object *temp;

    // Extract all fields from JSON
    if (json_object_object_get_ex(record, "ScheduleID", &temp))
        strncpy(scheduleid, json_object_get_string(temp), sizeof(scheduleid) - 1);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "ProjectID", &temp))
        strncpy(projectid, json_object_get_string(temp), sizeof(projectid) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "EmployeeID", &temp))
        strncpy(employeeid, json_object_get_string(temp), sizeof(employeeid) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "EmployeeCode", &temp))
        strncpy(employeecode, json_object_get_string(temp), sizeof(employeecode) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "PlaceID", &temp))
        strncpy(placeid, json_object_get_string(temp), sizeof(placeid) - 1);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "PlaceCode", &temp))
        strncpy(placecode, json_object_get_string(temp), sizeof(placecode) - 1);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "ModifiedUTC", &temp))
        strncpy(modifiedutc, json_object_get_string(temp), sizeof(modifiedutc) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "Timezone", &temp))
        strncpy(timezone, json_object_get_string(temp), sizeof(timezone) - 1);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "ScheduleNote", &temp))
        strncpy(schedulenote, json_object_get_string(temp), sizeof(schedulenote) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "Status", &temp))
        strncpy(status, json_object_get_string(temp), sizeof(status) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "DateTimeStart", &temp))
        strncpy(datetimestart, json_object_get_string(temp), sizeof(datetimestart) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "DateTimeStartUTC", &temp))
        strncpy(datetimestartutc, json_object_get_string(temp), sizeof(datetimestartutc) - 1);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "DateTimeEnd", &temp))
        strncpy(datetimeend, json_object_get_string(temp), sizeof(datetimeend) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "DateTimeEndUTC", &temp))
        strncpy(datetimeendutc, json_object_get_string(temp), sizeof(datetimeendutc) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "PlanDateTimeStart", &temp))
        strncpy(plandatetimestart, json_object_get_string(temp), sizeof(plandatetimestart) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "PlanDateTimeStartUTC", &temp))
        strncpy(plandatetimestartutc, json_object_get_string(temp), sizeof(plandatetimestartutc) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "PlanDateTimeEnd", &temp))
        strncpy(plandatetimeend, json_object_get_string(temp), sizeof(plandatetimeend) - 1);
    else
        null_indicators[16] = 1;

    if (json_object_object_get_ex(record, "PlanDateTimeEndUTC", &temp))
        strncpy(plandatetimeendutc, json_object_get_string(temp), sizeof(plandatetimeendutc) - 1);
    else
        null_indicators[17] = 1;

    // Meta fields will be set from batch metadata
    null_indicators[18] = 0; // metacollectiontotalcount

    // Bind parameters
    MYSQL_BIND bind[19];
    memset(bind, 0, sizeof(bind));

    // Bind all fields
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = scheduleid;
    bind[0].buffer_length = sizeof(scheduleid);
    bind[0].is_null = &null_indicators[0];

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = projectid;
    bind[1].buffer_length = sizeof(projectid);
    bind[1].is_null = &null_indicators[1];

    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = employeeid;
    bind[2].buffer_length = sizeof(employeeid);
    bind[2].is_null = &null_indicators[2];

    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = employeecode;
    bind[3].buffer_length = sizeof(employeecode);
    bind[3].is_null = &null_indicators[3];

    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = placeid;
    bind[4].buffer_length = sizeof(placeid);
    bind[4].is_null = &null_indicators[4];

    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = placecode;
    bind[5].buffer_length = sizeof(placecode);
    bind[5].is_null = &null_indicators[5];

    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = modifiedutc;
    bind[6].buffer_length = sizeof(modifiedutc);
    bind[6].is_null = &null_indicators[6];

    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = timezone;
    bind[7].buffer_length = sizeof(timezone);
    bind[7].is_null = &null_indicators[7];

    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = schedulenote;
    bind[8].buffer_length = sizeof(schedulenote);
    bind[8].is_null = &null_indicators[8];

    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = status;
    bind[9].buffer_length = sizeof(status);
    bind[9].is_null = &null_indicators[9];

    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = datetimestart;
    bind[10].buffer_length = sizeof(datetimestart);
    bind[10].is_null = &null_indicators[10];

    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = datetimestartutc;
    bind[11].buffer_length = sizeof(datetimestartutc);
    bind[11].is_null = &null_indicators[11];

    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = datetimeend;
    bind[12].buffer_length = sizeof(datetimeend);
    bind[12].is_null = &null_indicators[12];

    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = datetimeendutc;
    bind[13].buffer_length = sizeof(datetimeendutc);
    bind[13].is_null = &null_indicators[13];

    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = plandatetimestart;
    bind[14].buffer_length = sizeof(plandatetimestart);
    bind[14].is_null = &null_indicators[14];

    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = plandatetimestartutc;
    bind[15].buffer_length = sizeof(plandatetimestartutc);
    bind[15].is_null = &null_indicators[15];

    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = plandatetimeend;
    bind[16].buffer_length = sizeof(plandatetimeend);
    bind[16].is_null = &null_indicators[16];

    bind[17].buffer_type = MYSQL_TYPE_STRING;
    bind[17].buffer = plandatetimeendutc;
    bind[17].buffer_length = sizeof(plandatetimeendutc);
    bind[17].is_null = &null_indicators[17];

    bind[18].buffer_type = MYSQL_TYPE_LONG;
    bind[18].buffer = &metacollectiontotalcount;
    bind[18].is_null = &null_indicators[18];

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

int process_visitrealizations_batch(MYSQL *conn, const struct Endpoint *endpoint,
                                   struct json_object *batch,
                                   struct VisitRealizationBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct VisitRealizationBatchResult));
    
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
    struct json_object *realizations;
    if (!json_object_object_get_ex(batch, "VisitRealizations", &realizations)) {
        snprintf(result->error_message, sizeof(result->error_message), 
                "No VisitRealizations array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(realizations);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(realizations, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_visitrealization_record(conn, record) == 0) {
            if (mysql_query(conn, "COMMIT")) {
                snprintf(result->error_message, sizeof(result->error_message), 
                        "Failed to commit transaction: %s", mysql_error(conn));
                mysql_query(conn, "ROLLBACK");
                return -1;
            }
            result->records_inserted++;
        } else {
            mysql_query(conn, "ROLLBACK");
            result->records_failed++;
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to process visit realization record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing visit realizations: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_visitrealizations_batch(conn, result->last_id, realizations)) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_visitrealizations_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT vr.scheduleid, vr.employeeid, vr.placeid, "
                       "vr.status, vr.modifiedutc "
                       "FROM visitrealizations vr "
                       "ORDER BY vr.modifiedutc DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[5];
    char db_scheduleid[36];
    char db_employeeid[36];
    char db_placeid[36];
    char db_status[20];
    char db_modifiedutc[20];
    unsigned long db_scheduleid_length;
    unsigned long db_employeeid_length;
    unsigned long db_placeid_length;
    unsigned long db_status_length;
    unsigned long db_modifiedutc_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_STRING;
    bind_result[0].buffer = db_scheduleid;
    bind_result[0].buffer_length = sizeof(db_scheduleid);
    bind_result[0].length = &db_scheduleid_length;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_employeeid;
    bind_result[1].buffer_length = sizeof(db_employeeid);
    bind_result[1].length = &db_employeeid_length;

    bind_result[2].buffer_type = MYSQL_TYPE_STRING;
    bind_result[2].buffer = db_placeid;
    bind_result[2].buffer_length = sizeof(db_placeid);
    bind_result[2].length = &db_placeid_length;

    bind_result[3].buffer_type = MYSQL_TYPE_STRING;
    bind_result[3].buffer = db_status;
    bind_result[3].buffer_length = sizeof(db_status);
    bind_result[3].length = &db_status_length;

    bind_result[4].buffer_type = MYSQL_TYPE_STRING;
    bind_result[4].buffer = db_modifiedutc;
    bind_result[4].buffer_length = sizeof(db_modifiedutc);
    bind_result[4].length = &db_modifiedutc_length;

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
            struct json_object *realization = json_object_array_get_idx(original_data, i);
            struct json_object *scheduleid_obj, *employeeid_obj, *placeid_obj, 
                             *status_obj, *modifiedutc_obj;
            
            if (json_object_object_get_ex(realization, "ScheduleID", &scheduleid_obj) &&
                json_object_object_get_ex(realization, "EmployeeID", &employeeid_obj) &&
                json_object_object_get_ex(realization, "PlaceID", &placeid_obj) &&
                json_object_object_get_ex(realization, "Status", &status_obj) &&
                json_object_object_get_ex(realization, "ModifiedUTC", &modifiedutc_obj)) {
                
                const char *json_scheduleid = json_object_get_string(scheduleid_obj);
                const char *json_employeeid = json_object_get_string(employeeid_obj);
                const char *json_placeid = json_object_get_string(placeid_obj);
                const char *json_status = json_object_get_string(status_obj);
                const char *json_modifiedutc = json_object_get_string(modifiedutc_obj);

                if (strncmp(db_scheduleid, json_scheduleid, db_scheduleid_length) == 0 &&
                    strncmp(db_employeeid, json_employeeid, db_employeeid_length) == 0 &&
                    strncmp(db_placeid, json_placeid, db_placeid_length) == 0 &&
                    strncmp(db_status, json_status, db_status_length) == 0 &&
                    strncmp(db_modifiedutc, json_modifiedutc, db_modifiedutc_length) == 0) {
                    
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