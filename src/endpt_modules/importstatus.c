#include "endpt_modules/importstatus.h"
#include <stdio.h>
#include <string.h>
#include <mysql/mysql.h>
#include <stdbool.h>
#include <time.h>

static bool log_batch_status(const struct ImportStatusBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("import_status_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted,
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

static int process_import_warning(MYSQL *conn, long long importjobid, struct json_object *warning) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO import_warnings (importjobid, itemid, itemname, itemstatus) "
                       "VALUES (?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    char itemid[256] = {0};
    char itemname[256] = {0};
    char itemstatus[1024] = {0};
    bool null_indicators[4] = {0};

    struct json_object *temp;
    if (json_object_object_get_ex(warning, "ItemID", &temp))
        strncpy(itemid, json_object_get_string(temp), sizeof(itemid) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(warning, "ItemName", &temp))
        strncpy(itemname, json_object_get_string(temp), sizeof(itemname) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(warning, "ItemStatus", &temp))
        strncpy(itemstatus, json_object_get_string(temp), sizeof(itemstatus) - 1);
    else
        null_indicators[3] = 1;

    MYSQL_BIND bind[4];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &importjobid;

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = itemid;
    bind[1].buffer_length = strlen(itemid);
    bind[1].is_null = &null_indicators[1];

    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = itemname;
    bind[2].buffer_length = strlen(itemname);
    bind[2].is_null = &null_indicators[2];

    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = itemstatus;
    bind[3].buffer_length = strlen(itemstatus);
    bind[3].is_null = &null_indicators[3];

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

static int process_import_error(MYSQL *conn, long long importjobid, struct json_object *error) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO import_errors (importjobid, itemid, itemname, itemstatus) "
                       "VALUES (?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    char itemid[256] = {0};
    char itemname[256] = {0};
    char itemstatus[1024] = {0};
    bool null_indicators[4] = {0};

    struct json_object *temp;
    if (json_object_object_get_ex(error, "ItemID", &temp))
        strncpy(itemid, json_object_get_string(temp), sizeof(itemid) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(error, "ItemName", &temp))
        strncpy(itemname, json_object_get_string(temp), sizeof(itemname) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(error, "ItemStatus", &temp))
        strncpy(itemstatus, json_object_get_string(temp), sizeof(itemstatus) - 1);
    else
        null_indicators[3] = 1;

    MYSQL_BIND bind[4];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &importjobid;

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = itemid;
    bind[1].buffer_length = strlen(itemid);
    bind[1].is_null = &null_indicators[1];

    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = itemname;
    bind[2].buffer_length = strlen(itemname);
    bind[2].is_null = &null_indicators[2];

    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = itemstatus;
    bind[3].buffer_length = strlen(itemstatus);
    bind[3].is_null = &null_indicators[3];

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

int process_import_status_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO import_status (importjobid, importstatus, rowsinserted, "
                       "rowsupdated, rowsinvalid, rowstotal) VALUES (?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    long long importjobid = 0;
    char importstatus[21] = {0};
    int rowsinserted = 0;
    int rowsupdated = 0;
    int rowsinvalid = 0;
    int rowstotal = 0;

    bool null_indicators[6] = {0};

    struct json_object *temp;

    if (json_object_object_get_ex(record, "ImportJobID", &temp))
        importjobid = json_object_get_int64(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "ImportStatus", &temp))
        strncpy(importstatus, json_object_get_string(temp), sizeof(importstatus) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "RowsInserted", &temp))
        rowsinserted = json_object_get_int(temp);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "RowsUpdated", &temp))
        rowsupdated = json_object_get_int(temp);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "RowsInvalid", &temp))
        rowsinvalid = json_object_get_int(temp);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "RowsTotal", &temp))
        rowstotal = json_object_get_int(temp);
    else
        null_indicators[5] = 1;

    MYSQL_BIND bind[6];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &importjobid;
    bind[0].is_null = &null_indicators[0];

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = importstatus;
    bind[1].buffer_length = sizeof(importstatus);
    bind[1].is_null = &null_indicators[1];

    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = &rowsinserted;
    bind[2].is_null = &null_indicators[2];

    bind[3].buffer_type = MYSQL_TYPE_LONG;
    bind[3].buffer = &rowsupdated;
    bind[3].is_null = &null_indicators[3];

    bind[4].buffer_type = MYSQL_TYPE_LONG;
    bind[4].buffer = &rowsinvalid;
    bind[4].is_null = &null_indicators[4];

    bind[5].buffer_type = MYSQL_TYPE_LONG;
    bind[5].buffer = &rowstotal;
    bind[5].is_null = &null_indicators[5];

    if (mysql_stmt_bind_param(stmt, bind)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);

    // Process warnings if present
    struct json_object *warnings;
    if (json_object_object_get_ex(record, "Warnings", &warnings)) {
        int n_warnings = json_object_array_length(warnings);
        for (int i = 0; i < n_warnings; i++) {
            struct json_object *warning = json_object_array_get_idx(warnings, i);
            if (process_import_warning(conn, importjobid, warning) != 0) {
                return -1;
            }
        }
    }

    // Process errors if present
    struct json_object *errors;
    if (json_object_object_get_ex(record, "Errors", &errors)) {
        int n_errors = json_object_array_length(errors);
        for (int i = 0; i < n_errors; i++) {
            struct json_object *error = json_object_array_get_idx(errors, i);
            if (process_import_error(conn, importjobid, error) != 0) {
                return -1;
            }
        }
    }

    return 0;
}

int process_import_status_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ImportStatusBatchResult *result) {

    // Initialize result
    memset(result, 0, sizeof(struct ImportStatusBatchResult));
    
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
    struct json_object *statuses;
    if (!json_object_object_get_ex(batch, "ImportStatus", &statuses)) {
        snprintf(result->error_message, sizeof(result->error_message), 
                "No ImportStatus array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(statuses);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(statuses, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_import_status_record(conn, record) == 0) {
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
                    "Failed to process import status record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing import status: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_import_status_batch(conn, result->last_id, statuses)) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_import_status_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT is.importjobid, is.importstatus, is.rowstotal, "
                       "COUNT(DISTINCT iw.itemid) as warning_count, "
                       "COUNT(DISTINCT ie.itemid) as error_count "
                       "FROM import_status is "
                       "LEFT JOIN import_warnings iw ON is.importjobid = iw.importjobid "
                       "LEFT JOIN import_errors ie ON is.importjobid = ie.importjobid "
                       "WHERE is.importjobid <= ? "
                       "GROUP BY is.importjobid, is.importstatus, is.rowstotal "
                       "ORDER BY is.importjobid DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_param[1];
    memset(bind_param, 0, sizeof(bind_param));
    
    bind_param[0].buffer_type = MYSQL_TYPE_LONG;
    bind_param[0].buffer = &last_id;

    if (mysql_stmt_bind_param(stmt, bind_param)) {
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[5];
    long long db_jobid;
    char db_status[21];
    int db_rowstotal;
    long long db_warning_count;
    long long db_error_count;
    unsigned long db_status_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[0].buffer = &db_jobid;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_status;
    bind_result[1].buffer_length = sizeof(db_status);
    bind_result[1].length = &db_status_length;

    bind_result[2].buffer_type = MYSQL_TYPE_LONG;
    bind_result[2].buffer = &db_rowstotal;

    bind_result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[3].buffer = &db_warning_count;

    bind_result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[4].buffer = &db_error_count;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *status = json_object_array_get_idx(original_data, i);
            struct json_object *jobid_obj, *status_obj, *rowstotal_obj, 
                             *warnings_obj, *errors_obj;
            
            if (json_object_object_get_ex(status, "ImportJobID", &jobid_obj) &&
                json_object_object_get_ex(status, "ImportStatus", &status_obj) &&
                json_object_object_get_ex(status, "RowsTotal", &rowstotal_obj)) {
                
                long long json_jobid = json_object_get_int64(jobid_obj);
                const char *json_status = json_object_get_string(status_obj);
                int json_rowstotal = json_object_get_int(rowstotal_obj);

                if (db_jobid == json_jobid &&
                    strncmp(db_status, json_status, db_status_length) == 0 &&
                    db_rowstotal == json_rowstotal) {
                    
                    // Verify warning counts
                    int warning_count = 0;
                    if (json_object_object_get_ex(status, "Warnings", &warnings_obj)) {
                        warning_count = json_object_array_length(warnings_obj);
                    }
                    if (warning_count != db_warning_count) {
                        verification_passed = false;
                        break;
                    }

                    // Verify error counts
                    int error_count = 0;
                    if (json_object_object_get_ex(status, "Errors", &errors_obj)) {
                        error_count = json_object_array_length(errors_obj);
                    }
                    if (error_count != db_error_count) {
                        verification_passed = false;
                        break;
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