// src/endpt_modules/visits.c
#include "endpt_modules/visits.h"
#include <stdio.h>
#include <string.h>
#include <mysql/mysql.h>
#include <time.h>

bool log_batch_status(const struct VisitBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("visits_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] LastTimestamp: %lld, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->last_timestamp,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_visits_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                        struct json_object *batch, struct VisitBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct VisitBatchResult));
    
    // Extract metadata
    struct json_object *meta;
    if (json_object_object_get_ex(batch, "MetaCollectionResult", &meta)) {
        struct json_object *count_obj, *timestamp_obj;
        
        if (json_object_object_get_ex(meta, "TotalCount", &count_obj))
            result->total_count = json_object_get_int(count_obj);
        
        if (json_object_object_get_ex(meta, "LastTimeStamp", &timestamp_obj))
            result->last_timestamp = json_object_get_int64(timestamp_obj);
    }

    // Process records
    struct json_object *records;
    if (!json_object_object_get_ex(batch, "Visits", &records)) {
        snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                "No Visits array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(records);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(records, i);
        result->records_processed++;
        
        // Start transaction for each record
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_visit_record(conn, record) == 0) {
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
        }
    }

    // Verify batch if any records were processed
    if (result->records_processed > 0) {
        if (!verify_visits_batch(conn, result->last_timestamp, records)) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

int process_visit_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_visit(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int visitid = 0;
    long long timestamp = 0;
    char date[64] = {0};
    char representativecode[21] = {0};
    char representativename[81] = {0};
    bool explicitcheckin = false;
    char dateandtimestart[64] = {0};
    char dateandtimeend[64] = {0};
    char clientcode[51] = {0};
    char clientname[256] = {0};
    char streetaddress[256] = {0};
    char zip[21] = {0};
    char zipext[21] = {0};
    char city[256] = {0};
    char state[256] = {0};
    char country[256] = {0};
    char territory[81] = {0};
    long long latitudestart = 0;
    long long longitudestart = 0;
    long long latitudeend = 0;
    long long longitudeend = 0;
    int precisionstart = 0;
    int precisionend = 0;
    int visitstatusbyschedule = 0;
    bool visitended = false;
    int metacollectiontotalcount = 0;
    long long metacollectionlasttimestamp = 0;

    // NULL indicators
    bool null_indicators[27] = {0};

    // Extract values from JSON
    struct json_object *temp;

    if (json_object_object_get_ex(record, "VisitID", &temp))
        visitid = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "TimeStamp", &temp))
        timestamp = json_object_get_int64(temp);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "Date", &temp)) {
        const char *date_str = json_object_get_string(temp);
        // TODO: Consider using convert_date function here
        strncpy(date, date_str, sizeof(date) - 1);
    }
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "RepresentativeCode", &temp))
        strncpy(representativecode, json_object_get_string(temp), sizeof(representativecode) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "RepresentativeName", &temp))
        strncpy(representativename, json_object_get_string(temp), sizeof(representativename) - 1);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "ExplicitCheckIn", &temp))
        explicitcheckin = json_object_get_boolean(temp);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "DateAndTimeStart", &temp)) {
        const char *start_str = json_object_get_string(temp);
        strncpy(dateandtimestart, start_str, sizeof(dateandtimestart) - 1);
    }
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "DateAndTimeEnd", &temp)) {
        const char *end_str = json_object_get_string(temp);
        strncpy(dateandtimeend, end_str, sizeof(dateandtimeend) - 1);
    }
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "ClientCode", &temp))
        strncpy(clientcode, json_object_get_string(temp), sizeof(clientcode) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "ClientName", &temp))
        strncpy(clientname, json_object_get_string(temp), sizeof(clientname) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "StreetAddress", &temp))
        strncpy(streetaddress, json_object_get_string(temp), sizeof(streetaddress) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "ZIP", &temp))
        strncpy(zip, json_object_get_string(temp), sizeof(zip) - 1);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "ZIPExt", &temp))
        strncpy(zipext, json_object_get_string(temp), sizeof(zipext) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "City", &temp))
        strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "State", &temp))
        strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "Country", &temp))
        strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "Territory", &temp))
        strncpy(territory, json_object_get_string(temp), sizeof(territory) - 1);
    else
        null_indicators[16] = 1;

    if (json_object_object_get_ex(record, "LatitudeStart", &temp))
        latitudestart = json_object_get_int64(temp);
    else
        null_indicators[17] = 1;

    if (json_object_object_get_ex(record, "LongitudeStart", &temp))
        longitudestart = json_object_get_int64(temp);
    else
        null_indicators[18] = 1;

    if (json_object_object_get_ex(record, "LatitudeEnd", &temp))
        latitudeend = json_object_get_int64(temp);
    else
        null_indicators[19] = 1;

    if (json_object_object_get_ex(record, "LongitudeEnd", &temp))
        longitudeend = json_object_get_int64(temp);
    else
        null_indicators[20] = 1;

    if (json_object_object_get_ex(record, "PrecisionStart", &temp))
        precisionstart = json_object_get_int(temp);
    else
        null_indicators[21] = 1;

    if (json_object_object_get_ex(record, "PrecisionEnd", &temp))
        precisionend = json_object_get_int(temp);
    else
        null_indicators[22] = 1;

    if (json_object_object_get_ex(record, "VisitStatusBySchedule", &temp))
        visitstatusbyschedule = json_object_get_int(temp);
    else
        null_indicators[23] = 1;

    if (json_object_object_get_ex(record, "VisitEnded", &temp))
        visitended = json_object_get_boolean(temp);
    else
        null_indicators[24] = 1;

    // Meta fields will be set from batch metadata
    null_indicators[25] = 0; // metacollectiontotalcount
    null_indicators[26] = 0; // metacollectionlasttimestamp

    // Bind parameters
    MYSQL_BIND bind[27];
    memset(bind, 0, sizeof(bind));

    // VisitID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &visitid;
    bind[0].is_null = &null_indicators[0];

    // TimeStamp
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &timestamp;
    bind[1].is_null = &null_indicators[1];

    // Date
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = date;
    bind[2].buffer_length = sizeof(date);
    bind[2].is_null = &null_indicators[2];

    // RepresentativeCode
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = representativecode;
    bind[3].buffer_length = sizeof(representativecode);
    bind[3].is_null = &null_indicators[3];

    // RepresentativeName
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = representativename;
    bind[4].buffer_length = sizeof(representativename);
    bind[4].is_null = &null_indicators[4];

    // ExplicitCheckIn
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = &explicitcheckin;
    bind[5].is_null = &null_indicators[5];

    // DateAndTimeStart
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = dateandtimestart;
    bind[6].buffer_length = sizeof(dateandtimestart);
    bind[6].is_null = &null_indicators[6];

    // DateAndTimeEnd
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = dateandtimeend;
    bind[7].buffer_length = sizeof(dateandtimeend);
    bind[7].is_null = &null_indicators[7];

    // ClientCode
    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = clientcode;
    bind[8].buffer_length = sizeof(clientcode);
    bind[8].is_null = &null_indicators[8];

    // ClientName
    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = clientname;
    bind[9].buffer_length = sizeof(clientname);
    bind[9].is_null = &null_indicators[9];

    // StreetAddress
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = streetaddress;
    bind[10].buffer_length = sizeof(streetaddress);
    bind[10].is_null = &null_indicators[10];

    // ZIP
    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = zip;
    bind[11].buffer_length = sizeof(zip);
    bind[11].is_null = &null_indicators[11];

    // ZIPExt
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = zipext;
    bind[12].buffer_length = sizeof(zipext);
    bind[12].is_null = &null_indicators[12];

    // City
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = city;
    bind[13].buffer_length = sizeof(city);
    bind[13].is_null = &null_indicators[13];

    // State
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = state;
    bind[14].buffer_length = sizeof(state);
    bind[14].is_null = &null_indicators[14];

    // Country
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = country;
    bind[15].buffer_length = sizeof(country);
    bind[15].is_null = &null_indicators[15];

    // Territory
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = territory;
    bind[16].buffer_length = sizeof(territory);
    bind[16].is_null = &null_indicators[16];

    // LatitudeStart
    bind[17].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[17].buffer = &latitudestart;
    bind[17].is_null = &null_indicators[17];

    // LongitudeStart
    bind[18].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[18].buffer = &longitudestart;
    bind[18].is_null = &null_indicators[18];

    // LatitudeEnd
    bind[19].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[19].buffer = &latitudeend;
    bind[19].is_null = &null_indicators[19];

    // LongitudeEnd
    bind[20].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[20].buffer = &longitudeend;
    bind[20].is_null = &null_indicators[20];

    // PrecisionStart
    bind[21].buffer_type = MYSQL_TYPE_LONG;
    bind[21].buffer = &precisionstart;
    bind[21].is_null = &null_indicators[21];

    // PrecisionEnd
    bind[22].buffer_type = MYSQL_TYPE_LONG;
    bind[22].buffer = &precisionend;
    bind[22].is_null = &null_indicators[22];

    // VisitStatusBySchedule
    bind[23].buffer_type = MYSQL_TYPE_LONG;
    bind[23].buffer = &visitstatusbyschedule;
    bind[23].is_null = &null_indicators[23];

    // VisitEnded
    bind[24].buffer_type = MYSQL_TYPE_TINY;
    bind[24].buffer = &visitended;
    bind[24].is_null = &null_indicators[24];

    // MetaCollectionTotalCount
    bind[25].buffer_type = MYSQL_TYPE_LONG;
    bind[25].buffer = &metacollectiontotalcount;
    bind[25].is_null = &null_indicators[25];

    // MetaCollectionLastTimeStamp
    bind[26].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[26].buffer = &metacollectionlasttimestamp;
    bind[26].is_null = &null_indicators[26];

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

bool verify_visits_batch(MYSQL *conn, long long last_timestamp, 
                        struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT visitid, timestamp FROM visits "
                       "WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_param[1];
    memset(bind_param, 0, sizeof(bind_param));
    
    bind_param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_param[0].buffer = &last_timestamp;

    if (mysql_stmt_bind_param(stmt, bind_param)) {
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[2];
    int db_visitid;
    long long db_timestamp;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONG;
    bind_result[0].buffer = &db_visitid;
    bind_result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[1].buffer = &db_timestamp;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *item = json_object_array_get_idx(original_data, i);
            struct json_object *visitid_obj, *timestamp_obj;
            
            if (json_object_object_get_ex(item, "VisitID", &visitid_obj) &&
                json_object_object_get_ex(item, "TimeStamp", &timestamp_obj)) {
                
                int json_visitid = json_object_get_int(visitid_obj);
                long long json_timestamp = json_object_get_int64(timestamp_obj);
                
                if (db_visitid == json_visitid && db_timestamp == json_timestamp) {
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