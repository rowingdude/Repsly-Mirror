#include <stdbool.h>
#include "endpt_modules/dailyworkingtime.h"
#include <mysql/mysql.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

bool log_batch_status(const struct DailyWorkingTimeBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("dailyworkingtime_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_dailyworkingtime_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_dailyworkingtime(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int dailyworkingtimeid = 0;
    char date[20] = {0};
    char dateandtimestart[20] = {0};
    char dateandtimeend[20] = {0};
    int length = 0;
    int mileagestart = 0;
    int mileageend = 0;
    int mileagetotal = 0;
    long long latitudestart = 0;
    long long longitudestart = 0;
    long long latitudeend = 0;
    long long longitudeend = 0;
    char representativecode[20] = {0};
    char representativename[80] = {0};
    char note[255] = {0};
    char tag[1024] = {0};
    int noofvisits = 0;
    char minofvisits[20] = {0};
    char maxofvisits[20] = {0};
    int minmaxvisitstime = 0;
    int timeatclient = 0;
    int timeattravel = 0;
    int metacollectiontotalcount = 0;
    int metacollectionfirstid = 0;
    int metacollectionlastid = 0;

    // NULL indicators
    bool null_indicators[23] = {0};

    struct json_object *temp;


    // Extract all fields from JSON
    if (json_object_object_get_ex(record, "DailyWorkingTimeID", &temp))
        dailyworkingtimeid = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "Date", &temp))
        strncpy(date, json_object_get_string(temp), sizeof(date) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "DateAndTimeStart", &temp))
        strncpy(dateandtimestart, json_object_get_string(temp), sizeof(dateandtimestart) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "DateAndTimeEnd", &temp))
        strncpy(dateandtimeend, json_object_get_string(temp), sizeof(dateandtimeend) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "Length", &temp))
        length = json_object_get_int(temp);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "MileageStart", &temp))
        mileagestart = json_object_get_int(temp);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "MileageEnd", &temp))
        mileageend = json_object_get_int(temp);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "MileageTotal", &temp))
        mileagetotal = json_object_get_int(temp);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "LatitudeStart", &temp))
        latitudestart = json_object_get_int64(temp);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "LongitudeStart", &temp))
        longitudestart = json_object_get_int64(temp);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "LatitudeEnd", &temp))
        latitudeend = json_object_get_int64(temp);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "LongitudeEnd", &temp))
        longitudeend = json_object_get_int64(temp);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "RepresentativeCode", &temp))
        strncpy(representativecode, json_object_get_string(temp), sizeof(representativecode) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "RepresentativeName", &temp))
        strncpy(representativename, json_object_get_string(temp), sizeof(representativename) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "Tag", &temp))
        strncpy(tag, json_object_get_string(temp), sizeof(tag) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "NoOfVisits", &temp))
        noofvisits = json_object_get_int(temp);
    else
        null_indicators[16] = 1;

    if (json_object_object_get_ex(record, "MinOfVisits", &temp))
        strncpy(minofvisits, json_object_get_string(temp), sizeof(minofvisits) - 1);
    else
        null_indicators[17] = 1;

    if (json_object_object_get_ex(record, "MaxOfVisits", &temp))
        strncpy(maxofvisits, json_object_get_string(temp), sizeof(maxofvisits) - 1);
    else
        null_indicators[18] = 1;

    if (json_object_object_get_ex(record, "MinMaxVisitsTime", &temp))
        minmaxvisitstime = json_object_get_int(temp);
    else
        null_indicators[19] = 1;

    if (json_object_object_get_ex(record, "TimeAtClient", &temp))
        timeatclient = json_object_get_int(temp);
    else
        null_indicators[20] = 1;

    if (json_object_object_get_ex(record, "TimeAtTravel", &temp))
        timeattravel = json_object_get_int(temp);
    else
        null_indicators[21] = 1;

    null_indicators[22] = 0; // metacollectiontotalcount
    null_indicators[23] = 0; // metacollectionfirstid
    null_indicators[24] = 0; // metacollectionlastid

    // Bind all parameters
    MYSQL_BIND bind[25];
    memset(bind, 0, sizeof(bind));

    // DailyWorkingTimeID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &dailyworkingtimeid;
    bind[0].is_null = &null_indicators[0];

    // Date
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = date;
    bind[1].buffer_length = sizeof(date);
    bind[1].is_null = &null_indicators[1];

    // DateAndTimeStart
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = dateandtimestart;
    bind[2].buffer_length = sizeof(dateandtimestart);
    bind[2].is_null = &null_indicators[2];

    // DateAndTimeEnd
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = dateandtimeend;
    bind[3].buffer_length = sizeof(dateandtimeend);
    bind[3].is_null = &null_indicators[3];

    // Length
    bind[4].buffer_type = MYSQL_TYPE_LONG;
    bind[4].buffer = &length;
    bind[4].is_null = &null_indicators[4];

    // MileageStart
    bind[5].buffer_type = MYSQL_TYPE_LONG;
    bind[5].buffer = &mileagestart;
    bind[5].is_null = &null_indicators[5];

    // MileageEnd
    bind[6].buffer_type = MYSQL_TYPE_LONG;
    bind[6].buffer = &mileageend;
    bind[6].is_null = &null_indicators[6];

    // MileageTotal
    bind[7].buffer_type = MYSQL_TYPE_LONG;
    bind[7].buffer = &mileagetotal;
    bind[7].is_null = &null_indicators[7];

    // LatitudeStart
    bind[8].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[8].buffer = &latitudestart;
    bind[8].is_null = &null_indicators[8];

    // LongitudeStart
    bind[9].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[9].buffer = &longitudestart;
    bind[9].is_null = &null_indicators[9];

    // LatitudeEnd
    bind[10].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[10].buffer = &latitudeend;
    bind[10].is_null = &null_indicators[10];

    // LongitudeEnd
    bind[11].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[11].buffer = &longitudeend;
    bind[11].is_null = &null_indicators[11];

    // RepresentativeCode
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = representativecode;
    bind[12].buffer_length = sizeof(representativecode);
    bind[12].is_null = &null_indicators[12];

    // RepresentativeName
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = representativename;
    bind[13].buffer_length = sizeof(representativename);
    bind[13].is_null = &null_indicators[13];

    // Note
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = note;
    bind[14].buffer_length = sizeof(note);
    bind[14].is_null = &null_indicators[14];

    // Tag
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = tag;
    bind[15].buffer_length = sizeof(tag);
    bind[15].is_null = &null_indicators[15];

    // NoOfVisits
    bind[16].buffer_type = MYSQL_TYPE_LONG;
    bind[16].buffer = &noofvisits;
    bind[16].is_null = &null_indicators[16];

    // MinOfVisits
    bind[17].buffer_type = MYSQL_TYPE_STRING;
    bind[17].buffer = minofvisits;
    bind[17].buffer_length = sizeof(minofvisits);
    bind[17].is_null = &null_indicators[17];

    // MaxOfVisits
    bind[18].buffer_type = MYSQL_TYPE_STRING;
    bind[18].buffer = maxofvisits;
    bind[18].buffer_length = sizeof(maxofvisits);
    bind[18].is_null = &null_indicators[18];

    // MinMaxVisitsTime
    bind[19].buffer_type = MYSQL_TYPE_LONG;
    bind[19].buffer = &minmaxvisitstime;
    bind[19].is_null = &null_indicators[19];

    // TimeAtClient
    bind[20].buffer_type = MYSQL_TYPE_LONG;
    bind[20].buffer = &timeatclient;
    bind[20].is_null = &null_indicators[20];

    // TimeAtTravel
    bind[21].buffer_type = MYSQL_TYPE_LONG;
    bind[21].buffer = &timeattravel;
    bind[21].is_null = &null_indicators[21];

    // MetaCollectionTotalCount
    bind[22].buffer_type = MYSQL_TYPE_LONG;
    bind[22].buffer = &metacollectiontotalcount;
    bind[22].is_null = &null_indicators[22];

    // MetaCollectionFirstID
    bind[23].buffer_type = MYSQL_TYPE_LONG;
    bind[23].buffer = &metacollectionfirstid;
    bind[23].is_null = &null_indicators[23];

    // MetaCollectionLastID
    bind[24].buffer_type = MYSQL_TYPE_LONG;
    bind[24].buffer = &metacollectionlastid;
    bind[24].is_null = &null_indicators[24];

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

int process_dailyworkingtimes_batch(MYSQL *conn, const struct Endpoint *endpoint __attribute__((unused)),
                                   struct json_object *batch,
                                   struct DailyWorkingTimeBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct DailyWorkingTimeBatchResult));
    
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
    struct json_object *working_times;
    if (!json_object_object_get_ex(batch, "DailyWorkingTime", &working_times)) {
        snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                "No DailyWorkingTime array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(working_times);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(working_times, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_dailyworkingtime_record(conn, record) == 0) {
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
                    "Failed to process daily working time record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing daily working times: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_dailyworkingtimes_batch(conn, result->last_id, working_times)) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_dailyworkingtimes_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT dwt.dailyworkingtimeid, dwt.representativecode, "
                       "dwt.date, dwt.dateandtimestart, dwt.dateandtimeend, "
                       "dwt.length, dwt.mileagetotal, dwt.noofvisits, "
                       "dwt.timeatclient, dwt.timeattravel "
                       "FROM dailyworkingtime dwt "
                       "WHERE dwt.dailyworkingtimeid <= ? "
                       "ORDER BY dwt.dailyworkingtimeid DESC LIMIT 5";

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

    MYSQL_BIND bind_result[10];
    int db_id;
    char db_repcode[20];
    char db_date[20];
    char db_timestart[20];
    char db_timeend[20];
    int db_length;
    int db_mileage;
    int db_visits;
    int db_clienttime;
    int db_traveltime;
    unsigned long db_repcode_length;
    unsigned long db_date_length;
    unsigned long db_timestart_length;
    unsigned long db_timeend_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONG;
    bind_result[0].buffer = &db_id;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_repcode;
    bind_result[1].buffer_length = sizeof(db_repcode);
    bind_result[1].length = &db_repcode_length;

    bind_result[2].buffer_type = MYSQL_TYPE_STRING;
    bind_result[2].buffer = db_date;
    bind_result[2].buffer_length = sizeof(db_date);
    bind_result[2].length = &db_date_length;

    bind_result[3].buffer_type = MYSQL_TYPE_STRING;
    bind_result[3].buffer = db_timestart;
    bind_result[3].buffer_length = sizeof(db_timestart);
    bind_result[3].length = &db_timestart_length;

    bind_result[4].buffer_type = MYSQL_TYPE_STRING;
    bind_result[4].buffer = db_timeend;
    bind_result[4].buffer_length = sizeof(db_timeend);
    bind_result[4].length = &db_timeend_length;

    bind_result[5].buffer_type = MYSQL_TYPE_LONG;
    bind_result[5].buffer = &db_length;

    bind_result[6].buffer_type = MYSQL_TYPE_LONG;
    bind_result[6].buffer = &db_mileage;

    bind_result[7].buffer_type = MYSQL_TYPE_LONG;
    bind_result[7].buffer = &db_visits;

    bind_result[8].buffer_type = MYSQL_TYPE_LONG;
    bind_result[8].buffer = &db_clienttime;

    bind_result[9].buffer_type = MYSQL_TYPE_LONG;
    bind_result[9].buffer = &db_traveltime;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *dwt = json_object_array_get_idx(original_data, i);
            struct json_object *id_obj, *repcode_obj, *date_obj, *timestart_obj, 
                             *timeend_obj, *length_obj, *mileage_obj, *visits_obj,
                             *clienttime_obj, *traveltime_obj;
            
            if (json_object_object_get_ex(dwt, "DailyWorkingTimeID", &id_obj) &&
                json_object_object_get_ex(dwt, "RepresentativeCode", &repcode_obj) &&
                json_object_object_get_ex(dwt, "Date", &date_obj) &&
                json_object_object_get_ex(dwt, "DateAndTimeStart", &timestart_obj) &&
                json_object_object_get_ex(dwt, "DateAndTimeEnd", &timeend_obj) &&
                json_object_object_get_ex(dwt, "Length", &length_obj) &&
                json_object_object_get_ex(dwt, "MileageTotal", &mileage_obj) &&
                json_object_object_get_ex(dwt, "NoOfVisits", &visits_obj) &&
                json_object_object_get_ex(dwt, "TimeAtClient", &clienttime_obj) &&
                json_object_object_get_ex(dwt, "TimeAtTravel", &traveltime_obj)) {
                
                int json_id = json_object_get_int(id_obj);
                const char *json_repcode = json_object_get_string(repcode_obj);
                const char *json_date = json_object_get_string(date_obj);
                const char *json_timestart = json_object_get_string(timestart_obj);
                const char *json_timeend = json_object_get_string(timeend_obj);
                int json_length = json_object_get_int(length_obj);
                int json_mileage = json_object_get_int(mileage_obj);
                int json_visits = json_object_get_int(visits_obj);
                int json_clienttime = json_object_get_int(clienttime_obj);
                int json_traveltime = json_object_get_int(traveltime_obj);

                if (db_id == json_id &&
                    strncmp(db_repcode, json_repcode, db_repcode_length) == 0 &&
                    strncmp(db_date, json_date, db_date_length) == 0 &&
                    strncmp(db_timestart, json_timestart, db_timestart_length) == 0 &&
                    strncmp(db_timeend, json_timeend, db_timeend_length) == 0 &&
                    db_length == json_length &&
                    db_mileage == json_mileage &&
                    db_visits == json_visits &&
                    db_clienttime == json_clienttime &&
                    db_traveltime == json_traveltime) {
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
