#include "endpt_modules/clientnotes.h"
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>

bool log_batch_status(const struct ClientNoteBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("clientnotes_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_clientnote_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_client_note(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int clientnoteid = 0;
    long long timestamp = 0;
    char dateandtime[64] = {0};
    char representativecode[21] = {0};
    char representativename[81] = {0};
    char clientcode[51] = {0};
    char clientname[256] = {0};
    char streetaddress[256] = {0};
    char zip[21] = {0};
    char zipext[21] = {0};
    char city[256] = {0};
    char state[256] = {0};
    char country[256] = {0};
    char email[256] = {0};
    char phone[129] = {0};
    char mobile[129] = {0};
    char territory[81] = {0};
    long long longitude = 0;
    long long latitude = 0;
    char note[65535] = {0};  // Using TEXT type for note
    int visitid = 0;
    int metacollectiontotalcount = 0;
    int metacollectionfirstid = 0;
    int metacollectionlastid = 0;

    // NULL indicators
    bool null_indicators[24] = {0};

    // Extract values from JSON
    struct json_object *temp;

    if (json_object_object_get_ex(record, "ClientNoteID", &temp))
        clientnoteid = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "TimeStamp", &temp))
        timestamp = json_object_get_int64(temp);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "DateAndTime", &temp)) {
        const char *date_str = json_object_get_string(temp);
        strncpy(dateandtime, date_str, sizeof(dateandtime) - 1);
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

    if (json_object_object_get_ex(record, "ClientCode", &temp))
        strncpy(clientcode, json_object_get_string(temp), sizeof(clientcode) - 1);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "ClientName", &temp))
        strncpy(clientname, json_object_get_string(temp), sizeof(clientname) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "StreetAddress", &temp))
        strncpy(streetaddress, json_object_get_string(temp), sizeof(streetaddress) - 1);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "ZIP", &temp))
        strncpy(zip, json_object_get_string(temp), sizeof(zip) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "ZIPExt", &temp))
        strncpy(zipext, json_object_get_string(temp), sizeof(zipext) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "City", &temp))
        strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "State", &temp))
        strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "Country", &temp))
        strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "Email", &temp))
        strncpy(email, json_object_get_string(temp), sizeof(email) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "Phone", &temp))
        strncpy(phone, json_object_get_string(temp), sizeof(phone) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "Mobile", &temp))
        strncpy(mobile, json_object_get_string(temp), sizeof(mobile) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "Territory", &temp))
        strncpy(territory, json_object_get_string(temp), sizeof(territory) - 1);
    else
        null_indicators[16] = 1;

    if (json_object_object_get_ex(record, "Longitude", &temp))
        longitude = json_object_get_int64(temp);
    else
        null_indicators[17] = 1;

    if (json_object_object_get_ex(record, "Latitude", &temp))
        latitude = json_object_get_int64(temp);
    else
        null_indicators[18] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[19] = 1;

    if (json_object_object_get_ex(record, "VisitID", &temp))
        visitid = json_object_get_int(temp);
    else
        null_indicators[20] = 1;

    // Meta fields will be set from batch metadata
    null_indicators[21] = 0; // metacollectiontotalcount
    null_indicators[22] = 0; // metacollectionfirstid
    null_indicators[23] = 0; // metacollectionlastid

    // Bind parameters
    MYSQL_BIND bind[24];
    memset(bind, 0, sizeof(bind));

    // ClientNoteID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &clientnoteid;
    bind[0].is_null = &null_indicators[0];

    // TimeStamp
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &timestamp;
    bind[1].is_null = &null_indicators[1];

    // DateAndTime
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = dateandtime;
    bind[2].buffer_length = sizeof(dateandtime);
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

    // ClientCode
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = clientcode;
    bind[5].buffer_length = sizeof(clientcode);
    bind[5].is_null = &null_indicators[5];

    // ClientName
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = clientname;
    bind[6].buffer_length = sizeof(clientname);
    bind[6].is_null = &null_indicators[6];

    // StreetAddress
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = streetaddress;
    bind[7].buffer_length = sizeof(streetaddress);
    bind[7].is_null = &null_indicators[7];

    // ZIP
    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = zip;
    bind[8].buffer_length = sizeof(zip);
    bind[8].is_null = &null_indicators[8];

    // ZIPExt
    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = zipext;
    bind[9].buffer_length = sizeof(zipext);
    bind[9].is_null = &null_indicators[9];

    // City
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = city;
    bind[10].buffer_length = sizeof(city);
    bind[10].is_null = &null_indicators[10];

    // State
    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = state;
    bind[11].buffer_length = sizeof(state);
    bind[11].is_null = &null_indicators[11];

    // Country
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = country;
    bind[12].buffer_length = sizeof(country);
    bind[12].is_null = &null_indicators[12];

    // Email
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = email;
    bind[13].buffer_length = sizeof(email);
    bind[13].is_null = &null_indicators[13];

    // Phone
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = phone;
    bind[14].buffer_length = sizeof(phone);
    bind[14].is_null = &null_indicators[14];

    // Mobile
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = mobile;
    bind[15].buffer_length = sizeof(mobile);
    bind[15].is_null = &null_indicators[15];

    // Territory
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = territory;
    bind[16].buffer_length = sizeof(territory);
    bind[16].is_null = &null_indicators[16];

    // Longitude
    bind[17].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[17].buffer = &longitude;
    bind[17].is_null = &null_indicators[17];

    // Latitude
    bind[18].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[18].buffer = &latitude;
    bind[18].is_null = &null_indicators[18];

    // Note
    bind[19].buffer_type = MYSQL_TYPE_STRING;
    bind[19].buffer = note;
    bind[19].buffer_length = sizeof(note);
    bind[19].is_null = &null_indicators[19];

    // VisitID
    bind[20].buffer_type = MYSQL_TYPE_LONG;
    bind[20].buffer = &visitid;
    bind[20].is_null = &null_indicators[20];

    // MetaCollectionTotalCount
    bind[21].buffer_type = MYSQL_TYPE_LONG;
    bind[21].buffer = &metacollectiontotalcount;
    bind[21].is_null = &null_indicators[21];

    // MetaCollectionFirstID
    bind[22].buffer_type = MYSQL_TYPE_LONG;
    bind[22].buffer = &metacollectionfirstid;
    bind[22].is_null = &null_indicators[22];

    // MetaCollectionLastID
    bind[23].buffer_type = MYSQL_TYPE_LONG;
    bind[23].buffer = &metacollectionlastid;
    bind[23].is_null = &null_indicators[23];

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

int process_clientnotes_batch(MYSQL *conn, const struct Endpoint *endpoint __attribute__((unused)), 
                            struct json_object *batch, struct ClientNoteBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct ClientNoteBatchResult));
    
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
    struct json_object *records;
    if (!json_object_object_get_ex(batch, "ClientNotes", &records)) {
        snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                "No ClientNotes array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(records);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(records, i);
        result->records_processed++;
        
        if (process_clientnote_record(conn, record) == 0) {
            result->records_inserted++;
        } else {
            result->records_failed++;
        }
    }

    // Verify batch if any records were processed
    if (result->records_processed > 0) {
        if (!verify_clientnotes_batch(conn, result->last_id, records)) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_clientnotes_batch(MYSQL *conn, int last_id, 
                            struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT clientnoteid FROM clientnotes "
                       "WHERE clientnoteid <= ? ORDER BY clientnoteid DESC LIMIT 5";

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

    MYSQL_BIND bind_result[1];
    int db_clientnoteid;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONG;
    bind_result[0].buffer = &db_clientnoteid;

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
            struct json_object *id_obj;
            
            if (json_object_object_get_ex(item, "ClientNoteID", &id_obj)) {
                int json_id = json_object_get_int(id_obj);
                if (db_clientnoteid == json_id) {
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