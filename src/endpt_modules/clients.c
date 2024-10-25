// src/endpt_modules/clients.c
#include "endpt_modules/clients.h"
#include <mysql/mysql.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

void log_batch_status(const struct ClientBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("clients_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] LastTimestamp: %lld, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->last_timestamp, 
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_client_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        return -1;
    }

    const char *query = "CALL upsert_client(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int clientid = 0;
    long long timestamp = 0;
    char code[51] = {0};
    char name[256] = {0};
    bool active = 0;
    char tag[1024] = {0};
    char territory[81] = {0};
    char representativecode[21] = {0};
    char representativename[81] = {0};
    char streetaddress[256] = {0};
    char zip[21] = {0};
    char zipext[21] = {0};
    char city[256] = {0};
    char state[256] = {0};
    char country[256] = {0};
    char email[256] = {0};
    char phone[129] = {0};
    char mobile[129] = {0};
    char website[256] = {0};
    char contactname[256] = {0};
    char contacttitle[51] = {0};
    char note[256] = {0};
    char status[1024] = {0};
    char accountcode[1024] = {0};
    long long lasttimestamp = 0;
    int metacollectiontotalcount = 0;
    long long metacollectionlasttimestamp = 0;

    // NULL indicators
    bool null_indicators[27] = {0};

    // Extract values from JSON
    struct json_object *temp;

    if (json_object_object_get_ex(record, "ClientID", &temp))
        clientid = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "TimeStamp", &temp))
        timestamp = json_object_get_int64(temp);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "Code", &temp))
        strncpy(code, json_object_get_string(temp), sizeof(code) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "Name", &temp))
        strncpy(name, json_object_get_string(temp), sizeof(name) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "Active", &temp))
        active = json_object_get_boolean(temp);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "Tag", &temp))
        strncpy(tag, json_object_get_string(temp), sizeof(tag) - 1);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "Territory", &temp))
        strncpy(territory, json_object_get_string(temp), sizeof(territory) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "RepresentativeCode", &temp))
        strncpy(representativecode, json_object_get_string(temp), sizeof(representativecode) - 1);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "RepresentativeName", &temp))
        strncpy(representativename, json_object_get_string(temp), sizeof(representativename) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "StreetAddress", &temp))
        strncpy(streetaddress, json_object_get_string(temp), sizeof(streetaddress) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "ZIP", &temp))
        strncpy(zip, json_object_get_string(temp), sizeof(zip) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "ZIPExt", &temp))
        strncpy(zipext, json_object_get_string(temp), sizeof(zipext) - 1);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "City", &temp))
        strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "State", &temp))
        strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "Country", &temp))
        strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "Email", &temp))
        strncpy(email, json_object_get_string(temp), sizeof(email) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "Phone", &temp))
        strncpy(phone, json_object_get_string(temp), sizeof(phone) - 1);
    else
        null_indicators[16] = 1;

    if (json_object_object_get_ex(record, "Mobile", &temp))
        strncpy(mobile, json_object_get_string(temp), sizeof(mobile) - 1);
    else
        null_indicators[17] = 1;

    if (json_object_object_get_ex(record, "Website", &temp))
        strncpy(website, json_object_get_string(temp), sizeof(website) - 1);
    else
        null_indicators[18] = 1;

    if (json_object_object_get_ex(record, "ContactName", &temp))
        strncpy(contactname, json_object_get_string(temp), sizeof(contactname) - 1);
    else
        null_indicators[19] = 1;

    if (json_object_object_get_ex(record, "ContactTitle", &temp))
        strncpy(contacttitle, json_object_get_string(temp), sizeof(contacttitle) - 1);
    else
        null_indicators[20] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[21] = 1;

    if (json_object_object_get_ex(record, "Status", &temp))
        strncpy(status, json_object_get_string(temp), sizeof(status) - 1);
    else
        null_indicators[22] = 1;

    if (json_object_object_get_ex(record, "AccountCode", &temp))
        strncpy(accountcode, json_object_get_string(temp), sizeof(accountcode) - 1);
    else
        null_indicators[23] = 1;

    // Meta fields might come from the batch metadata rather than individual record
    lasttimestamp = timestamp;  // Using the record's timestamp as last timestamp
    null_indicators[24] = 0;

    metacollectiontotalcount = 0;  // These will be set from batch metadata
    null_indicators[25] = 0;

    metacollectionlasttimestamp = 0;  // These will be set from batch metadata
    null_indicators[26] = 0;

    // Bind parameters
    MYSQL_BIND bind[27];
    memset(bind, 0, sizeof(bind));

    // ClientID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &clientid;
    bind[0].is_null = &null_indicators[0];

    // TimeStamp
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &timestamp;
    bind[1].is_null = &null_indicators[1];

    // Code
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = code;
    bind[2].buffer_length = sizeof(code);
    bind[2].is_null = &null_indicators[2];

    // Name
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = name;
    bind[3].buffer_length = sizeof(name);
    bind[3].is_null = &null_indicators[3];

    // Active
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = &active;
    bind[4].is_null = &null_indicators[4];

    // Tag
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = tag;
    bind[5].buffer_length = sizeof(tag);
    bind[5].is_null = &null_indicators[5];

    // Territory
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = territory;
    bind[6].buffer_length = sizeof(territory);
    bind[6].is_null = &null_indicators[6];

    // RepresentativeCode
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = representativecode;
    bind[7].buffer_length = sizeof(representativecode);
    bind[7].is_null = &null_indicators[7];

    // RepresentativeName
    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = representativename;
    bind[8].buffer_length = sizeof(representativename);
    bind[8].is_null = &null_indicators[8];

    // StreetAddress
    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = streetaddress;
    bind[9].buffer_length = sizeof(streetaddress);
    bind[9].is_null = &null_indicators[9];

    // ZIP
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = zip;
    bind[10].buffer_length = sizeof(zip);
    bind[10].is_null = &null_indicators[10];

    // ZIPExt
    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = zipext;
    bind[11].buffer_length = sizeof(zipext);
    bind[11].is_null = &null_indicators[11];

    // City
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = city;
    bind[12].buffer_length = sizeof(city);
    bind[12].is_null = &null_indicators[12];

    // State
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = state;
    bind[13].buffer_length = sizeof(state);
    bind[13].is_null = &null_indicators[13];

    // Country
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = country;
    bind[14].buffer_length = sizeof(country);
    bind[14].is_null = &null_indicators[14];

    // Email
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = email;
    bind[15].buffer_length = sizeof(email);
    bind[15].is_null = &null_indicators[15];

    // Phone
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = phone;
    bind[16].buffer_length = sizeof(phone);
    bind[16].is_null = &null_indicators[16];

    // Mobile
    bind[17].buffer_type = MYSQL_TYPE_STRING;
    bind[17].buffer = mobile;
    bind[17].buffer_length = sizeof(mobile);
    bind[17].is_null = &null_indicators[17];

    // Website
    bind[18].buffer_type = MYSQL_TYPE_STRING;
    bind[18].buffer = website;
    bind[18].buffer_length = sizeof(website);
    bind[18].is_null = &null_indicators[18];

    // ContactName
    bind[19].buffer_type = MYSQL_TYPE_STRING;
    bind[19].buffer = contactname;
    bind[19].buffer_length = sizeof(contactname);
    bind[19].is_null = &null_indicators[19];

    // ContactTitle
    bind[20].buffer_type = MYSQL_TYPE_STRING;
    bind[20].buffer = contacttitle;
    bind[20].buffer_length = sizeof(contacttitle);
    bind[20].is_null = &null_indicators[20];

    // Note
    bind[21].buffer_type = MYSQL_TYPE_STRING;
    bind[21].buffer = note;
    bind[21].buffer_length = sizeof(note);
    bind[21].is_null = &null_indicators[21];

    // Status
    bind[22].buffer_type = MYSQL_TYPE_STRING;
    bind[22].buffer = status;
    bind[22].buffer_length = sizeof(status);
    bind[22].is_null = &null_indicators[22];

    // AccountCode
    bind[23].buffer_type = MYSQL_TYPE_STRING;
    bind[23].buffer = accountcode;
    bind[23].buffer_length = sizeof(accountcode);
    bind[23].is_null = &null_indicators[23];

    // LastTimeStamp
    bind[24].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[24].buffer = &lasttimestamp;
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

ProcessStatus process_clients_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct ClientBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct ClientBatchResult));
    
    // Extract metadata
    struct json_object *meta;
    if (json_object_object_get_ex(batch, "MetaCollectionResult", &meta)) {
        struct json_object *count_obj, *timestamp_obj;
        
        if (json_object_object_get_ex(meta, "TotalCount", &count_obj))
            result->total_count = json_object_get_int(count_obj);
        
        if (json_object_object_get_ex(meta, "LastTimeStamp", &timestamp_obj))
            result->last_timestamp = json_object_get_int64(timestamp_obj);
    }
    
    // Process each record in the batch
    struct json_object *records;
    if (!json_object_object_get_ex(batch, "Clients", &records)) {
        snprintf(result->error_message, sizeof(result->error_message), 
                "No Clients array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(records);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(records, i);
        result->records_processed++;
        
        // Start transaction for each record
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        bool success = true;
        
        // Process main client record
        if (process_client_record(conn, record) != 0) {
            success = false;
        }

        // If main record successful, process custom fields and price lists
        if (success) {
            struct json_object *custom_fields, *price_lists;
            int clientid = 0;
            struct json_object *clientid_obj;
            
            if (json_object_object_get_ex(record, "ClientID", &clientid_obj))
                clientid = json_object_get_int(clientid_obj);

            if (json_object_object_get_ex(record, "CustomFields", &custom_fields)) {
                if (process_client_custom_fields(conn, clientid, custom_fields) != 0) {
                    success = false;
                }
            }

            if (json_object_object_get_ex(record, "PriceLists", &price_lists)) {
                if (process_client_price_lists(conn, clientid, price_lists) != 0) {
                    success = false;
                }
            }
        }

        if (success) {
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
        }
    }

    // Verify batch if any records were processed
    if (result->records_processed > 0) {
        if (!verify_clients_batch(conn, result->last_timestamp, records)) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

int process_client_custom_fields(MYSQL *conn, int clientid, struct json_object *custom_fields) {
    if (!custom_fields || !json_object_is_type(custom_fields, json_type_array)) {
        return 0;  // No custom fields to process
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_client_customfield(?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    MYSQL_BIND bind[3];
    bool null_indicators[3] = {0};
    char field[256] = {0};
    char value[1024] = {0};

    memset(bind, 0, sizeof(bind));

    // ClientID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &clientid;
    bind[0].is_null = &null_indicators[0];

    // Field
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = field;
    bind[1].buffer_length = sizeof(field);
    bind[1].is_null = &null_indicators[1];

    // Value
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = value;
    bind[2].buffer_length = sizeof(value);
    bind[2].is_null = &null_indicators[2];

    int array_len = json_object_array_length(custom_fields);
    for (int i = 0; i < array_len; i++) {
        struct json_object *item = json_object_array_get_idx(custom_fields, i);
        struct json_object *field_obj, *value_obj;

        if (json_object_object_get_ex(item, "Field", &field_obj) &&
            json_object_object_get_ex(item, "Value", &value_obj)) {
            
            strncpy(field, json_object_get_string(field_obj), sizeof(field) - 1);
            strncpy(value, json_object_get_string(value_obj), sizeof(value) - 1);

            if (mysql_stmt_bind_param(stmt, bind)) {
                mysql_stmt_close(stmt);
                return -1;
            }

            if (mysql_stmt_execute(stmt)) {
                mysql_stmt_close(stmt);
                return -1;
            }
        }
    }

    mysql_stmt_close(stmt);
    return 0;
}

int process_client_price_lists(MYSQL *conn, int clientid, struct json_object *price_lists) {
    if (!price_lists || !json_object_is_type(price_lists, json_type_array)) {
        return 0;  // No price lists to process
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_client_pricelist(?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    MYSQL_BIND bind[2];
    bool null_indicators[2] = {0};
    char name[256] = {0};

    memset(bind, 0, sizeof(bind));

    // ClientID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &clientid;
    bind[0].is_null = &null_indicators[0];

    // Name
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = name;
    bind[1].buffer_length = sizeof(name);
    bind[1].is_null = &null_indicators[1];

    int array_len = json_object_array_length(price_lists);
    for (int i = 0; i < array_len; i++) {
        struct json_object *item = json_object_array_get_idx(price_lists, i);
        struct json_object *name_obj;

        if (json_object_object_get_ex(item, "Name", &name_obj)) {
            strncpy(name, json_object_get_string(name_obj), sizeof(name) - 1);

            if (mysql_stmt_bind_param(stmt, bind)) {
                mysql_stmt_close(stmt);
                return -1;
            }

            if (mysql_stmt_execute(stmt)) {
                mysql_stmt_close(stmt);
                return -1;
            }
        }
    }

    mysql_stmt_close(stmt);
    return 0;
}

bool verify_clients_batch(MYSQL *conn, long long last_timestamp, 
                         struct json_object *original_data) {
    if (!conn || !original_data) return false;

    // Verify last 5 records from the batch
    const char *query = "SELECT clientid, timestamp FROM clients "
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
    int db_clientid;
    long long db_timestamp;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONG;
    bind_result[0].buffer = &db_clientid;
    bind_result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[1].buffer = &db_timestamp;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        // Verify this record exists in the original data with matching timestamp
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *item = json_object_array_get_idx(original_data, i);
            struct json_object *clientid_obj, *timestamp_obj;
            
            if (json_object_object_get_ex(item, "ClientID", &clientid_obj) &&
                json_object_object_get_ex(item, "TimeStamp", &timestamp_obj)) {
                
                int json_clientid = json_object_get_int(clientid_obj);
                long long json_timestamp = json_object_get_int64(timestamp_obj);
                
                if (db_clientid == json_clientid && db_timestamp == json_timestamp) {
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

