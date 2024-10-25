#include "endpt_modules/representatives.h"
#include <stdio.h>
#include <string.h>
#include <mysql/mysql.h>
#include <time.h>

static bool log_batch_status(const struct RepresentativeBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("representatives_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted,
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

static int process_representative_territory(MYSQL *conn, const char *rep_code, struct json_object *territory) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO representative_territories (representative_code, territory_path) VALUES (?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    const char *territory_path = json_object_get_string(territory);
    
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)rep_code;
    bind[0].buffer_length = strlen(rep_code);

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)territory_path;
    bind[1].buffer_length = strlen(territory_path);

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

static int process_representative_attribute(MYSQL *conn, const char *rep_code, 
                                         const char *title, const char *type, 
                                         struct json_object *value) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO representative_attributes "
                       "(representative_code, title, type, value) VALUES (?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    const char *value_str = json_object_get_string(value);
    
    MYSQL_BIND bind[4];
    memset(bind, 0, sizeof(bind));
    
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)rep_code;
    bind[0].buffer_length = strlen(rep_code);

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)title;
    bind[1].buffer_length = strlen(title);

    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)type;
    bind[2].buffer_length = strlen(type);

    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = (void*)value_str;
    bind[3].buffer_length = strlen(value_str);

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

static int clear_representative_related_records(MYSQL *conn, const char *rep_code) {
    const char *queries[] = {
        "DELETE FROM representative_territories WHERE representative_code = ?",
        "DELETE FROM representative_attributes WHERE representative_code = ?"
    };

    for (int i = 0; i < 2; i++) {
        MYSQL_STMT *stmt = mysql_stmt_init(conn);
        if (!stmt) return -1;

        if (mysql_stmt_prepare(stmt, queries[i], strlen(queries[i]))) {
            mysql_stmt_close(stmt);
            return -1;
        }

        MYSQL_BIND bind[1];
        memset(bind, 0, sizeof(bind));
        
        bind[0].buffer_type = MYSQL_TYPE_STRING;
        bind[0].buffer = (void*)rep_code;
        bind[0].buffer_length = strlen(rep_code);

        if (mysql_stmt_bind_param(stmt, bind)) {
            mysql_stmt_close(stmt);
            return -1;
        }

        if (mysql_stmt_execute(stmt)) {
            mysql_stmt_close(stmt);
            return -1;
        }

        mysql_stmt_close(stmt);
    }

    return 0;
}

int process_representative_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_representative(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    char code[20] = {0};
    char name[80] = {0};
    char note[255] = {0};
    char email[256] = {0};
    char phone[128] = {0};
    bool active = false;
    char address1[256] = {0};
    char address2[256] = {0};
    char city[256] = {0};
    char state[256] = {0};
    char zipcode[20] = {0};
    char zipcodeext[20] = {0};
    char country[256] = {0};
    char countrycode[20] = {0};

    // NULL indicators
    bool null_indicators[14] = {0};

    struct json_object *temp;

    // Extract all fields from JSON
    if (json_object_object_get_ex(record, "Code", &temp))
        strncpy(code, json_object_get_string(temp), sizeof(code) - 1);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "Name", &temp))
        strncpy(name, json_object_get_string(temp), sizeof(name) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "Email", &temp))
        strncpy(email, json_object_get_string(temp), sizeof(email) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "Phone", &temp))
        strncpy(phone, json_object_get_string(temp), sizeof(phone) - 1);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "Active", &temp))
        active = json_object_get_boolean(temp);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "Address1", &temp))
        strncpy(address1, json_object_get_string(temp), sizeof(address1) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "Address2", &temp))
        strncpy(address2, json_object_get_string(temp), sizeof(address2) - 1);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "City", &temp))
        strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "State", &temp))
        strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "ZipCode", &temp))
        strncpy(zipcode, json_object_get_string(temp), sizeof(zipcode) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "ZipCodeExt", &temp))
        strncpy(zipcodeext, json_object_get_string(temp), sizeof(zipcodeext) - 1);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "Country", &temp))
        strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "CountryCode", &temp))
        strncpy(countrycode, json_object_get_string(temp), sizeof(countrycode) - 1);
    else
        null_indicators[13] = 1;

    MYSQL_BIND bind[14];
    memset(bind, 0, sizeof(bind));

    // Code
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = code;
    bind[0].buffer_length = sizeof(code);
    bind[0].is_null = &null_indicators[0];

    // Name
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = name;
    bind[1].buffer_length = sizeof(name);
    bind[1].is_null = &null_indicators[1];

    // Note
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = note;
    bind[2].buffer_length = sizeof(note);
    bind[2].is_null = &null_indicators[2];

    // Email
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = email;
    bind[3].buffer_length = sizeof(email);
    bind[3].is_null = &null_indicators[3];

    // Phone
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = phone;
    bind[4].buffer_length = sizeof(phone);
    bind[4].is_null = &null_indicators[4];

    // Active
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = &active;
    bind[5].is_null = &null_indicators[5];

    // Address1
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = address1;
    bind[6].buffer_length = sizeof(address1);
    bind[6].is_null = &null_indicators[6];

    // Address2
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = address2;
    bind[7].buffer_length = sizeof(address2);
    bind[7].is_null = &null_indicators[7];

    // City
    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = city;
    bind[8].buffer_length = sizeof(city);
    bind[8].is_null = &null_indicators[8];

    // State
    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = state;
    bind[9].buffer_length = sizeof(state);
    bind[9].is_null = &null_indicators[9];

    // ZipCode
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = zipcode;
    bind[10].buffer_length = sizeof(zipcode);
    bind[10].is_null = &null_indicators[10];

    // ZipCodeExt
    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = zipcodeext;
    bind[11].buffer_length = sizeof(zipcodeext);
    bind[11].is_null = &null_indicators[11];

    // Country
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = country;
    bind[12].buffer_length = sizeof(country);
    bind[12].is_null = &null_indicators[12];

    // CountryCode
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = countrycode;
    bind[13].buffer_length = sizeof(countrycode);
    bind[13].is_null = &null_indicators[13];

    if (mysql_stmt_bind_param(stmt, bind)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);

    // After main record insert, process related records
    if (!null_indicators[0]) {  // If we have a valid code
        // Clear existing related records
        if (clear_representative_related_records(conn, code) != 0) {
            return -1;
        }

        // Process territories
        struct json_object *territories;
        if (json_object_object_get_ex(record, "Territories", &territories)) {
            int n_territories = json_object_array_length(territories);
            for (int i = 0; i < n_territories; i++) {
                struct json_object *territory = json_object_array_get_idx(territories, i);
                if (process_representative_territory(conn, code, territory) != 0) {
                    return -1;
                }
            }
        }

        // Process attributes
        struct json_object *attributes;
        if (json_object_object_get_ex(record, "Attributes", &attributes)) {
            json_object_object_foreach(attributes, key, value) {
                struct json_object *type;
                const char *type_str = "string";  // default type
                if (json_object_object_get_ex(value, "Type", &type)) {
                    type_str = json_object_get_string(type);
                }
                struct json_object *val;
                if (json_object_object_get_ex(value, "Value", &val)) {
                    if (process_representative_attribute(conn, code, key, type_str, val) != 0) {
                        return -1;
                    }
                }
            }
        }
    }

    return 0;
}

int process_representatives_batch(MYSQL *conn, const struct Endpoint *endpoint,
                                struct json_object *batch,
                                struct RepresentativeBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct RepresentativeBatchResult));
    
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
    struct json_object *representatives;
    if (!json_object_object_get_ex(batch, "Representatives", &representatives)) {
        snprintf(result->error_message, sizeof(result->error_message), 
                "No Representatives array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(representatives);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(representatives, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        if (process_representative_record(conn, record) == 0) {
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
                    "Failed to process representative record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing representatives: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_representatives_batch(conn, result->last_id, representatives)) {
            snprintf(result->error_message, sizeof(result->error_message), 
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_representatives_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT r.code, r.name, r.email, r.active, "
                       "COUNT(DISTINCT rt.territory_path) as territory_count, "
                       "COUNT(DISTINCT ra.title) as attribute_count "
                       "FROM representatives r "
                       "LEFT JOIN representative_territories rt ON r.code = rt.representative_code "
                       "LEFT JOIN representative_attributes ra ON r.code = ra.representative_code "
                       "GROUP BY r.code, r.name, r.email, r.active "
                       "ORDER BY r.code LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[6];
    char db_code[20];
    char db_name[80];
    char db_email[256];
    bool db_active;
    long long db_territory_count;
    long long db_attribute_count;
    unsigned long db_code_length;
    unsigned long db_name_length;
    unsigned long db_email_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_STRING;
    bind_result[0].buffer = db_code;
    bind_result[0].buffer_length = sizeof(db_code);
    bind_result[0].length = &db_code_length;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_name;
    bind_result[1].buffer_length = sizeof(db_name);
    bind_result[1].length = &db_name_length;

    bind_result[2].buffer_type = MYSQL_TYPE_STRING;
    bind_result[2].buffer = db_email;
    bind_result[2].buffer_length = sizeof(db_email);
    bind_result[2].length = &db_email_length;

    bind_result[3].buffer_type = MYSQL_TYPE_TINY;
    bind_result[3].buffer = &db_active;

    bind_result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[4].buffer = &db_territory_count;

    bind_result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[5].buffer = &db_attribute_count;

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
            struct json_object *rep = json_object_array_get_idx(original_data, i);
            struct json_object *code_obj, *name_obj, *email_obj, *active_obj;
            
            if (json_object_object_get_ex(rep, "Code", &code_obj) &&
                json_object_object_get_ex(rep, "Name", &name_obj) &&
                json_object_object_get_ex(rep, "Email", &email_obj) &&
                json_object_object_get_ex(rep, "Active", &active_obj)) {
                
                const char *json_code = json_object_get_string(code_obj);
                const char *json_name = json_object_get_string(name_obj);
                const char *json_email = json_object_get_string(email_obj);
                bool json_active = json_object_get_boolean(active_obj);

                if (strncmp(db_code, json_code, db_code_length) == 0 &&
                    strncmp(db_name, json_name, db_name_length) == 0 &&
                    strncmp(db_email, json_email, db_email_length) == 0 &&
                    db_active == json_active) {
                    
                    // Verify territory count
                    struct json_object *territories;
                    if (json_object_object_get_ex(rep, "Territories", &territories)) {
                        int json_territory_count = json_object_array_length(territories);
                        if (json_territory_count != db_territory_count) {
                            verification_passed = false;
                            break;
                        }
                    }
                    
                    // Verify attribute count
                    struct json_object *attributes;
                    if (json_object_object_get_ex(rep, "Attributes", &attributes)) {
                        int json_attribute_count = json_object_object_length(attributes);
                        if (json_attribute_count != db_attribute_count) {
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