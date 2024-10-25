#include "endpt_modules/users.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>

static bool log_batch_status(const struct UserBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("users_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

static int process_user_territory(MYSQL *conn, const char *user_id, struct json_object *territory) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO user_territories (user_id, territory_path) VALUES (?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Get territory path
    const char *territory_path = json_object_get_string(territory);
    
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)user_id;
    bind[0].buffer_length = strlen(user_id);

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

static int process_user_attribute(MYSQL *conn, const char *user_id, const char *title, struct json_object *value) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO user_attributes (user_id, title, value) VALUES (?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    const char *value_str = json_object_get_string(value);
    
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)user_id;
    bind[0].buffer_length = strlen(user_id);

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)title;
    bind[1].buffer_length = strlen(title);

    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = (void*)value_str;
    bind[2].buffer_length = strlen(value_str);

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

static int process_user_permission(MYSQL *conn, const char *user_id, struct json_object *permission) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO user_permissions (user_id, permission) VALUES (?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    const char *permission_str = json_object_get_string(permission);
    
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = (void*)user_id;
    bind[0].buffer_length = strlen(user_id);

    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = (void*)permission_str;
    bind[1].buffer_length = strlen(permission_str);

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

static int clear_user_related_records(MYSQL *conn, const char *user_id) {
    // Delete existing records before inserting new ones
    const char *queries[] = {
        "DELETE FROM user_territories WHERE user_id = ?",
        "DELETE FROM user_attributes WHERE user_id = ?",
        "DELETE FROM user_permissions WHERE user_id = ?"
    };

    for (int i = 0; i < 3; i++) {
        MYSQL_STMT *stmt = mysql_stmt_init(conn);
        if (!stmt) return -1;

        if (mysql_stmt_prepare(stmt, queries[i], strlen(queries[i]))) {
            mysql_stmt_close(stmt);
            return -1;
        }

        MYSQL_BIND bind[1];
        memset(bind, 0, sizeof(bind));
        
        bind[0].buffer_type = MYSQL_TYPE_STRING;
        bind[0].buffer = (void*)user_id;
        bind[0].buffer_length = strlen(user_id);

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

int process_user_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_user(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    char id[36] = {0};
    char code[20] = {0};
    char name[80] = {0};
    char email[100] = {0};
    bool active = false;
    char role[80] = {0};
    char note[255] = {0};
    char phone[128] = {0};
    bool sendemailenabled = false;
    char address1[256] = {0};
    char address2[256] = {0};
    char city[256] = {0};
    char state[256] = {0};
    char zipcode[20] = {0};
    char zipcodeext[20] = {0};
    char country[256] = {0};
    char countrycode[20] = {0};
    int metacollectiontotalcount = 0;
    long long metacollectionlasttimestamp = 0;

    // NULL indicators
    bool null_indicators[19] = {0};

    struct json_object *temp;

    // Extract all fields from JSON
    if (json_object_object_get_ex(record, "ID", &temp))
        strncpy(id, json_object_get_string(temp), sizeof(id) - 1);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "Code", &temp))
        strncpy(code, json_object_get_string(temp), sizeof(code) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "Name", &temp))
        strncpy(name, json_object_get_string(temp), sizeof(name) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "Email", &temp))
        strncpy(email, json_object_get_string(temp), sizeof(email) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "Active", &temp))
        active = json_object_get_boolean(temp);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "Role", &temp))
        strncpy(role, json_object_get_string(temp), sizeof(role) - 1);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "Phone", &temp))
        strncpy(phone, json_object_get_string(temp), sizeof(phone) - 1);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "SendEmailEnabled", &temp))
        sendemailenabled = json_object_get_boolean(temp);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "Address1", &temp))
        strncpy(address1, json_object_get_string(temp), sizeof(address1) - 1);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "Address2", &temp))
        strncpy(address2, json_object_get_string(temp), sizeof(address2) - 1);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "City", &temp))
        strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
    else
        null_indicators[11] = 1;

    if (json_object_object_get_ex(record, "State", &temp))
        strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
    else
        null_indicators[12] = 1;

    if (json_object_object_get_ex(record, "ZipCode", &temp))
        strncpy(zipcode, json_object_get_string(temp), sizeof(zipcode) - 1);
    else
        null_indicators[13] = 1;

    if (json_object_object_get_ex(record, "ZipCodeExt", &temp))
        strncpy(zipcodeext, json_object_get_string(temp), sizeof(zipcodeext) - 1);
    else
        null_indicators[14] = 1;

    if (json_object_object_get_ex(record, "Country", &temp))
        strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
    else
        null_indicators[15] = 1;

    if (json_object_object_get_ex(record, "CountryCode", &temp))
        strncpy(countrycode, json_object_get_string(temp), sizeof(countrycode) - 1);
    else
        null_indicators[16] = 1;

    // Meta fields will be set from batch metadata
    null_indicators[17] = 0; // metacollectiontotalcount
    null_indicators[18] = 0; // metacollectionlasttimestamp

    // Bind parameters
    MYSQL_BIND bind[19];
    memset(bind, 0, sizeof(bind));

    // ID
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = id;
    bind[0].buffer_length = sizeof(id);
    bind[0].is_null = &null_indicators[0];

    // Code
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = code;
    bind[1].buffer_length = sizeof(code);
    bind[1].is_null = &null_indicators[1];

    // Name
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = name;
    bind[2].buffer_length = sizeof(name);
    bind[2].is_null = &null_indicators[2];

    // Email
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = email;
    bind[3].buffer_length = sizeof(email);
    bind[3].is_null = &null_indicators[3];

    // Active
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = &active;
    bind[4].is_null = &null_indicators[4];

    // Role
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = role;
    bind[5].buffer_length = sizeof(role);
    bind[5].is_null = &null_indicators[5];

    // Note
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = note;
    bind[6].buffer_length = sizeof(note);
    bind[6].is_null = &null_indicators[6];

    // Phone
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = phone;
    bind[7].buffer_length = sizeof(phone);
    bind[7].is_null = &null_indicators[7];

    // SendEmailEnabled
    bind[8].buffer_type = MYSQL_TYPE_TINY;
    bind[8].buffer = &sendemailenabled;
    bind[8].is_null = &null_indicators[8];

    // Address1
    bind[9].buffer_type = MYSQL_TYPE_STRING;
    bind[9].buffer = address1;
    bind[9].buffer_length = sizeof(address1);
    bind[9].is_null = &null_indicators[9];

    // Address2
    bind[10].buffer_type = MYSQL_TYPE_STRING;
    bind[10].buffer = address2;
    bind[10].buffer_length = sizeof(address2);
    bind[10].is_null = &null_indicators[10];

    // City
    bind[11].buffer_type = MYSQL_TYPE_STRING;
    bind[11].buffer = city;
    bind[11].buffer_length = sizeof(city);
    bind[11].is_null = &null_indicators[11];

    // State
    bind[12].buffer_type = MYSQL_TYPE_STRING;
    bind[12].buffer = state;
    bind[12].buffer_length = sizeof(state);
    bind[12].is_null = &null_indicators[12];

    // ZipCode
    bind[13].buffer_type = MYSQL_TYPE_STRING;
    bind[13].buffer = zipcode;
    bind[13].buffer_length = sizeof(zipcode);
    bind[13].is_null = &null_indicators[13];

    // ZipCodeExt
    bind[14].buffer_type = MYSQL_TYPE_STRING;
    bind[14].buffer = zipcodeext;
    bind[14].buffer_length = sizeof(zipcodeext);
    bind[14].is_null = &null_indicators[14];

    // Country
    bind[15].buffer_type = MYSQL_TYPE_STRING;
    bind[15].buffer = country;
    bind[15].buffer_length = sizeof(country);
    bind[15].is_null = &null_indicators[15];

    // CountryCode
    bind[16].buffer_type = MYSQL_TYPE_STRING;
    bind[16].buffer = countrycode;
    bind[16].buffer_length = sizeof(countrycode);
    bind[16].is_null = &null_indicators[16];

    // MetaCollectionTotalCount
    bind[17].buffer_type = MYSQL_TYPE_LONG;
    bind[17].buffer = &metacollectiontotalcount;
    bind[17].is_null = &null_indicators[17];

    // MetaCollectionLastTimestamp
    bind[18].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[18].buffer = &metacollectionlasttimestamp;
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

    struct json_object *id_obj;
    if (json_object_object_get_ex(record, "ID", &id_obj)) {
        const char *user_id = json_object_get_string(id_obj);

        // Clear existing related records
        if (clear_user_related_records(conn, user_id) != 0) {
            return -1;
        }

        // Process territories
        struct json_object *territories;
        if (json_object_object_get_ex(record, "Territories", &territories)) {
            int n_territories = json_object_array_length(territories);
            for (int i = 0; i < n_territories; i++) {
                struct json_object *territory = json_object_array_get_idx(territories, i);
                if (process_user_territory(conn, user_id, territory) != 0) {
                    return -1;
                }
            }
        }

        // Process attributes
        struct json_object *attributes;
        if (json_object_object_get_ex(record, "Attributes", &attributes)) {
            json_object_object_foreach(attributes, key, val) {
                if (process_user_attribute(conn, user_id, key, val) != 0) {
                    return -1;
                }
            }
        }

        // Process permissions
        struct json_object *permissions;
        if (json_object_object_get_ex(record, "Permissions", &permissions)) {
            int n_permissions = json_object_array_length(permissions);
            for (int i = 0; i < n_permissions; i++) {
                struct json_object *permission = json_object_array_get_idx(permissions, i);
                if (process_user_permission(conn, user_id, permission) != 0) {
                    return -1;
                }
            }
        }
    }

    return 0;
}


int process_users_batch(MYSQL *conn, const struct Endpoint *endpoint,
                       struct json_object *batch, struct UserBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct UserBatchResult));
    
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
    struct json_object *users;
    if (!json_object_object_get_ex(batch, "Users", &users)) {
        snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                "No Users array found in response");
        return -1;
    }

    size_t n_records = json_object_array_length(users);
    for (size_t i = 0; i < n_records; i++) {
        struct json_object *record = json_object_array_get_idx(users, i);
        result->records_processed++;
        
        if (mysql_query(conn, "START TRANSACTION")) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Failed to start transaction: %s", mysql_error(conn));
            return -1;
        }

        bool success = true;
        
        // First process the main user record
        if (process_user_record(conn, record) != 0) {
            success = false;
        } else {
            // Get user ID for related records
            struct json_object *id_obj;
            char user_id[36] = {0};
            if (json_object_object_get_ex(record, "ID", &id_obj)) {
                strncpy(user_id, json_object_get_string(id_obj), sizeof(user_id) - 1);

                // Process territories if present
                struct json_object *territories;
                if (json_object_object_get_ex(record, "Territories", &territories)) {
                    size_t n_territories = json_object_array_length(territories);
                    for (size_t j = 0; j < n_territories && success; j++) {
                        struct json_object *territory = json_object_array_get_idx(territories, j);
                        struct json_object *territory_obj = json_object_new_object();
                        
                        json_object_object_add(territory_obj, "UserID", 
                            json_object_new_string(user_id));
                        json_object_object_add(territory_obj, "TerritoryPath", 
                            json_object_get(territory));
                            
                        if (process_user_territory_record(conn, territory_obj) != 0) {
                            success = false;
                        }
                        json_object_put(territory_obj);
                    }
                }

                // Process attributes if present
                struct json_object *attributes;
                if (json_object_object_get_ex(record, "Attributes", &attributes)) {
                    struct json_object *attr_key;
                    json_object_object_foreach(attributes, key, val) {
                        struct json_object *attr_obj = json_object_new_object();
                        
                        json_object_object_add(attr_obj, "UserID", 
                            json_object_new_string(user_id));
                        json_object_object_add(attr_obj, "Title", 
                            json_object_new_string(key));
                        json_object_object_add(attr_obj, "Value", 
                            json_object_get(val));
                            
                        if (process_user_attribute_record(conn, attr_obj) != 0) {
                            success = false;
                        }
                        json_object_put(attr_obj);
                        if (!success) break;
                    }
                }

                // Process permissions if present
                struct json_object *permissions;
                if (json_object_object_get_ex(record, "Permissions", &permissions)) {
                    size_t n_permissions = json_object_array_length(permissions);
                    for (size_t j = 0; j < n_permissions && success; j++) {
                        struct json_object *permission = json_object_array_get_idx(permissions, j);
                        struct json_object *perm_obj = json_object_new_object();
                        
                        json_object_object_add(perm_obj, "UserID", 
                            json_object_new_string(user_id));
                        json_object_object_add(perm_obj, "Permission", 
                            json_object_get(permission));
                            
                        if (process_user_permission_record(conn, perm_obj) != 0) {
                            success = false;
                        }
                        json_object_put(perm_obj);
                    }
                }
            } else {
                success = false;
            }
        }

        if (success) {
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
                    "Failed to process user record");
        }

        if (result->records_processed % 10 == 0) {
            printf("\rProcessing users: %d/%zu records", 
                   result->records_processed, n_records);
            fflush(stdout);
        }
    }

    printf("\n");

    if (result->records_processed > 0) {
        if (!verify_users_batch(conn, result->last_id, users)) {
            snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                    "Batch verification failed");
            return -1;
        }
    }

    result->success = (result->records_failed == 0);
    log_batch_status(result);
    
    return 0;
}

bool verify_users_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT u.id, u.code, u.name, u.email, "
                       "COUNT(DISTINCT ut.territory_path) as territory_count, "
                       "COUNT(DISTINCT ua.title) as attribute_count, "
                       "COUNT(DISTINCT up.permission) as permission_count "
                       "FROM users u "
                       "LEFT JOIN user_territories ut ON u.id = ut.user_id "
                       "LEFT JOIN user_attributes ua ON u.id = ua.user_id "
                       "LEFT JOIN user_permissions up ON u.id = up.user_id "
                       "GROUP BY u.id, u.code, u.name, u.email "
                       "ORDER BY u.id DESC LIMIT 5";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_result[7];
    char db_id[36];
    char db_code[20];
    char db_name[80];
    char db_email[100];
    long long db_territory_count;
    long long db_attribute_count;
    long long db_permission_count;
    unsigned long db_id_length;
    unsigned long db_code_length;
    unsigned long db_name_length;
    unsigned long db_email_length;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_STRING;
    bind_result[0].buffer = db_id;
    bind_result[0].buffer_length = sizeof(db_id);
    bind_result[0].length = &db_id_length;

    bind_result[1].buffer_type = MYSQL_TYPE_STRING;
    bind_result[1].buffer = db_code;
    bind_result[1].buffer_length = sizeof(db_code);
    bind_result[1].length = &db_code_length;

    bind_result[2].buffer_type = MYSQL_TYPE_STRING;
    bind_result[2].buffer = db_name;
    bind_result[2].buffer_length = sizeof(db_name);
    bind_result[2].length = &db_name_length;

    bind_result[3].buffer_type = MYSQL_TYPE_STRING;
    bind_result[3].buffer = db_email;
    bind_result[3].buffer_length = sizeof(db_email);
    bind_result[3].length = &db_email_length;

    bind_result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[4].buffer = &db_territory_count;

    bind_result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[5].buffer = &db_attribute_count;

    bind_result[6].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[6].buffer = &db_permission_count;

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
            struct json_object *user = json_object_array_get_idx(original_data, i);
            struct json_object *id_obj, *code_obj, *name_obj, *email_obj;
            
            if (json_object_object_get_ex(user, "ID", &id_obj) &&
                json_object_object_get_ex(user, "Code", &code_obj) &&
                json_object_object_get_ex(user, "Name", &name_obj) &&
                json_object_object_get_ex(user, "Email", &email_obj)) {
                
                const char *json_id = json_object_get_string(id_obj);
                const char *json_code = json_object_get_string(code_obj);
                const char *json_name = json_object_get_string(name_obj);
                const char *json_email = json_object_get_string(email_obj);

                if (strncmp(db_id, json_id, db_id_length) == 0 &&
                    strncmp(db_code, json_code, db_code_length) == 0 &&
                    strncmp(db_name, json_name, db_name_length) == 0 &&
                    strncmp(db_email, json_email, db_email_length) == 0) {
                    
                    // Verify territory count
                    struct json_object *territories;
                    if (json_object_object_get_ex(user, "Territories", &territories)) {
                        int json_territory_count = json_object_array_length(territories);
                        if (json_territory_count != db_territory_count) {
                            verification_passed = false;
                            break;
                        }
                    }
                    
                    // Verify attribute count
                    struct json_object *attributes;
                    if (json_object_object_get_ex(user, "Attributes", &attributes)) {
                        int json_attribute_count = json_object_object_length(attributes);
                        if (json_attribute_count != db_attribute_count) {
                            verification_passed = false;
                            break;
                        }
                    }
                    
                    // Verify permission count
                    struct json_object *permissions;
                    if (json_object_object_get_ex(user, "Permissions", &permissions)) {
                        int json_permission_count = json_object_array_length(permissions);
                        if (json_permission_count != db_permission_count) {
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

