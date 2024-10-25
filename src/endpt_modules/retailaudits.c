// src/endpt_modules/retailaudits.c
#include "endpt_modules/retailaudits.h"
#include <stdio.h>
#include <string.h>
#include <mysql/mysql.h>
#include <time.h>

bool log_batch_status(const struct RetailAuditBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("retailaudits_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_retailaudit_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_retail_audit(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    int retailauditid = 0;
    char retailauditname[51] = {0};
    bool cancelled = false;
    char clientcode[51] = {0};
    char clientname[256] = {0};
    char dateandtime[64] = {0};
    char representativecode[21] = {0};
    char representativename[81] = {0};
    char note[256] = {0};
    int visitid = 0;
    int metacollectiontotalcount = 0;
    int metacollectionfirstid = 0;
    int metacollectionlastid = 0;;

    // NULL indicators
    bool null_indicators[13] = {0};

    // Extract values from JSON
    struct json_object *temp;

    if (json_object_object_get_ex(record, "RetailAuditID", &temp))
        retailauditid = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "RetailAuditName", &temp))
        strncpy(retailauditname, json_object_get_string(temp), sizeof(retailauditname) - 1);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "Cancelled", &temp))
        cancelled = json_object_get_boolean(temp);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "ClientCode", &temp))
        strncpy(clientcode, json_object_get_string(temp), sizeof(clientcode) - 1);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "ClientName", &temp))
        strncpy(clientname, json_object_get_string(temp), sizeof(clientname) - 1);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "DateAndTime", &temp)) {
        const char *date_str = json_object_get_string(temp);
        strncpy(dateandtime, date_str, sizeof(dateandtime) - 1);
    }
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "RepresentativeCode", &temp))
        strncpy(representativecode, json_object_get_string(temp), sizeof(representativecode) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "RepresentativeName", &temp))
        strncpy(representativename, json_object_get_string(temp), sizeof(representativename) - 1);
    else
        null_indicators[7] = 1;

    if (json_object_object_get_ex(record, "Note", &temp))
        strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
    else
        null_indicators[8] = 1;

    if (json_object_object_get_ex(record, "VisitID", &temp))
        visitid = json_object_get_int(temp);
    else
        null_indicators[9] = 1;

    // Meta fields will be set from batch metadata
    null_indicators[10] = 0; // metacollectiontotalcount
    null_indicators[11] = 0; // metacollectionfirstid
    null_indicators[12] = 0; // metacollectionlastid

    // Bind parameters
    MYSQL_BIND bind[13];
    memset(bind, 0, sizeof(bind));

    // RetailAuditID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &retailauditid;
    bind[0].is_null = &null_indicators[0];

    // RetailAuditName
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = retailauditname;
    bind[1].buffer_length = sizeof(retailauditname);
    bind[1].is_null = &null_indicators[1];

    // Cancelled
    bind[2].buffer_type = MYSQL_TYPE_TINY;
    bind[2].buffer = &cancelled;
    bind[2].is_null = &null_indicators[2];

    // ClientCode
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = clientcode;
    bind[3].buffer_length = sizeof(clientcode);
    bind[3].is_null = &null_indicators[3];

    // ClientName
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = clientname;
    bind[4].buffer_length = sizeof(clientname);
    bind[4].is_null = &null_indicators[4];

    // DateAndTime
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = dateandtime;
    bind[5].buffer_length = sizeof(dateandtime);
    bind[5].is_null = &null_indicators[5];

    // RepresentativeCode
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = representativecode;
    bind[6].buffer_length = sizeof(representativecode);
    bind[6].is_null = &null_indicators[6];

    // RepresentativeName
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = representativename;
    bind[7].buffer_length = sizeof(representativename);
    bind[7].is_null = &null_indicators[7];

    // Note
    bind[8].buffer_type = MYSQL_TYPE_STRING;
    bind[8].buffer = note;
    bind[8].buffer_length = sizeof(note);
    bind[8].is_null = &null_indicators[8];

    // VisitID
    bind[9].buffer_type = MYSQL_TYPE_LONG;
    bind[9].buffer = &visitid;
    bind[9].is_null = &null_indicators[9];

    // MetaCollectionTotalCount
    bind[10].buffer_type = MYSQL_TYPE_LONG;
    bind[10].buffer = &metacollectiontotalcount;
    bind[10].is_null = &null_indicators[10];

    // MetaCollectionFirstID
    bind[11].buffer_type = MYSQL_TYPE_LONG;
    bind[11].buffer = &metacollectionfirstid;
    bind[11].is_null = &null_indicators[11];

    // MetaCollectionLastID
    bind[12].buffer_type = MYSQL_TYPE_LONG;
    bind[12].buffer = &metacollectionlastid;
    bind[12].is_null = &null_indicators[12];

    if (mysql_stmt_bind_param(stmt, bind)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_execute(stmt)) {
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);
    return retailauditid;  // Return the ID for child records
}

int process_retailaudit_items(MYSQL *conn, int retailauditid, struct json_object *items) {
    if (!items || !json_object_is_type(items, json_type_array)) {
        return 0;  // No items to process
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_retail_audit_item(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    char productgroupcode[21] = {0};
    char productgroupname[81] = {0};
    char productcode[21] = {0};
    char productname[81] = {0};
    bool present = false;
    double price = 0.0;
    bool promotion = false;
    double shelfshare = 0.0;
    double shelfsharepercent = 0.0;
    bool soldout = false;
    int stock = 0;

    bool null_indicators[12] = {0};
    MYSQL_BIND bind[12];
    memset(bind, 0, sizeof(bind));

    // RetailAuditID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &retailauditid;
    bind[0].is_null = &null_indicators[0];

    // ProductGroupCode
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = productgroupcode;
    bind[1].buffer_length = sizeof(productgroupcode);
    bind[1].is_null = &null_indicators[1];

    // ProductGroupName
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = productgroupname;
    bind[2].buffer_length = sizeof(productgroupname);
    bind[2].is_null = &null_indicators[2];

    // ProductCode
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = productcode;
    bind[3].buffer_length = sizeof(productcode);
    bind[3].is_null = &null_indicators[3];

    // ProductName
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = productname;
    bind[4].buffer_length = sizeof(productname);
    bind[4].is_null = &null_indicators[4];

    // Present
    bind[5].buffer_type = MYSQL_TYPE_TINY;
    bind[5].buffer = &present;
    bind[5].is_null = &null_indicators[5];

    // Price
    bind[6].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[6].buffer = &price;
    bind[6].is_null = &null_indicators[6];

    // Promotion
    bind[7].buffer_type = MYSQL_TYPE_TINY;
    bind[7].buffer = &promotion;
    bind[7].is_null = &null_indicators[7];

    // ShelfShare
    bind[8].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[8].buffer = &shelfshare;
    bind[8].is_null = &null_indicators[8];

    // ShelfSharePercent
    bind[9].buffer_type = MYSQL_TYPE_DECIMAL;
    bind[9].buffer = &shelfsharepercent;
    bind[9].is_null = &null_indicators[9];

    // SoldOut
    bind[10].buffer_type = MYSQL_TYPE_TINY;
    bind[10].buffer = &soldout;
    bind[10].is_null = &null_indicators[10];

    // Stock
    bind[11].buffer_type = MYSQL_TYPE_LONG;
    bind[11].buffer = &stock;
    bind[11].is_null = &null_indicators[11];

    int array_len = json_object_array_length(items);
    for (int i = 0; i < array_len; i++) {
        struct json_object *item = json_object_array_get_idx(items, i);
        struct json_object *temp;

        // Reset all null indicators
        memset(null_indicators, 0, sizeof(null_indicators));

        if (json_object_object_get_ex(item, "ProductGroupCode", &temp))
            strncpy(productgroupcode, json_object_get_string(temp), sizeof(productgroupcode) - 1);
        else
            null_indicators[1] = 1;

        if (json_object_object_get_ex(item, "ProductGroupName", &temp))
            strncpy(productgroupname, json_object_get_string(temp), sizeof(productgroupname) - 1);
        else
            null_indicators[2] = 1;

        if (json_object_object_get_ex(item, "ProductCode", &temp))
            strncpy(productcode, json_object_get_string(temp), sizeof(productcode) - 1);
        else
            null_indicators[3] = 1;

        if (json_object_object_get_ex(item, "ProductName", &temp))
            strncpy(productname, json_object_get_string(temp), sizeof(productname) - 1);
        else
            null_indicators[4] = 1;

        if (json_object_object_get_ex(item, "Present", &temp))
            present = json_object_get_boolean(temp);
        else
            null_indicators[5] = 1;

        if (json_object_object_get_ex(item, "Price", &temp))
            price = json_object_get_double(temp);
        else
            null_indicators[6] = 1;

        if (json_object_object_get_ex(item, "Promotion", &temp))
            promotion = json_object_get_boolean(temp);
        else
            null_indicators[7] = 1;

        if (json_object_object_get_ex(item, "ShelfShare", &temp))
            shelfshare = json_object_get_double(temp);
        else
            null_indicators[8] = 1;

        if (json_object_object_get_ex(item, "ShelfSharePercent", &temp))
            shelfsharepercent = json_object_get_double(temp);
        else
            null_indicators[9] = 1;

        if (json_object_object_get_ex(item, "SoldOut", &temp))
            soldout = json_object_get_boolean(temp);
        else
            null_indicators[10] = 1;

        if (json_object_object_get_ex(item, "Stock", &temp))
            stock = json_object_get_int(temp);
        else
            null_indicators[11] = 1;

        if (mysql_stmt_bind_param(stmt, bind)) {
            mysql_stmt_close(stmt);
            return -1;
        }

        if (mysql_stmt_execute(stmt)) {
            mysql_stmt_close(stmt);
            return -1;
        }
    }

    mysql_stmt_close(stmt);
    return 0;
}

int process_retailaudit_customfields(MYSQL *conn, int retailauditid, struct json_object *customfields) {
    if (!customfields || !json_object_is_type(customfields, json_type_array)) {
        return 0;  // No custom fields to process
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "CALL upsert_retail_audit_customfield(?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    bool null_indicators[3] = {0};
    char field[256] = {0};
    char value[256] = {0};

    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));

    // RetailAuditID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &retailauditid;
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

    int array_len = json_object_array_length(customfields);
    for (int i = 0; i < array_len; i++) {
        struct json_object *item = json_object_array_get_idx(customfields, i);
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


bool verify_retailaudits_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
    if (!conn || !original_data) return false;

    const char *query = "SELECT ra.retailauditid, COUNT(rai.productcode) as item_count, "
                       "COUNT(rac.field) as field_count "
                       "FROM retailaudits ra "
                       "LEFT JOIN retailaudititems rai ON ra.retailauditid = rai.retailauditid "
                       "LEFT JOIN retailauditcustomfields rac ON ra.retailauditid = rac.retailauditid "
                       "WHERE ra.retailauditid <= ? "
                       "GROUP BY ra.retailauditid "
                       "ORDER BY ra.retailauditid DESC LIMIT 5";

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

    MYSQL_BIND bind_result[3];
    int db_retailauditid;
    long long db_item_count;
    long long db_field_count;
    memset(bind_result, 0, sizeof(bind_result));

    bind_result[0].buffer_type = MYSQL_TYPE_LONG;
    bind_result[0].buffer = &db_retailauditid;
    bind_result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[1].buffer = &db_item_count;
    bind_result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[2].buffer = &db_field_count;

    if (mysql_stmt_bind_result(stmt, bind_result)) {
        mysql_stmt_close(stmt);
        return false;
    }

    bool verification_passed = true;
    while (mysql_stmt_fetch(stmt) == 0) {
        bool found = false;
        int array_len = json_object_array_length(original_data);
        
        for (int i = 0; i < array_len && !found; i++) {
            struct json_object *audit = json_object_array_get_idx(original_data, i);
            struct json_object *id_obj;
            
            if (json_object_object_get_ex(audit, "RetailAuditID", &id_obj)) {
                int json_id = json_object_get_int(id_obj);
                if (db_retailauditid == json_id) {
                    // Verify item counts
                    struct json_object *items;
                    if (json_object_object_get_ex(audit, "Item", &items)) {
                        int json_item_count = json_object_array_length(items);
                        if (json_item_count != db_item_count) {
                            verification_passed = false;
                            break;
                        }
                    }
                    
                    // Verify custom field counts
                    struct json_object *fields;
                    if (json_object_object_get_ex(audit, "CustomFields", &fields)) {
                        int json_field_count = json_object_array_length(fields);
                        if (json_field_count != db_field_count) {
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