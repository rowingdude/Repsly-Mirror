#include "pricelistitems.h"
#include <stdbool.h>
#include <mysql/mysql.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static bool log_batch_status(const struct PriceListItemBatchResult *result) {
    time_t now;
    time(&now);
    
    FILE *log = fopen("pricelistitems_processing.log", "a");
    if (!log) return false;
    
    fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
            ctime(&now), result->first_id, result->last_id,
            result->records_processed, result->records_inserted, 
            result->records_failed, result->total_count,
            result->success ? "SUCCESS" : result->error_message);
    
    fclose(log);
    return true;
}

int process_pricelistitem_record(MYSQL *conn, struct json_object *record) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    const char *query = "INSERT INTO pricelistitems (id, productid, productcode, price, active, clientid, manufactureid, dateavailablefrom, dateavailableto, minquantity, maxquantity, pricelistid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    if (mysql_stmt_prepare(stmt, query, strlen(query))) {
        mysql_stmt_close(stmt);
        return -1;
    }

    // Field variables
    struct PriceListItem item;
    memset(&item, 0, sizeof(item));

    // NULL indicators
    bool null_indicators[12] = {0};

    struct json_object *temp;

    // Extract fields from JSON
    if (json_object_object_get_ex(record, "id", &temp))
        item.id = json_object_get_int(temp);
    else
        null_indicators[0] = 1;

    if (json_object_object_get_ex(record, "ProductID", &temp))
        item.productid = json_object_get_int(temp);
    else
        null_indicators[1] = 1;

    if (json_object_object_get_ex(record, "ProductCode", &temp))
        strncpy(item.productcode, json_object_get_string(temp), sizeof(item.productcode) - 1);
    else
        null_indicators[2] = 1;

    if (json_object_object_get_ex(record, "Price", &temp))
        item.price = json_object_get_double(temp);
    else
        null_indicators[3] = 1;

    if (json_object_object_get_ex(record, "Active", &temp))
        item.active = json_object_get_boolean(temp);
    else
        null_indicators[4] = 1;

    if (json_object_object_get_ex(record, "ClientID", &temp))
        strncpy(item.clientid, json_object_get_string(temp), sizeof(item.clientid) - 1);
    else
        null_indicators[5] = 1;

    if (json_object_object_get_ex(record, "ManufactureID", &temp))
        strncpy(item.manufactureid, json_object_get_string(temp), sizeof(item.manufactureid) - 1);
    else
        null_indicators[6] = 1;

    if (json_object_object_get_ex(record, "DateAvailableFrom", &temp)) {
        const char *date_str = json_object_get_string(temp);
        strptime(date_str, "%Y-%m-%d %H:%M:%S", &item.dateavailablefrom);
    } else {
        null_indicators[7] = 1;
    }

    if (json_object_object_get_ex(record, "DateAvailableTo", &temp)) {
        const char *date_str = json_object_get_string(temp);
        strptime(date_str, "%Y-%m-%d %H:%M:%S", &item.dateavailableto);
    } else {
        null_indicators[8] = 1;
    }

    if (json_object_object_get_ex(record, "MinQuantity", &temp))
        item.minquantity = json_object_get_int(temp);
    else
        null_indicators[9] = 1;

    if (json_object_object_get_ex(record, "MaxQuantity", &temp))
        item.maxquantity = json_object_get_int(temp);
    else
        null_indicators[10] = 1;

    if (json_object_object_get_ex(record, "PriceListID", &temp))
        item.pricelistid = json_object_get_int(temp);
    else
        null_indicators[11] = 1;

    // Bind parameters
    MYSQL_BIND bind[12];
    memset(bind, 0, sizeof(bind));

    // ID
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &item.id;
    bind[0].is_null = &null_indicators[0];

    // ProductID
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &item.productid;
    bind[1].is_null = &null_indicators[1];

    // ProductCode
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = item.productcode;
    bind[2].buffer_length = sizeof(item.productcode);
    bind[2].is_null = &null_indicators[2];

    // Price
    bind[3].buffer_type = MYSQL_TYPE_DOUBLE;
    bind[3].buffer = &item.price;
    bind[3].is_null = &null_indicators[3];

    // Active
    bind[4].buffer_type = MYSQL_TYPE_TINY;
    bind[4].buffer = &item.active;
    bind[4].is_null = &null_indicators[4];

    // ClientID
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = item.clientid;
    bind[5].buffer_length = sizeof(item.clientid);
    bind[5].is_null = &null_indicators[5];

    // ManufactureID
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = item.manufactureid;
    bind[6].buffer_length = sizeof(item.manufactureid);
    bind[6].is_null = &null_indicators[6];

    // DateAvailableFrom
    bind[7].buffer_type = MYSQL_TYPE_DATETIME;
    bind[7].buffer = &item.dateavailablefrom;
    bind[7].is_null = &null_indicators[7];

    // DateAvailableTo
    bind[8].buffer_type = MYSQL_TYPE_DATETIME;
    bind[8].buffer = &item.dateavailableto;
    bind[8].is_null = &null_indicators[8];

    // MinQuantity
    bind[9].buffer_type = MYSQL_TYPE_LONG;
    bind[9].buffer = &item.minquantity;
    bind[9].is_null = &null_indicators[9];

    // MaxQuantity
    bind[10].buffer_type = MYSQL_TYPE_LONG;
    bind[10].buffer = &item.maxquantity;
    bind[10].is_null = &null_indicators[10];

    // PriceListID
    bind[11].buffer_type = MYSQL_TYPE_LONG;
    bind[11].buffer = &item.pricelistid;
    bind[11].is_null = &null_indicators[11];

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


int process_pricelistitems_batch(MYSQL *conn, struct json_object *batch, struct PriceListItemBatchResult *result) {
    // Initialize result
    memset(result, 0, sizeof(struct PriceListItemBatchResult));
    
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

    // Process each record
    struct json_object *items_array;
    if (json_object_object_get_ex(batch, "Items", &items_array)) {
        int item_count = json_object_array_length(items_array);
        result->records_processed = item_count;
        
        for (int i = 0; i < item_count; i++) {
            struct json_object *item_record = json_object_array_get_idx(items_array, i);
            if (process_pricelistitem_record(conn, item_record) == 0) {
                result->records_inserted++;
            } else {
                result->records_failed++;
            }
        }
    }

    result->success = result->records_failed == 0;
    log_batch_status(result);
    
    return 0;
}