#ifndef PRICELISTITEMS_H
#define PRICELISTITEMS_H

#include <mysql/mysql.h>
#include <json-c/json.h>

struct PriceListItem {
    int id;
    int productid;
    char productcode[256];
    double price;
    bool active;
    char clientid[256];
    char manufactureid[256];
    struct tm dateavailablefrom;
    struct tm dateavailableto;
    int minquantity;
    int maxquantity;
    int pricelistid;
};

struct PriceListItemBatchResult {
    int total_count;
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    bool success;
    char error_message[256];
};

int process_pricelistitem_record(MYSQL *conn, struct json_object *record);
int process_pricelistitems_batch(MYSQL *conn, struct json_object *batch, struct PriceListItemBatchResult *result);
bool verify_pricelistitems_batch(MYSQL *conn, int last_id, struct json_object *original_data);

#endif 