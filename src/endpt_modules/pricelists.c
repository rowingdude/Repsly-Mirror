#include "endpt_modules/pricelists.h"
#include <stdio.h>
#include <string.h>
#include <mysql/mysql.h>
#include <stdbool.h>
#include <time.h>

static bool log_batch_status(const struct PricelistBatchResult *result) {

   time_t now;
   time(&now);
   
   FILE *log = fopen("pricelists_processing.log", "a");
   if (!log) return false;
   
   fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
           ctime(&now), result->first_id, result->last_id,
           result->records_processed, result->records_inserted, 
           result->records_failed, result->total_count,
           result->success ? "SUCCESS" : result->error_message);
   
   fclose(log);
   return true;
}

int process_pricelist_record(MYSQL *conn, struct json_object *record) {

   MYSQL_STMT *stmt = mysql_stmt_init(conn);

   if (!stmt) return -1;

   const char *query = "CALL upsert_pricelist(?, ?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   char name[256] = {0};
   bool isdefault = false;
   bool active = false;
   bool useprices = false;

   // NULL indicators
   bool null_indicators[4] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "Name", &temp))
       strncpy(name, json_object_get_string(temp), sizeof(name) - 1);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "IsDefault", &temp))
       isdefault = json_object_get_boolean(temp);
   else
       null_indicators[1] = 1;

   if (json_object_object_get_ex(record, "Active", &temp))
       active = json_object_get_boolean(temp);
   else
       null_indicators[2] = 1;

   if (json_object_object_get_ex(record, "UsesPrices", &temp))
       useprices = json_object_get_boolean(temp);
   else
       null_indicators[3] = 1;

   MYSQL_BIND bind[4];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_STRING;
   bind[0].buffer = name;
   bind[0].buffer_length = sizeof(name);
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_TINY;
   bind[1].buffer = &isdefault;
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_TINY;
   bind[2].buffer = &active;
   bind[2].is_null = &null_indicators[2];

   bind[3].buffer_type = MYSQL_TYPE_TINY;
   bind[3].buffer = &useprices;
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

int process_pricelistitem_record(MYSQL *conn, struct json_object *record) {
   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_pricelistitem(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   int productid = 0;
   char productcode[256] = {0};
   double price = 0.0;
   bool active = false;
   char clientid[256] = {0};
   char manufactureid[256] = {0};
   char dateavailablefrom[20] = {0};
   char dateavailableto[20] = {0};
   int minquantity = 0;
   int maxquantity = 0;
   int pricelistid = 0;

   // NULL indicators
   bool null_indicators[11] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "ProductID", &temp))
       productid = json_object_get_int(temp);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "ProductCode", &temp))
       strncpy(productcode, json_object_get_string(temp), sizeof(productcode) - 1);
   else
       null_indicators[1] = 1;

   if (json_object_object_get_ex(record, "Price", &temp))
       price = json_object_get_double(temp);
   else
       null_indicators[2] = 1;

   if (json_object_object_get_ex(record, "Active", &temp))
       active = json_object_get_boolean(temp);
   else
       null_indicators[3] = 1;

   if (json_object_object_get_ex(record, "ClientID", &temp))
       strncpy(clientid, json_object_get_string(temp), sizeof(clientid) - 1);
   else
       null_indicators[4] = 1;

   if (json_object_object_get_ex(record, "ManufactureID", &temp))
       strncpy(manufactureid, json_object_get_string(temp), sizeof(manufactureid) - 1);
   else
       null_indicators[5] = 1;

   if (json_object_object_get_ex(record, "DateAvailableFrom", &temp))
       strncpy(dateavailablefrom, json_object_get_string(temp), sizeof(dateavailablefrom) - 1);
   else
       null_indicators[6] = 1;

   if (json_object_object_get_ex(record, "DateAvailableTo", &temp))
       strncpy(dateavailableto, json_object_get_string(temp), sizeof(dateavailableto) - 1);
   else
       null_indicators[7] = 1;

   if (json_object_object_get_ex(record, "MinQuantity", &temp))
       minquantity = json_object_get_int(temp);
   else
       null_indicators[8] = 1;

   if (json_object_object_get_ex(record, "MaxQuantity", &temp))
       maxquantity = json_object_get_int(temp);
   else
       null_indicators[9] = 1;

   if (json_object_object_get_ex(record, "PricelistID", &temp))
       pricelistid = json_object_get_int(temp);
   else
       null_indicators[10] = 1;

   MYSQL_BIND bind[11];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &productid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = productcode;
   bind[1].buffer_length = sizeof(productcode);
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_DOUBLE;
   bind[2].buffer = &price;
   bind[2].is_null = &null_indicators[2];

   bind[3].buffer_type = MYSQL_TYPE_TINY;
   bind[3].buffer = &active;
   bind[3].is_null = &null_indicators[3];

   bind[4].buffer_type = MYSQL_TYPE_STRING;
   bind[4].buffer = clientid;
   bind[4].buffer_length = sizeof(clientid);
   bind[4].is_null = &null_indicators[4];

   bind[5].buffer_type = MYSQL_TYPE_STRING;
   bind[5].buffer = manufactureid;
   bind[5].buffer_length = sizeof(manufactureid);
   bind[5].is_null = &null_indicators[5];

   bind[6].buffer_type = MYSQL_TYPE_STRING;
   bind[6].buffer = dateavailablefrom;
   bind[6].buffer_length = sizeof(dateavailablefrom);
   bind[6].is_null = &null_indicators[6];

   bind[7].buffer_type = MYSQL_TYPE_STRING;
   bind[7].buffer = dateavailableto;
   bind[7].buffer_length = sizeof(dateavailableto);
   bind[7].is_null = &null_indicators[7];

   bind[8].buffer_type = MYSQL_TYPE_LONG;
   bind[8].buffer = &minquantity;
   bind[8].is_null = &null_indicators[8];

   bind[9].buffer_type = MYSQL_TYPE_LONG;
   bind[9].buffer = &maxquantity;
   bind[9].is_null = &null_indicators[9];

   bind[10].buffer_type = MYSQL_TYPE_LONG;
   bind[10].buffer = &pricelistid;
   bind[10].is_null = &null_indicators[10];

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

int process_pricelists_batch(MYSQL *conn, const struct Endpoint *endpoint, struct json_object *batch, struct PricelistBatchResult *result) {
    
   memset(result, 0, sizeof(struct PricelistBatchResult));
   
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

   struct json_object *pricelists;
   if (!json_object_object_get_ex(batch, "Pricelists", &pricelists)) {
       snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
               "No Pricelists array found in response");
       return -1;
   }

   size_t n_records = json_object_array_length(pricelists);
   for (size_t i = 0; i < n_records; i++) {
       struct json_object *record = json_object_array_get_idx(pricelists, i);
       result->records_processed++;
       
       if (mysql_query(conn, "START TRANSACTION")) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Failed to start transaction: %s", mysql_error(conn));
           return -1;
       }

       bool success = true;
       
       if (process_pricelist_record(conn, record) != 0) {
           success = false;
       } else {
           struct json_object *items;
           if (json_object_object_get_ex(record, "Items", &items)) {
               size_t n_items = json_object_array_length(items);
               for (size_t j = 0; j < n_items; j++) {
                   struct json_object *item = json_object_array_get_idx(items, j);
                   if (process_pricelistitem_record(conn, item) != 0) {
                       success = false;
                       break;
                   }
               }
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
                   "Failed to process pricelist record");
       }

       if (result->records_processed % 10 == 0) {
           printf("\rProcessing pricelists: %d/%zu records", 
                  result->records_processed, n_records);
           fflush(stdout);
       }
   }

   printf("\n");

   if (result->records_processed > 0) {
       if (!verify_pricelists_batch(conn, result->last_id, pricelists)) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Batch verification failed");
           return -1;
       }
   }

   result->success = (result->records_failed == 0);
   log_batch_status(result);
   
   return 0;
}

bool verify_pricelists_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
   if (!conn || !original_data) return false;

   const char *query = "SELECT p.id, p.name, p.isdefault, p.active, p.useprices, "
                      "       COUNT(pi.id) AS item_count "
                      "FROM pricelists p "
                      "LEFT JOIN pricelistitems pi ON p.id = pi.pricelistid "
                      "GROUP BY p.id "
                      "ORDER BY p.id DESC LIMIT 5";

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return false;

   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return false;
   }

   MYSQL_BIND bind_result[6];
   int db_id;
   char db_name[256];
   unsigned long db_name_length;
   bool db_isdefault;
   bool db_active;
   bool db_useprices;
   long long db_item_count;
   memset(bind_result, 0, sizeof(bind_result));

   bind_result[0].buffer_type = MYSQL_TYPE_LONG;
   bind_result[0].buffer = &db_id;

   bind_result[1].buffer_type = MYSQL_TYPE_STRING;
   bind_result[1].buffer = db_name;
   bind_result[1].buffer_length = sizeof(db_name);

   bind_result[2].buffer_type = MYSQL_TYPE_TINY;
   bind_result[2].buffer = &db_isdefault;

   bind_result[3].buffer_type = MYSQL_TYPE_TINY;
   bind_result[3].buffer = &db_active;

   bind_result[4].buffer_type = MYSQL_TYPE_TINY;
   bind_result[4].buffer = &db_useprices;

   bind_result[5].buffer_type = MYSQL_TYPE_LONGLONG;
   bind_result[5].buffer = &db_item_count;

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
           struct json_object *pricelist = json_object_array_get_idx(original_data, i);
           struct json_object *name_obj, *isdefault_obj, *active_obj, *useprices_obj, *items;
           
           if (json_object_object_get_ex(pricelist, "Name", &name_obj) &&
               json_object_object_get_ex(pricelist, "IsDefault", &isdefault_obj) &&
               json_object_object_get_ex(pricelist, "Active", &active_obj) &&
               json_object_object_get_ex(pricelist, "UsesPrices", &useprices_obj)) {
               const char *json_name = json_object_get_string(name_obj);
               bool json_isdefault = json_object_get_boolean(isdefault_obj);
               bool json_active = json_object_get_boolean(active_obj);
               bool json_useprices = json_object_get_boolean(useprices_obj);

               if (strncmp(db_name, json_name, db_name_length) == 0 &&
                   db_isdefault == json_isdefault &&
                   db_active == json_active &&
                   db_useprices == json_useprices) {
                   // Verify pricelist items count
                   if (json_object_object_get_ex(pricelist, "Items", &items)) {
                       int json_item_count = json_object_array_length(items);
                       if (json_item_count != db_item_count) {
                           verification_passed = false;
                           break;
                       }
                   } else if (db_item_count != 0) {
                       // If no pricelist items in JSON but we have them in DB
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