#include "endpt_modules/products.h"
#include <mysql/mysql.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>

static bool log_batch_status(const struct ProductBatchResult *result) {
   time_t now;
   time(&now);
   
   FILE *log = fopen("products_processing.log", "a");
   if (!log) return false;
   
   fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
           ctime(&now), result->first_id, result->last_id,
           result->records_processed, result->records_inserted, 
           result->records_failed, result->total_count,
           result->success ? "SUCCESS" : result->error_message);
   
   fclose(log);
   return true;
}

int process_product_record(MYSQL *conn, struct json_object *record) {
   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_product(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   char code[256] = {0};
   char name[1024] = {0};
   char productgroupcode[21] = {0};
   char productgroupname[81] = {0};
   bool active = false;
   char tag[1024] = {0};
   double unitprice = 0.0;
   char ean[21] = {0};
   char note[1001] = {0};
   char imageurl[1024] = {0};
   char masterproduct[256] = {0};
   int metacollectiontotalcount = 0;
   int metacollectionfirstid = 0;
   int metacollectionlastid = 0;

   // NULL indicators
   bool null_indicators[14] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "Code", &temp))
       strncpy(code, json_object_get_string(temp), sizeof(code) - 1);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "Name", &temp))
       strncpy(name, json_object_get_string(temp), sizeof(name) - 1);
   else
       null_indicators[1] = 1;

   if (json_object_object_get_ex(record, "ProductGroupCode", &temp))
       strncpy(productgroupcode, json_object_get_string(temp), sizeof(productgroupcode) - 1);
   else
       null_indicators[2] = 1;

   if (json_object_object_get_ex(record, "ProductGroupName", &temp))
       strncpy(productgroupname, json_object_get_string(temp), sizeof(productgroupname) - 1);
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

   if (json_object_object_get_ex(record, "UnitPrice", &temp))
       unitprice = json_object_get_double(temp);
   else
       null_indicators[6] = 1;

   if (json_object_object_get_ex(record, "EAN", &temp))
       strncpy(ean, json_object_get_string(temp), sizeof(ean) - 1);
   else
       null_indicators[7] = 1;

   if (json_object_object_get_ex(record, "Note", &temp))
       strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
   else
       null_indicators[8] = 1;

   if (json_object_object_get_ex(record, "ImageUrl", &temp))
       strncpy(imageurl, json_object_get_string(temp), sizeof(imageurl) - 1);
   else
       null_indicators[9] = 1;

   if (json_object_object_get_ex(record, "MasterProduct", &temp))
       strncpy(masterproduct, json_object_get_string(temp), sizeof(masterproduct) - 1);
   else
       null_indicators[10] = 1;

   // Meta fields will be set from batch metadata
   null_indicators[11] = 0; // metacollectiontotalcount
   null_indicators[12] = 0; // metacollectionfirstid
   null_indicators[13] = 0; // metacollectionlastid

   MYSQL_BIND bind[14];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_STRING;
   bind[0].buffer = code;
   bind[0].buffer_length = sizeof(code);
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = name;
   bind[1].buffer_length = sizeof(name);
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_STRING;
   bind[2].buffer = productgroupcode;
   bind[2].buffer_length = sizeof(productgroupcode);
   bind[2].is_null = &null_indicators[2];

   bind[3].buffer_type = MYSQL_TYPE_STRING;
   bind[3].buffer = productgroupname;
   bind[3].buffer_length = sizeof(productgroupname);
   bind[3].is_null = &null_indicators[3];

   bind[4].buffer_type = MYSQL_TYPE_TINY;
   bind[4].buffer = &active;
   bind[4].is_null = &null_indicators[4];

   bind[5].buffer_type = MYSQL_TYPE_STRING;
   bind[5].buffer = tag;
   bind[5].buffer_length = sizeof(tag);
   bind[5].is_null = &null_indicators[5];

   bind[6].buffer_type = MYSQL_TYPE_DOUBLE;
   bind[6].buffer = &unitprice;
   bind[6].is_null = &null_indicators[6];

   bind[7].buffer_type = MYSQL_TYPE_STRING;
   bind[7].buffer = ean;
   bind[7].buffer_length = sizeof(ean);
   bind[7].is_null = &null_indicators[7];

   bind[8].buffer_type = MYSQL_TYPE_STRING;
   bind[8].buffer = note;
   bind[8].buffer_length = sizeof(note);
   bind[8].is_null = &null_indicators[8];

   bind[9].buffer_type = MYSQL_TYPE_STRING;
   bind[9].buffer = imageurl;
   bind[9].buffer_length = sizeof(imageurl);
   bind[9].is_null = &null_indicators[9];

   bind[10].buffer_type = MYSQL_TYPE_STRING;
   bind[10].buffer = masterproduct;
   bind[10].buffer_length = sizeof(masterproduct);
   bind[10].is_null = &null_indicators[10];

   bind[11].buffer_type = MYSQL_TYPE_LONG;
   bind[11].buffer = &metacollectiontotalcount;
   bind[11].is_null = &null_indicators[11];

   bind[12].buffer_type = MYSQL_TYPE_LONG;
   bind[12].buffer = &metacollectionfirstid;
   bind[12].is_null = &null_indicators[12];

   bind[13].buffer_type = MYSQL_TYPE_LONG;
   bind[13].buffer = &metacollectionlastid;
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
   return 0;
}

int process_product_packagingcodes(MYSQL *conn, const char *productcode, 
                                struct json_object *packagingcodes) {
   if (!packagingcodes || !json_object_is_type(packagingcodes, json_type_array)) {
       return 0;  // No packaging codes to process
   }

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_product_packagingcode(?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   bool null_indicators[3] = {0};
   char packagingcode[256] = {0};
   bool isset = false;

   MYSQL_BIND bind[3];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_STRING;
   bind[0].buffer = (void*)productcode;
   bind[0].buffer_length = strlen(productcode);
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = packagingcode;
   bind[1].buffer_length = sizeof(packagingcode);
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_TINY;
   bind[2].buffer = &isset;
   bind[2].is_null = &null_indicators[2];

   int array_len = json_object_array_length(packagingcodes);
   for (int i = 0; i < array_len; i++) {
       struct json_object *code = json_object_array_get_idx(packagingcodes, i);
       struct json_object *temp;

       memset(null_indicators, 0, sizeof(null_indicators));

       if (json_object_object_get_ex(code, "Value", &temp))
           strncpy(packagingcode, json_object_get_string(temp), sizeof(packagingcode) - 1);
       else
           null_indicators[1] = 1;

       if (json_object_object_get_ex(code, "IsSet", &temp))
           isset = json_object_get_boolean(temp);
       else
           null_indicators[2] = 1;

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

int process_products_batch(MYSQL *conn, const struct Endpoint *endpoint, 
                        struct json_object *batch, struct ProductBatchResult *result) {
   memset(result, 0, sizeof(struct ProductBatchResult));
   
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

   struct json_object *records;
   if (!json_object_object_get_ex(batch, "Products", &records)) {
       snprintf(result->error_message, sizeof(result->error_message), 
               "No Products array found in response");
       return -1;
   }

   size_t n_records = json_object_array_length(records);
   for (size_t i = 0; i < n_records; i++) {
       struct json_object *record = json_object_array_get_idx(records, i);
       result->records_processed++;
       
       if (mysql_query(conn, "START TRANSACTION")) {
           snprintf(result->error_message, sizeof(result->error_message), 
                   "Failed to start transaction: %s", mysql_error(conn));
           return -1;
       }

       bool success = true;
       
       // Get product code for packaging codes processing
       struct json_object *code_obj;
       char productcode[256] = {0};
       if (json_object_object_get_ex(record, "Code", &code_obj)) {
           strncpy(productcode, json_object_get_string(code_obj), sizeof(productcode) - 1);
       }

       if (process_product_record(conn, record) != 0) {
           success = false;
       } else {
           struct json_object *packagingcodes;
           if (json_object_object_get_ex(record, "Packaging", &packagingcodes)) {
               struct json_object *codes;
               if (json_object_object_get_ex(packagingcodes, "Codes", &codes)) {
                   if (process_product_packagingcodes(conn, productcode, codes) != 0) {
                       success = false;
                   }
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
           snprintf(result->error_message, sizeof(result->error_message), 
                   "Failed to process product record %s", productcode);
       }

       if (result->records_processed % 10 == 0) {
           printf("\rProcessing products: %d/%zu records", 
                  result->records_processed, n_records);
           fflush(stdout);
       }
   }

   printf("\n");

   if (result->records_processed > 0) {
       if (!verify_products_batch(conn, result->last_id, records)) {
           snprintf(result->error_message, sizeof(result->error_message), 
                   "Batch verification failed");
           return -1;
       }
   }

   result->success = (result->records_failed == 0);
   log_batch_status(result);
   
   return 0;
}

bool verify_products_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
   if (!conn || !original_data) return false;

   const char *query = "SELECT p.code, COUNT(ppc.packagingcode) as packaging_count "
                      "FROM products p "
                      "LEFT JOIN productpackagingcodes ppc ON p.code = ppc.productcode "
                      "GROUP BY p.code "
                      "ORDER BY p.code DESC LIMIT 5";

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return false;

   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return false;
   }

   MYSQL_BIND bind_result[2];
   char db_code[256];
   unsigned long db_code_length;
   long long db_packaging_count;
   memset(bind_result, 0, sizeof(bind_result));

   bind_result[0].buffer_type = MYSQL_TYPE_STRING;
   bind_result[0].buffer = db_code;
   bind_result[0].buffer_length = sizeof(db_code);
   bind_result[0].length = &db_code_length;

   bind_result[1].buffer_type = MYSQL_TYPE_LONGLONG;
   bind_result[1].buffer = &db_packaging_count;

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
           struct json_object *product = json_object_array_get_idx(original_data, i);
           struct json_object *code_obj;
           
           if (json_object_object_get_ex(product, "Code", &code_obj)) {
               const char *json_code = json_object_get_string(code_obj);
               if (strncmp(db_code, json_code, db_code_length) == 0) {
                   // Verify packaging codes count
                   struct json_object *packaging, *codes;
                   if (json_object_object_get_ex(product, "Packaging", &packaging) &&
                       json_object_object_get_ex(packaging, "Codes", &codes)) {
                       int json_packaging_count = json_object_array_length(codes);
                       if (json_packaging_count != db_packaging_count) {
                           verification_passed = false;
                           break;
                       }
                   } else if (db_packaging_count != 0) {
                       // If no packaging codes in JSON but we have them in DB
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