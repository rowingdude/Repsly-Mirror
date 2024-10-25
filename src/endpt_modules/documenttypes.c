#include "endpt_modules/documenttypes.h"
#include <mysql/mysql.h>

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>

bool log_batch_status(const struct DocumentTypeBatchResult *result) {
   time_t now;
   time(&now);
   
   FILE *log = fopen("documenttypes_processing.log", "a");
   if (!log) return false;
   
   fprintf(log, "[%s] Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
           ctime(&now), result->records_processed, result->records_inserted, 
           result->records_failed, result->total_count,
           result->success ? "SUCCESS" : result->error_message);
   
   fclose(log);
   return true;
}

int process_documenttype_record(MYSQL *conn, struct json_object *record) {
   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_document_type(?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   int documenttypeid = 0;
   char documenttypename[256] = {0};

   // NULL indicators
   bool null_indicators[2] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "DocumentTypeID", &temp))
       documenttypeid = json_object_get_int(temp);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "DocumentTypeName", &temp))
       strncpy(documenttypename, json_object_get_string(temp), sizeof(documenttypename) - 1);
   else
       null_indicators[1] = 1;

   MYSQL_BIND bind[2];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &documenttypeid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = documenttypename;
   bind[1].buffer_length = sizeof(documenttypename);
   bind[1].is_null = &null_indicators[1];

   if (mysql_stmt_bind_param(stmt, bind)) {
       mysql_stmt_close(stmt);
       return -1;
   }

   if (mysql_stmt_execute(stmt)) {
       mysql_stmt_close(stmt);
       return -1;
   }

   mysql_stmt_close(stmt);
   return documenttypeid;
}

int process_documenttype_statuses(MYSQL *conn, int documenttypeid, struct json_object *statuses) {
   if (!statuses || !json_object_is_type(statuses, json_type_array)) {
       return 0;  // No statuses to process
   }

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_document_status(?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   bool null_indicators[3] = {0};
   int documentstatusid = 0;
   char documentstatusname[256] = {0};

   MYSQL_BIND bind[3];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &documentstatusid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_LONG;
   bind[1].buffer = &documenttypeid;
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_STRING;
   bind[2].buffer = documentstatusname;
   bind[2].buffer_length = sizeof(documentstatusname);
   bind[2].is_null = &null_indicators[2];

   int array_len = json_object_array_length(statuses);
   for (int i = 0; i < array_len; i++) {
       struct json_object *status = json_object_array_get_idx(statuses, i);
       struct json_object *temp;

       memset(null_indicators, 0, sizeof(null_indicators));

       if (json_object_object_get_ex(status, "DocumentStatusID", &temp))
           documentstatusid = json_object_get_int(temp);
       else
           null_indicators[0] = 1;

       if (json_object_object_get_ex(status, "DocumentStatusName", &temp))
           strncpy(documentstatusname, json_object_get_string(temp), sizeof(documentstatusname) - 1);
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

int process_documenttype_pricelists(MYSQL *conn, int documenttypeid, struct json_object *pricelists) {
   if (!pricelists || !json_object_is_type(pricelists, json_type_array)) {
       return 0;  // No pricelists to process
   }

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_document_type_pricelist(?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   bool null_indicators[3] = {0};
   int pricelistid = 0;
   char pricelistname[256] = {0};

   MYSQL_BIND bind[3];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &pricelistid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_LONG;
   bind[1].buffer = &documenttypeid;
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_STRING;
   bind[2].buffer = pricelistname;
   bind[2].buffer_length = sizeof(pricelistname);
   bind[2].is_null = &null_indicators[2];

   int array_len = json_object_array_length(pricelists);
   for (int i = 0; i < array_len; i++) {
       struct json_object *pricelist = json_object_array_get_idx(pricelists, i);
       struct json_object *temp;

       memset(null_indicators, 0, sizeof(null_indicators));

       if (json_object_object_get_ex(pricelist, "PricelistID", &temp))
           pricelistid = json_object_get_int(temp);
       else
           null_indicators[0] = 1;

       if (json_object_object_get_ex(pricelist, "PricelistName", &temp))
           strncpy(pricelistname, json_object_get_string(temp), sizeof(pricelistname) - 1);
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

int process_documenttypes_batch(MYSQL *conn, const struct Endpoint *endpoint __attribute__((unused)), 
                             struct json_object *batch, struct DocumentTypeBatchResult *result) {
   // Initialize result
   memset(result, 0, sizeof(struct DocumentTypeBatchResult));
   
   // Process records
   struct json_object *records;
   if (!json_object_object_get_ex(batch, "DocumentTypes", &records)) {
       snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
               "No DocumentTypes array found in response");
       return -1;
   }

   result->total_count = json_object_array_length(records);
   
   for (int i = 0; i < result->total_count; i++) {
       struct json_object *record = json_object_array_get_idx(records, i);
       result->records_processed++;
       
       // Start transaction
       if (mysql_query(conn, "START TRANSACTION")) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Failed to start transaction: %s", mysql_error(conn));
           return -1;
       }

       bool success = true;
       int documenttypeid = process_documenttype_record(conn, record);
       
       if (documenttypeid <= 0) {
           success = false;
       } else {
           // Process statuses if present
           struct json_object *statuses;
           if (json_object_object_get_ex(record, "Statuses", &statuses)) {
               if (process_documenttype_statuses(conn, documenttypeid, statuses) != 0) {
                   success = false;
               }
           }

           // Process pricelists if present
           struct json_object *pricelists;
           if (json_object_object_get_ex(record, "Pricelists", &pricelists)) {
               if (process_documenttype_pricelists(conn, documenttypeid, pricelists) != 0) {
                   success = false;
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
                   "Failed to process document type record %d", documenttypeid);
       }

       // Log progress
       if (result->records_processed % 10 == 0) {
           printf("\rProcessing document types: %d/%d records", 
                  result->records_processed, result->total_count);
           fflush(stdout);
       }
   }

   printf("\n");  // New line after progress indicator

   // Verify batch if any records were processed
   if (result->records_processed > 0) {
       if (!verify_documenttypes_batch(conn, records)) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Batch verification failed");
           return -1;
       }
   }

   result->success = (result->records_failed == 0);
   log_batch_status(result);
   
   return 0;
}

bool verify_documenttypes_batch(MYSQL *conn, struct json_object *original_data) {
   if (!conn || !original_data) return false;

   const char *query = "SELECT dt.documenttypeid, "
                      "COUNT(DISTINCT ds.documentstatusid) as status_count, "
                      "COUNT(DISTINCT dtp.pricelistid) as pricelist_count "
                      "FROM documenttypes dt "
                      "LEFT JOIN documentstatuses ds ON dt.documenttypeid = ds.documenttypeid "
                      "LEFT JOIN documenttypepricelists dtp ON dt.documenttypeid = dtp.documenttypeid "
                      "GROUP BY dt.documenttypeid "
                      "ORDER BY dt.documenttypeid DESC LIMIT 5";

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return false;

   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return false;
   }

   MYSQL_BIND bind_result[3];
   int db_documenttypeid;
   long long db_status_count;
   long long db_pricelist_count;
   memset(bind_result, 0, sizeof(bind_result));

   bind_result[0].buffer_type = MYSQL_TYPE_LONG;
   bind_result[0].buffer = &db_documenttypeid;
   bind_result[1].buffer_type = MYSQL_TYPE_LONGLONG;
   bind_result[1].buffer = &db_status_count;
   bind_result[2].buffer_type = MYSQL_TYPE_LONGLONG;
   bind_result[2].buffer = &db_pricelist_count;

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
           struct json_object *doctype = json_object_array_get_idx(original_data, i);
           struct json_object *id_obj;
           
           if (json_object_object_get_ex(doctype, "DocumentTypeID", &id_obj)) {
               int json_id = json_object_get_int(id_obj);
               if (db_documenttypeid == json_id) {
                   // Verify status counts
                   struct json_object *statuses;
                   if (json_object_object_get_ex(doctype, "Statuses", &statuses)) {
                       int json_status_count = json_object_array_length(statuses);
                       if (json_status_count != db_status_count) {
                           verification_passed = false;
                           break;
                       }
                   }
                   
                   // Verify pricelist counts
                   struct json_object *pricelists;
                   if (json_object_object_get_ex(doctype, "Pricelists", &pricelists)) {
                       int json_pricelist_count = json_object_array_length(pricelists);
                       if (json_pricelist_count != db_pricelist_count) {
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

