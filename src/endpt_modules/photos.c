#include <mysql/mysql.h>
#include "endpt_modules/photos.h"
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

bool log_batch_status(const struct PhotoBatchResult *result) {
   time_t now;
   time(&now);
   
   FILE *log = fopen("photos_processing.log", "a");
   if (!log) return false;
   
   fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
           ctime(&now), result->first_id, result->last_id,
           result->records_processed, result->records_inserted, 
           result->records_failed, result->total_count,
           result->success ? "SUCCESS" : result->error_message);
   
   fclose(log);
   return true;
}

int process_photo_record(MYSQL *conn, struct json_object *record) {
   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_photo(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   int photoid = 0;
   char clientcode[50] = {0};
   char clientname[256] = {0};
   char note[1000] = {0};
   char dateandtime[20] = {0};
   char photourl[512] = {0};
   char representativecode[20] = {0};
   char representativename[80] = {0};
   int visitid = 0;
   char tag[1024] = {0};
   int metacollectiontotalcount = 0;
   int metacollectionfirstid = 0;
   int metacollectionlastid = 0;

   // NULL indicators
   bool null_indicators[12] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "PhotoID", &temp))
       photoid = json_object_get_int(temp);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "ClientCode", &temp))
       strncpy(clientcode, json_object_get_string(temp), sizeof(clientcode) - 1);
   else
       null_indicators[1] = 1;

   if (json_object_object_get_ex(record, "ClientName", &temp))
       strncpy(clientname, json_object_get_string(temp), sizeof(clientname) - 1);
   else
       null_indicators[2] = 1;

   if (json_object_object_get_ex(record, "Note", &temp))
       strncpy(note, json_object_get_string(temp), sizeof(note) - 1);
   else
       null_indicators[3] = 1;

   if (json_object_object_get_ex(record, "DateAndTime", &temp))
       strncpy(dateandtime, json_object_get_string(temp), sizeof(dateandtime) - 1);
   else
       null_indicators[4] = 1;

   if (json_object_object_get_ex(record, "PhotoUrl", &temp))
       strncpy(photourl, json_object_get_string(temp), sizeof(photourl) - 1);
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

   if (json_object_object_get_ex(record, "VisitID", &temp))
       visitid = json_object_get_int(temp);
   else
       null_indicators[8] = 1;

   if (json_object_object_get_ex(record, "Tag", &temp))
       strncpy(tag, json_object_get_string(temp), sizeof(tag) - 1);
   else
       null_indicators[9] = 1;

   // Meta fields will be set from batch metadata
   null_indicators[10] = 0; // metacollectiontotalcount
   null_indicators[11] = 0; // metacollectionfirstid
   null_indicators[12] = 0; // metacollectionlastid

   MYSQL_BIND bind[12];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &photoid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = clientcode;
   bind[1].buffer_length = sizeof(clientcode);
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_STRING;
   bind[2].buffer = clientname;
   bind[2].buffer_length = sizeof(clientname);
   bind[2].is_null = &null_indicators[2];

   bind[3].buffer_type = MYSQL_TYPE_STRING;
   bind[3].buffer = note;
   bind[3].buffer_length = sizeof(note);
   bind[3].is_null = &null_indicators[3];

   bind[4].buffer_type = MYSQL_TYPE_STRING;
   bind[4].buffer = dateandtime;
   bind[4].buffer_length = sizeof(dateandtime);
   bind[4].is_null = &null_indicators[4];

   bind[5].buffer_type = MYSQL_TYPE_STRING;
   bind[5].buffer = photourl;
   bind[5].buffer_length = sizeof(photourl);
   bind[5].is_null = &null_indicators[5];

   bind[6].buffer_type = MYSQL_TYPE_STRING;
   bind[6].buffer = representativecode;
   bind[6].buffer_length = sizeof(representativecode);
   bind[6].is_null = &null_indicators[6];

   bind[7].buffer_type = MYSQL_TYPE_STRING;
   bind[7].buffer = representativename;
   bind[7].buffer_length = sizeof(representativename);
   bind[7].is_null = &null_indicators[7];

   bind[8].buffer_type = MYSQL_TYPE_LONG;
   bind[8].buffer = &visitid;
   bind[8].is_null = &null_indicators[8];

   bind[9].buffer_type = MYSQL_TYPE_STRING;
   bind[9].buffer = tag;
   bind[9].buffer_length = sizeof(tag);
   bind[9].is_null = &null_indicators[9];

   bind[10].buffer_type = MYSQL_TYPE_LONG;
   bind[10].buffer = &metacollectiontotalcount;
   bind[10].is_null = &null_indicators[10];

   bind[11].buffer_type = MYSQL_TYPE_LONG;
   bind[11].buffer = &metacollectionfirstid;
   bind[11].is_null = &null_indicators[11];

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
   return 0;
}

int process_photos_batch(MYSQL *conn, const struct Endpoint *endpoint  __attribute__((unused)), struct json_object *batch, struct PhotoBatchResult *result) {
    
   memset(result, 0, sizeof(struct PhotoBatchResult));
   
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

   struct json_object *photos;
   if (!json_object_object_get_ex(batch, "Photos", &photos)) {
       snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
               "No Photos array found in response");
       return -1;
   }

   size_t n_records = json_object_array_length(photos);
   for (size_t i = 0; i < n_records; i++) {
       struct json_object *record = json_object_array_get_idx(photos, i);
       result->records_processed++;
       
       if (mysql_query(conn, "START TRANSACTION")) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Failed to start transaction: %s", mysql_error(conn));
           return -1;
       }

       bool success = true;
       
       if (process_photo_record(conn, record) != 0) {
           success = false;
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
                   "Failed to process photo record");
       }

       if (result->records_processed % 10 == 0) {
           printf("\rProcessing photos: %d/%zu records", 
                  result->records_processed, n_records);
           fflush(stdout);
       }
   }

   printf("\n");

   if (result->records_processed > 0) {
       if (!verify_photos_batch(conn, result->last_id, photos)) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Batch verification failed");
           return -1;
       }
   }

   result->success = (result->records_failed == 0);
   log_batch_status(result);
   
   return 0;
}

bool verify_photos_batch(MYSQL *conn, int last_id __attribute__((unused)), struct json_object *original_data) {

   if (!conn || !original_data) return false;

   const char *query = "SELECT p.photoid, p.clientcode, p.clientname, "
                      "       COUNT(p.tag) AS tag_count "
                      "FROM photos p "
                      "GROUP BY p.photoid "
                      "ORDER BY p.photoid DESC LIMIT 5";

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return false;

   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return false;
   }

   MYSQL_BIND bind_result[4];
   int db_photoid;
   char db_clientcode[50];
   unsigned long db_clientcode_length;
   char db_clientname[256];
   unsigned long db_clientname_length;
   long long db_tag_count;
   memset(bind_result, 0, sizeof(bind_result));

   bind_result[0].buffer_type = MYSQL_TYPE_LONG;
   bind_result[0].buffer = &db_photoid;

   bind_result[1].buffer_type = MYSQL_TYPE_STRING;
   bind_result[1].buffer = db_clientcode;
   bind_result[1].buffer_length = sizeof(db_clientcode);
   bind_result[1].length = &db_clientcode_length;

   bind_result[2].buffer_type = MYSQL_TYPE_STRING;
   bind_result[2].buffer = db_clientname;
   bind_result[2].buffer_length = sizeof(db_clientname);
   bind_result[2].length = &db_clientname_length;

   bind_result[3].buffer_type = MYSQL_TYPE_LONGLONG;
   bind_result[3].buffer = &db_tag_count;

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
           struct json_object *photo = json_object_array_get_idx(original_data, i);
           struct json_object *photoid_obj, *clientcode_obj, *clientname_obj, *tag;
           
           if (json_object_object_get_ex(photo, "PhotoID", &photoid_obj) &&
               json_object_object_get_ex(photo, "ClientCode", &clientcode_obj) &&
               json_object_object_get_ex(photo, "ClientName", &clientname_obj)) {
               int json_photoid = json_object_get_int(photoid_obj);
               const char *json_clientcode = json_object_get_string(clientcode_obj);
               const char *json_clientname = json_object_get_string(clientname_obj);

               if (db_photoid == json_photoid &&
                   strncmp(db_clientcode, json_clientcode, db_clientcode_length) == 0 &&
                   strncmp(db_clientname, json_clientname, db_clientname_length) == 0) {
                   // Verify tags count
                   if (json_object_object_get_ex(photo, "Tag", &tag)) {
                       int json_tag_count = strlen(json_object_get_string(tag));
                       if (json_tag_count != db_tag_count) {
                           verification_passed = false;
                           break;
                       }
                   } else if (db_tag_count != 0) {
                       // If no tags in JSON but we have them in DB
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