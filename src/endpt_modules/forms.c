#include "endpt_modules/forms.h"
#include <mysql/mysql.h>

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>

bool log_batch_status(const struct FormBatchResult *result) {
   time_t now;
   time(&now);
   
   FILE *log = fopen("forms_processing.log", "a");
   if (!log) return false;
   
   fprintf(log, "[%s] FirstID: %d, LastID: %d, Processed: %d, Inserted: %d, Failed: %d, Total Count: %d, Status: %s\n",
           ctime(&now), result->first_id, result->last_id,
           result->records_processed, result->records_inserted, 
           result->records_failed, result->total_count,
           result->success ? "SUCCESS" : result->error_message);
   
   fclose(log);
   return true;
}

int process_form_record(MYSQL *conn, struct json_object *record) {
   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_form(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   int formid = 0;
   char formname[256] = {0};
   char clientcode[50] = {0};
   char clientname[256] = {0};
   char dateandtime[20] = {0};
   char representativecode[20] = {0};
   char representativename[80] = {0};
   char streetaddress[256] = {0};
   char zip[20] = {0};
   char zipext[20] = {0};
   char city[256] = {0};
   char state[256] = {0};
   char country[256] = {0};
   char email[256] = {0};
   char phone[128] = {0};
   char mobile[128] = {0};
   char territory[80] = {0};
   long long longitude = 0;
   long long latitude = 0;
   char signatureurl[512] = {0};
   char visitstart[20] = {0};
   char visitend[20] = {0};
   int visitid = 0;
   int metacollectiontotalcount = 0;
   int metacollectionfirstid = 0;
   int metacollectionlastid = 0;

   // NULL indicators
   bool null_indicators[25] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "FormID", &temp))
       formid = json_object_get_int(temp);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "FormName", &temp))
       strncpy(formname, json_object_get_string(temp), sizeof(formname) - 1);
   else
       null_indicators[1] = 1;

   if (json_object_object_get_ex(record, "ClientCode", &temp))
       strncpy(clientcode, json_object_get_string(temp), sizeof(clientcode) - 1);
   else
       null_indicators[2] = 1;

   if (json_object_object_get_ex(record, "ClientName", &temp))
       strncpy(clientname, json_object_get_string(temp), sizeof(clientname) - 1);
   else
       null_indicators[3] = 1;

   if (json_object_object_get_ex(record, "DateAndTime", &temp))
       strncpy(dateandtime, json_object_get_string(temp), sizeof(dateandtime) - 1);
   else
       null_indicators[4] = 1;

   if (json_object_object_get_ex(record, "RepresentativeCode", &temp))
       strncpy(representativecode, json_object_get_string(temp), sizeof(representativecode) - 1);
   else
       null_indicators[5] = 1;

   if (json_object_object_get_ex(record, "RepresentativeName", &temp))
       strncpy(representativename, json_object_get_string(temp), sizeof(representativename) - 1);
   else
       null_indicators[6] = 1;

   if (json_object_object_get_ex(record, "StreetAddress", &temp))
       strncpy(streetaddress, json_object_get_string(temp), sizeof(streetaddress) - 1);
   else
       null_indicators[7] = 1;

   if (json_object_object_get_ex(record, "Zip", &temp))
       strncpy(zip, json_object_get_string(temp), sizeof(zip) - 1);
   else
       null_indicators[8] = 1;

   if (json_object_object_get_ex(record, "ZipExt", &temp))
       strncpy(zipext, json_object_get_string(temp), sizeof(zipext) - 1);
   else
       null_indicators[9] = 1;

   if (json_object_object_get_ex(record, "City", &temp))
       strncpy(city, json_object_get_string(temp), sizeof(city) - 1);
   else
       null_indicators[10] = 1;

   if (json_object_object_get_ex(record, "State", &temp))
       strncpy(state, json_object_get_string(temp), sizeof(state) - 1);
   else
       null_indicators[11] = 1;

   if (json_object_object_get_ex(record, "Country", &temp))
       strncpy(country, json_object_get_string(temp), sizeof(country) - 1);
   else
       null_indicators[12] = 1;

   if (json_object_object_get_ex(record, "Email", &temp))
       strncpy(email, json_object_get_string(temp), sizeof(email) - 1);
   else
       null_indicators[13] = 1;

   if (json_object_object_get_ex(record, "Phone", &temp))
       strncpy(phone, json_object_get_string(temp), sizeof(phone) - 1);
   else
       null_indicators[14] = 1;

   if (json_object_object_get_ex(record, "Mobile", &temp))
       strncpy(mobile, json_object_get_string(temp), sizeof(mobile) - 1);
   else
       null_indicators[15] = 1;

   if (json_object_object_get_ex(record, "Territory", &temp))
       strncpy(territory, json_object_get_string(temp), sizeof(territory) - 1);
   else
       null_indicators[16] = 1;

   if (json_object_object_get_ex(record, "Longitude", &temp))
       longitude = json_object_get_int64(temp);
   else
       null_indicators[17] = 1;

   if (json_object_object_get_ex(record, "Latitude", &temp))
       latitude = json_object_get_int64(temp);
   else
       null_indicators[18] = 1;

   if (json_object_object_get_ex(record, "SignatureUrl", &temp))
       strncpy(signatureurl, json_object_get_string(temp), sizeof(signatureurl) - 1);
   else
       null_indicators[19] = 1;

   if (json_object_object_get_ex(record, "VisitStart", &temp))
       strncpy(visitstart, json_object_get_string(temp), sizeof(visitstart) - 1);
   else
       null_indicators[20] = 1;

   if (json_object_object_get_ex(record, "VisitEnd", &temp))
       strncpy(visitend, json_object_get_string(temp), sizeof(visitend) - 1);
   else
       null_indicators[21] = 1;

   if (json_object_object_get_ex(record, "VisitID", &temp))
       visitid = json_object_get_int(temp);
   else
       null_indicators[22] = 1;

   if (json_object_object_get_ex(record, "MetaCollectionTotalCount", &temp))
       metacollectiontotalcount = json_object_get_int(temp);
   else
       null_indicators[23] = 1;

   if (json_object_object_get_ex(record, "MetaCollectionFirstID", &temp))
       metacollectionfirstid = json_object_get_int(temp);
   else
       null_indicators[24] = 1;

   if (json_object_object_get_ex(record, "MetaCollectionLastID", &temp))
       metacollectionlastid = json_object_get_int(temp);
   else
       null_indicators[24] = 1;

   MYSQL_BIND bind[25];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &formid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = formname;
   bind[1].buffer_length = sizeof(formname);
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_STRING;
   bind[2].buffer = clientcode;
   bind[2].buffer_length = sizeof(clientcode);
   bind[2].is_null = &null_indicators[2];

   bind[3].buffer_type = MYSQL_TYPE_STRING;
   bind[3].buffer = clientname;
   bind[3].buffer_length = sizeof(clientname);
   bind[3].is_null = &null_indicators[3];

   bind[4].buffer_type = MYSQL_TYPE_STRING;
   bind[4].buffer = dateandtime;
   bind[4].buffer_length = sizeof(dateandtime);
   bind[4].is_null = &null_indicators[4];

   bind[5].buffer_type = MYSQL_TYPE_STRING;
   bind[5].buffer = representativecode;
   bind[5].buffer_length = sizeof(representativecode);
   bind[5].is_null = &null_indicators[5];

   bind[6].buffer_type = MYSQL_TYPE_STRING;
   bind[6].buffer = representativename;
   bind[6].buffer_length = sizeof(representativename);
   bind[6].is_null = &null_indicators[6];

   bind[7].buffer_type = MYSQL_TYPE_STRING;
   bind[7].buffer = streetaddress;
   bind[7].buffer_length = sizeof(streetaddress);
   bind[7].is_null = &null_indicators[7];

   bind[8].buffer_type = MYSQL_TYPE_STRING;
   bind[8].buffer = zip;
   bind[8].buffer_length = sizeof(zip);
   bind[8].is_null = &null_indicators[8];

   bind[9].buffer_type = MYSQL_TYPE_STRING;
   bind[9].buffer = zipext;
   bind[9].buffer_length = sizeof(zipext);
   bind[9].is_null = &null_indicators[9];

   bind[10].buffer_type = MYSQL_TYPE_STRING;
   bind[10].buffer = city;
   bind[10].buffer_length = sizeof(city);
   bind[10].is_null = &null_indicators[10];

   bind[11].buffer_type = MYSQL_TYPE_STRING;
   bind[11].buffer = state;
   bind[11].buffer_length = sizeof(state);
   bind[11].is_null = &null_indicators[11];

   bind[12].buffer_type = MYSQL_TYPE_STRING;
   bind[12].buffer = country;
   bind[12].buffer_length = sizeof(country);
   bind[12].is_null = &null_indicators[12];

   bind[13].buffer_type = MYSQL_TYPE_STRING;
   bind[13].buffer = email;
   bind[13].buffer_length = sizeof(email);
   bind[13].is_null = &null_indicators[13];

   bind[14].buffer_type = MYSQL_TYPE_STRING;
   bind[14].buffer = phone;
   bind[14].buffer_length = sizeof(phone);
   bind[14].is_null = &null_indicators[14];

   bind[15].buffer_type = MYSQL_TYPE_STRING;
   bind[15].buffer = mobile;
   bind[15].buffer_length = sizeof(mobile);
   bind[15].is_null = &null_indicators[15];

   bind[16].buffer_type = MYSQL_TYPE_STRING;
   bind[16].buffer = territory;
   bind[16].buffer_length = sizeof(territory);
   bind[16].is_null = &null_indicators[16];

   bind[17].buffer_type = MYSQL_TYPE_LONGLONG;
   bind[17].buffer = &longitude;
   bind[17].is_null = &null_indicators[17];

   bind[18].buffer_type = MYSQL_TYPE_LONGLONG;
   bind[18].buffer = &latitude;
   bind[18].is_null = &null_indicators[18];

   bind[19].buffer_type = MYSQL_TYPE_STRING;
   bind[19].buffer = signatureurl;
   bind[19].buffer_length = sizeof(signatureurl);
   bind[19].is_null = &null_indicators[19];

   bind[20].buffer_type = MYSQL_TYPE_STRING;
   bind[20].buffer = visitstart;
   bind[20].buffer_length = sizeof(visitstart);
   bind[20].is_null = &null_indicators[20];

   bind[21].buffer_type = MYSQL_TYPE_STRING;
   bind[21].buffer = visitend;
   bind[21].buffer_length = sizeof(visitend);
   bind[21].is_null = &null_indicators[21];

   bind[22].buffer_type = MYSQL_TYPE_LONG;
   bind[22].buffer = &visitid;
   bind[22].is_null = &null_indicators[22];

   bind[23].buffer_type = MYSQL_TYPE_LONG;
   bind[23].buffer = &metacollectiontotalcount;
   bind[23].is_null = &null_indicators[23];

   bind[24].buffer_type = MYSQL_TYPE_LONG;
   bind[24].buffer = &metacollectionfirstid;
   bind[24].is_null = &null_indicators[24];

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

int process_formitem_record(MYSQL *conn, struct json_object *record, int formid) {
   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return -1;

   const char *query = "CALL upsert_formitem(?, ?, ?, ?)";
   
   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return -1;
   }

   // Field variables
   char field[256] = {0};
   char value[1024] = {0};
   int itemorder = 0;

   // NULL indicators
   bool null_indicators[3] = {0};

   struct json_object *temp;

   if (json_object_object_get_ex(record, "Field", &temp))
       strncpy(field, json_object_get_string(temp), sizeof(field) - 1);
   else
       null_indicators[0] = 1;

   if (json_object_object_get_ex(record, "Value", &temp))
       strncpy(value, json_object_get_string(temp), sizeof(value) - 1);
   else
       null_indicators[1] = 1;

   if (json_object_object_get_ex(record, "ItemOrder", &temp))
       itemorder = json_object_get_int(temp);
   else
       null_indicators[2] = 1;

   MYSQL_BIND bind[4];
   memset(bind, 0, sizeof(bind));

   bind[0].buffer_type = MYSQL_TYPE_LONG;
   bind[0].buffer = &formid;
   bind[0].is_null = &null_indicators[0];

   bind[1].buffer_type = MYSQL_TYPE_STRING;
   bind[1].buffer = field;
   bind[1].buffer_length = sizeof(field);
   bind[1].is_null = &null_indicators[1];

   bind[2].buffer_type = MYSQL_TYPE_STRING;
   bind[2].buffer = value;
   bind[2].buffer_length = sizeof(value);
   bind[2].is_null = &null_indicators[2];

   bind[3].buffer_type = MYSQL_TYPE_LONG;
   bind[3].buffer = &itemorder;
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

int process_forms_batch(MYSQL *conn, const struct Endpoint *endpoint,
                        struct json_object *batch, struct FormBatchResult *result) {
   memset(result, 0, sizeof(struct FormBatchResult));
   
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

   struct json_object *forms;
   if (!json_object_object_get_ex(batch, "Forms", &forms)) {
       snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
               "No Forms array found in response");
       return -1;
   }

   size_t n_records = json_object_array_length(forms);
   for (size_t i = 0; i < n_records; i++) {
       struct json_object *record = json_object_array_get_idx(forms, i);
       result->records_processed++;
       
       if (mysql_query(conn, "START TRANSACTION")) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Failed to start transaction: %s", mysql_error(conn));
           return -1;
       }

       bool success = true;
       
       if (process_form_record(conn, record) != 0) {
           success = false;
       } else {
           struct json_object *items;
           if (json_object_object_get_ex(record, "Items", &items)) {
               size_t n_items = json_object_array_length(items);
               for (size_t j = 0; j < n_items; j++) {
                   struct json_object *item = json_object_array_get_idx(items, j);
                   if (process_formitem_record(conn, item, json_object_get_int(record)) != 0) {
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
                   "Failed to process form record");
       }

       if (result->records_processed % 10 == 0) {
           printf("\rProcessing forms: %d/%zu records", 
                  result->records_processed, n_records);
           fflush(stdout);
       }
   }

   printf("\n");

   if (result->records_processed > 0) {
       if (!verify_forms_batch(conn, result->last_id, forms)) {
           snprintf(result->error_message, ERROR_MESSAGE_SIZE,  
                   "Batch verification failed");
           return -1;
       }
   }

   result->success = (result->records_failed == 0);
   log_batch_status(result);
   
   return 0;
}

bool verify_forms_batch(MYSQL *conn, int last_id, struct json_object *original_data) {
   if (!conn || !original_data) return false;

   const char *query = "SELECT f.formid, f.formname, f.clientcode, f.clientname, "
                      "       COUNT(fi.field) AS item_count "
                      "FROM forms f "
                      "LEFT JOIN formitems fi ON f.formid = fi.formid "
                      "GROUP BY f.formid "
                      "ORDER BY f.formid DESC LIMIT 5";

   MYSQL_STMT *stmt = mysql_stmt_init(conn);
   if (!stmt) return false;

   if (mysql_stmt_prepare(stmt, query, strlen(query))) {
       mysql_stmt_close(stmt);
       return false;
   }

   MYSQL_BIND bind_result[5];
   int db_formid;
   char db_formname[256];
   unsigned long db_formname_length;
   char db_clientcode[50];
   unsigned long db_clientcode_length;
   char db_clientname[256];
   unsigned long db_clientname_length;
   long long db_item_count;
   memset(bind_result, 0, sizeof(bind_result));

   bind_result[0].buffer_type = MYSQL_TYPE_LONG;
   bind_result[0].buffer = &db_formid;

   bind_result[1].buffer_type = MYSQL_TYPE_STRING;
   bind_result[1].buffer = db_formname;
   bind_result[1].buffer_length = sizeof(db_formname);
   bind_result[1].length = &db_formname_length;

   bind_result[2].buffer_type = MYSQL_TYPE_STRING;
   bind_result[2].buffer = db_clientcode;
   bind_result[2].buffer_length = sizeof(db_clientcode);
   bind_result[2].length = &db_clientcode_length;

   bind_result[3].buffer_type = MYSQL_TYPE_STRING;
   bind_result[3].buffer = db_clientname;
   bind_result[3].buffer_length = sizeof(db_clientname);
   bind_result[3].length = &db_clientname_length;

   bind_result[4].buffer_type = MYSQL_TYPE_LONGLONG;
   bind_result[4].buffer = &db_item_count;

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
           struct json_object *form = json_object_array_get_idx(original_data, i);
           struct json_object *formid_obj, *formname_obj, *clientcode_obj, *clientname_obj, *items;
           
           if (json_object_object_get_ex(form, "FormID", &formid_obj) &&
               json_object_object_get_ex(form, "FormName", &formname_obj) &&
               json_object_object_get_ex(form, "ClientCode", &clientcode_obj) &&
               json_object_object_get_ex(form, "ClientName", &clientname_obj)) {
               int json_formid = json_object_get_int(formid_obj);
               const char *json_formname = json_object_get_string(formname_obj);
               const char *json_clientcode = json_object_get_string(clientcode_obj);
               const char *json_clientname = json_object_get_string(clientname_obj);

               if (db_formid == json_formid &&
                   strncmp(db_formname, json_formname, db_formname_length) == 0 &&
                   strncmp(db_clientcode, json_clientcode, db_clientcode_length) == 0 &&
                   strncmp(db_clientname, json_clientname, db_clientname_length) == 0) {
                   // Verify form items count
                   if (json_object_object_get_ex(form, "Items", &items)) {
                       int json_item_count = json_object_array_length(items);
                       if (json_item_count != db_item_count) {
                           verification_passed = false;
                           break;
                       }
                   } else if (db_item_count != 0) {
                       // If no form items in JSON but we have them in DB
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