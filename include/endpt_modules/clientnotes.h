#ifndef CLIENTNOTES_PROCESSING_H
#define CLIENTNOTES_PROCESSING_H

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>
#include <json-c/json.h>

#include "../endpoints.h"

#define DATE_TIME_SIZE 64
#define CODE_SIZE 21
#define NAME_SIZE 256
#define ADDRESS_SIZE 256
#define ZIP_SIZE 21
#define CITY_STATE_COUNTRY_SIZE 256
#define EMAIL_SIZE 256
#define PHONE_SIZE 129
#define TERRITORY_SIZE 81
#define NOTE_SIZE 65535
#define ERROR_MESSAGE_SIZE 256

struct ClientNoteBatchResult {
    int first_id;
    int last_id;
    int records_processed;
    int records_inserted;
    int records_failed;
    int total_count;
    bool success;
    char error_message[ERROR_MESSAGE_SIZE];
};

bool log_batch_status(const struct ClientNoteBatchResult *result);
int process_clientnote_record(MYSQL *conn, struct json_object *record);
bool verify_clientnotes_batch(MYSQL *conn, int last_id, struct json_object *original_data);
bool init_mysql_stmt_bindings(MYSQL_STMT *stmt, MYSQL_BIND *bind, int *clientnoteid,
                              long long *timestamp, char *dateandtime, char *representativecode,
                              char *representativename, char *clientcode, char *clientname,
                              char *streetaddress, char *zip, char *zipext, char *city, char *state,
                              char *country, char *email, char *phone, char *mobile, char *territory,
                              long long *longitude, long long *latitude, char *note, int *visitid,
                              int *metacollectiontotalcount, int *metacollectionfirstid,
                              int *metacollectionlastid, bool *null_indicators);

extern bool null_indicators[24];

#endif 
