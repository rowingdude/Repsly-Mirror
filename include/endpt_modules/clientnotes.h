#ifndef CLIENTNOTES_PROCESSING_H
#define CLIENTNOTES_PROCESSING_H

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <mysql/mysql.h>
#include <json-c/json.h>

// Constants for buffer sizes
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

// Struct for holding batch processing results
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

// Function to log batch processing status
static bool log_batch_status(const struct ClientNoteBatchResult *result);

// Function to process a client note record
int process_clientnote_record(MYSQL *conn, struct json_object *record);

// Function to initialize the MySQL statement with query bindings
bool init_mysql_stmt_bindings(MYSQL_STMT *stmt, MYSQL_BIND *bind, int *clientnoteid,
                              long long *timestamp, char *dateandtime, char *representativecode,
                              char *representativename, char *clientcode, char *clientname,
                              char *streetaddress, char *zip, char *zipext, char *city, char *state,
                              char *country, char *email, char *phone, char *mobile, char *territory,
                              long long *longitude, long long *latitude, char *note, int *visitid,
                              int *metacollectiontotalcount, int *metacollectionfirstid,
                              int *metacollectionlastid, bool *null_indicators);

// Null indicator array for fields that may be NULL in the dtabase
extern bool null_indicators[24];

#endif // CLIENTNOTES_PROCESSING_H
