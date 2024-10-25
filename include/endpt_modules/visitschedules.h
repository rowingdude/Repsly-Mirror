#ifndef VISITSCHEDULES_H
#define VISITSCHEDULES_H

#include <stdbool.h>
#include <mysql/mysql.h>
#include <json-c/json.h>

// Structure to hold batch processing results
struct VisitScheduleBatchResult {
    int first_id;                // ID of the first record
    int last_id;                 // ID of the last record
    int records_processed;       // Total records processed
    int records_inserted;        // Total records inserted
    int records_failed;          // Total records failed
    int total_count;             // Total count of records in the batch
    bool success;                // Status of the batch processing
    char error_message[256];     // Error message if any failure occurs
};

// Function prototypes
int process_visitschedules_batch(MYSQL *conn, const struct Endpoint *endpoint,
                                  struct json_object *batch,
                                  struct VisitScheduleBatchResult *result);

int process_visitschedule_record(MYSQL *conn, struct json_object *record);

bool verify_visitschedules_batch(MYSQL *conn, int last_id, struct json_object *original_data);

// Static function for logging batch status (should not be exposed in header)
static bool log_batch_status(const struct VisitScheduleBatchResult *result);

#endif