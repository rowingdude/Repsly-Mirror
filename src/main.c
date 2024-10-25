#include "main.h"
#include "endpoints.h"
#include "sql_conn.h"
#include "logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <mysql/mysql.h>

size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *)userp;

    char *ptr = realloc(mem->memory, mem->size + realsize + 1);
    if (!ptr) return 0;

    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;

    return realsize;
}

int process_endpoint(MYSQL *conn, const struct Endpoint *endpoint) {
    CURL *curl;
    CURLcode res;
    struct MemoryStruct chunk = {0};
    char url[MAX_URL_LENGTH];
    int total_records = 0;
    
    curl = curl_easy_init();
    if (!curl) {
        logMessage(LOG_LEVEL_ERROR, "Failed to initialize CURL for endpoint: %s", endpoint->name);
        return -1;
    }

    chunk.memory = malloc(1);
    chunk.size = 0;

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    curl_easy_setopt(curl, CURLOPT_USERNAME, getenv("REPSLY_API_USERNAME"));
    curl_easy_setopt(curl, CURLOPT_PASSWORD, getenv("REPSLY_API_PASSWORD"));

    switch (endpoint->pagination_type) {
        case TIMESTAMP:
            snprintf(url, sizeof(url), "%s%lld", endpoint->url_format, 
                    endpoint->meta.last_timestamp);
            break;
        case SKIP:
            snprintf(url, sizeof(url), "%s%lld/%d", endpoint->url_format,
                    endpoint->meta.last_timestamp, endpoint->meta.skip_count);
            break;
        case DATERANGE:
            snprintf(url, sizeof(url), "%s%s/%s", endpoint->url_format,
                    endpoint->meta.date_start, endpoint->meta.date_end);
            break;
        default:
            strncpy(url, endpoint->url_format, sizeof(url) - 1);
            url[sizeof(url) - 1] = '\0';
            break;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    
    logMessage(LOG_LEVEL_INFO, "Processing endpoint: %s", endpoint->name);
    logMessage(LOG_LEVEL_DEBUG, "URL: %s", url);

    res = curl_easy_perform(curl);
    if(res != CURLE_OK) {
        logMessage(LOG_LEVEL_ERROR, "curl_easy_perform() failed: %s", 
                  curl_easy_strerror(res));
        free(chunk.memory);
        curl_easy_cleanup(curl);
        return -1;
    }

    struct json_object *response = json_tokener_parse(chunk.memory);
    if (!response) {
        logMessage(LOG_LEVEL_ERROR, "Failed to parse JSON response");
        free(chunk.memory);
        curl_easy_cleanup(curl);
        return -1;
    }

    // Process batch based on endpoint type
    // Note: Individual endpoint processing would be called here

    json_object_put(response);
    free(chunk.memory);
    curl_easy_cleanup(curl);

    return total_records;
}

int main() {
    // Check environment variables
    const char *required_env[] = {
        "REPSLY_API_USERNAME", 
        "REPSLY_API_PASSWORD",
        "MYSQL_USERNAME", 
        "MYSQL_PASSWORD"
    };
    
    const size_t num_required_env = sizeof(required_env)/sizeof(required_env[0]);
    for (size_t i = 0; i < num_required_env; i++) {
        if (!getenv(required_env[i])) {
            fprintf(stderr, "Error: Required environment variable %s not set.\n", 
                    required_env[i]);
            return 1;
        }
    }

    if (!initLogging()) {
        fprintf(stderr, "Failed to initialize logging\n");
        return 1;
    }

    MYSQL *conn = db_connect();
    if (!conn) {
        logMessage(LOG_LEVEL_FATAL, "Failed to connect to database");
        finalizeLogging();
        return 1;
    }

    if (!db_init_tracking(conn)) {
        logMessage(LOG_LEVEL_FATAL, "Failed to initialize endpoint tracking");
        db_disconnect(conn);
        finalizeLogging();
        return 1;
    }

    curl_global_init(CURL_GLOBAL_ALL);

    for (int i = 0; i < NUM_ENDPOINTS; i++) {
        if (process_endpoint(conn, &endpoints[i]) != 0) {
            logMessage(LOG_LEVEL_ERROR, "Error processing endpoint: %s", 
                      endpoints[i].name);
        }
    }

    curl_global_cleanup();
    db_disconnect(conn);
    finalizeLogging();
    return 0;
}