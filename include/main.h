#ifndef MAIN_H
#define MAIN_H

#include <curl/curl.h>
#include <json-c/json.h>
#include <mysql/mysql.h>
#include <stdbool.h>

#define MAX_URL_LENGTH 256
#define MAX_BUFFER 16384

struct MemoryStruct {
    char *memory;
    size_t size;
};

size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp);
int process_endpoint(MYSQL *conn, const struct Endpoint *endpoint);

#endif