#ifndef ENDPOINTS_H
#define ENDPOINTS_H

#include <stdbool.h>
#include <time.h>

enum PaginationType {
    NONE,           
    ID,             
    TIMESTAMP,      
    SKIP,           
    DATERANGE       
};

struct EndpointMeta {
    long long last_id;
    char last_timestamp[64];
    char date_start[64];
    char date_end[64];
    int skip_count;
    int total_count;
    time_t last_sync;
};

struct Endpoint {
    const char *name;
    const char *key;
    const char *url_format;
    enum PaginationType pagination_type;
    const char *tracking_query;
    const char *update_query;
    struct EndpointMeta meta;
};

extern struct Endpoint endpoints[];
extern const int NUM_ENDPOINTS;


#endif