#include "sql_conn.h"
#include "endpoints.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

MYSQL* db_connect(void) {
    MYSQL* conn = mysql_init(NULL);
    if (conn == NULL) {
        fprintf(stderr, "mysql_init() failed\n");
        return NULL;
    }

    const char* db_user = getenv("MYSQL_USERNAME");
    const char* db_pass = getenv("MYSQL_PASSWORD");
    
    if (!db_user || !db_pass) {
        fprintf(stderr, "Database credentials not set in environment variables\n");
        mysql_close(conn);
        return NULL;
    }

    if (!mysql_real_connect(conn, "localhost", db_user, db_pass, 
                           "repsly", 0, NULL, 0)) {
        fprintf(stderr, "Connection error: %s\n", mysql_error(conn));
        mysql_close(conn);
        return NULL;
    }

    return conn;
}

void db_disconnect(MYSQL* conn) {
    if (conn) {
        mysql_close(conn);
    }
}

bool db_ensure_connection(MYSQL* conn) {
    if (!conn) return false;
    
    if (mysql_ping(conn) != 0) {
        fprintf(stderr, "Connection lost, attempting to reconnect...\n");
        if (mysql_real_connect(conn, "localhost", 
                             getenv("MYSQL_USERNAME"),
                             getenv("MYSQL_PASSWORD"),
                             "repsly", 0, NULL, 0) == NULL) {
            fprintf(stderr, "Reconnection failed: %s\n", mysql_error(conn));
            return false;
        }
        printf("Reconnection successful\n");
    }
    return true;
}

bool db_init_tracking(MYSQL* conn) {
    if (!db_ensure_connection(conn)) return false;

    // Check if table exists, if not create it
    const char* check_table = 
        "CREATE TABLE IF NOT EXISTS endpoint_tracking ("
        "endpoint_name VARCHAR(50) PRIMARY KEY,"
        "last_id BIGINT DEFAULT 0,"
        "last_timestamp VARCHAR(64) DEFAULT '0',"
        "date_start VARCHAR(64),"
        "date_end VARCHAR(64),"
        "skip_count INT DEFAULT 0,"
        "total_count INT DEFAULT 0,"
        "last_sync TIMESTAMP NULL,"
        "last_status VARCHAR(20) DEFAULT 'PENDING'"
        ")";

    if (mysql_query(conn, check_table)) {
        fprintf(stderr, "Table creation failed: %s\n", mysql_error(conn));
        return false;
    }

    // Initialize tracking for each endpoint if not exists
    const char* init_endpoint =
        "INSERT IGNORE INTO endpoint_tracking "
        "(endpoint_name, last_sync) VALUES (?, NULL)";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        fprintf(stderr, "mysql_stmt_init() failed\n");
        return false;
    }

    if (mysql_stmt_prepare(stmt, init_endpoint, strlen(init_endpoint))) {
        fprintf(stderr, "mysql_stmt_prepare() failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    
    char endpoint_name[50];
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = endpoint_name;
    bind[0].buffer_length = sizeof(endpoint_name);

    if (mysql_stmt_bind_param(stmt, bind)) {
        fprintf(stderr, "mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    extern struct Endpoint endpoints[];
    extern const int NUM_ENDPOINTS;

    for (int i = 0; i < NUM_ENDPOINTS; i++) {
        strncpy(endpoint_name, endpoints[i].name, sizeof(endpoint_name) - 1);
        if (mysql_stmt_execute(stmt)) {
            fprintf(stderr, "mysql_stmt_execute() failed for endpoint %s: %s\n",
                    endpoint_name, mysql_stmt_error(stmt));
            mysql_stmt_close(stmt);
            return false;
        }
    }

    mysql_stmt_close(stmt);
    return true;
}