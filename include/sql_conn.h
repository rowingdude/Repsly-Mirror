#ifndef SQL_CONN_H
#define SQL_CONN_H

#include <mysql/mysql.h>
#include <stdbool.h>

MYSQL* db_connect(void);
void db_disconnect(MYSQL* conn);
bool db_ensure_connection(MYSQL* conn);
bool db_init_tracking(MYSQL* conn);

#endif