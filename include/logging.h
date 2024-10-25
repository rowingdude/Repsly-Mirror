#ifndef LOGGING_H
#define LOGGING_H

#include <stdbool.h>
#define ERROR_MESSAGE_SIZE 1024
typedef enum {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL
} LogLevel;

bool initLogging(void);
void logMessage(LogLevel level, const char *format, ...);
void finalizeLogging(void);

#endif