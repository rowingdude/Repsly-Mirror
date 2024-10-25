#include "logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>
#include <string.h>

static FILE *logFile = NULL;
static LogLevel logLevel = LOG_LEVEL_INFO;

bool initLogging(void) {
    char logFileName[256];
    time_t currentTime;
    struct tm *localTime;

    time(&currentTime);
    localTime = localtime(&currentTime);

    snprintf(logFileName, sizeof(logFileName), "/tmp/Repsly_ETL_%04d-%02d-%02d_%02d-%02d-%02d.log",
             localTime->tm_year + 1900, localTime->tm_mon + 1, localTime->tm_mday,
             localTime->tm_hour, localTime->tm_min, localTime->tm_sec);

    logFile = fopen(logFileName, "w");
    if (logFile == NULL) {
        fprintf(stderr, "Error: Failed to open log file '%s'\n", logFileName);
        return false;
    }

    fprintf(logFile, "Repsly ETL Logging started at %04d-%02d-%02d %02d:%02d:%02d\n",
            localTime->tm_year + 1900, localTime->tm_mon + 1, localTime->tm_mday,
            localTime->tm_hour, localTime->tm_min, localTime->tm_sec);
    fflush(logFile);

    return true;
}

void logMessage(LogLevel level, const char *format, ...) {
    if (level < logLevel)
        return;

    time_t currentTime;
    struct tm *localTime;
    char timeStr[20];
    va_list args;

    time(&currentTime);
    localTime = localtime(&currentTime);
    strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S", localTime);

    fprintf(logFile, "[%s] [", timeStr);

    switch (level) {
        case LOG_LEVEL_DEBUG:
            fprintf(logFile, "DEBUG");
            break;
        case LOG_LEVEL_INFO:
            fprintf(logFile, "INFO");
            break;
        case LOG_LEVEL_WARN:
            fprintf(logFile, "WARN");
            break;
        case LOG_LEVEL_ERROR:
            fprintf(logFile, "ERROR");
            break;
        case LOG_LEVEL_FATAL:
            fprintf(logFile, "FATAL");
            break;
    }

    fprintf(logFile, "] ");

    va_start(args, format);
    vfprintf(logFile, format, args);
    va_end(args);

    fprintf(logFile, "\n");
    fflush(logFile);
}

void finalizeLogging(void) {
    if (logFile) {
        fprintf(logFile, "Repsly ETL Logging finished.\n");
        fclose(logFile);
    }
}