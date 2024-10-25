CC = gcc
CFLAGS = -Wall -Wextra -Werror -Wpedantic -Wformat=2 -Wformat-security \
         -Wnull-dereference -Wstack-protector -fstack-protector-strong \
         -I/usr/include/mysql -Iinclude
LDFLAGS = -L/usr/lib/mysql -lmysqlclient -lcurl -ljson-c

SRC_DIR = src
ENDPT_DIR = src/endpt_modules
LOG_DIR = src/logging
INC_DIR = include
OBJ_DIR = obj
BIN_DIR = bin

# Source files
SRCS = $(wildcard $(SRC_DIR)/*.c)
ENDPT_SRCS = $(wildcard $(ENDPT_DIR)/*.c)
LOG_SRCS = $(wildcard $(LOG_DIR)/*.c)

# Object files
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o) \
       $(ENDPT_SRCS:$(ENDPT_DIR)/%.c=$(OBJ_DIR)/endpt_%.o) \
       $(LOG_SRCS:$(LOG_DIR)/%.c=$(OBJ_DIR)/log_%.o)

# Main target
TARGET = $(BIN_DIR)/repsly_etl

.PHONY: all clean directories

all: directories $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

# Rule for compiling source files in SRC_DIR
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# Rule for compiling source files in ENDPT_DIR
$(OBJ_DIR)/endpt_%.o: $(ENDPT_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# Rule for compiling source files in LOG_DIR
$(OBJ_DIR)/log_%.o: $(LOG_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

directories:
	@mkdir -p $(OBJ_DIR) $(BIN_DIR)

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)

-include $(OBJS:.o=.d)

# Dependency generation for SRC_DIR
$(OBJ_DIR)/%.d: $(SRC_DIR)/%.c
	@set -e; rm -f $@; \
	$(CC) -MM -I$(INC_DIR) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,$(OBJ_DIR)/\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

# Dependency generation for ENDPT_DIR
$(OBJ_DIR)/endpt_%.d: $(ENDPT_DIR)/%.c
	@set -e; rm -f $@; \
	$(CC) -MM -I$(INC_DIR) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,$(OBJ_DIR)/endpt_\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

# Dependency generation for LOG_DIR
$(OBJ_DIR)/log_%.d: $(LOG_DIR)/%.c
	@set -e; rm -f $@; \
	$(CC) -MM -I$(INC_DIR) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,$(OBJ_DIR)/log_\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

.PARALLEL:
MAKEFLAGS += -j8
