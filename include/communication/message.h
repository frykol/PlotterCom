#pragma once
#include <stdlib.h>

typedef struct message message_t;

int create_message(message_t **msg, const char *buffer, size_t buffer_size);
int free_message(message_t *msg);

const char *message_data(const message_t *msg);
size_t message_size(const message_t *msg);
