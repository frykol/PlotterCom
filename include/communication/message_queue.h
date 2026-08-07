#pragma once
#include "communication/message.h"
#include <stdint.h>

typedef struct message_queue message_queue_t;

int message_queue_init(message_queue_t **msgq, size_t init_size);
int message_queue_add(message_queue_t *msgq, message_t *msg);
int message_queue_pop(message_queue_t *msgq, message_t **msg);
int message_queue_try_pop(message_queue_t *msgq, message_t **msg);
uint8_t message_queue_empty(message_queue_t *msgq);
uint8_t message_queue_full(message_queue_t *msgq);
size_t message_queue_element_count(message_queue_t *msgq);
int message_queue_free(message_queue_t *msgq);
