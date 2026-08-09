#pragma once
#include "communication/message_queue.h"
#include <pthread.h>
#include <stdbool.h>

#define MAX_CLIENT_NAME 255

typedef struct {
  int client_fd;
  size_t client_id;
  size_t *active_clients;
  char client_name[MAX_CLIENT_NAME];
  message_queue_t *client_message_queue;
  message_queue_t *server_message_queue;
  bool *occupied;

  pthread_mutex_t *mutex;
  pthread_cond_t *server_cond;

  pthread_mutex_t client_mutex;
  bool *shutdown;
} client_context_t;

void *client_handle_connection(void *client_context);
