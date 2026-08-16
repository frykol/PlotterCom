#include "communication/client.h"

#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "communication/message.h"
#include "communication/message_queue.h"

#define MESSAGE_MAX_CHUNK 4096
#define HEADER_MESSAGE_SIZE 8

typedef struct {
  void *buffer;
  size_t *buffer_size;
  int payload_size;
  uint64_t *header_message_size;
} payload_context_t;

bool is_client_connected(int result) {
  if (result == 0) {
    printf("Client gracefull shutdown\n");
    return false;
  }
  if (result < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      printf("No data\n");
      return true;
    } else {
      printf("Client error \n");
      return false;
    }
  }
  return true;
}

void send_messages(client_context_t *client_ctx) {
  message_t *server_message;
  while (!message_queue_try_pop(client_ctx->server_message_queue,
                                &server_message)) {
    const char *data = (char *)message_data(server_message);
    size_t data_len = message_size(server_message);
    int send_result = 0;
    while (send_result != data_len) {
      // TODO: Make counter of failed send messages; For example 10 drops the
      // connection
      int send_result_chunk =
          send(client_ctx->client_fd, (void *)&data[send_result],
               data_len - send_result, 0);
      if (send_result_chunk == -1) {
        printf("Error on send from client %zu\n", client_ctx->client_id);
        continue;
      }
      send_result += send_result_chunk;
      if (send_result != data_len) {
        printf("Client [%zu] bytes send mismatch; should send %zu, send: %d\n",
               client_ctx->client_id, data_len, send_result);
      }
    }
    free_message(server_message);
  }
}

void get_header_message_size(payload_context_t *payload_context) {
  if (*payload_context->header_message_size != 0) {
    return;
  }

  if (*payload_context->buffer_size < HEADER_MESSAGE_SIZE) {
    return;
  }

  memcpy(payload_context->header_message_size, payload_context->buffer,
         sizeof(uint64_t));
  *payload_context->header_message_size =
      be64toh(*payload_context->header_message_size);
}

void save_message(client_context_t *client_ctx,
                  payload_context_t *payload_context, bool *try_parse_message) {

  const size_t offset = HEADER_MESSAGE_SIZE;

  if (*payload_context->buffer_size < HEADER_MESSAGE_SIZE) {
    return;
  }
  size_t characters_len = payload_context->payload_size / sizeof(char);
  printf("Arrived: %d bytes -> %zu characters\n", payload_context->payload_size,
         characters_len);

  if (*payload_context->buffer_size <
      *payload_context->header_message_size + offset) {
    return;
  }

  printf("Data avalible, characters len:%zu\n", characters_len);
  // TODO: Delete this; printf only for testing in specific enviroment
  printf("%.*s\n\n", payload_context->buffer_size,
         (char *)payload_context->buffer);

  message_t *message;
  int message_result =
      create_message(&message, (char *)payload_context->buffer + offset,
                     *payload_context->header_message_size);

  if (message_result != 0) {
    printf("Error creating message\n");
    return;
  }

  size_t remaining_len = *payload_context->buffer_size -
                         (*payload_context->header_message_size + offset);

  if (remaining_len > 0) {
    memmove((char *)payload_context->buffer,
            (char *)&payload_context
                ->buffer[*payload_context->header_message_size + offset],
            remaining_len);
    *payload_context->buffer_size = remaining_len;
    *try_parse_message = true;
  } else {
    *payload_context->buffer_size = 0;
  }

  *payload_context->header_message_size = 0;
  int message_queue_result =
      message_queue_add(client_ctx->client_message_queue, message);
  if (message_queue_result == -1) {
    // TODO: Send client message warning about full server message queue
    free_message(message);
  }
}

// TODO: Make system that warns client about error's occcured
void *client_handle_connection(void *client_context) {
  client_context_t *client_ctx = (client_context_t *)client_context;

  printf("New Connection\nConnected on free id: %zu\n", client_ctx->client_id);

  struct pollfd fd;
  fd.fd = client_ctx->client_fd;
  fd.events = POLLIN;

  char test_buffer[MESSAGE_MAX_CHUNK];
  size_t buffer_size = 0;

  uint64_t header_message_size = 0;
  bool try_parse_message = true;
  while (1) {
    pthread_mutex_lock(client_ctx->mutex);
    bool shutdown = *client_ctx->shutdown;
    pthread_mutex_unlock(client_ctx->mutex);
    if (shutdown) {
      break;
    }

    send_messages(client_ctx);

    // TODO: Check errno.
    int pool_result = poll(&fd, 1, 100);
    if (pool_result == 0) {
      header_message_size = 0;
      continue;
    }

    if (fd.revents & POLLERR) {
      printf("Connection error\n");
      break;
    }
    if (fd.revents & POLLIN) {
      int result =
          recv(client_ctx->client_fd, &test_buffer[buffer_size],
               MESSAGE_MAX_CHUNK - (buffer_size * sizeof(char)) - 1, 0);

      if (result > 0) {
        buffer_size += result;
      }

      payload_context_t payload_context;
      payload_context.buffer = test_buffer;
      payload_context.buffer_size = &buffer_size;
      payload_context.payload_size = result;
      payload_context.header_message_size = &header_message_size;

      if (!is_client_connected(result)) {
        break;
      }

      if (result > 0) {
        try_parse_message = true;
        while (try_parse_message) {
          try_parse_message = false;
          get_header_message_size(&payload_context);

          if (header_message_size + HEADER_MESSAGE_SIZE > MESSAGE_MAX_CHUNK) {
            printf("[Client %zu]: Header to large: %" PRIu64 ", max: %d\n",
                   client_ctx->client_id, header_message_size,
                   MESSAGE_MAX_CHUNK);
            // TODO: Send message to client warning about ignored message
            break;
          }
          save_message(client_ctx, &payload_context, &try_parse_message);
        }
      }
    }
    if (fd.revents & (POLLHUP)) {
      printf("Connection was shutdown by client\n");
      break;
    }
  }
  close(client_ctx->client_fd);
  printf("Client of id: %zu disconnected\n", client_ctx->client_id);
  pthread_mutex_lock(client_ctx->mutex);

  *client_ctx->occupied = false;
  (*client_ctx->active_clients)--;
  pthread_cond_signal(client_ctx->server_cond);

  pthread_mutex_t *client_mutex = &client_ctx->client_mutex;

  pthread_mutex_lock(client_mutex);

  pthread_mutex_unlock(client_ctx->mutex);
  message_queue_free(client_ctx->client_message_queue);
  message_queue_free(client_ctx->server_message_queue);

  pthread_mutex_unlock(client_mutex);
  pthread_mutex_destroy(&client_ctx->client_mutex);
  free(client_context);
  return (void *)0;
}
