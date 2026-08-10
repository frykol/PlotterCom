#include "communication/client.h"

#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <inttypes.h>

#include "communication/message.h"
#include "communication/message_queue.h"

#define MESSAGE_MAX_CHUNK 4096
#define HEADER_MESSAGE_SIZE 8

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

void get_header_message_size(const void* buffer, size_t* buffer_size, uint64_t* header_message_size){
  if (*header_message_size != 0){
    return;
  }

  if (*buffer_size < HEADER_MESSAGE_SIZE){
    return;
  }

  memcpy(header_message_size, buffer, sizeof(uint64_t));
  *header_message_size = be64toh(*header_message_size); 
}
void save_message(client_context_t *client_ctx, int result, char *buffer,
                  size_t *buffer_size, uint64_t* header_message_size) {

  const size_t offset = HEADER_MESSAGE_SIZE;

  if(*buffer_size < HEADER_MESSAGE_SIZE){
    return;
  }
  size_t characters_len = result / sizeof(char);
  printf("Arrived: %d bytes -> %zu characters\n", result, characters_len);

  if (*buffer_size >= *header_message_size + offset) {
    printf("Data avalible, characters len:%zu\n", characters_len);
    printf("%s\n\n", buffer);


    message_t *message;
    int message_result =
        create_message(&message, (void*)buffer + offset, *buffer_size-offset);

    if (message_result != 0) {
      printf("Error creating message\n");
    }

    *buffer_size = 0;
    *header_message_size = 0;
    message_queue_add(client_ctx->client_message_queue, message);
  }
}

void *client_handle_connection(void *client_context) {
  printf("CONNECTED\n");
  client_context_t *client_ctx = (client_context_t *)client_context;

  printf("New Connection\nConnected on free id: %zu\n", client_ctx->client_id);

  struct pollfd fd;
  fd.fd = client_ctx->client_fd;
  fd.events = POLLIN;

  char test_buffer[MESSAGE_MAX_CHUNK];
  size_t buffer_size = 0;

  uint64_t header_message_size = 0;
  while (1) {
    pthread_mutex_lock(client_ctx->mutex);
    bool shutdown = *client_ctx->shutdown;
    pthread_mutex_unlock(client_ctx->mutex);
    if (shutdown) {
      break;
    }

    message_t *server_message;
    while (!message_queue_try_pop(client_ctx->server_message_queue,
                                  &server_message)) {
      const char *data = (char*)message_data(server_message);
      size_t data_len = message_size(server_message);
      send(client_ctx->client_fd, (void *)data, data_len, 0);
      free_message(server_message);
    }

    int pool_result = poll(&fd, 1, 100);
    if (pool_result == 0) {
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
      buffer_size += result;

      if (!is_client_connected(result)) {
        break;
      }

      if (result > 0) {
        get_header_message_size(test_buffer, &buffer_size,  &header_message_size);
        save_message(client_ctx, result, test_buffer, &buffer_size, &header_message_size);
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
