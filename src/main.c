#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <pthread.h>

#include "communication/message.h"
#include "communication/message_queue.h"

#define MESSAGE_MAX_CHUNK 4096
message_queue_t *test_message_queue;

int create_server(struct sockaddr_in *server_addr, int *server_socket) {
  int result = 0;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);

  uint16_t port = 8080;

  server_addr->sin_family = AF_INET;
  server_addr->sin_port = htons(port);
  server_addr->sin_addr.s_addr = htonl(INADDR_ANY);

  result =
      bind(server_fd, (struct sockaddr *)server_addr, sizeof(*server_addr));

  if (result != 0) {
    printf("Cannot bind server: %d\n", result);
    return result;
  }

  result = listen(server_fd, SOMAXCONN);
  if (result != 0) {
    printf("Listen error: %d\n", result);
    return result;
  }

  *server_socket = server_fd;
  return result;
}

void *handle_connection(void *arg) {
  int client_fd = *(int *)arg;
  message_queue_init(&test_message_queue, 10);

  char test_buffer[MESSAGE_MAX_CHUNK];
  size_t buffer_size = 0;
  while (1) {
    int result = recv(client_fd, &test_buffer[buffer_size],
                      MESSAGE_MAX_CHUNK - (buffer_size * sizeof(char)) - 1, 0);
    if (result == 0) {
      printf("Client gracefull shutdown\n");
      break;
    }
    if (result < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        printf("No data\n");
      } else {
        printf("Client error \n");
        break;
      }
    } else {
      size_t characters_len = result / sizeof(char);
      printf("Arrived: %d bytes -> %zu characters\n", result, characters_len);
      buffer_size += result / sizeof(char);
      if (test_buffer[buffer_size - 1] == '\0') {
        printf("Data avalible, characters len:%zu\n", characters_len);
        printf("%s\n", test_buffer);
        printf("\n");

        message_t *message;
        int message_result =
            create_message(&message, test_buffer, buffer_size * sizeof(char));
        if (message_result != 0) {
          printf("Error creating message\n");
        }
        printf("GETTING DATA\n");
        const char *data = message_data(message);
        printf("TEST PRINTF MESSAGE\n");
        printf("%s\n", data);
        printf("MESSAGE SIZE: %zu\n", message_size(message));
        printf("END PRINTF MESSAGE\n\n");
        memset(test_buffer, '0', MESSAGE_MAX_CHUNK);
        buffer_size = 0;
        message_queue_add(test_message_queue, message);
      }
    }
  }

  printf("QUEUE READING\n");
  printf("NUMBER OF MESSAGES: %zu\n",
         message_queue_element_count(test_message_queue));

  while (!message_queue_empty(test_message_queue)) {
    message_t *message;
    message_queue_pop(test_message_queue, &message);
    printf("%s <- number of char: %zu\n", message_data(message),
           message_size(message));
    free_message(message);
  }

  message_queue_free(test_message_queue);

  return (void *)0;
}

int main(void) {
  pthread_t client_thread;

  struct sockaddr_in server;

  int server_fd;
  int result = create_server(&server, &server_fd);

  if (result != 0) {
    return result;
  }

  struct sockaddr_in client_addr;
  int client_len = sizeof(client_addr);
  int client_sock = accept(server_fd, (struct sockaddr *)&client_addr,
                           (socklen_t *)&client_len);

  if (client_sock < 0) {
    printf("Error on accept\n");
    return -1;
  }

  int p_result = pthread_create(&client_thread, NULL, &handle_connection,
                                (void *)&client_sock);
  if (p_result != 0) {
    printf("Failed to create thread: %d\n", p_result);
    return p_result;
  }

  p_result = pthread_join(client_thread, NULL);
  if (p_result != 0) {
    printf("failed to join thread: %d\n", p_result);
  }

  close(client_sock);

  return 0;
}
