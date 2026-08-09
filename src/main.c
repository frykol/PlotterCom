#include "communication/message.h"
#include "communication/message_queue.h"
#include "communication/server.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_COMMAND_TOKENS 10

void test_shell() {
  char command_buffer[256];

  printf("----------------\n");
  printf("TEST SHELL\n");
  printf("----------------\n");

  while (fgets(command_buffer, sizeof(command_buffer), stdin) != NULL) {
    char *token = strtok(command_buffer, " ");

    command_buffer[strcspn(command_buffer, "\n")] = '\0';

    char *tokens[MAX_COMMAND_TOKENS];
    size_t token_count = 0;

    while (token != NULL && token_count < MAX_COMMAND_TOKENS) {
      tokens[token_count] = token;
      token_count++;
      token = strtok(NULL, " ");
    }

    if (token_count == 1) {
      if (!strcmp(tokens[0], "q")) {
        break;
      }
    }

    if (token_count == 2) {
      if (!strcmp(tokens[0], "GET")) {
        int test_idx = atoi(tokens[1]);
        message_queue_t *msg = get_client_message_queue(test_idx);
        if (msg == NULL) {
          printf("CLIENT: %d DOESN'T EXITSTS\n", test_idx);
          continue;
        }
        size_t num = message_queue_element_count(msg);
        printf("MESSAGES IN: %d client -> %zu\n", test_idx, num);
        printf("CLIENT MESSAGES\n");
        message_t *message;
        while (!message_queue_try_pop(msg, &message)) {
          const char *data = message_data(message);
          printf("%s\n", data);
          free_message(message);
        }
        message_queue_free(msg);
        printf("\n");
      }
    }

    if (token_count > 2) {
      if (!strcmp(tokens[0], "SEND")) {
        int test_idx = atoi(tokens[1]);
        // temporary buffer for testing sending message
        char message_buffer[256];
        size_t message_len = 0;
        for (size_t token_idx = 2; token_idx < token_count; token_idx++) {
          size_t token_len = strlen(tokens[token_idx]);
          memcpy(&message_buffer[message_len], tokens[token_idx], token_len);
          message_len += token_len;
          message_buffer[message_len++] = ' ';
        }
        message_len--;
        send_client_message(test_idx, message_buffer, message_len);
        printf("\n");
      }
    }
  }

  printf("----------------\n");
  printf("TEST SHELL EXIT\n");
  printf("----------------\n");
}

int main(void) {
  pthread_t server_thread;

  init_server();
  test_shell();
  stop_server();

  return 0;
}
