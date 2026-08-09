#include "communication/message.h"
#include <string.h>

struct message {
  char *message;
  size_t size; // In bytes
};

// allocates memory; needs to be freed later
int create_message(message_t **msg, const char *buffer, size_t buffer_size) {
  if (buffer == NULL) {
    return -1;
  }

  *msg = (message_t *)malloc(sizeof(message_t));

  message_t *message = *msg;

  message->message = (char *)malloc(buffer_size);
  if (message->message == NULL) {
    return -1;
  }

  message->size = buffer_size;
  memcpy(message->message, buffer, buffer_size);
  return 0;
}

int free_message(message_t *msg) {
  if (msg == NULL) {
    return -1;
  }
  free(msg->message);
  msg->size = 0;
  free(msg);
  return 0;
}

const char *message_data(const message_t *msg) { return msg->message; }
size_t message_size(const message_t *msg) { return msg->size; }
