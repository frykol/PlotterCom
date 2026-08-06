#include "communication/message_queue.h"

struct message_queue {
  message_t **messages;
  size_t head;
  size_t tail;
  size_t capacity;
};

int message_queue_init(message_queue_t **msgq, size_t init_size) {
  *msgq = (message_queue_t *)malloc(sizeof(message_queue_t));

  message_queue_t *message_queue = *msgq;

  message_queue->messages =
      (message_t **)malloc((init_size + 1) * sizeof(*message_queue->messages));
  if (message_queue->messages == NULL) {
    return -1;
  }
  message_queue->capacity = init_size;
  message_queue->head = 0;
  message_queue->tail = 0;
  return 0;
}

int message_queue_add(message_queue_t *msgq, message_t *msg) {
  if (msgq == NULL || msg == NULL) {
    return -1;
  }

  size_t next_tail = (msgq->tail + 1) % (msgq->capacity + 1);

  if (next_tail == msgq->head) {
    return -1;
  }

  msgq->messages[msgq->tail] = msg;
  msgq->tail = next_tail;
  return 0;
}

/* after reading message, user must dealocate it */
int message_queue_pop(message_queue_t *msgq, message_t **msg) {
  if (msgq == NULL || msg == NULL) {
    return -1;
  }
  if (msgq->head == msgq->tail) {
    return -1;
  }

  *msg = msgq->messages[msgq->head];
  msgq->head = (msgq->head + 1) % (msgq->capacity + 1);
  return 0;
}

uint8_t message_queue_empty(message_queue_t *msgq) {
  return msgq->head == msgq->tail;
}

uint8_t message_queue_full(message_queue_t *msgq) {
  size_t next_tail = (msgq->tail + 1) % (msgq->capacity + 1);

  return next_tail == msgq->head;
}

size_t message_queue_element_count(message_queue_t *msgq) {
  if (msgq->head == msgq->tail) {
    return 0;
  }
  if (msgq->head < msgq->tail) {
    return msgq->tail - msgq->head;
  }
  return msgq->capacity - msgq->head + msgq->tail;
}

int message_queue_free(message_queue_t *msgq) {
  if (msgq == NULL) {
    return -1;
  }
  while (!message_queue_empty(msgq)) {
    message_t *to_free;
    message_queue_pop(msgq, &to_free);
    free_message(to_free);
  }
  free(msgq->messages);
  msgq->head = 0;
  msgq->capacity = 0;
  msgq->tail = 0;
  free(msgq);
  return 0;
}
