#include "communication/message_queue.h"
#include <pthread.h>
#include <stdio.h>

struct message_queue {
  message_t **messages;
  size_t head;
  size_t tail;
  size_t capacity;
  pthread_mutex_t mutex;
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
  pthread_mutex_init(&message_queue->mutex, NULL);
  return 0;
}

int message_queue_add(message_queue_t *msgq, message_t *msg) {
  if (msgq == NULL || msg == NULL) {
    return -1;
  }

  pthread_mutex_lock(&msgq->mutex);

  size_t next_tail = (msgq->tail + 1) % (msgq->capacity + 1);

  if (next_tail == msgq->head) {
    pthread_mutex_unlock(&msgq->mutex);
    return -1;
  }

  msgq->messages[msgq->tail] = msg;
  msgq->tail = next_tail;

  pthread_mutex_unlock(&msgq->mutex);

  return 0;
}

int message_queue_add_unsafe(message_queue_t *msgq, message_t *msg) {
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
int message_queue_pop_unsafe(message_queue_t *msgq, message_t **msg) {
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

int message_queue_try_pop(message_queue_t *msgq, message_t **msg) {
  pthread_mutex_lock(&msgq->mutex);
  int is_empty = msgq->head == msgq->tail;
  if (!is_empty) {
    *msg = msgq->messages[msgq->head];
    msgq->head = (msgq->head + 1) % (msgq->capacity + 1);
  }

  pthread_mutex_unlock(&msgq->mutex);
  return is_empty;
}

uint8_t message_queue_empty_unsafe(message_queue_t *msgq) {
  return msgq->head == msgq->tail;
}

uint8_t message_queue_full(message_queue_t *msgq) {
  size_t next_tail = (msgq->tail + 1) % (msgq->capacity + 1);

  return next_tail == msgq->head;
}

int message_queue_move(message_queue_t *dest, message_queue_t *src) {
  if (dest == src) {
    return -1;
  }
  if (dest == NULL || src == NULL) {
    return -1;
  }
  pthread_mutex_lock(&dest->mutex);
  pthread_mutex_lock(&src->mutex);
  message_t *src_message;
  while (!message_queue_empty_unsafe(src)) {
    message_queue_pop_unsafe(src, &src_message);
    message_queue_add_unsafe(dest, src_message);
  }
  pthread_mutex_unlock(&src->mutex);
  pthread_mutex_unlock(&dest->mutex);
  return 0;
}

int message_queue_move_init(message_queue_t **dest, message_queue_t *src) {
  if (src == NULL) {
    return -1;
  }
  pthread_mutex_lock(&src->mutex);
  size_t src_capacity = src->capacity;
  pthread_mutex_unlock(&src->mutex);
  int init_result = message_queue_init(dest, src_capacity);

  if (init_result != 0) {
    return -1;
  }
  int move_result = message_queue_move(*dest, src);
  if (move_result != 0) {
    message_queue_free(*dest);
  }
  return move_result;
}

size_t message_queue_element_count(message_queue_t *msgq) {
  pthread_mutex_lock(&msgq->mutex);
  size_t count;
  if (msgq->head == msgq->tail) {
    count = 0;
  } else if (msgq->head < msgq->tail) {
    count = msgq->tail - msgq->head;
  } else if (msgq->head > msgq->tail) {
    count = msgq->capacity - msgq->head + msgq->tail + 1;
  }
  pthread_mutex_unlock(&msgq->mutex);
  return count;
}

int message_queue_free(message_queue_t *msgq) {
  if (msgq == NULL) {
    return -1;
  }
  message_t *to_free;
  while (!message_queue_try_pop(msgq, &to_free)) {
    free_message(to_free);
  }
  pthread_mutex_lock(&msgq->mutex);
  free(msgq->messages);
  msgq->head = 0;
  msgq->capacity = 0;
  msgq->tail = 0;
  pthread_mutex_unlock(&msgq->mutex);
  pthread_mutex_destroy(&msgq->mutex);
  free(msgq);
  return 0;
}
