#include "communication/server.h"

#include <arpa/inet.h>
#include <bits/pthreadtypes.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>

#include "communication/message_queue.h"
#include "communication/client.h"

#define MESSAGE_MAX_CHUNK 4096
#define MESSAGE_QUEUE_SIZE 25
#define MAX_CONNECTIONS 10

struct server_context{
  int server_socket;
  pthread_t server_thread;
  pthread_mutex_t server_mutex;
  pthread_cond_t server_cond;

  bool server_clients_occupied[MAX_CONNECTIONS];
  size_t clients_id[MAX_CONNECTIONS];

  client_context_t* clients_contexts[MAX_CONNECTIONS];

  size_t active_clients;
  size_t client_counter;
  bool started;
  bool shutdown;
};

server_context_t server_ctx = {
  .server_mutex = PTHREAD_MUTEX_INITIALIZER,
  .server_cond = PTHREAD_COND_INITIALIZER,
  .client_counter = 0,
  .started = false,
  .shutdown = false
};

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


void* server_listen(void* arg){
  server_context_t* ctx = (server_context_t*)arg;

  while(1){
    pthread_mutex_lock(&ctx->server_mutex);
    size_t current_clients = ctx->active_clients;
    bool shutdown = ctx->shutdown;
    pthread_mutex_unlock(&ctx->server_mutex);
    if(shutdown){
      break;
    }
    if(current_clients == MAX_CONNECTIONS){
      pthread_cond_wait(&ctx->server_cond, &ctx->server_mutex);
    }
    struct sockaddr_in client_addr;
    int client_len = sizeof(client_addr);
    int client_sock = accept(ctx->server_socket, (struct sockaddr *)&client_addr,
                            (socklen_t *)&client_len);

    if (client_sock < 0) {
      printf("Error on accept\n");
      continue;
    }

    size_t client_number = 0;
    int free_index = -1;
    pthread_mutex_lock(&ctx->server_mutex);
      for(int i = 0; i < MAX_CONNECTIONS; i++){
        if(ctx->server_clients_occupied[i] == false){
          free_index = i;
          ctx->server_clients_occupied[i] = true;
          break;
        }
      }
    pthread_mutex_unlock(&ctx->server_mutex);

    if(free_index == -1){
      close(client_sock);
      continue;
    }

    ctx->clients_id[free_index] = ctx->client_counter;

    message_queue_t* client_message_queue;
    message_queue_init(&client_message_queue, MESSAGE_QUEUE_SIZE);
    client_context_t* client_ctx = (client_context_t*)malloc(sizeof(client_context_t));
    client_ctx->mutex = &ctx->server_mutex;
    client_ctx->occupied = &ctx->server_clients_occupied[free_index];
    client_ctx->client_id = ctx->client_counter++;
    client_ctx->active_clients = &ctx->active_clients;
    client_ctx->client_fd = client_sock;
    client_ctx->message_queue = client_message_queue;
    client_ctx->server_cond = &ctx->server_cond;
    client_ctx->shutdown = &ctx->shutdown;
    pthread_mutex_init(&client_ctx->client_mutex, NULL);
    
    pthread_t client_thread;
    int client_thread_res = pthread_create(&client_thread,
                                          NULL, 
                                          &client_handle_connection,
                                          (void*)client_ctx);
    if(client_thread_res != 0){
      printf("Failed to create client\n");

      pthread_mutex_lock(&ctx->server_mutex);
      ctx->server_clients_occupied[free_index] = false;
      pthread_mutex_unlock(&ctx->server_mutex);

      close(client_sock);
      message_queue_free(client_message_queue);
      pthread_mutex_destroy(&client_ctx->client_mutex);
      free(client_ctx);
      continue;
    }
    pthread_mutex_lock(&ctx->server_mutex);
    ctx->active_clients++;
    pthread_mutex_unlock(&ctx->server_mutex);
    ctx->clients_contexts[free_index] = client_ctx;

    pthread_detach(client_thread);
  }
  printf("SHUTING DOWN\n");
  pthread_mutex_lock(&ctx->server_mutex);

  while (ctx->active_clients > 0){
    pthread_cond_wait(&ctx->server_cond, &ctx->server_mutex);
  }

  pthread_mutex_unlock(&ctx->server_mutex);


  return (void*)0;
}

int init_server(){
  struct sockaddr_in server_addr;

  int server_fd = 0; 
  int server_result = create_server(&server_addr, &server_fd);
  
  server_ctx.server_socket = server_fd;

  int server_thread_res = pthread_create(&server_ctx.server_thread, NULL, &server_listen, (void*)&server_ctx);

  if (server_thread_res != 0) {
    printf("Failed to create thread: %d\n", server_thread_res);
    return server_thread_res;
  }

  server_ctx.started = true;

  return 0;
}

int stop_server(){
  pthread_mutex_lock(&server_ctx.server_mutex);
  server_ctx.shutdown = true;
  pthread_mutex_unlock(&server_ctx.server_mutex);
  shutdown(server_ctx.server_socket, SHUT_RDWR);
  close(server_ctx.server_socket);

  int server_thread_res = pthread_join(server_ctx.server_thread, NULL);
  if (server_thread_res != 0) {
    printf("failed to join thread: %d\n", server_thread_res);
    return server_thread_res;
  }

  pthread_mutex_destroy(&server_ctx.server_mutex);

  printf("Server sucessfull shutdown");
  return 0;
}

message_queue_t* get_client_message_queue(int client_id){
  if (!server_ctx.started) {
    return NULL;
  }
  int target_client_id = -1;
  for(int i = 0; i < MAX_CONNECTIONS; i++){
    if (server_ctx.clients_id[i] == client_id){
      target_client_id = i;
      break;
    }
  }
  if (target_client_id == -1){
    return NULL;
  }
  if (server_ctx.server_clients_occupied[target_client_id] == false){
    return NULL;
  }
  return server_ctx.clients_contexts[client_id]->message_queue;
}
