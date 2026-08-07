#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "communication/message_queue.h"
#include "communication/server.h"

void test_shell(){
  char command_buffer[256];

  printf("----------------\n");
  printf("TEST SHELL\n");
  printf("----------------\n");

  while (fgets(command_buffer, sizeof(command_buffer), stdin) != NULL){
    printf("%s\n", command_buffer);
    if(strlen(command_buffer) == 2 && command_buffer[0] == 'q'){
      break;
    }
    if(strlen(command_buffer) >= 2){
      int test_idx = atoi(command_buffer);
      message_queue_t* msg = get_client_message_queue(test_idx); 
      if(msg == NULL){
        printf("CLIENT: %d DOESN'T EXITSTS\n", test_idx);
        continue;
      }
      size_t num = message_queue_element_count(msg);
      printf("MESSAGES IN: %d client -> %zu\n", test_idx, num);
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
