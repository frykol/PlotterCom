#pragma once
#include <communication/message_queue.h>

typedef struct server_context server_context_t;

int init_server();
int stop_server();
message_queue_t* get_client_message_queue(int client_id);
