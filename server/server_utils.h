#ifndef SERVER_UTILS_H_INCLUDED
#define SERVER_UTILS_H_INCLUDED

#include "common.h"
#include <pthread.h>

#define MAX_CLIENT_NAME 20

typedef struct ClientInfo {
    pthread_t receiver_thread;
    pthread_t sender_thread;
    int clientSFD;
    char name[MAX_CLIENT_NAME];
    int *exit_flag;
    pthread_mutex_t mutex;
} ClientInfo;

extern ClientInfo* client;

void change_client_name(const char *new_name, ClientInfo* info);
void warn_client(int clientSFD, const char *msg);

#endif //SERVER_UTILS_H_INCLUDED