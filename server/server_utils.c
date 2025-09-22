#include "server_utils.h"
#include <pthread.h>

void change_client_name(const char *new_name, ClientInfo* info);

ClientInfo* client;

void warn_client(int clientSFD, const char *msg) {
    if (send(clientSFD, msg, strlen(msg), 0) >= 0) {
        printf("[LOG] " LOG_MSG_NOTF_SV "\n");
    } else {
        perror("[ERR] " ERR_MSG_NOTF);
        *client->exit_flag = 1;
        pthread_exit(NULL);
    }
}

void change_client_name(const char *new_name, ClientInfo* info) {
    pthread_mutex_lock(&(info->mutex));
    snprintf(info->name, MAX_CLIENT_NAME, "%s", new_name);
    pthread_mutex_unlock(&(info->mutex));
}
