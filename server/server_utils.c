#include "server_utils.h"
#include <pthread.h>

void change_client_name(const char *new_name, ClientInfo* info);
void disconnect_client();

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

void parse_command(const char *comm, ClientInfo* info) {
    char cmd[10], arg[MAX_CLIENT_NAME];

    int n = sscanf(comm, "%9s %20[^\n]", cmd, arg);

    if(n == 1 && !strcmp(cmd, "exit")) {
        disconnect_client(info);
    } else if(n == 2 && !strcmp(cmd, "nome")) {
        change_client_name(arg, info);
    } else {
        warn_client(info->clientSFD, ERR_MSG_UNK_COMM);
    }
}

void change_client_name(const char *new_name, ClientInfo* info) {
    pthread_mutex_lock(&(info->mutex));
    snprintf(info->name, MAX_CLIENT_NAME, "%s", new_name);
    pthread_mutex_unlock(&(info->mutex));
}

void disconnect_client(ClientInfo* info) {
    warn_client(info->clientSFD, MSG_DC_SUCCESS);
    *info->exit_flag = 1;
}
