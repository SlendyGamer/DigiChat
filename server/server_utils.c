#include "server_utils.h"
#include <pthread.h>

void change_client_name(const char *new_name);
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

void parse_command(const char *comm) {
    char cmd[10], arg[MAX_CLIENT_NAME];

    int n = sscanf(comm, "%9s %20[^\n]", cmd, arg);

    if(n == 1 && !strcmp(cmd, "exit")) {
        disconnect_client();
    } else if(n == 2 && !strcmp(cmd, "nome")) {
        change_client_name(arg);
    } else {
        warn_client(client->clientSFD, ERR_MSG_UNK_COMM);
    }
}

void change_client_name(const char *new_name) {
    pthread_mutex_lock(&(client->mutex));
    snprintf(client->name, MAX_CLIENT_NAME, "%s", new_name);
    pthread_mutex_unlock(&(client->mutex));
}

void disconnect_client() {
    warn_client(client->clientSFD, MSG_DC_SUCCESS);
    *client->exit_flag = 1;
}
