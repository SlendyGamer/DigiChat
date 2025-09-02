#include "server_utils.h"
#include <pthread.h>

struct ClientSocket *AnalisarConexao(int serverSFD)
{
    struct sockaddr_in clienteAddr;
    socklen_t clienteAddrSize = sizeof(clienteAddr);
    int clienteSFD = accept(serverSFD, (struct sockaddr*)&clienteAddr, &clienteAddrSize);

    struct ClientSocket* socket = malloc(sizeof(struct ClientSocket));
    socket->conexaoAddr = clienteAddr;
    socket->conexaoSFD = clienteSFD;
    socket->respostaServer = clienteSFD>0;

    if(!socket->respostaServer)
    {
        socket->erro = clienteSFD;
    }

    return socket;
}

void warn_client(int clientSFD, const char *msg) {
    if (send(clientSFD, msg, strlen(msg), 0) >= 0) {
        printf("[LOG] " LOG_MSG_NOTF_SV "\n");
    } else {
        perror("[ERR] " ERR_MSG_NOTF);
        pthread_exit(NULL);
    }
}