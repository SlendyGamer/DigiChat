#include "server_utils.h"

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