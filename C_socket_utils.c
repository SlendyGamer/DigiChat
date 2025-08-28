#include "C_socket_utils.h"

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

int CriarSocketTCP_IPV4()
{

    return socket(AF_INET, SOCK_STREAM, 0);
}

struct sockaddr_in* CriarEndereco_IPV4(char *ip, int porta)
{
    struct sockaddr_in *addr = malloc(sizeof(struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(porta);

    if (strlen(ip) == 0)
    {
        addr->sin_addr.s_addr = INADDR_ANY;
    }
    else
    {
        inet_pton(AF_INET, ip, &addr->sin_addr.s_addr);
    }
    
    return addr; 
}