#ifndef C_SOCKET_UTILS_H_INCLUDED
#define C_SOCKET_UTILS_H_INCLUDED

#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <malloc.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>

struct ClientSocket
{
    int conexaoSFD;
    struct sockaddr_in conexaoAddr;
    int erro;
    bool respostaServer;
};

struct sockaddr_in* CriarEndereco_IPV4(char *ip, int port);

int CriarSocketTCP_IPV4();

struct ClientSocket *AnalisarConexao(int serverSFD);


#endif //C_SOCKET_UTILS_H_INCLUDED