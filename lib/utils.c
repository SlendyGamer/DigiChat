#include "common.h"

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