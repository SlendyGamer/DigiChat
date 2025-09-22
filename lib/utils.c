#include "common.h"
#include <sys/ioctl.h>
#include <errno.h>

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

int socket_has_data_to_read(int sockFD) {
    int count;
    if (ioctl(sockFD, FIONREAD, &count) < 0) {
        perror("[ERR] Erro ao verificar existencia de dados na fila de recepcao do socket");
        return -1;
    }
    return count > 0;
}

ssize_t secure_send(int sockfd, const void *buf, size_t len) {
    size_t total_sent = 0;
    const char *ptr = (const char *)buf;

    while (total_sent < len) {
        ssize_t n = send(sockfd, ptr + total_sent, len - total_sent, 0);

        if (n < 0) {
            fprintf(stderr, "[ERR] Erro ao enviar mensagem - ");
            switch(errno) {
                case EINTR:
                    // chamada interrompida por sinal, tentar de novo
                    continue;
                case EAGAIN:
                    // socket non-blocking sem espaço: tentar de novo
                    continue;
                case EPIPE:
                case ECONNRESET:
                    fprintf(stderr, "\t\tConexao fechada pelo peer\n");
                    return -1;
                case EBADF:
                case ENOTSOCK:
                    fprintf(stderr, "\t\tSocket invalido\n");
                    return -1;
                case EFAULT:
                    fprintf(stderr, "\t\tBuffer invalido\n");
                    return -1;
                case EINVAL:
                    fprintf(stderr, "\t\tFlags invalidas em send()\n");
                    return -1;
                case ENOMEM:
                case ENOBUFS:
                    fprintf(stderr, "\t\tRecursos insuficientes no sistema para enviar dados\n");
                    return -1;
                default:
                    fprintf(stderr, "\t\tErro desconhecido em send(): %s\n", strerror(errno));
                    return -1;
            }
        }

        total_sent += n;
    }

    return total_sent;
}