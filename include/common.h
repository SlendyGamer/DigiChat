#ifndef COMMON_H_INCLUDED
#define COMMON_H_INCLUDED

#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <malloc.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>

#define MAX_MSG_LEN 1024
#define MAX_ACTIVE_CLIENTS 5

#define ERR_MSG_CREATE_SOCKET "Erro ao criar o socket"
#define ERR_MSG_ADDRESS_ASSOCIATION "Erro ao associar socket e ipv4"
#define ERR_MSG_ACCEPT_CONN "Erro ao aceitar conexao"
#define ERR_MSG_READ_IN "Erro ao ler mensagem da entrada"
#define ERR_MSG_PROCESS_MSG_CLT "Nao foi possivel processar a mensagem enviada. Por favor, tente de novo."
#define ERR_MSG_BUF_OVERFLOW_CLT "Sua mensagem deve se limitar a 1023 caracteres!"
#define ERR_MSG_SEND "Erro ao enviar mensagem"
#define ERR_MSG_NOTF "Erro ao enviar notificacao"
#define ERR_MSG_UNK_COMM "Comando desconhecido!"

#define LOG_MSG_SERVER_STARTED "Servidor iniciado com sucesso"
#define LOG_MSG_NOTF_SV "Cliente notificado!"
#define LOG_MSG_FORCE_QUIT_SV "Cliente desconectou forcosamente ou caiu!"
#define LOG_MSG_DC_SV "Cliente se desconectou!"
#define LOG_MSG_NEW_CONN_SV "Cliente se conectou!"

#define MSG_DC_SUCCESS "Voce foi desconectado com sucesso!"

struct sockaddr_in* CriarEndereco_IPV4(char *ip, int port);

int CriarSocketTCP_IPV4();

int socket_has_data_to_read(int sockFD);

ssize_t secure_send(int sockfd, const void *buf, size_t len);

#endif //COMMON_H_INCLUDED