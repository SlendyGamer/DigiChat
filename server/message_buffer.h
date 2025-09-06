#ifndef MESSAGE_BUFFER_H
#define MESSAGE_BUFFER_H

#include <pthread.h>
#include "server_utils.h"

#define MAX_MSG_LEN 1024

typedef struct Message {
    int senderSFD;
    char sender_name[MAX_CLIENT_NAME];
    char time[10];
    char content[MAX_MSG_LEN];
    int remaining_reads;
    struct Message *next;
} Message;

typedef struct {
    Message *head;
    Message *tail;
    int total_readers;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} MessageBuffer;

extern MessageBuffer* buf;

/*
    Cria um buffer (fila) de mensagens e inicia mutex e cond
*/
MessageBuffer* buffer_init(int total_readers);

/*
    Destroi o buffer desalocando cada elemento junto da estrutura principal e retorna nulo para a referencia de quem chama
*/
MessageBuffer* buffer_destroy(MessageBuffer *buffer);

/*
    Inicializa novo nó (mensagem), coloca na fila respeitando o mutex, desaloca alguma se necessario e envia um "aviso" (cond) para as threads de leitura
*/
void buffer_enqueue(MessageBuffer *buffer, int senderSFD, const char *sender_name, const char *time, const char *msg);

/*
    Le o proximo (em relacao ao no do endereco passado) e atualiza o cursor, respeitando o mutex e 
*/
Message* buffer_read_next(MessageBuffer *buffer, Message **cursor);

/*
    Atualiza o total_readers no buffer para que seja possivel aumentar ou decrementar o numero de leitores de cada mensagem, respeitando o mutex
*/
void buffer_update_readers(MessageBuffer *buffer, int dif);

/*
    Retorna o final da fila para que um novo leitor possa começar apenas das mensagens novas, respeitando o mutex
*/
Message* buffer_get_tail(MessageBuffer *buffer);

#endif //MESSAGE_BUFFER_H_INCLUDED