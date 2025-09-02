#ifndef MESSAGE_BUFFER_H
#define MESSAGE_BUFFER_H

#include <pthread.h>

#define MAX_MSG_LEN 1024

typedef struct Message {
    int senderSFD;
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

/*
    Cria um buffer (fila) de mensagens e inicia mutex e cond
*/
MessageBuffer* buffer_init(int total_readers);

/*
    Destroi o buffer desalocando cada elemento junto da estrutura principal e retorna nulo para a referencia de quem chama
*/
MessageBuffer* buffer_destroy(MessageBuffer *buf);

/*
    Inicializa novo nó (mensagem), coloca na fila respeitando o mutex, desaloca alguma se necessario e envia um "aviso" (cond) para as threads de leitura
*/
void buffer_enqueue(MessageBuffer *buf, int senderSFD, const char *msg);

/*
    Le o proximo (em relacao ao no do endereco passado) e atualiza o cursor, respeitando o mutex e 
*/
Message* buffer_read_next(MessageBuffer *buf, Message **cursor);

#endif //MESSAGE_BUFFER_H_INCLUDED