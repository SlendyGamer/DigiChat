#include "message_buffer.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

void buffer_clean(MessageBuffer *buf);

/*
    Cria um buffer (fila) de mensagens e inicia mutex e cond
*/
MessageBuffer* buffer_init(int total_readers) {
    MessageBuffer *buf = (MessageBuffer *)malloc(sizeof(MessageBuffer));
    if(!buf) return NULL;

    buf->head = NULL;
    buf->tail = NULL;
    buf->total_readers = total_readers;
    pthread_mutex_init(&buf->mutex, NULL);
    pthread_cond_init(&buf->cond, NULL);

    return buf;
}

/*
    Destroi o buffer desalocando cada elemento junto da estrutura principal e retorna nulo para a referencia de quem chama
*/
MessageBuffer* buffer_destroy(MessageBuffer *buf) {
    if (!buf) return NULL;

    Message *aux = buf->head;
    while(aux) {
        Message *tmp = aux->next;
        free(aux);
        aux = tmp;
    }

    pthread_mutex_destroy(&buf->mutex);
    pthread_cond_destroy(&buf->cond);

    free(buf);
    return NULL;
}

/*
    Inicializa novo nó (mensagem), coloca na fila respeitando o mutex, desaloca alguma se necessario e envia um "aviso" (cond) para as threads de leitura
*/
void buffer_enqueue(MessageBuffer *buf, int senderSFD, const char *msg) {
    if(!buf || !msg) return;

    Message *new_msg = (Message *)malloc(sizeof(Message));
    if(!new_msg) return;

    new_msg->senderSFD = senderSFD;
    strncpy(new_msg->content, msg, MAX_MSG_LEN);
    new_msg->content[MAX_MSG_LEN - 1] = '\0';
    new_msg->remaining_reads = buf->total_readers;
    new_msg->next = NULL;

    pthread_mutex_lock(&(buf->mutex));

    if(buf->tail) {
        buf->tail->next = new_msg;
        buf->tail = new_msg;
    } else {
        buf->head = buf->tail = new_msg;
    }

    buffer_clean(buf);

    pthread_cond_broadcast(&(buf->cond));
    pthread_mutex_unlock(&(buf->mutex));
}

/*
    Le o proximo (em relacao ao no do endereco passado) e atualiza o cursor, respeitando o mutex e 
*/
Message* buffer_read_next(MessageBuffer *buf, Message **cursor) {
    if(!buf || !cursor) return NULL;

    pthread_mutex_lock(&(buf->mutex));

    // enquanto a fila nao tiver um elemento ou ainda nao existir um proximo para ser lido
    while(*cursor == NULL ? buf->head == NULL : (*cursor)->next == NULL) {
        pthread_cond_wait(&buf->cond, &buf->mutex);
    }

    Message *next_msg;

    // leitura do primeiro da fila X demais leituras
    if(*cursor == NULL) {
        next_msg = buf->head;
    } else {
        next_msg = (*cursor)->next;
    }

    (next_msg->remaining_reads)--;

    *cursor = next_msg;

    pthread_mutex_unlock(&(buf->mutex));

    return next_msg;
}

/*
    Retira do buffer (desaloca) mensagens que ja foram acessadas por todos e cuja proxima tambem (para nao perder a referencia dos cursores)
    Deve ser chamado quando uma nova mensagem for inserida (menos ciclos que quando é lida)
    Funcao privada (nao disponivel no header)
    Nao manipula mutex, deve ser usada como acao dentro de mutex ja travado
*/
void buffer_clean(MessageBuffer *buf) {
    if(!(buf) || !(buf->head)) return;

    while(buf->head->remaining_reads <= 0 && buf->head->next && buf->head->next->remaining_reads <= 0) {
        Message *tmp = buf->head;
        buf->head = tmp->next;
        free(tmp);
    }
}

// TODO: rotina para analisar possivel valores persistentes em remaining_reads