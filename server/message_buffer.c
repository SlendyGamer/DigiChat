#include "message_buffer.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

void buffer_clean(MessageBuffer *buffer);

MessageBuffer* buf;

/*
    Cria um buffer (fila) de mensagens e inicia mutex e cond
*/
MessageBuffer* buffer_init(int total_readers) {
    MessageBuffer *buffer = (MessageBuffer *)malloc(sizeof(MessageBuffer));
    if(!buffer) return NULL;

    buffer->head = NULL;
    buffer->tail = NULL;
    buffer->total_readers = total_readers;
    pthread_mutex_init(&buffer->mutex, NULL);
    pthread_cond_init(&buffer->cond, NULL);
    
    return buffer;
}

/*
    Destroi o buffer desalocando cada elemento junto da estrutura principal e retorna nulo para a referencia de quem chama
*/
MessageBuffer* buffer_destroy(MessageBuffer *buffer) {
    if (!buffer) return NULL;

    Message *aux = buffer->head;
    while(aux) {
        Message *tmp = aux->next;
        free(aux);
        aux = tmp;
    }

    pthread_mutex_destroy(&buffer->mutex);
    pthread_cond_destroy(&buffer->cond);

    free(buffer);
    return NULL;
}

/*
    Inicializa novo nó (mensagem), coloca na fila respeitando o mutex, desaloca alguma se necessario e envia um "aviso" (cond) para as threads de leitura
*/
void buffer_enqueue(MessageBuffer *buffer, int senderSFD, const char *sender_name, const char *time, const char *msg) {
    if(!buffer || !msg) return;

    Message *new_msg = (Message *)malloc(sizeof(Message));
    if(!new_msg) return;

    new_msg->senderSFD = senderSFD;
    strncpy(new_msg->sender_name, sender_name, MAX_CLIENT_NAME - 1);
    strncpy(new_msg->content, msg, MAX_MSG_LEN - 1);
    strncpy(new_msg->time, time, 9);
    new_msg->content[MAX_MSG_LEN - 1] = '\0';
    new_msg->remaining_reads = buffer->total_readers;
    new_msg->next = NULL;

    pthread_mutex_lock(&(buffer->mutex));

    if(buffer->tail) {
        buffer->tail->next = new_msg;
        buffer->tail = new_msg;
    } else {
        buffer->head = buffer->tail = new_msg;
    }

    buffer_clean(buffer);

    pthread_cond_broadcast(&(buffer->cond));
    pthread_mutex_unlock(&(buffer->mutex));
}

/*
    Le o proximo (em relacao ao no do endereco passado) e atualiza o cursor, respeitando o mutex e o estado da fila
*/
Message* buffer_read_next(MessageBuffer *buffer, Message **cursor) {
    if(!buffer || !cursor) return NULL;

    pthread_mutex_lock(&(buffer->mutex));
    
    // enquanto a fila nao tiver um elemento ou ainda nao existir um proximo para ser lido
    while(*cursor == NULL ? buffer->head == NULL : (*cursor)->next == NULL) {
        pthread_cond_wait(&buffer->cond, &buffer->mutex);
    }
    
    Message *next_msg;

    // leitura do primeiro da fila X demais leituras
    if(*cursor == NULL) {
        next_msg = buffer->head;
    } else {
        next_msg = (*cursor)->next;
    }
    
    (next_msg->remaining_reads)--;

    *cursor = next_msg;
    
    pthread_mutex_unlock(&(buffer->mutex));
    
    return next_msg;
}

/*
    Retira do buffer (desaloca) mensagens que ja foram acessadas por todos e cuja proxima tambem (para nao perder a referencia dos cursores)
    Deve ser chamado quando uma nova mensagem for inserida (menos ciclos que quando é lida)
    Funcao privada (nao disponivel no header)
    Nao manipula mutex, deve ser usada como acao dentro de mutex ja travado
*/
void buffer_clean(MessageBuffer *buffer) {
    if(!(buffer) || !(buf->head)) return;

    while(buffer->head->remaining_reads <= 0 && buffer->head->next && buffer->head->next->remaining_reads <= 0) {
        Message *tmp = buffer->head;
        buffer->head = tmp->next;
        free(tmp);
    }
}

/*
    Atualiza o total_readers no buffer para que seja possivel aumentar ou decrementar o numero de leitores de cada mensagem, respeitando o mutex
*/
void buffer_update_readers(MessageBuffer *buffer, int dif) {
    if(!buffer) return;

    pthread_mutex_lock(&(buffer->mutex));
    buffer->total_readers += dif;
    pthread_mutex_unlock(&(buffer->mutex));
}

/*
    Retorna o final da fila para que um novo leitor possa começar apenas das mensagens novas, respeitando o mutex
*/
Message* buffer_get_tail(MessageBuffer *buffer) {
    if(!buffer) return NULL;
    
    pthread_mutex_lock(&(buffer->mutex));
    Message *tail = buffer->tail;
    pthread_mutex_unlock(&(buffer->mutex));

    return tail;
}

// TODO: rotina para analisar possivel valores persistentes em remaining_reads