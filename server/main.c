#include "common.h"
#include "server_utils.h"
#include "message_buffer.h"
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

/*
    SERVER
    Thread 1: espera por entrada da rede e armazena em estrutura compartilhada, além de criar thread secundária
    Thread 2: varre estrutura compartilhada e envia mensagens para exibir aos clientes
*/

void* receiver_thread();
void* sender_thread(void* arg);
void* timer_thread(void* arg);

extern MessageBuffer* buf;
extern ClientInfo* client;

int main()
{
    // cria socket do servidor
    int serverSFD = CriarSocketTCP_IPV4();
    if(serverSFD < 0) {
        perror("[ERR] " ERR_MSG_CREATE_SOCKET);
        exit(1);
    }

    // cria endereço do servidor, passando ip vazio para tratar como INNADDR_ANY (aceita conexoes de qualquer interface)
    struct sockaddr_in *serverAddr = CriarEndereco_IPV4("", 2000);

    // associa socket ao endereco
    int resposta = bind(serverSFD, (struct sockaddr*) serverAddr, sizeof(*serverAddr));
    if (resposta == 0)
    {
        printf("----- " LOG_MSG_SERVER_STARTED " -----\n\n");
    }
    else
    {
        perror("[ERR] " ERR_MSG_ADDRESS_ASSOCIATION);
        exit(2);
    }
    // libera struct de endereço ja utilizada
    if(serverAddr) free(serverAddr);

    // inicia buffer de mensagens (fila de mensagens compartilhada na memória)
    buf = buffer_init(0);

    // inicia a struct de dados do cliente
    client = (ClientInfo *)malloc(sizeof(ClientInfo));
    client->exit_flag = (int *)malloc(sizeof(int));
    *(client->exit_flag) = 0;
    pthread_mutex_init(&client->mutex, NULL);

    // colocar server em modo de escuta por novas conexoes
    listen(serverSFD, 1);

    // analisa e aceita (ou recusa) novas conexoes
    struct sockaddr_in clientAddr;
    socklen_t clientAddrSize = sizeof(clientAddr);
    client->clientSFD = accept(serverSFD, (struct sockaddr*)&clientAddr, &clientAddrSize);

    if (client->clientSFD < 0)
    {
        perror("[ERR] " ERR_MSG_ACCEPT_CONN);
        exit(3);
    }

    buffer_update_readers(buf, 1);
    printf("[LOG]" LOG_MSG_NEW_CONN_SV "\n");

    // inicia thread de timer para orquestrar mensagens de data e hora
    pthread_t tid_timer;
    pthread_create(&tid_timer, NULL, timer_thread, &serverSFD);

    // cria threads
    pthread_create(&client->receiver_thread, NULL, receiver_thread, NULL);
    pthread_create(&client->sender_thread, NULL, sender_thread, &serverSFD);

    // espera as threads
    pthread_detach(tid_timer);
    pthread_join(client->receiver_thread, NULL);
    pthread_join(client->sender_thread, NULL);

    // libera recursos e finaliza
    close(client->clientSFD);
    shutdown(serverSFD, SHUT_RDWR);
    pthread_mutex_destroy(&client->mutex);
    if(client) {
        if(client->exit_flag) free(client->exit_flag);
        free(client);
    }
    buf = buffer_destroy(buf);

    printf("\n\n");

    return 0;
}

/*
    THREAD RECEPTORA
    recebe mensagens do cliente e as coloca no buffer (fila) para que sejam enviadas pelas threads remetentes
    interpreta comandos e lida com validacoes de entrada
    envia mensagem de erro na entrada ao cliente caso ocorra
*/
void* receiver_thread() {
    char msg[1024];
    int i, n;
    bool buf_overflow = false;
    time_t currentTime;
    struct tm *tzTime;
    char timeStr[12];

    while (1) {
        // TODO: espera enquanto nao tem nada na fila de recepcao do kernel

        // se a flag foi ativada, encerra a thread
        if(*client->exit_flag) pthread_exit(NULL);
        // lê os caracteres que estao na fila de recepcao do kernel ate encontrar '\n' ou estourar o limite do buffer (msg)
        for(i = 0; i < 1024; i++) {
            n = recv(client->clientSFD, &(msg[i]), 1, 0);
            if(n < 0) {
                if(errno == ECONNRESET) {
                    printf("[LOG] " LOG_MSG_FORCE_QUIT_SV "\n");
                } else {
                    perror("[ERR] " ERR_MSG_READ_IN);
                    warn_client(client->clientSFD, "ERRO: " ERR_MSG_PROCESS_MSG_CLT);
                }

                *client->exit_flag = 1;
                pthread_exit(NULL);
            } else if(n == 0) {
                printf("[LOG] " LOG_MSG_DC_SV "\n");
                
                *client->exit_flag = 1;
                pthread_exit(NULL);
            }
            
            if(msg[i] == '\n') {
                msg[i] = '\0';
                break;
            } else if (i == 1023) {
                buf_overflow = true;
                break;
            }
        }

        // caso tenha estourado o buffer, limpa o restante na fila de recepcao ate o proximo '\n' e envia mensagem de erro ao cliente
        if(buf_overflow) {
            while(msg[0] != '\n' && socket_has_data_to_read(client->clientSFD)) {
                n = recv(client->clientSFD, msg, 1, 0);
                if(n < 0) {
                    if(errno == ECONNRESET) {
                        printf("[LOG] " LOG_MSG_DC_SV "\n");
                    } else {
                        perror("[ERR] " ERR_MSG_READ_IN);
                    }

                    break;
                } else if(n == 0) {
                    printf("[LOG] " LOG_MSG_DC_SV "\n");

                    break;
                }
            }

            warn_client(client->clientSFD, "ERRO: " ERR_MSG_BUF_OVERFLOW_CLT);

            buf_overflow = false;
        // caso contrario nao tenha ocorrido estouro, adiciona a mensagem na fila
        } else {
            // TODO: interpretar comandos e adicionar nome e horario
            if(msg[0] == ':') {
                parse_command(&msg[1]);
            } else {
                currentTime = time(NULL);
                tzTime = localtime(&currentTime);
                
                snprintf(
                    timeStr,
                    sizeof(timeStr),
                    "%02d:%02d:%02d",
                    tzTime->tm_hour,
                    tzTime->tm_min,
                    tzTime->tm_sec
                );
                buffer_enqueue(buf, client->clientSFD, client->name, timeStr, msg);
                printf("[LOG] [CLIENTE] %s\n", msg);
            }
        }
    }

    *client->exit_flag = 1;   
    pthread_exit(NULL);
}

/*
    THREAD REMETENTE
    de acordo com o exposto no buffer (fila) de mensagens, envia mensagens ao cliente
    customiza o formato de envio de acordo com o os sockets fonte e destino
    acompanha a fila de mensagens com seu proprio cursor
*/
void* sender_thread(void* arg) {
    int *serverSFD = (int *)arg;
    char formatted_msg[MAX_CLIENT_NAME + MAX_MSG_LEN + 13];
    Message *msg;
    Message **cursor = (Message **)malloc(sizeof(Message *));
    *cursor = buffer_get_tail(buf);

    // TODO: formata a mensagem para enviar
    while(1) {
        if(*client->exit_flag) pthread_exit(NULL);
        msg = buffer_read_next(buf, cursor);
        if(msg->senderSFD != *serverSFD) {
            snprintf(
                formatted_msg,
                sizeof(formatted_msg),
                "%s (%s): %s",
                msg->sender_name,
                msg->time,
                msg->content
            );
        } else {
            snprintf(
                formatted_msg,
                sizeof(formatted_msg),
                "--\n %s \n--",
                msg->content
            );
        }

        if(send(client->clientSFD, formatted_msg, strlen(formatted_msg), 0) < 0) {
            perror("[ERR] " ERR_MSG_SEND);
            break;
        }
    }

    free(cursor);
    *client->exit_flag = 1;
    pthread_exit(NULL);
}

void* timer_thread(void* arg) {
    int *serverSFD = (int *)arg;
    time_t currentTime;
    struct tm *tzTime;
    char timeMsg[58];

    setenv("TZ", "America/Sao_Paulo", 1);
    tzset();

    while(1) {
        currentTime = time(NULL);
        tzTime = localtime(&currentTime);
        
        snprintf(
            timeMsg,
            sizeof(timeMsg),
            "Horario: %02d:%02d:%02d GMT-3 %02d/%02d/%04d",
            tzTime->tm_hour,
            tzTime->tm_min,
            tzTime->tm_sec,
            tzTime->tm_mday,
            tzTime->tm_mon + 1,
            tzTime->tm_year + 1900
        );

        printf("[LOG] Time Message - %s\n", timeMsg);
        buffer_enqueue(buf, *serverSFD, "", "", timeMsg);

        if(tzTime->tm_sec < 60) sleep(60 - tzTime->tm_sec);
    }
}

// TODO: thread que lida com comandos no servidor (shutdown)
// TODO: funcao de limpeza fallback -- se escalar, necessario dividir timer thread em mais threads
//*****// TODO: separar threads em server_threads.c
//*****// TODO: padronizar contantes de mensagens de erro e centralizar manutencao em common.h (tambem util para transmitir mensagens sem consumir tanta rede)
//*****// TODO: protecao de encerramento de conexao pelo lado do cliente e estrategia de renovacao
// TODO: melhorar logs pelo lado do servidor para identificar melhor ocorrencias especificas
//*****// TODO: revisar fluxos de interrupcao de threads e programa
// TODO: modularizar encerramento do servidor e encerramento de socket de cliente para lidar com erros especificos
// TODO: modularizar leitura de 1 char