#include "common.h"
#include "server_utils.h"
#include "message_buffer.h"
#include <pthread.h>
#include <time.h>
#include <unistd.h>

/*
    SERVER
    Thread 1: espera por entrada da rede e armazena em estrutura compartilhada, além de criar thread secundária
    Thread 2: varre estrutura compartilhada e envia mensagens para exibir aos clientes
*/

void* receiver_thread(void* arg);
void* sender_thread(void* arg);
void* timer_thread();

MessageBuffer* buf;

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

    // associa socket ao endereco e libera struct de endereco ja utilizada
    int resposta = bind(serverSFD, (struct sockaddr*) serverAddr, sizeof(*serverAddr));
    if(serverAddr) free(serverAddr);
    if (resposta == 0)
    {
        printf("----- " LOG_MSG_SERVER_STARTED " -----\n\n");
    }
    else
    {
        perror("[ERR] " ERR_MSG_ADDRESS_ASSOCIATION);
        exit(2);
    }

    // inicia buffer de mensagens (fila de mensagens compartilhada na memória)
    buf = buffer_init(0);

    // inicia thread de timer para orquestrar mensagens de data e hora
    pthread_t tid_timer;
    int serverSFDArg = serverSFD;
    pthread_create(&tid_timer, NULL, timer_thread, &serverSFDArg);

    // colocar server em modo de escuta por novas conexoes
    listen(serverSFD, 1);

    // analisa e aceita (ou recusa) novas conexoes
    struct ClientSocket *clientS = AnalisarConexao(serverSFD);
    if (clientS->erro < 0)
    {
        perror("[ERR] " ERR_MSG_ACCEPT_CONN);
        exit(3);
    }

    // cria objetos para controle de threads (argumento e tids)
    int clientSFDArg = clientS->conexaoSFD;
    pthread_t tid_recv, tid_send;

    // cria threads
    pthread_create(&tid_recv, NULL, receiver_thread, &clientSFDArg);
    pthread_create(&tid_send, NULL, sender_thread, &clientSFDArg);

    // espera as threads terminarem
    pthread_join(tid_recv, NULL);
    pthread_join(tid_send, NULL);
    pthread_join(tid_timer, NULL);

    // libera recursos e finaliza
    close(clientS->conexaoSFD);
    if(clientS) free(clientS);
    shutdown(serverSFD, SHUT_RDWR);
    buf = buffer_destroy(buf);

    return 0;
}

/*
    THREAD RECEPTORA
    recebe mensagens do cliente e as coloca no buffer (fila) para que sejam enviadas pelas threads remetentes
    interpreta comandos e lida com validacoes de entrada
    envia mensagem de erro na entrada ao cliente caso ocorra
*/
void* receiver_thread(void* arg) {
    int *clientSFD = (int *)arg;
    char msg[1024];
    int i, n;
    bool buf_overflow = false;

    while (1) {
        // TODO: espera enquanto nao tem nada na fila de recepcao do kernel

        // lê os caracteres que estao na fila de recepcao do kernel ate encontrar '\n' ou estourar o limite do buffer (msg)
        for(i = 0; i < 1024; i++) {
            n = recv(*clientSFD, &(msg[i]), 1, 0);
            if(n < 0) {
                perror("[ERR] " ERR_MSG_READ_IN);
                warn_client(*clientSFD, "ERRO: " ERR_MSG_PROCESS_MSG_CLT);
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
            while(msg[0] != '\n' && socket_has_data_to_read(*clientSFD)) {
                n = recv(*clientSFD, msg, 1, 0);
                if(n < 0) {
                    perror("[ERR] " ERR_MSG_READ_IN);
                }
            }

            warn_client(*clientSFD, "ERRO: " ERR_MSG_BUF_OVERFLOW_CLT);

            buf_overflow = false;
        // caso contrario nao tenha ocorrido estouro, adiciona a mensagem na fila
        } else {
            buffer_enqueue(buf, *clientSFD, msg);
            printf("[LOG] [CLIENTE] %s\n", msg);
        }
    }

    return NULL;
}

/*
    THREAD REMETENTE
    de acordo com o exposto no buffer (fila) de mensagens, envia mensagens ao cliente
    customiza o formato de envio de acordo com o os sockets fonte e destino
    acompanha a fila de mensagens com seu proprio cursor
*/
void* sender_thread(void* arg) {
    int *clientSFD = (int *)arg;
    Message *msg;
    Message **cursor = (Message **)malloc(sizeof(Message *));
    *cursor = NULL;

    while(1) {
        msg = buffer_read_next(buf, cursor);
        if(send(*clientSFD, msg->content, strlen(msg->content), 0) < 0) {
            perror("[ERR] " ERR_MSG_SEND);
            break;
        }
    }

    free(cursor);
    return NULL;
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
            "--\nHorario: %02d:%02d:%02d GMT-3 %02d/%02d/%04d\n--\n",
            tzTime->tm_hour,
            tzTime->tm_min,
            tzTime->tm_sec,
            tzTime->tm_mday,
            tzTime->tm_mon + 1,
            tzTime->tm_year + 1900
        );

        printf("[LOG] Time Message\n%s", timeMsg);
        buffer_enqueue(buf, *serverSFD, timeMsg);

        sleep(60 - tzTime->tm_sec);
    }
}

// TODO: thread que lida com comandos no servidor (shutdown)
// TODO: funcao de limpeza fallback -- se escalar, necessario dividir timer thread em mais threads
// TODO: separar threads em server_threads.c
// TODO: padronizar contantes de mensagens de erro e centralizar manutencao em common.h (tambem util para transmitir mensagens sem consumir tanta rede)
// TODO: protecao de encerramento de conexao pelo lado do cliente e estrategia de renovacao
// TODO: melhorar logs pelo lado do servidor para identificar melhor ocorrencias especificas
// TODO: revisar fluxos de interrupcao de threads e programa