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
void cleanup_client(ClientInfo* ci);
int get_client_count();
void shutdown_server();
void* receiver_thread();
void* sender_thread(void* arg);
void* timer_thread();
void disconnect_client(int clientSFD);
void parse_command(const char *comm, ClientInfo* info);

extern MessageBuffer* buf;
//extern ClientInfo* client;

typedef struct ClientNode {
    ClientInfo* client_info;
    struct ClientNode* next;
} ClientNode;

ClientNode* client_list_head = NULL;
pthread_mutex_t client_list_mutex = PTHREAD_MUTEX_INITIALIZER;
int server_shutdown_flag = 0;
pthread_mutex_t shutdown_mutex = PTHREAD_MUTEX_INITIALIZER;
int serverSFD;

int main()
{
    // cria socket do servidor
    serverSFD = CriarSocketTCP_IPV4();
    if(serverSFD < 0) {
        fprintf(stderr, "[ERR] Erro ao criar socket para o servidor - ");
        switch(errno) {
            case EACCES:
            case EPERM:
                fprintf(stderr, "\t\tPermissao negada: voce não pode criar esse tipo de socket.\n");
                break;
            case EMFILE:
                fprintf(stderr, "\t\tLimite de descritores atingido para este processo.\n");
                break;
            case ENFILE:
                fprintf(stderr, "\t\tLimite global de descritores atingido no sistema.\n");
                break;
            case EAFNOSUPPORT:
                fprintf(stderr, "\t\tFamilia de endereços nao suportada.\n");
                break;
            case EPROTONOSUPPORT:
                fprintf(stderr, "\t\tProtocolo não suportado para este tipo de socket.\n");
                break;
            case ENOBUFS:
            case ENOMEM:
                fprintf(stderr, "\t\tMemoria insuficiente para criar o socket.\n");
                break;
            case EINVAL:
                fprintf(stderr, "\t\tArgumentos invalidos para socket().\n");
                break;
            default:
                fprintf(stderr, "\t\tErro desconhecido ao criar socket: %s\n", strerror(errno));
        }
        exit(EXIT_FAILURE);
    }

    // cria endereço do servidor, passando ip vazio para tratar como INNADDR_ANY (aceita conexoes de qualquer interface)
    struct sockaddr_in *serverAddr = CriarEndereco_IPV4("", 2000);
    if (serverAddr == NULL) {
        fprintf(stderr, "[ERR] Falha ao alocar bytes de memoria para o endereco do servidor.\n");
        exit(EXIT_FAILURE);
    }

    // associa socket ao endereco
    int resposta = bind(serverSFD, (struct sockaddr*) serverAddr, sizeof(*serverAddr));
    if (resposta == 0)
    {
        printf("----- " LOG_MSG_SERVER_STARTED " -----\n\n");
    }
    else
    {
        fprintf(stderr, "[ERR] Erro ao associar socket ao endereco do servidor - ");
        switch(errno) {
            case EACCES:
                fprintf(stderr, "\t\tPermissao negada para usar a porta.\n");
                break;
            case EADDRINUSE:
                fprintf(stderr, "\t\tPorta ja esta em uso.\n");
                break;
            case EADDRNOTAVAIL:
                fprintf(stderr, "\t\tEndereço IP nao disponivel na maquina.\n");
                break;
            case EBADF:
            case ENOTSOCK:
                fprintf(stderr, "\t\tSocket invalido.\n");
                break;
            default:
                fprintf(stderr, "\t\tErro desconhecido ao fazer bind: %s\n", strerror(errno));
        }
        exit(EXIT_FAILURE);
    }
    // libera struct de endereço ja utilizada
    if(serverAddr) free(serverAddr);
    
    // inicia buffer de mensagens (fila de mensagens compartilhada na memória)
    buf = buffer_init(0);
    if (!buf) {
        fprintf(stderr, "[ERR] Falha ao alocar bytes de memoria para o buffer de mensagens.\n");
        exit(EXIT_FAILURE);
    }
    
    // colocar server em modo de escuta por novas conexoes
    if (listen(serverSFD, 5) < 0) {
        fprintf(stderr, "[ERR] Erro ao colocar servidor em modo de escuta por novas conexoes - ");
        switch(errno) {
            case EBADF:
            case ENOTSOCK:
                fprintf(stderr, "\t\tSocket invalido.\n");
                break;
            case EOPNOTSUPP:
                fprintf(stderr, "\t\tEste tipo de socket nao suporta listen.\n");
                break;
            case EADDRINUSE:
                fprintf(stderr, "\t\tEndereco ou porta ja em uso.\n");
                break;
            case EINVAL:
                fprintf(stderr, "\t\tSocket nao esta associado a um endereco (bind nao chamado?)\n");
                break;
            case ENOMEM:
                fprintf(stderr, "\t\tMemoria insuficiente para a fila de conexoes.\n");
                break;
            default:
                fprintf(stderr, "\t\tErro desconhecido ao chamar listen: %s\n", strerror(errno));
        }
        exit(EXIT_FAILURE);
    } // mantem ate 5 conexoes na fila de espera

    // TODO: INCORPORAR CRIAÇÃO DOS DADOS DO CLIENT E ACCEPT EM UMA THREAD INDEPENDENTE, O QUE PERMITIRA MULTIPLOS USUARIOS
    
    // inicia thread de timer para orquestrar mensagens de data e hora
    pthread_t tid_timer;
    pthread_create(&tid_timer, NULL, timer_thread, NULL);
    pthread_detach(tid_timer);
    
    struct sockaddr_in clientAddr;
    socklen_t clientAddrSize = sizeof(clientAddr);
    
    while(true)
    {
        pthread_mutex_lock(&shutdown_mutex);
        if (server_shutdown_flag) {
            pthread_mutex_unlock(&shutdown_mutex);
            break;
        }
        pthread_mutex_unlock(&shutdown_mutex);
        
        // analisa e aceita (ou recusa) novas conexoes
        int clientSFD = accept(serverSFD, (struct sockaddr*)&clientAddr, &clientAddrSize);
        if (clientSFD < 0)
        {
            fprintf(stderr, "[ERR] Erro ao aceitar conexao - ");
            switch(errno) {
                case EAGAIN:
                    fprintf(stderr, "\t\tNao ha conexoes pendentes (socket non-blocking)\n");
                    break;
                case EBADF:
                case ENOTSOCK:
                    fprintf(stderr, "\t\tSocket invalido.\n");
                    break;
                case EOPNOTSUPP:
                    fprintf(stderr, "\t\tSocket nao suporta accept.\n");
                    break;
                case EINTR:
                    fprintf(stderr, "\t\tAccept interrompido por sinal, tente novamente.\n");
                    break;
                case EMFILE:
                    fprintf(stderr, "\t\tLimite de descritores do processo atingido.\n");
                    break;
                case ENFILE:
                    fprintf(stderr, "\t\tLimite de descritores do sistema atingido.\n");
                    break;
                case ECONNABORTED:
                    fprintf(stderr, "\t\tConexao do cliente abortada.\n");
                    break;
                case ENOMEM:
                    fprintf(stderr, "\t\tMemoria insuficiente para aceitar conexao.\n");
                    break;
                default:
                    fprintf(stderr, "\t\tErro desconhecido em accept: %s\n", strerror(errno));
            }
            exit(EXIT_FAILURE);
        }

        if(get_client_count() > MAX_ACTIVE_CLIENTS) {
            ssize_t bytes_sent = secure_send(clientSFD, "O limite de usuarios foi atingido! Tente novamente mais tarde.\n", 70);
            if (bytes_sent < 0) {
                close(clientSFD);
                exit(EXIT_FAILURE);
            }

            close(clientSFD);
            continue;
        }
        //int current = get_client_count();
        //if (current >= MAX_ACTIVE_CLIENTS) {
        //    const char *msg = "SERVIDOR LOTADO. Tente novamente mais tarde.\n";
        //    send(clientSFD, msg, strlen(msg), 0); /* ignora erro de envio aqui */
        //    close(clientSFD);
        //    printf("[LOG] Rejeitada conexão FD=%d — servidor cheio (%d/%d)\n", clientSFD, current, MAX_ACTIVE_CLIENTS);
        //    continue;
        //}
        
        printf("[LOG] Nova conexão aceita: FD=%d\n", clientSFD);

        time_t currentTime;
        struct tm *tzTime;
        char welcome_msg[100];
        currentTime = time(NULL);
        tzTime = localtime(&currentTime);
                
        snprintf(
            welcome_msg,
            sizeof(welcome_msg),
            "<%02d:%02d:%02d> CONECTADO!\nBem-vindo ao DigiChat!\nDigite :nome <seunome> para se identificar.\n",
            tzTime->tm_hour,
            tzTime->tm_min,
            tzTime->tm_sec
        );
        ssize_t bytes_sent = secure_send(clientSFD, welcome_msg, sizeof(welcome_msg));
        if (bytes_sent < 0) {
            close(clientSFD);
            exit(EXIT_FAILURE);
        }
        // inicia a struct de dados do cliente
        ClientInfo* client = (ClientInfo *)malloc(sizeof(ClientInfo));
        if (!client) {
            fprintf(stderr, "[ERR] Falha ao alocar bytes de memoria para a estrutura do cliente.\n");
            close(clientSFD);
            exit(EXIT_FAILURE);
        }

        client->clientSFD = clientSFD;
        client->exit_flag = (int *)malloc(sizeof(int));
        if (!client->exit_flag) {
            fprintf(stderr, "[ERR] Falha ao alocar bytes de memoria para a flag de saida.\n");
            free(client);
            close(clientSFD);
            exit(EXIT_FAILURE);
        }
        *(client->exit_flag) = 0;
        //TODO: client->name = (char*) malloc(MAX_CLIENT_NAME + 1); (DEFINIR MAX_CLIENT_NAME TAMBEM)
        /*if (!client->name) {
            perror("[ERR] Falha ao alocar nome do cliente");
            free(client->exit_flag);
            free(client);
            close(clientSFD);
            continue;
        }*/

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddr.sin_addr, ip, sizeof(ip));
        char ipv4[25];
        snprintf(
            ipv4,
            sizeof(ipv4),
            "%s:%d",
            ip,
            ntohs(clientAddr.sin_port)
        );
        
        strncpy(client->name, ipv4, MAX_CLIENT_NAME - 1);
        client->name[MAX_CLIENT_NAME - 1] = '\0';
        pthread_mutex_init(&client->mutex, NULL);
        client->receiver_thread = 0;
        client->sender_thread = 0;

        pthread_mutex_lock(&client_list_mutex);
        ClientNode* new_node = (ClientNode*) malloc(sizeof(ClientNode));
        if (!new_node) {
            perror("[ERR] Falha ao alocar ClientNode");
            pthread_mutex_unlock(&client_list_mutex);
            cleanup_client(client);
            close(clientSFD);
            continue;
        }
        new_node->client_info = client;
        new_node->next = client_list_head;
        client_list_head = new_node;
        pthread_mutex_unlock(&client_list_mutex);

        // Atualiza contador de readers no buffer
        buffer_update_readers(buf, 1);
        printf("[LOG]" LOG_MSG_NEW_CONN_SV "\n");

        // cria threads
        if (pthread_create(&client->receiver_thread, NULL, receiver_thread, client) != 0)
        {
            perror("[ERR] Falha ao criar receiver thread");
            pthread_mutex_lock(&client_list_mutex);
            // Remove da lista em caso de erro
            ClientNode* current = client_list_head;
            ClientNode* prev = NULL;
            while (current && current->client_info != client) {
                prev = current;
                current = current->next;
            }
            if (current) {
                if (prev) {
                    prev->next = current->next;
                } else {
                    client_list_head = current->next;
                }
                free(current);
            }
            pthread_mutex_unlock(&client_list_mutex);
            cleanup_client(client);
            close(clientSFD);
            buffer_update_readers(buf, -1);  // Decrementa
            continue;
        }
        if (pthread_create(&client->sender_thread, NULL, sender_thread, client) != 0) 
        {
            perror("[ERR] Falha ao criar sender thread");
            *(client->exit_flag) = 1;  // Sinaliza para receiver sair
            pthread_join(client->receiver_thread, NULL);
            pthread_mutex_lock(&client_list_mutex);
            // Remove da lista
            ClientNode* current = client_list_head;
            ClientNode* prev = NULL;
            while (current && current->client_info != client) {
                prev = current;
                current = current->next;
            }
            if (current) {
                if (prev) {
                    prev->next = current->next;
                } else {
                    client_list_head = current->next;
                }
                free(current);
            }
            pthread_mutex_unlock(&client_list_mutex);
            cleanup_client(client);
            close(clientSFD);
            buffer_update_readers(buf, -1);
            continue;
        }

        pthread_detach(client->receiver_thread);
        pthread_detach(client->sender_thread);

        usleep(100000); // 100ms - tempo para threads inicializarem

        printf("[LOG] " LOG_MSG_NEW_CONN_SV " Total de clientes: %d\n", get_client_count());
    }

        // Limpeza final do servidor
        printf("[LOG] Iniciando shutdown do servidor...\n");
        shutdown_server();

    // Fecha socket do servidor
        close(serverSFD);

        printf("\n\n----- Servidor finalizado -----\n");
    return 0;
/*
    // espera as threads
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
*/

}

int get_client_count() {
    int count = 0;
    pthread_mutex_lock(&client_list_mutex);
    ClientNode* current = client_list_head;
    while (current) {
        count++;
        current = current->next;
    }
    pthread_mutex_unlock(&client_list_mutex);
    return count;
}

void cleanup_client(ClientInfo* ci) {
    if (!ci) return;
    
    // Sinaliza threads para sair
    if (ci->exit_flag) {
        *(ci->exit_flag) = 1;
    }
    
    // Fecha socket
    if (ci->clientSFD >= 0) {
        close(ci->clientSFD);
    }
    
    // Libera mutex
    pthread_mutex_destroy(&ci->mutex);
    
    // Libera exit_flag
    if (ci->exit_flag) {
        free(ci->exit_flag);
    }

    free(ci);
}

void shutdown_server() {
    // Sinaliza shutdown
    pthread_mutex_lock(&shutdown_mutex);
    server_shutdown_flag = 1;
    pthread_mutex_unlock(&shutdown_mutex);

    // Limpa todos os clientes
    pthread_mutex_lock(&client_list_mutex);
    ClientNode* current = client_list_head;
    while (current) {
        ClientInfo* ci = current->client_info;
        // Sinaliza para threads saírem
        if (ci->exit_flag) {
            *(ci->exit_flag) = 1;
        }
        // Remove da lista
        ClientNode* next = current->next;
        free(current);
        current = next;
        
        // Limpa ClientInfo (threads já foram sinalizadas para sair)
        if (ci) {
            cleanup_client(ci);
            buffer_update_readers(buf, -1);
        }
    }
    client_list_head = NULL;
    pthread_mutex_unlock(&client_list_mutex);

    // Destrói buffer
    if (buf) {
        buf = buffer_destroy(buf);
    }

    // Destrói mutexes globais
    pthread_mutex_destroy(&client_list_mutex);
    pthread_mutex_destroy(&shutdown_mutex);
}

void disconnect_client(int clientSFD) {
    pthread_mutex_lock(&client_list_mutex);
    ClientNode *current = client_list_head, *prev = NULL;
    while (current && current->client_info->clientSFD != clientSFD) {
        prev = current;
        current = current->next;
    }

    if(current) {
        if(prev) {
            if(current->next)
                prev->next = current->next;
        } else {
            client_list_head = current->next;
        }

        // usleep(100000);
        // if(current->client_info->exit_flag) free(current->client_info->exit_flag);
        // if(current->client_info) free(current->client_info);
        // if(current) free(current);

        // close(clientSFD);

        buffer_update_readers(buf, -1);
    }

    pthread_mutex_unlock(&client_list_mutex);
    printf("[LOG] Cliente desconectado! Total de clientes: %d\n", get_client_count());
}

void parse_command(const char *comm, ClientInfo* info) {
    char cmd[10], arg[MAX_CLIENT_NAME];

    int n = sscanf(comm, "%9s %20[^\n]", cmd, arg);

    if(n == 1 && !strcmp(cmd, "exit")) {
        disconnect_client(info->clientSFD);
    } else if(n == 2 && !strcmp(cmd, "nome")) {
        change_client_name(arg, info);
    } else {
        warn_client(info->clientSFD, ERR_MSG_UNK_COMM);
    }
}

/*
    THREAD RECEPTORA
    recebe mensagens do cliente e as coloca no buffer (fila) para que sejam enviadas pelas threads remetentes
    interpreta comandos e lida com validacoes de entrada
    envia mensagem de erro na entrada ao cliente caso ocorra
*/
void* receiver_thread(void* arg) {
    ClientInfo* client = (ClientInfo*) arg;
    char msg[1024];
    int i, n;
    bool buf_overflow = false;
    time_t currentTime;
    struct tm *tzTime;
    char timeStr[12];

    while (1) {
        // TODO: espera enquanto nao tem nada na fila de recepcao do kernel

        // se a flag foi ativada, encerra a thread
        if(!client || *(client->exit_flag) == 1) {
            disconnect_client(client->clientSFD);
            pthread_exit(NULL);
        }
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
                break;
            } else if(n == 0) {
                printf("[LOG] " LOG_MSG_DC_SV "\n");
                
                *client->exit_flag = 1;
                break;
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
                parse_command(&msg[1], client);
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
    disconnect_client(client->clientSFD);
    pthread_exit(NULL);
}

/*
    THREAD REMETENTE
    de acordo com o exposto no buffer (fila) de mensagens, envia mensagens ao cliente
    customiza o formato de envio de acordo com o os sockets fonte e destino
    acompanha a fila de mensagens com seu proprio cursor
*/
void* sender_thread(void* arg) {
    ClientInfo* client = (ClientInfo*) arg;
    char formatted_msg[MAX_CLIENT_NAME + MAX_MSG_LEN + 32];
    Message *msg;
    Message **cursor = (Message **)malloc(sizeof(Message *));
    *cursor = buffer_get_tail(buf);

    // TODO: formata a mensagem para enviar
    while(1) {
        if(!client || *client->exit_flag == 1) {
            disconnect_client(client->clientSFD);
            pthread_exit(NULL);
        }
        msg = buffer_read_next(buf, cursor);
        if(msg->senderSFD == client->clientSFD) {
            // MENSAGEM PRÓPRIA - [Você]
            snprintf(
                formatted_msg,
                sizeof(formatted_msg),
                "[VOCE] (%s): %s\n",
                msg->time,
                msg->content
            );
        } else if(msg->senderSFD != serverSFD) {
            snprintf(
                formatted_msg,
                sizeof(formatted_msg),
                "%s (%s): %s\n",
                msg->sender_name,
                msg->time,
                msg->content
            );
        } else {
            snprintf(
                formatted_msg,
                sizeof(formatted_msg),
                "--\n %s \n--\n",
                msg->content
            );
        }

        if(send(client->clientSFD, formatted_msg, strlen(formatted_msg), 0) < 0) {
            if(*client->exit_flag == 0)
                perror("[ERR] " ERR_MSG_SEND);
            break;
        }
    }

    free(cursor);
    *client->exit_flag = 1;
    disconnect_client(client->clientSFD);
    pthread_exit(NULL);
}

void* timer_thread() {
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
        buffer_enqueue(buf, serverSFD, "", "", timeMsg);

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