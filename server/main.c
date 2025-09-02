#include "common.h"
#include "server_utils.h"
#include "message_buffer.h"
#include <pthread.h>

/*
    SERVER
    Thread 1: espera por entrada da rede e armazena em estrutura compartilhada, além de criar thread secundária
    Thread 2: varre estrutura compartilhada e envia mensagens para exibir aos clientes
*/

void* receiver_thread(void* arg);
void* sender_thread(void* arg);

MessageBuffer* buf;

int main()
{
    // cria socket do servidor
    int serverSFD = CriarSocketTCP_IPV4();
    if(serverSFD < 0) {
        perror("[ERR] Erro ao criar socket para server");
        exit(1);
    }

    // cria endereço do servidor, passando ip vazio para tratar como INNADDR_ANY (aceita conexoes de qualquer interface)
    struct sockaddr_in *serverAddr = CriarEndereco_IPV4("", 2000);

    // associa socket ao endereco e libera struct de endereco ja utilizada
    int resposta = bind(serverSFD, (struct sockaddr*) serverAddr, sizeof(*serverAddr));
    if(serverAddr) free(serverAddr);
    if (resposta == 0)
    {
        printf("----- Server iniciado com sucesso -----\n\n");
    }
    else
    {
        perror("[ERR] Erro ao associar socket e ipv4\n");
        exit(2);
    }

    // inicia buffer de mensagens (fila de mensagens compartilhada na memória)
    buf = buffer_init(0);

    // colocar server em modo de escuta por novas conexoes
    listen(serverSFD, 1);

    // analisa e aceita (ou recusa) novas conexoes
    struct ClientSocket *clientS = AnalisarConexao(serverSFD);
    if (clientS->erro < 0)
    {
        perror("[ERR] Erro ao aceitar conexao");
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
                perror("[ERR] Erro ao ler mensagem da entrada");
                warn_client(*clientSFD, "ERRO: Nao foi possivel processar a mensagem enviada. Por favor, tente de novo");
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
                    perror("[ERR] Erro ao ler mensagem da entrada");
                }
            }

            warn_client(*clientSFD, "ERRO: Sua mensagem deve se limitar a 1023 caracteres!");

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

    while (1) {
        msg = buffer_read_next(buf, cursor);
        if (send(*clientSFD, msg->content, strlen(msg->content), 0) < 0) {
            perror("[ERR] Erro ao enviar mensagem ao cliente");
            break;
        }
    }

    free(cursor);
    return NULL;
}

// TODO: thread que lida com comandos no servidor (shutdown)
// TODO: thread que lida com timers do servidor (funcao de limpeza fallback e registro de mensagens de horario) -- se escalar, necessario dividir em mais threads
// TODO: separar threads em server_threads.c
// TODO: padronizar contantes de mensagens de erro e centralizar manutencao em common.h (tambem util para transmitir mensagens sem consumir tanta rede)
// TODO: protecao de encerramento de conexao pelo lado do cliente e estrategia de renovacao
// TODO: melhorar logs pelo lado do servidor para identificar melhor ocorrencias especificas