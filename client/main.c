// gcc -Wall -Wextra -O2 -pthread client.c ../lib/utils.c -o client
#include "common.h"
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <errno.h>

#define MAX_INPUT MAX_MSG_LEN
#define MAX_CLIENT_NAME 20

static int sockfd = -1;
static atomic_bool rodando = 1;
static pthread_t th_rx, th_tx;

// encerra o cliente (Ctrl+C ou :exit)
static void encerrar(int sig) {
    (void)sig;
    rodando = 0;
    if (sockfd != -1) shutdown(sockfd, SHUT_RDWR);
}

// envia mensagem, garantindo que todos os bytes foram enviados
static int send_all(int fd, const void *buf, size_t len) {
    const char *p = buf;
    size_t left = len;
    while (left) {
        ssize_t n = send(fd, p, left, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return 0;
        p += n;
        left -= (size_t)n;
    }
    return (int)len;
}

// THREAD 2 – recebe mensagens do servidor e imprime as no terminal
static void* rx_thread(void *arg) {
    (void)arg;
    char buf[MAX_INPUT + 1];
    while (rodando) {
        ssize_t n = recv(sockfd, buf, MAX_INPUT, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            fprintf(stderr, "[CLIENT] Erro ao receber");
            break;
        }
        if (n == 0) {
            printf("\n[CLIENT] Servidor encerrou a conexão.\n");
            break;
        }
        buf[n] = '\0';
        fputs(buf, stdout);
        fflush(stdout);
    }
    rodando = 0;
    return NULL;
}

// THREAD 1 – lê do teclado e envia ao servidor
static void* tx_thread(void *arg) {
    (void)arg;
    char *linha = NULL;
    size_t cap = 0;


    while (rodando) {
        printf("\n> ");
        fflush(stdout);

        ssize_t n = getline(&linha, &cap, stdin);
        if (n < 0) {
            printf("\n[CLIENT] Entrada fechada.\n");
            break;
        }

        if (n > 0 && linha[n-1] != '\n') {
            // garante que termina qualquer cadeia de caracteres sempre termina com \n
            linha = realloc(linha, n + 2);
            linha[n] = '\n';
            linha[n+1] = '\0';
            n++;
        }

        // aplica limite para caracteres definido em common.h
        if (n >= MAX_MSG_LEN) {
            printf("[CLIENT] %s\n", ERR_MSG_BUF_OVERFLOW_CLT);
            continue; // não envia nada, volta para o prompt
        }

        // comando local de saída
        if (strncmp(linha, ":exit", 5) == 0) {
            send_all(sockfd, linha, n);
            break;
        }

        // envia para o servidor
        if (send_all(sockfd, linha, n) < 0) {
            fprintf(stderr, "[CLIENT] Erro ao enviar");
            break;
        }
    }

    free(linha);
    rodando = 0;
    return NULL;
}

// static bool capturar_nome() {
//     char *linha = NULL;
//     size_t cap = 0;
//     char nome[MAX_CLIENT_NAME];
    
//     fflush(stdout);
    
//     // Lê nome uma única vez
//     ssize_t n = getline(&linha, &cap, stdin);
//     if (n < 0) {
//         printf("[CLIENT] Erro na leitura do nome.\n");
//         free(linha);
//         return false;
//     }
    
//     if (n > 0 && linha[n-1] == '\n') {
//         linha[n-1] = '\0';  // Remove \n
//         n--;
//     }
    
//     // Valida nome
//     if (n == 0 || strlen(linha) == 0) {
//         printf("[CLIENT] Nome inválido. Conexão cancelada.\n");
//         free(linha);
//         return false;
//     }
    
//     if (n >= MAX_CLIENT_NAME) {
//         printf("[CLIENT] Nome muito longo. Máximo %d caracteres.\n", MAX_CLIENT_NAME - 1);
//         free(linha);
//         return false;
//     }
    
//     // Copia nome válido
//     strncpy(nome, linha, MAX_CLIENT_NAME - 1);
//     nome[MAX_CLIENT_NAME - 1] = '\0';
    
//     // Envia para servidor: :nome <nome>
//     char nome_comando[MAX_CLIENT_NAME + 10];
//     snprintf(nome_comando, sizeof(nome_comando), ":nome %s\n", nome);
    
//     if (send_all(sockfd, nome_comando, strlen(nome_comando)) < 0) {
//         fprintf(stderr, "[CLIENT] Erro ao enviar nome");
//         free(linha);
//         return false;
//     }
    
//     printf("[CLIENT] Nome '%s' enviado ao servidor.\n", nome);
//     free(linha);
//     return true;
// }

int main(/*int argc, char **argv*/) {
    // if (argc < 3) {
    //     fprintf(stderr, "Uso: %s <ip> <porta>\n", argv[0]);
    //     return 1;
    // }

    // const char *ip = argv[1];
    // int porta = atoi(argv[2]);

    // trata Ctrl+C
    struct sigaction sa;
    sa.sa_handler = encerrar;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);

    // cria socket e conecta
    sockfd = CriarSocketTCP_IPV4();
    if (sockfd < 0) {
        fprintf(stderr, "[CLIENT] Erro ao criar socket");
        return 1;
    }

    struct sockaddr_in *addr = CriarEndereco_IPV4("127.0.0.1", 2000);;
    if (connect(sockfd, (struct sockaddr*)addr, sizeof(*addr)) < 0) {
        fprintf(stderr, "[CLIENT] Erro ao conectar\n");
        free(addr);
        close(sockfd);
        return 1;
    }
    free(addr);

    printf("[CLIENT] Conectado ao servidor. Aguarde a mensagem inicial...\n");

    usleep(200000); // 200ms - tempo para server inicializar threads
    // cria threads
    if (pthread_create(&th_rx, NULL, rx_thread, NULL) != 0) {
        fprintf(stderr, "[CLIENT] Erro ao criar thread RX");
        close(sockfd);
        return 1;
    }

    printf("Aguardando mensagem do servidor...\n");
    /*
    if (!capturar_nome()) {
        // Falha na captura do nome
        rodando = 0;
        pthread_join(th_rx, NULL);
        close(sockfd);
        printf("[CLIENT] Falha na autenticação. Conexão encerrada.\n");
        return 1;
    }
    */
    if (pthread_create(&th_tx, NULL, tx_thread, NULL) != 0) {
        fprintf(stderr, "[CLIENT] Erro ao criar thread TX");
        rodando = 0;
        shutdown(sockfd, SHUT_RDWR);
        pthread_join(th_rx, NULL);
        close(sockfd);
        return 1;
    }

    // espera ambas terminarem
    pthread_join(th_tx, NULL);
    rodando = 0;
    shutdown(sockfd, SHUT_RDWR);
    pthread_join(th_rx, NULL);
    close(sockfd);
    return 0;
}