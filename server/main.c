#include "common.h"
#include "server_utils.h"

/*
    SERVER
    Thread 1: espera por entrada da rede e armazena em estrutura compartilhada, além de criar thread secundária
    Thread 2: varre estrutura compartilhada e envia mensagens para exibir aos clientes
*/

int main()
{
    int serverSFD = CriarSocketTCP_IPV4(); //cria o socket do servidor

    struct sockaddr_in *serverAddr = CriarEndereco_IPV4("", 2000); //passa ip vazio para criar o endereco do servidor, que sera tratado como INADDR_ANY(aceita conexoes de qualquer interface, desde que seja na porta 2000)

    int resposta= bind(serverSFD, (struct sockaddr*) serverAddr, sizeof(*serverAddr)); //associa socket ao endereco
    if (resposta == 0) //valida se bind ocorreu com sucesso
    {
        printf("server conectado com sucesso\n");
    }
    else
    {
        printf("err\n");
        exit(2);
    }

    listen(serverSFD, 10); //coloca server em modo de escuta, ouvindo por novas conexoes, com um limite de 10

    struct ClientSocket *clientS = AnalisarConexao(serverSFD); //analisa novas conexoes e as aceita ou nao
    
    if (clientS->erro < 0) //se houver algume erro, cancela a execucao
    {
        perror("accept");
        exit(3);
    }

    char buffer[1024];
    while(true)
    {
        int n = recv(clientS->conexaoSFD, buffer, 1024, 0); //recebe os dados enviados pelo cliente e armazena no buffer, devera ser um para cada cliente talvez?

        if (n > 0) //funcao recv retorna >0 se houve bytes lidos ou zero se o cliente fechou a conexao
        {
            buffer[n] = '\0';
            printf("response was: %s\n", buffer);
        }
        else if(n <=0 ) //<0 apenas para casos de erros tambem estarem inclusos
        {
            break;
        }
    }

    close(clientS->conexaoSFD); //fecha o socket de comunicacao com o cliente
    shutdown(serverSFD, SHUT_RDWR); //fecha o socket do servidor de vez
    
    
    return 0;
}
