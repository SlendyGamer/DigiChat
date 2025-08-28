#include "C_socket_utils.h"

int main()
{
    int SFD = CriarSocketTCP_IPV4(); //Cria um Socket File descriptor com dominio ipv4, tipo TCP e protocolo 0
    
    
    struct sockaddr_in *addr = CriarEndereco_IPV4("127.0.0.1", 2000); //cria uma estrutura de endereco de rede usando dominio IPV4
    int resposta = connect(SFD, (struct sockaddr*) addr, sizeof (*addr)); //tenta estabelecer conexao com servidor explicitado em addr

    if (resposta == 0)//connect() retorna 0 se conexao for bem sucedida e -1 se falhou
    {
        printf("sucesso ao conectar!");
    }
    else
    {
        printf("fail");
        exit(2);
    }
    
    char *linhaDeInput = NULL;
    size_t linhaDeInputTam = 0; //serao utilizados por getline para capturar o que usuario do client digitar
    printf("Voce esta conectado, tente digitar algo!\n\n");

    while (true)
    {
        ssize_t CaracteresTam = getline(&linhaDeInput, &linhaDeInputTam, stdin); //captura caracteres digitados até CR, incluindo ele
        if(CaracteresTam>0)
        {
            if (strcmp(linhaDeInput, "/exit\n") == 0) //se digitado "/exit" e, em seguida Enter, encerra conexao
            {
                break;
            }
            ssize_t CaracteresEnviados = send(SFD, linhaDeInput, CaracteresTam, 0); //caso input seja diferente de exit, envia esse input ao server
        }
    }
    close(SFD); //fecha o socket que esta aberto e encerra a conexao com o servidor
}



