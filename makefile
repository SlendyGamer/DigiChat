CC = gcc
CFLAGS = -Wall -Wextra -O2

# pega todos os arquivos que começam com main e terminam em .c
MAINS = $(wildcard main*.c DigiChatServer.c)
# gera lista de executáveis (sem o .c)
TARGETS = $(MAINS:.c=)
COMMONSRC = $(filter-out $(MAINS), $(ALLSRC))

# pega todos os .c da pasta
ALLSRC = $(wildcard *.c)
# arquivos auxiliares (sem os mains)
COMMONSRC = $(filter-out $(MAINS), $(ALLSRC))

all: $(TARGETS)

# regra para cada main
%: %.c $(COMMONSRC)
	$(CC) $(CFLAGS) -o $@ $^ 

clean:
	rm -f $(TARGETS) *.o