CC = gcc
CFLAGS = -Wall -Wextra -O2 -I../include

# pega todos os arquivos que começam com main e terminam em .c
MAINS = $(wildcard main*.c)
# gera lista de executáveis (sem o .c)
TARGETS = $(MAINS:.c=)

# pega todos os .c da pasta
ALLSRC = $(wildcard *.c) ../lib/utils.c
# arquivos auxiliares (sem os mains)
COMMONSRC = $(filter-out $(MAINS), $(ALLSRC))

all: $(TARGETS)

# regra para cada main
%: %.c $(COMMONSRC)
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(TARGETS) *.o
