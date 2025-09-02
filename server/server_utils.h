#ifndef SERVER_UTILS_H_INCLUDED
#define SERVER_UTILS_H_INCLUDED

#include "common.h"

struct ClientSocket *AnalisarConexao(int serverSFD);

void warn_client(int clientSFD, const char *msg);

#endif //SERVER_UTILS_H_INCLUDED