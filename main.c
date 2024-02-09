#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <microhttpd.h>
#include <regex.h>
#include <time.h>
#include <ctype.h>

#define PORT 9999

PGconn *conn;

struct transacao {
  char valor[11];
  char tipo;
  char descricao[11];
  char realizada_em[28];
};

struct cliente {
  char id[2];
  int saldo;
  int limite;
  struct transacao *ultimas_transacoes[11];
};

struct post_status {
  int status;
  char data[1024];
};

int send_response(struct MHD_Connection *connection, const char *response, int http_code) {
  struct MHD_Response *mhd_response = MHD_create_response_from_buffer(strlen(response), (void *)response, MHD_RESPMEM_PERSISTENT);
  MHD_add_response_header(mhd_response, "Content-Type", "application/json");
  MHD_add_response_header(mhd_response, "Connection", "keep-alive");
  int ret = MHD_queue_response(connection, http_code, mhd_response);
  MHD_destroy_response(mhd_response);

  return ret;
}

void now(char *dt) {
  time_t t = time(NULL);
  struct tm tm = *localtime(&t);
  struct timeval tv;
  gettimeofday(&tv, NULL);
  sprintf(dt, "%d-%02d-%02dT%02d:%02d:%02d.%06dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, (int)tv.tv_usec);
}

void monta_ultimas_transacoes(struct transacao *transacoes_array[11], char *ultimas_transacoes) {
  char transacoes[2000];
  char *token = ultimas_transacoes;

  int i = 0;
  while (*token) {
    switch (*token) {
      case '\\':
        token++;
        break;
      case '"':
        if (*(token + 1) != '{') {
          transacoes[i] = *token;
          i++;
        }
        token++;
        break;
      case '}':
        transacoes[i] = *token;
        i++;
        if (*(token + 1) == '"') {
          token += 2;
        } else token++;
        break;
      default:
        transacoes[i] = *token;
        i++;
        token++;
      break;
    }
  }

  transacoes[0] = '[';
  transacoes[i - 1] = ']';
  transacoes[i] = '\0';

  if (strcmp(transacoes, "[]") == 0) {
    return;
  }

  struct transacao *t = (struct transacao *)malloc(sizeof(struct transacao));

  char *token_saveptr = NULL;
  char *token2 = strtok_r(transacoes, "[},{]", &token_saveptr);
  char *key_value_saveptr = NULL;

  i = 0;
  int valores = 0;
  while (token2) {
    char key_value[100];
    strcpy(key_value, token2);

    char *key = strtok_r(key_value, ":", &key_value_saveptr);
    while (*key != '"')
      key++;
    char *value = strtok_r(NULL, ":", &key_value_saveptr);

    char *key_saveptr = NULL;
    char *key_token = strtok_r(key, "\"", &key_saveptr);

    while (!key_token) {
      token2 = strtok_r(NULL, "[},{]", &token_saveptr);
    }

    if (strcmp(key_token, "valor") == 0) {
      strcpy(t -> valor, value);
      valores++;
    } else if (strcmp(key_token, "tipo") == 0) {
      t -> tipo = value[2];
      valores++;
    } else if (strcmp(key_token, "descricao") == 0) {
      char *descricao_saveptr = NULL;
      char *token_descricao = strtok_r(value, "\"", &descricao_saveptr);
      token_descricao = strtok_r(NULL, "\"", &descricao_saveptr);
      strcpy(t -> descricao, token_descricao);
      valores++;
    } else if (strcmp(key_token, "realizada_em") == 0) {
      char *token_realizada_em_2 = strtok_r(NULL, ":" , &key_value_saveptr);
      char *token_realizada_em_3 = strtok_r(NULL, ":" , &key_value_saveptr);

      char realizada_em[28];
      char *realizada_em_saveptr = NULL;
      char *token_realizada_em = strtok_r(value, "\"", &realizada_em_saveptr);
      
      token_realizada_em = strtok_r(NULL, "\"", &realizada_em_saveptr);
      sprintf(realizada_em, "%s:%s:%s", token_realizada_em, token_realizada_em_2, token_realizada_em_3);
      realizada_em[strlen(realizada_em) - 1] = '\0';
      strcpy(t -> realizada_em, realizada_em);
      valores++;
    }

    if (valores == 4) {
      transacoes_array[i] = t;
      t = (struct transacao *)malloc(sizeof(struct transacao));
      valores = 0;
      i++;
    }

    token2 = strtok_r(NULL, "[},{]", &token_saveptr);
  }

  if (i > 0) {
    transacoes_array[i] = NULL;
  }
}

void get_cliente_id(char *id, const char *url, char *regex_expression) {
    regex_t regex;
    int reti = regcomp(&regex, regex_expression, REG_EXTENDED | REG_ICASE | REG_STARTEND);
    if (reti) {
      return;
    }

    regmatch_t matches[2];
    reti = regexec(&regex, url, 2, matches, 0);

    if (!reti) {
      strncpy(id, url + matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so);
      id[matches[1].rm_eo - matches[1].rm_so] = '\0';
    } else {
      char regex_error[100];
      regerror(reti, &regex, regex_error, sizeof(regex_error));
      printf("Regex match failed: %s\n", regex_error);
    }

    regfree(&regex);
}

int get_cliente(struct cliente *c, char *id) {
  char query[70];
  sprintf(query, "select saldo, limite, ultimas_transacoes from clientes where id = %s", id);

  if (PQstatus(conn) == CONNECTION_BAD) {
    return -1;
  }

  PGresult *res = PQexec(conn, query);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    PQclear(res);
    return -2;
  }

  if (PQntuples(res) == 0) {
    PQclear(res);
    return -3;
  }

  char *transacoes = PQgetvalue(res, 0, 2);

  monta_ultimas_transacoes(c -> ultimas_transacoes, transacoes);
  strcpy(c -> id, id);
  c -> saldo = atoi(PQgetvalue(res, 0, 0));
  c -> limite = atoi(PQgetvalue(res, 0, 1));
  PQclear(res);
  return 0;
}

void serializar_transacoes(char *transacoes, struct transacao **ultimas_transacoes) {
  if (ultimas_transacoes == NULL) {
    transacoes[0] = '{';
    transacoes[1] = '}';
    transacoes[2] = '\0';
    return;
  }

  transacoes[0] = '{';
  transacoes[1] = '\0';
  int i = 0;
  while (ultimas_transacoes[i] != NULL) {
    char transacao[200];
    sprintf(transacao, "\"{\\\"valor\\\":%s,\\\"tipo\\\":\\\"%c\\\",\\\"descricao\\\":\\\"%s\\\",\\\"realizada_em\\\":\\\"%s\\\"}\",", ultimas_transacoes[i]->valor, ultimas_transacoes[i]->tipo, ultimas_transacoes[i]->descricao, ultimas_transacoes[i]->realizada_em);
    strcat(transacoes, transacao);
    i++;
  }

  transacoes[strlen(transacoes) - 1] = '}';
  transacoes[strlen(transacoes)] = '\0';
}

int salva_cliente(struct cliente *c, struct transacao *t) {
  char query[2000];
  if (c -> ultimas_transacoes[0] == NULL) {
    c -> ultimas_transacoes[0] = t;
    c -> ultimas_transacoes[1] = NULL;
  } else {
    int i = 0;
    while (c -> ultimas_transacoes[i] != NULL && i < 9) {
      i++;
    }
    c -> ultimas_transacoes[i + 1] = NULL;

    while (i >= 1) {
      c -> ultimas_transacoes[i] = c -> ultimas_transacoes[i - 1];
      i--;
    }

    c -> ultimas_transacoes[0] = t;
  }

  char ultimas_transacoes[2000];
  serializar_transacoes(ultimas_transacoes, c -> ultimas_transacoes);
  sprintf(query, "update clientes set saldo = %d, ultimas_transacoes = '%s' where id = %s", c->saldo, ultimas_transacoes, c->id);

  if (PQstatus(conn) == CONNECTION_BAD) {
    return 0;
  }

  PGresult *res = PQexec(conn, query);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    printf("Error: %s\n", PQerrorMessage(conn));
    PQclear(res);
    return 0;
  }

  PQclear(res);
  return 1;
}

void parse_transacao(struct transacao *t, const char *data) {
  char *data_copy = (char *)data;

  while (*data_copy != '{')
    data_copy++;
  data_copy++;

  char *token_saveptr = NULL;
  char *token = strtok_r((char *)data_copy, "\n", &token_saveptr);
  char trimmed_data[1024];
  while (token) {
    char *trimmed_token = (char *)malloc(strlen(token) + 1);
    int i = 0;
    while (isspace(token[i]) || isblank(token[i])) {
      i++;
    }
    strcpy(trimmed_token, token + i);

    char *end = trimmed_token + strlen(trimmed_token) - 1;
    while (end > trimmed_token && isspace(*end) || isblank(*end)) {
      end--;
    }
    end[1] = '\0';

    strcat(trimmed_data, trimmed_token);
    free(trimmed_token);
    token = strtok_r(NULL, "\n",&token_saveptr);

  }
  trimmed_data[strlen(trimmed_data) - 1] = '\0'; 
  char *trimmed_data_saveptr = NULL;
  char *key_value_saveptr = NULL;

  token = strtok_r(trimmed_data, ",", &trimmed_data_saveptr);
  while (token) {
    char *key = strtok_r(token, ":", &key_value_saveptr);
    while (*key != '"')
      key++;
    char *value = strtok_r(NULL, ":", &key_value_saveptr);

    char *key_saveptr = NULL;
    char *key_token = strtok_r(key, "\"", &key_saveptr);
    while (!key_token) {
      token = strtok_r(NULL, ",", &key_saveptr);
    }

    if (strcmp(key_token, "valor") == 0) {
      strcpy(t -> valor, value);
    } else if (strcmp(key_token, "tipo") == 0) {
      char *tipo_saveptr = NULL;
      char *token_tipo = strtok_r(value, "\"", &tipo_saveptr);
      token_tipo = strtok_r(NULL, "\"", &tipo_saveptr);
      t -> tipo = token_tipo[0];
    } else if (strcmp(key_token, "descricao") == 0) {
      char *descricao_saveptr = NULL;
      char *token_descricao = strtok_r(value, "\"", &descricao_saveptr);
      token_descricao = strtok_r(NULL, "\"", &descricao_saveptr);
      if (token_descricao != NULL) {
        strcpy(t -> descricao, token_descricao);
      }
    }
    token = strtok_r(NULL, ",", &trimmed_data_saveptr);
  }
  now(t -> realizada_em);
}

int valida_transacao(struct transacao *t) {
  if (strlen(t -> valor) == 0 || strchr(t -> valor, '.') > 0) {
    return 0;
  }

  if (t -> tipo != 'c' && t -> tipo != 'd') {
    return 0;
  }

  if (strlen(t->descricao) == 0 || strlen(t->descricao) > 10){
    return 0;
  }

  return 1;
}

void free_cliente(struct cliente *c) {
  int i = 0;
  while (c -> ultimas_transacoes[i]) {
    i++;
  }
  while (i > 0) {
    free(c -> ultimas_transacoes[i - 1]);
    i--;
  }
  free(c);
}

void serializa_ultimas_transacoes(char *transacoes, struct transacao **ultimas_transacoes) {
  if (ultimas_transacoes[0] == NULL) {
    transacoes[0] = '[';
    transacoes[1] = ']';
    transacoes[2] = '\0';
    return;
  }

  transacoes[0] = '[';
  transacoes[1] = '\0';
  int i = 0;
  while (ultimas_transacoes[i] != NULL) {
    char transacao[300];
    sprintf(transacao, "{\"valor\":%s,\"tipo\":\"%c\",\"descricao\":\"%s\",\"realizada_em\":\"%s\"},", ultimas_transacoes[i]->valor, ultimas_transacoes[i]->tipo, ultimas_transacoes[i]->descricao, ultimas_transacoes[i]->realizada_em);
    strcat(transacoes, transacao);
    i++;
  }
  transacoes[strlen(transacoes) - 1] = ']';
  transacoes[strlen(transacoes)] = '\0';
}

enum MHD_Result handle_request(void *cls, struct MHD_Connection *connection, const char *url, const char *method, const char *version, const char *upload_data, size_t *upload_data_size, void **con_cls) {

  if (strcmp(method, "POST") == 0) {
    struct post_status *post = (struct post_status *)*con_cls;

    if (!post) {
      post = (struct post_status *)malloc(sizeof(struct post_status));
      post->status = 0;
      *con_cls = post;
    }

    if (!post->status) {
      post->status = 1;
      return MHD_YES;
    } else {
      if (*upload_data_size) {
        strncat(post->data, upload_data, *upload_data_size);
        *upload_data_size = 0;
        return MHD_YES;
      } else {
        printf("%s %s\n", method, url);
        char id[2] = "0";
        get_cliente_id(id, url, "\\/clientes\\/([0-9]+)\\/transacoes");

        if (strcmp(id, "0") == 0) {
          return send_response(connection, "{\"error\": \"Invalid URL\"}", 400);
        }

        printf("%s\n", post -> data);

        struct transacao *t = (struct transacao *)malloc(sizeof(struct transacao));
        parse_transacao(t, post -> data);
        if (!valida_transacao(t)) {
          return send_response(connection, "{\"error\": \"Unprocessable Entity\"}", 422);
        }

        free(post);

        struct cliente *c = (struct cliente *)malloc(sizeof(struct cliente));
        c -> ultimas_transacoes[0] = NULL;

        switch (get_cliente(c, id)) {
          case 0: 
            {
              int novo_saldo = c->saldo;

              if (t->tipo == 'c') {
                novo_saldo += atoi(t->valor);
              } else {
                novo_saldo -= atoi(t->valor);
              }

              if (novo_saldo < -c->limite) {
                free_cliente(c);
                return send_response(connection, "{\"error\": \"Insufficient funds\"}", 422);
              }

              c -> saldo = novo_saldo;

              if (!salva_cliente(c, t)) {
                free_cliente(c);
                return send_response(connection, "{\"error\": \"Failed to save the client\"}", 500);
              }

              char response[2000];
              sprintf(response, "{\"limite\": %d,\"saldo\": %d}", c->limite, c->saldo);
              enum MHD_Result result = send_response(connection, response, 200);
              free_cliente(c);
              return result;
            }
          case -1:
            {
              return send_response(connection, "{\"error\": \"Failed to connect to the database\"}", 500);
            }

          case -2:
            {
              return send_response(connection, "{\"error\": \"Failed to execute the query\"}", 500);
            }

          case -3:
            {
              return send_response(connection, "{\"error\": \"Client not found\"}", 404);
            }
        }

      }
    }
    return MHD_NO;
  } 

  if (strcmp(method, "GET") == 0) {
    printf("%s %s\n", method, url);

    char id[2] = "0";
    get_cliente_id(id, url, "\\/clientes\\/([0-9]+)\\/extrato");

    if (strcmp(id, "0") == 0) {
      return send_response(connection, "{\"error\": \"Invalid URL\"}", 400);
    }

    struct cliente *c = (struct cliente *)malloc(sizeof(struct cliente));
    c -> ultimas_transacoes[0] = NULL;

    switch (get_cliente(c, id)) {
      case 0: 
      {
        char data_extrato[28];
        now(data_extrato);

        char response[2000];

        char transacoes[2000];
        serializa_ultimas_transacoes(transacoes, c -> ultimas_transacoes);

        sprintf(response, "{\"saldo\":{\"total\":%d,\"limite\":%d,\"data_extrato\":\"%s\"},\"ultimas_transacoes\":%s}", c->saldo, c->limite, data_extrato, transacoes);

        enum MHD_Result result = send_response(connection, response, 200);

        free_cliente(c);

        return result;
      }
      case -1:
      {
        return send_response(connection, "{\"error\": \"Failed to connect to the database\"}", 500);
      }

      case -2:
      {
        return send_response(connection, "{\"error\": \"Failed to execute the query\"}", 500);
      }

      case -3:
      {
        return send_response(connection, "{\"error\": \"Client not found\"}", 404);
      }
    }
  }
  return send_response(connection, "{\"error\": \"Invalid Request\"}", 400);
}

int main() {
  struct MHD_Daemon *daemon = MHD_start_daemon(
      MHD_USE_SELECT_INTERNALLY | MHD_SUPPRESS_DATE_NO_CLOCK | MHD_USE_EPOLL_LINUX_ONLY | MHD_USE_EPOLL_TURBO,
      PORT, NULL, NULL, 
      &handle_request, 
      NULL, 
      MHD_OPTION_CONNECTION_TIMEOUT, 120,
      MHD_OPTION_THREAD_POOL_SIZE, 4,
      MHD_OPTION_END);

  if (daemon) {
    conn = PQconnectdb("host=localhost dbname=rinha_backend_2024_q1_dev user=postgres password=postgres");
    printf("Server running on port %d\n", PORT);
    getchar();
    PQfinish(conn);
    MHD_stop_daemon(daemon);
  } else {
    printf("Failed to start the server\n");
  }
  return 0;
}


