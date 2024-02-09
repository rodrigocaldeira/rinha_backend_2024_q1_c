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

struct cliente {
  int saldo;
  int limite;
  char *ultimas_transacoes;
};

struct post_status {
  int status;
  char *data;
};

struct request_transacao {
  char *valor;
  char tipo;
  char *descricao;
};

int send_response(struct MHD_Connection *connection, const char *response, int http_code) {

  enum MHD_ResponseMemoryMode response_mode = http_code == 200 ? MHD_RESPMEM_MUST_FREE : MHD_RESPMEM_PERSISTENT;

  struct MHD_Response *mhd_response = MHD_create_response_from_buffer(strlen(response), (void *)response, response_mode);
  MHD_add_response_header(mhd_response, "Content-Type", "application/json");
  int ret = MHD_queue_response(connection, http_code, mhd_response);
  MHD_destroy_response(mhd_response);

  return ret;
}

char *now() {
  time_t t = time(NULL);
  struct tm tm = *localtime(&t);
  struct timeval tv;
  gettimeofday(&tv, NULL);
  char *now = (char *)malloc(100);
  sprintf(now, "%d-%02d-%02dT%02d:%02d:%02d.%06dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, (int)tv.tv_usec);
  return now;
}

char *monta_ultimas_transacoes(char *ultimas_transacoes) {
  char *transacoes = (char *)malloc(2000);
  char *token = ultimas_transacoes;
  char *inicio_transacoes = transacoes;

  while (*token) {
    switch (*token) {
      case '\\':
        token++;
        break;
      case '"':
        if (*(token + 1) != '{') {
          *transacoes = *token;
          transacoes++;
        }
        token++;
        break;
      case '}':
        *transacoes = *token;
        transacoes++;
        if (*(token + 1) == '"') {
          token += 2;
        } else token++;
        break;
      default:
        *transacoes = *token;
        transacoes++;
        token++;
      break;
    }
  }

  *inicio_transacoes = '[';
  *(transacoes - 1) = ']';
  *transacoes = '\0';

  return inicio_transacoes;
}

char *get_cliente_id(const char *url, char *regex_expression) {
    regex_t regex;
    int reti = regcomp(&regex, regex_expression, REG_EXTENDED | REG_ICASE | REG_STARTEND);
    if (reti) {
      return NULL;
    }

    regmatch_t matches[2];
    reti = regexec(&regex, url, 2, matches, 0);

    char *id;

    if (!reti) {
      id = (char *)malloc(matches[1].rm_eo - matches[1].rm_so + 1);
      strncpy(id, url + matches[1].rm_so, matches[1].rm_eo - matches[1].rm_so);
      id[matches[1].rm_eo - matches[1].rm_so] = '\0';
    } else {
      char regex_error[100];
      regerror(reti, &regex, regex_error, sizeof(regex_error));
      printf("Regex match failed: %s\n", regex_error);
      return NULL;
    }

    regfree(&regex);

    return id;
}

int get_cliente(struct cliente *c, char *id) {
  char *query = (char *)malloc(70);
  sprintf(query, "select saldo, limite, ultimas_transacoes from clientes where id = %s", id);

  if (query) {
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

    char *ultimas_transacoes = monta_ultimas_transacoes(PQgetvalue(res, 0, 2));
    c->saldo = atoi(PQgetvalue(res, 0, 0));
    c->limite = atoi(PQgetvalue(res, 0, 1));
    c->ultimas_transacoes = ultimas_transacoes;
    PQclear(res);
  }
  return 0;
}

void monta_request_transacao(struct request_transacao *rt, const char *data) {
  char *data_copy = (char *)data;

  while (*data_copy != '{')
    data_copy++;
  data_copy++;

  char *token = strtok((char *)data_copy, "\n");
  char *trimmed_data = (char *)malloc(1024);
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
    token = strtok(NULL, "\n");

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
      rt -> valor = value;
    } else if (strcmp(key_token, "tipo") == 0) {
      char *tipo_saveptr = NULL;
      char *token_tipo = strtok_r(value, "\"", &tipo_saveptr);
      token_tipo = strtok_r(NULL, "\"", &tipo_saveptr);
      rt -> tipo = token_tipo[0];
    } else if (strcmp(key_token, "descricao") == 0) {
      char *descricao_saveptr = NULL;
      char *token_descricao = strtok_r(value, "\"", &descricao_saveptr);
      token_descricao = strtok_r(NULL, "\"", &descricao_saveptr);
      rt -> descricao = token_descricao;
    }
    token = strtok_r(NULL, ",", &trimmed_data_saveptr);
  }
}

int valida_request_transacao(struct request_transacao *rt) {
  if (strlen(rt -> valor) == 0 || strchr(rt -> valor, '.') > 0) {
    return 0;
  }

  if (rt -> tipo != 'c' && rt -> tipo != 'd') {
    return 0;
  }

  if (rt -> descricao == NULL || strlen(rt->descricao) == 0 || strlen(rt->descricao) > 10){
    return 0;
  }

  return 1;
}

enum MHD_Result handle_request(void *cls, struct MHD_Connection *connection, const char *url, const char *method, const char *version, const char *upload_data, size_t *upload_data_size, void **con_cls) {

  if (strcmp(method, "POST") == 0) {

    struct post_status *post = (struct post_status *)*con_cls;

    if (!post) {
      post = (struct post_status *)malloc(sizeof(struct post_status));
      post->status = 0;
      post->data = (char *)malloc(1024);
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
        struct request_transacao *rt = (struct request_transacao *)malloc(sizeof(struct request_transacao));
        monta_request_transacao(rt, post -> data);
        if (!valida_request_transacao(rt)) {
          return send_response(connection, "{\"error\": \"Unprocessable Entity\"}", 422);
        }
        printf("Valor: %s\nTipo: %c\nDescricao: %s\n", rt->valor, rt->tipo, rt->descricao);
        post -> status = 0;
        free(post->data);
        free(post);
        char *response = (char *)malloc(100);
        sprintf(response, "{\"limite\":10000,\"saldo\":10}");
        return send_response(connection, response, 200);
      }
    }
    return MHD_NO;
  } 

  if (strcmp(method, "GET") == 0) {
    printf("%s %s\n", method, url);

    char *id = get_cliente_id(url, "\\/clientes\\/([0-9]+)\\/extrato");

    if (!id) {
      return send_response(connection, "{\"error\": \"Invalid URL\"}", 400);
    }

    struct cliente *c = (struct cliente *)malloc(sizeof(struct cliente));
    switch (get_cliente(c, id)) {
      case 0: 
      {
        char *data_extrato = now();

        char *response = (char *)malloc(2000);
        sprintf(response, "{\"saldo\":{\"total\":%d,\"limite\":%d,\"data_extrato\":\"%s\"},\"ultimas_transacoes\":%s}", c->saldo, c->limite, data_extrato, c->ultimas_transacoes);

        free(c);
        return send_response(connection, response, 200);
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
  struct MHD_Daemon *daemon = MHD_start_daemon(MHD_USE_INTERNAL_POLLING_THREAD, PORT, NULL, NULL, &handle_request, NULL, MHD_OPTION_END);
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


