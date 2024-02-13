#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>
#include <microhttpd.h>
#include <regex.h>
#include <time.h>
#include <ctype.h>
#include <pthread.h>
#include <signal.h>
#include <cJSON.h>

int port;
int pool_size;
int threads;
int conn_index = 0;
struct MHD_Daemon *main_daemon;
char *connection_string;

typedef struct {
  PGconn *conn;
  int em_uso;
  pthread_mutex_t lock;
} _connection;

typedef struct {
  _connection **connections;
  pthread_mutex_t lock;
} _pool;

_pool *pool;

void create_pool() {
  printf("Creating the connection pool of size %d\n", pool_size);

  PQinitOpenSSL(0, 0);
  PQinitSSL(0);

  pool = (_pool *)malloc(sizeof(_pool));
  pool -> connections = (_connection **)malloc(pool_size * sizeof(_connection *));
  pthread_mutex_init(&pool->lock, NULL);

  for (int i = 0; i < pool_size; i++) {
    pool -> connections[i] = (_connection *)malloc(sizeof(_connection));
    pool -> connections[i] -> conn = PQconnectdb(connection_string);
    pool -> connections[i] -> em_uso = 0;
    pthread_mutex_init(&pool->connections[i] -> lock, NULL);
  }
}

PGconn *get_connection() {
  pthread_mutex_lock(&pool->lock);
  for (int i = 0; i < pool_size; i++) {
    if (!pool->connections[i] -> em_uso) {
      pthread_mutex_lock(&pool->connections[i] -> lock);
      pool->connections[i] -> em_uso = 1;
      pthread_mutex_unlock(&pool->lock);
      return pool->connections[i] -> conn;
    }
  }
  pthread_mutex_unlock(&pool->lock);
  return NULL;
}

void release_connection(PGconn *conn) {
  pthread_mutex_lock(&pool->lock);
  for (int i = 0; i < pool_size; i++) {
    if (pool->connections[i] -> conn == conn) {
      pool->connections[i] -> em_uso = 0;
      pthread_mutex_unlock(&pool->connections[i] -> lock);
      pthread_mutex_unlock(&pool->lock);
      return;
    }
  }
  pthread_mutex_unlock(&pool->lock);
}

void close_pool() {
  printf("Closing the connection pool\n");
  for (int i = 0; i < pool_size; i++) {
    PQfinish(pool->connections[i] -> conn);
    pthread_mutex_destroy(&pool->connections[i] -> lock);
  }
  pthread_mutex_destroy(&pool->lock);
  free(pool);
}

struct transacao {
  char valor[11];
  char tipo;
  char descricao[11];
  char realizada_em[28];
  int em_uso;
};

struct cliente {
  char id[2];
  int saldo;
  int limite;
  struct transacao ultimas_transacoes[10];
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

void monta_ultimas_transacoes(struct transacao transacoes_array[11], char *ultimas_transacoes) {
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

  cJSON *json = cJSON_Parse(ultimas_transacoes);
  
  int j = 0;
  cJSON *item = NULL;
  cJSON_ArrayForEach(item, json) {
    cJSON *valor = cJSON_GetObjectItem(item, "valor");
    cJSON *tipo = cJSON_GetObjectItem(item, "tipo");
    cJSON *descricao = cJSON_GetObjectItem(item, "descricao");
    cJSON *realizada_em = cJSON_GetObjectItem(item, "realizada_em");

    sprintf(transacoes_array[j].valor, "%d", valor -> valueint);
    transacoes_array[j].tipo = tipo -> valuestring[0];
    strcpy(transacoes_array[j].descricao, descricao -> valuestring);
    strcpy(transacoes_array[j].realizada_em, realizada_em -> valuestring);
    transacoes_array[j].em_uso = 1;
    j++;
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

  PGconn *pg_conn = get_connection();
  PGresult *res = PQexec(pg_conn, query);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    printf("Failed to execute the query: %s\n", PQerrorMessage(pg_conn));
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
  release_connection(pg_conn);
  return 0;
}

void serializar_transacoes(char *transacoes, struct cliente *c) {
  if (!c -> ultimas_transacoes[0].em_uso) {
    transacoes[0] = '{';
    transacoes[1] = '}';
    transacoes[2] = '\0';
    return;
  }

  transacoes[0] = '{';
  transacoes[1] = '\0';
  int i = 0;
  while (c -> ultimas_transacoes[i].em_uso) {
    char transacao[200];
    sprintf(transacao, 
        "\"{\\\"valor\\\":%s,\\\"tipo\\\":\\\"%c\\\",\\\"descricao\\\":\\\"%s\\\",\\\"realizada_em\\\":\\\"%s\\\"}\",", 
        c -> ultimas_transacoes[i].valor, 
        c -> ultimas_transacoes[i].tipo, 
        c -> ultimas_transacoes[i].descricao, 
        c -> ultimas_transacoes[i].realizada_em);

    strcat(transacoes, transacao);
    i++;
  }

  transacoes[strlen(transacoes) - 1] = '}';
  transacoes[strlen(transacoes)] = '\0';
}

int salva_cliente(struct cliente *c, struct transacao t) {
  char query[2000];
  if (!c -> ultimas_transacoes[0].em_uso) {
    c -> ultimas_transacoes[0] = t;
    c -> ultimas_transacoes[0].em_uso = 1;
  } else {
    int i = 0;
    while (c -> ultimas_transacoes[i].em_uso && i < 9) {
      i++;
    }
    c -> ultimas_transacoes[i + 1].em_uso = 0;

    while (i >= 1) {
      c -> ultimas_transacoes[i] = c -> ultimas_transacoes[i - 1];
      i--;
    }

    c -> ultimas_transacoes[0] = t;
  }

  char ultimas_transacoes[2000];
  serializar_transacoes(ultimas_transacoes, c);
  sprintf(query, "update clientes set saldo = %d, ultimas_transacoes = '%s' where id = %s", c->saldo, ultimas_transacoes, c->id);

  PGconn *pg_conn = get_connection();

  PGresult *res = PQexec(pg_conn, query);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    printf("Failed to execute the query: %s\n", PQerrorMessage(pg_conn));
    PQclear(res);
    release_connection(pg_conn);
    return 0;
  }

  PQclear(res);
  release_connection(pg_conn);
  return 1;
}

struct cliente *inicia_cliente() {
  struct cliente *c = (struct cliente *)malloc(sizeof(struct cliente));
  for (int i = 0; i < 10; i++) {
    c -> ultimas_transacoes[i].em_uso = 0;
  }
  c -> id[0] = '\0';
  c -> saldo = 0;
  c -> limite = 0;
  return c;
}

void parse_transacao(struct transacao *t, const char *data) {
  char *data_copy = (char *)data;

  while (*data_copy != '{')
    data_copy++;

  cJSON *json = cJSON_Parse(data_copy);
  cJSON *valor = cJSON_GetObjectItem(json, "valor");
  cJSON *tipo = cJSON_GetObjectItem(json, "tipo");
  cJSON *descricao = cJSON_GetObjectItem(json, "descricao");

  double valor_double = valor -> valuedouble;
  if (valor_double - (int)valor_double > 0) {
    sprintf(t -> valor, "%.2f", valor -> valuedouble);
  } else {
    sprintf(t -> valor, "%d", valor -> valueint);
  }

  t -> tipo = tipo -> valuestring[0];
  if (descricao -> valuestring)
    strcpy(t -> descricao, descricao -> valuestring);

  cJSON_Delete(json);

  now(t -> realizada_em);
}

int valida_transacao(struct transacao t) {
  if (strlen(t.valor) == 0 || strchr(t.valor, '.') > 0) {
    return 0;
  }

  if (t.tipo != 'c' && t.tipo != 'd') {
    return 0;
  }

  if (strlen(t.descricao) == 0 || strlen(t.descricao) > 10){
    return 0;
  }

  return 1;
}

void serializa_ultimas_transacoes(char *transacoes, struct cliente *c) {
  if (!c -> ultimas_transacoes[0].em_uso) {
    transacoes[0] = '[';
    transacoes[1] = ']';
    transacoes[2] = '\0';
    return;
  }

  transacoes[0] = '[';
  transacoes[1] = '\0';
  int i = 0;
  while (c -> ultimas_transacoes[i].em_uso) {
    char transacao[300];
    
    sprintf(transacao, 
        "{\"valor\":%s,\"tipo\":\"%c\",\"descricao\":\"%s\",\"realizada_em\":\"%s\"},", 
        c -> ultimas_transacoes[i].valor, 
        c -> ultimas_transacoes[i].tipo, 
        c -> ultimas_transacoes[i].descricao, 
        c -> ultimas_transacoes[i].realizada_em);

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

        char *data = (char *)malloc(strlen(post -> data) + 1);
        strcpy(data, post -> data);
        char *ptr_data = data;
        while (*data != '{')
          data++;

        struct transacao t;
        parse_transacao(&t, data);
        
        free(ptr_data);

        if (!valida_transacao(t)) {
          return send_response(connection, "{\"error\": \"Unprocessable Entity\"}", 422);
        }

        free(post);

        struct cliente *c = inicia_cliente();

        switch (get_cliente(c, id)) {
          case 0: 
            {
              int novo_saldo = c->saldo;

              if (t.tipo == 'c') {
                novo_saldo += atoi(t.valor);
              } else {
                novo_saldo -= atoi(t.valor);
              }

              if (novo_saldo < -c->limite) {
                free(c);
                return send_response(connection, "{\"error\": \"Insufficient funds\"}", 422);
              }

              c -> saldo = novo_saldo;

              if (!salva_cliente(c, t)) {
                free(c);
                return send_response(connection, "{\"error\": \"Failed to save the client\"}", 500);
              }

              char response[2000];
              sprintf(response, "{\"limite\": %d,\"saldo\": %d}", c->limite, c->saldo);
              free(c);
              enum MHD_Result result = send_response(connection, response, 200);
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

    struct cliente *c = inicia_cliente();

    switch (get_cliente(c, id)) {
      case 0: 
      {
        char data_extrato[28];
        now(data_extrato);

        char response[2000];

        char transacoes[2000];
        serializa_ultimas_transacoes(transacoes, c);

        sprintf(response, "{\"saldo\":{\"total\":%d,\"limite\":%d,\"data_extrato\":\"%s\"},\"ultimas_transacoes\":%s}", c->saldo, c->limite, data_extrato, transacoes);

        enum MHD_Result result = send_response(connection, response, 200);

        free(c);

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

void sighandler(int signum) {
  if (signum == SIGINT || signum == SIGTERM) {
    printf("\nShutting down the service\n");
    close_pool();
    MHD_stop_daemon(main_daemon);
    exit(0);
  }
}

int main() {

  char *ptr_port = getenv("PORT");
  if (ptr_port == NULL) {
    port = 8080;
  } else {
    port = atoi(ptr_port);
  }

  char *ptr_pool_size = getenv("POOL_SIZE");
  if (ptr_pool_size != NULL) {
    pool_size = atoi(ptr_pool_size);
  } else {
    pool_size = 10;
  }

  char *ptr_threads = getenv("THREADS");
  if (ptr_threads != NULL) {
    threads = atoi(ptr_threads);
  } else {
    threads = 2;
  }

  char *ptr_connection_string = getenv("CONNECTION_STRING");
  if (ptr_connection_string != NULL) {
    connection_string = malloc(strlen(ptr_connection_string));
    strcpy(connection_string, ptr_connection_string);
  } else {
    connection_string = "postgres://postgres:postgres@localhost/rinha_backend_2024_q1_dev";
  }

  main_daemon = MHD_start_daemon(
      MHD_USE_EPOLL_INTERNAL_THREAD,
      port, NULL, NULL, 
      &handle_request, 
      NULL, 
      MHD_OPTION_CONNECTION_TIMEOUT, 30,
      MHD_OPTION_THREAD_POOL_SIZE, threads,
      MHD_OPTION_END);

  if (!main_daemon) {
    printf("Failed to start the server\n");
    return 1;
  } else {
    printf("Threads: %d\n", threads);
  }

  if (signal(SIGINT, sighandler) == SIG_ERR) {
    printf("Failed to set the signal handler\n");
    return 1;
  }

  if (signal(SIGTERM, sighandler) == SIG_ERR) {
    printf("Failed to set the signal handler\n");
    return 1;
  }

  create_pool();
  printf("Server running on port %d.\n", port);

  while (1);
}

