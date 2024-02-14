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
struct MHD_Daemon *main_daemon;

typedef struct {
  PGconn *conn;
  int em_uso;
  pthread_mutex_t lock;
} _connection;

typedef struct {
  _connection **connections;
  pthread_mutex_t lock;
} _pool;

typedef struct {
  char valor[11];
  char tipo;
  char descricao[11];
  char realizada_em[28];
  int em_uso;
} transacao;

typedef struct {
  char id[2];
  int saldo;
  int limite;
  transacao ultimas_transacoes[10];
} cliente;

struct post_status {
  int status;
  char data[1024];
};

_pool pool;

void create_pool(char *connection_string) {
  printf("Creating the connection pool of size %d\n", pool_size);

  pool.connections = malloc(pool_size * sizeof(_connection *));
  pthread_mutex_init(&pool.lock, NULL);

  for (int i = 0; i < pool_size; i++) {
    pool.connections[i] = (_connection *)calloc(1, sizeof(_connection));
    pool.connections[i] -> conn = PQconnectdb(connection_string);
    pool.connections[i] -> em_uso = 0;
    pthread_mutex_init(&pool.connections[i] -> lock, NULL);
  }
}

PGconn *get_connection() {
  pthread_mutex_lock(&pool.lock);
  PGconn *pg_conn = NULL;
  while (pg_conn == NULL) {
    for (int i = 0; i < pool_size; i++) {
      if (!pool.connections[i] -> em_uso) {
        pthread_mutex_lock(&pool.connections[i] -> lock);
        pool.connections[i] -> em_uso = 1;
        pg_conn = pool.connections[i] -> conn;
        break;
      }
    }
  }
  pthread_mutex_unlock(&pool.lock);
  return pg_conn;
}

void release_connection(PGconn *conn) {
  pthread_mutex_lock(&pool.lock);
  for (int i = 0; i < pool_size; i++) {
    if (pool.connections[i] -> conn == conn) {
      pool.connections[i] -> em_uso = 0;
      pthread_mutex_unlock(&pool.connections[i] -> lock);
      break;
    }
  }
  pthread_mutex_unlock(&pool.lock);
}

void close_pool() {
  printf("Closing the connection pool\n");
  for (int i = 0; i < pool_size; i++) {
    PQfinish(pool.connections[i] -> conn);
    pthread_mutex_destroy(&pool.connections[i] -> lock);
  }
  pthread_mutex_destroy(&pool.lock);
}


int send_response(struct MHD_Connection *connection, const char *response, int http_code, int free_response) {
  enum MHD_ResponseMemoryMode mode = free_response ? MHD_RESPMEM_MUST_FREE : MHD_RESPMEM_PERSISTENT;
  struct MHD_Response *mhd_response = MHD_create_response_from_buffer(strlen(response), (void *)response, mode);

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

void monta_ultimas_transacoes(transacao transacoes_array[10], char *ultimas_transacoes) {
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

  cJSON *json = cJSON_Parse(transacoes);

  if (json == NULL) {
    fprintf(stderr, "Failed to parse the JSON data\n");
    return;
  }
  
  int j = 0;
  cJSON *item = NULL;
  cJSON_ArrayForEach(item, json) {
    cJSON *valor = cJSON_GetObjectItem(item, "valor");
    cJSON *tipo = cJSON_GetObjectItem(item, "tipo");
    cJSON *descricao = cJSON_GetObjectItem(item, "descricao");
    cJSON *realizada_em = cJSON_GetObjectItem(item, "realizada_em");

    sprintf(transacoes_array[j].valor, "%d", valor -> valueint);
    transacoes_array[j].tipo = tipo -> valuestring[0];
    strncpy(transacoes_array[j].descricao, descricao -> valuestring, 10);
    transacoes_array[j].descricao[10] = '\0';
    strncpy(transacoes_array[j].realizada_em, realizada_em -> valuestring, 27);
    transacoes_array[j].realizada_em[27] = '\0';
    transacoes_array[j].em_uso = 1;
    j++;
  }

  cJSON_Delete(json);
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

int get_cliente(cliente *c, char *id) {
  char query[70];
  sprintf(query, "select saldo, limite, ultimas_transacoes from clientes where id = %s", id);

  PGconn *pg_conn = get_connection();
  PGresult *res = PQexec(pg_conn, query);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    printf("Failed to execute the query: %s\n", PQerrorMessage(pg_conn));
    PQclear(res);
    release_connection(pg_conn);
    return -2;
  }

  if (PQntuples(res) == 0) {
    PQclear(res);
    release_connection(pg_conn);
    return -3;
  }

  char *transacoes = PQgetvalue(res, 0, 2);

  monta_ultimas_transacoes(c -> ultimas_transacoes, transacoes);
  strncpy(c -> id, id, 2);
  c -> saldo = atoi(PQgetvalue(res, 0, 0));
  c -> limite = atoi(PQgetvalue(res, 0, 1));
  PQclear(res);
  release_connection(pg_conn);
  return 0;
}

void serializar_transacoes(char *transacoes, cliente *c) {
  if (!c -> ultimas_transacoes[0].em_uso) {
    transacoes[0] = '{';
    transacoes[1] = '}';
    transacoes[2] = '\0';
    return;
  }

  transacoes[0] = '{';
  transacoes[1] = '\0';
  int i = 0;
  while (i < 10 && c -> ultimas_transacoes[i].em_uso) {
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

int salva_cliente(cliente *c, transacao t) {
  char query[2000];
  if (!c -> ultimas_transacoes[0].em_uso) {
    memcpy(&c -> ultimas_transacoes[0], &t, sizeof(transacao));
    c -> ultimas_transacoes[0].em_uso = 1;
  } else {
    int i = 0;
    while (i < 8 && c -> ultimas_transacoes[i].em_uso) {
      i++;
    }

    c -> ultimas_transacoes[i + 1].em_uso = 0;

    while (i >= 1) {
      memcpy(&c -> ultimas_transacoes[i], &c -> ultimas_transacoes[i - 1], sizeof(transacao));
      i--;
    }

    memcpy(&c -> ultimas_transacoes[0], &t, sizeof(transacao));
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

cliente *inicia_cliente() {
  cliente *c = (cliente *)calloc(1, sizeof(cliente));
  if (c == NULL) {
    fprintf(stderr, "Failed to allocate memory for the client\n");
  }
  return c;
}

void parse_transacao(transacao *t, char *data) {
  cJSON *json = cJSON_Parse(data);
  if (json == NULL) {
    fprintf(stderr, "Failed to parse the JSON data\n");
    return;
  }

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
  if (descricao -> valuestring != NULL && strlen(descricao -> valuestring) <= 10){
    strncpy(t -> descricao, descricao -> valuestring, 10);
    t -> descricao[10] = '\0';
  } else {
    t -> descricao[0] = '\0';
  }

  cJSON_Delete(json);

  now(t -> realizada_em);
  t -> em_uso = 1;
}

int valida_transacao(transacao *t) {
  if (strlen(t -> valor) == 0 || strchr(t -> valor, '.') > 0) {
    return 0;
  }

  if (t -> tipo != 'c' && t -> tipo != 'd') {
    return 0;
  }

  if (strlen(t -> descricao) == 0){
    return 0;
  }

  return 1;
}

void serializa_ultimas_transacoes(char *transacoes, cliente *c) {
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
      post = (struct post_status *)calloc(1, sizeof(struct post_status));
      *con_cls = post;
    }

    if (!post->status) {
      post -> status = 1;
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
          return send_response(connection, "{\"error\": \"Invalid URL\"}", 400, 0);
        }

        char *data = (char *)calloc(1, 1024);
        strncpy(data, post -> data, 1024);
        char *ptr_data = data;
        while (*ptr_data != '{')
          ptr_data++;

        transacao t;
        parse_transacao(&t, ptr_data);
        
        free(data);
        free(post);

        if (!valida_transacao(&t)) {
          return send_response(connection, "{\"error\": \"Unprocessable Entity\"}", 422, 0);
        }

        cliente *c = inicia_cliente();

        switch (get_cliente(c, id)) {
          case 0: 
            {
              int novo_saldo = c->saldo;

              if (t.tipo == 'c') {
                novo_saldo += atoi(t.valor);
              } else {
                novo_saldo -= atoi(t.valor);
              }

              if (novo_saldo < -c -> limite) {
                free(c);
                return send_response(connection, "{\"error\": \"Insufficient funds\"}", 422, 0);
              }

              c -> saldo = novo_saldo;

              if (!salva_cliente(c, t)) {
                free(c);
                return send_response(connection, "{\"error\": \"Failed to save the client\"}", 500, 0);
              }

              char *response = (char *)calloc(1, 2000);
              sprintf(response, "{\"limite\": %d,\"saldo\": %d}", c->limite, c->saldo);
              free(c);
              return send_response(connection, response, 200, 1);
            }
          case -1:
            {
              free(c);
              return send_response(connection, "{\"error\": \"Failed to connect to the database\"}", 500, 0);
            }

          case -2:
            {
              free(c);
              return send_response(connection, "{\"error\": \"Failed to execute the query\"}", 500, 0);
            }

          case -3:
            {
              free(c);
              return send_response(connection, "{\"error\": \"Client not found\"}", 404, 0);
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
      return send_response(connection, "{\"error\": \"Invalid URL\"}", 400, 0);
    }

    cliente *c = inicia_cliente();

    switch (get_cliente(c, id)) {
      case 0: 
      {
        char data_extrato[28];
        now(data_extrato);

        char *response = (char *)calloc(1, 2000);

        char transacoes[2000];
        serializa_ultimas_transacoes(transacoes, c);

        sprintf(response, "{\"saldo\":{\"total\":%d,\"limite\":%d,\"data_extrato\":\"%s\"},\"ultimas_transacoes\":%s}", c->saldo, c->limite, data_extrato, transacoes);

        free(c);

        return send_response(connection, response, 200, 1);
      }
      case -1:
      {
        return send_response(connection, "{\"error\": \"Failed to connect to the database\"}", 500, 0);
      }

      case -2:
      {
        return send_response(connection, "{\"error\": \"Failed to execute the query\"}", 500, 0);
      }

      case -3:
      {
        return send_response(connection, "{\"error\": \"Client not found\"}", 404, 0);
      }
    }
  }
  return send_response(connection, "{\"error\": \"Invalid Request\"}", 400, 0);
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
  int threads = 2;

  if (ptr_threads != NULL) {
    threads = atoi(ptr_threads);
  }

  char *connection_string = getenv("CONNECTION_STRING");

  if (signal(SIGINT, sighandler) == SIG_ERR) {
    printf("Failed to set the signal handler\n");
    return 1;
  }

  if (signal(SIGTERM, sighandler) == SIG_ERR) {
    printf("Failed to set the signal handler\n");
    return 1;
  }

  create_pool(connection_string);
  // free(connection_string);

  main_daemon = MHD_start_daemon(
      MHD_USE_EPOLL_INTERNAL_THREAD | MHD_USE_EPOLL_TURBO,
      port, NULL, NULL, 
      &handle_request, 
      NULL, 
      MHD_OPTION_CONNECTION_TIMEOUT, 30,
      MHD_OPTION_THREAD_POOL_SIZE, threads,
      MHD_OPTION_LISTENING_ADDRESS_REUSE, 1,
      MHD_OPTION_END);

  if (!main_daemon) {
    printf("Failed to start the server\n");
    return 1;
  } else {
    printf("Threads: %d\n", threads);
  }

  printf("Server running on port %d.\n", port);

  while (1);
}

