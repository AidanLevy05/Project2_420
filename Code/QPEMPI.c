/*
 * QPEMPI.c
 *
 * Parallel MPI implementation of the query processing engine.
 * Root process loads the database and queries, broadcasts the data to all
 * ranks, and every rank evaluates the WHERE clause on a disjoint range of
 * tuples. Query results are printed in rank order for each query to preserve
 * deterministic output.
 */

#include <ctype.h>
#include <limits.h>
#include <mpi.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "../btree/btree.h"

typedef struct {
  int ID;
  char Model[20];
  int YearMake;
  char Color[20];
  int Price;
  char Dealer[20];
} CarInventory;

typedef struct {
  char select_attrs[6][20];
  int num_select_attrs;
  char where_raw[256];
} Query;

typedef struct {
  char *data;
  size_t len;
  size_t cap;
} Buffer;

enum {
  TAG_RECORD_COUNT = 1,
  TAG_RECORD_DATA = 2,
};

/*
Function prototypes
*/
int car_compare(const void *a, const void *b, void *udata);
struct btree *load_database(const char *filename);
void print_all_tuples(struct btree *tree);
bool print_iter(const void *item, void *udata);
void load_queries(const char *filename, Query **queries, int *num_queries);
int match_where(const CarInventory *car, const char *where_raw);

static void buffer_init(Buffer *buf);
static void buffer_free(Buffer *buf);
static bool buffer_append(Buffer *buf, const char *data, size_t len);
static bool buffer_appendf(Buffer *buf, const char *fmt, ...);
static void bcast_bytes(void *data, size_t bytes, int root, MPI_Comm comm);
static void send_bytes(const void *data, size_t bytes, int dest, int tag,
                       MPI_Comm comm);
static void recv_bytes(void *data, size_t bytes, int src, int tag,
                       MPI_Comm comm);
static CarInventory *btree_to_array(struct btree *tree, size_t *out_count);
static void compute_bounds(long long total, int size, int rank,
                           long long *start, long long *end);
static bool append_selected(const CarInventory *car, const Query *q,
                            Buffer *buf);

/*
Buffer helpers
*/
static void buffer_init(Buffer *buf) {
  buf->data = NULL;
  buf->len = 0;
  buf->cap = 0;
}

static void buffer_free(Buffer *buf) {
  free(buf->data);
  buf->data = NULL;
  buf->len = 0;
  buf->cap = 0;
}

static bool buffer_reserve(Buffer *buf, size_t needed) {
  if (needed <= buf->cap) {
    return true;
  }
  size_t new_cap = buf->cap ? buf->cap : 256;
  while (new_cap < needed) {
    new_cap *= 2;
  }
  char *tmp = realloc(buf->data, new_cap);
  if (!tmp) {
    return false;
  }
  buf->data = tmp;
  buf->cap = new_cap;
  return true;
}

static bool buffer_append(Buffer *buf, const char *data, size_t len) {
  if (!buffer_reserve(buf, buf->len + len + 1)) {
    return false;
  }
  memcpy(buf->data + buf->len, data, len);
  buf->len += len;
  buf->data[buf->len] = '\0';
  return true;
}

static bool buffer_appendf(Buffer *buf, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int needed = vsnprintf(NULL, 0, fmt, ap);
  va_end(ap);
  if (needed < 0) {
    return false;
  }
  size_t total = buf->len + (size_t)needed + 1;
  if (!buffer_reserve(buf, total)) {
    return false;
  }
  va_start(ap, fmt);
  vsnprintf(buf->data + buf->len, buf->cap - buf->len, fmt, ap);
  va_end(ap);
  buf->len += (size_t)needed;
  return true;
}

static void bcast_bytes(void *data, size_t bytes, int root, MPI_Comm comm) {
  size_t offset = 0;
  char *ptr = (char *)data;
  while (offset < bytes) {
    size_t remaining = bytes - offset;
    int chunk = remaining > (size_t)INT_MAX ? INT_MAX : (int)remaining;
    MPI_Bcast(ptr + offset, chunk, MPI_BYTE, root, comm);
    offset += (size_t)chunk;
  }
}

static void send_bytes(const void *data, size_t bytes, int dest, int tag,
                       MPI_Comm comm) {
  size_t offset = 0;
  const char *ptr = (const char *)data;
  while (offset < bytes) {
    size_t remaining = bytes - offset;
    int chunk = remaining > (size_t)INT_MAX ? INT_MAX : (int)remaining;
    MPI_Send(ptr + offset, chunk, MPI_BYTE, dest, tag, comm);
    offset += (size_t)chunk;
  }
}

static void recv_bytes(void *data, size_t bytes, int src, int tag,
                       MPI_Comm comm) {
  size_t offset = 0;
  char *ptr = (char *)data;
  while (offset < bytes) {
    size_t remaining = bytes - offset;
    int chunk = remaining > (size_t)INT_MAX ? INT_MAX : (int)remaining;
    MPI_Recv(ptr + offset, chunk, MPI_BYTE, src, tag, comm, MPI_STATUS_IGNORE);
    offset += (size_t)chunk;
  }
}

static void compute_bounds(long long total, int size, int rank,
                           long long *start, long long *end) {
  if (total <= 0 || size <= 0) {
    *start = 0;
    *end = 0;
    return;
  }
  long long base = total / size;
  long long rem = total % size;
  long long extra = rank < rem ? 1 : 0;
  long long prefix = rank < rem ? rank : rem;
  *start = rank * base + prefix;
  *end = *start + base + extra;
  if (*end > total) {
    *end = total;
  }
}

typedef struct {
  CarInventory *arr;
  size_t index;
} ToArrayCtx;

static bool to_array_cb(const void *item, void *udata) {
  ToArrayCtx *ctx = (ToArrayCtx *)udata;
  ctx->arr[ctx->index++] = *(const CarInventory *)item;
  return true;
}

static CarInventory *btree_to_array(struct btree *tree, size_t *out_count) {
  size_t count = btree_count(tree);
  CarInventory *arr = NULL;
  if (count > 0) {
    arr = malloc(count * sizeof(CarInventory));
    if (!arr) {
      *out_count = 0;
      return NULL;
    }
    ToArrayCtx ctx = {.arr = arr, .index = 0};
    btree_ascend(tree, NULL, to_array_cb, &ctx);
  }
  *out_count = count;
  return arr;
}

/*
Main function
*/
int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  clock_t startTime = clock();
  int world_rank = 0;
  int world_size = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  const char *filename = "db/db.txt";
  const char *queryfile = "db/sql.txt";
  if (argc >= 2) {
    filename = argv[1];
  }
  if (argc >= 3) {
    queryfile = argv[2];
  }

  struct btree *tree = NULL;
  Query *queries = NULL;
  int num_queries = 0;
  CarInventory *records = NULL;
  size_t record_count = 0;
  long long record_count_ll = 0;

  if (world_rank == 0) {
    tree = load_database(filename);
    if (!tree) {
      fprintf(stderr, "Error: Failed to load database from %s\n", filename);
      record_count_ll = -1;
    } else {
      record_count = btree_count(tree);
      record_count_ll = (long long)record_count;
      printf("Loaded %zu tuples from %s\n", record_count, filename);
      if (record_count <= 10) {
        printf("Printing all tuples for debugging:\n");
        print_all_tuples(tree);
      }

      load_queries(queryfile, &queries, &num_queries);
      printf("Processing %d queries from %s\n", num_queries, queryfile);
      records = btree_to_array(tree, &record_count);
      record_count_ll = (long long)record_count;
      if (record_count_ll > 0 && !records) {
        fprintf(stderr, "Error: Failed to materialize B-tree into array\n");
        record_count_ll = -1;
      }
    }
  }

  MPI_Bcast(&record_count_ll, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
  if (record_count_ll < 0) {
    if (tree) {
      btree_free(tree);
    }
    MPI_Finalize();
    return 1;
  }

  long long local_start = 0;
  long long local_end = 0;
  compute_bounds(record_count_ll, world_size, world_rank, &local_start,
                 &local_end);
  long long local_count_ll = local_end - local_start;

  CarInventory *local_records = NULL;
  bool owns_local_records = false;

  if (record_count_ll > 0) {
    if (world_rank == 0) {
      if (local_count_ll > 0) {
        local_records = records + local_start;
      }
      for (int dest = 1; dest < world_size; ++dest) {
        long long dest_start = 0;
        long long dest_end = 0;
        compute_bounds(record_count_ll, world_size, dest, &dest_start,
                       &dest_end);
        long long dest_count = dest_end - dest_start;
        MPI_Send(&dest_count, 1, MPI_LONG_LONG, dest, TAG_RECORD_COUNT,
                 MPI_COMM_WORLD);
        if (dest_count > 0) {
          size_t bytes = (size_t)dest_count * sizeof(CarInventory);
          send_bytes(records + dest_start, bytes, dest, TAG_RECORD_DATA,
                     MPI_COMM_WORLD);
        }
      }
    } else {
      MPI_Recv(&local_count_ll, 1, MPI_LONG_LONG, 0, TAG_RECORD_COUNT,
               MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      if (local_count_ll > 0) {
        size_t alloc_bytes = (size_t)local_count_ll * sizeof(CarInventory);
        local_records = (CarInventory *)malloc(alloc_bytes);
        if (!local_records) {
          fprintf(stderr,
                  "Rank %d: out of memory allocating %lld local records\n",
                  world_rank, local_count_ll);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
        recv_bytes(local_records, alloc_bytes, 0, TAG_RECORD_DATA,
                   MPI_COMM_WORLD);
        owns_local_records = true;
      }
    }
  }

  if (local_count_ll > 0 && !local_records) {
    fprintf(stderr, "Rank %d: missing local data buffer for %lld records\n",
            world_rank, local_count_ll);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  MPI_Bcast(&num_queries, 1, MPI_INT, 0, MPI_COMM_WORLD);
  if (world_rank != 0 && num_queries > 0) {
    queries = malloc((size_t)num_queries * sizeof(Query));
    if (!queries) {
      fprintf(stderr, "Rank %d: out of memory allocating queries buffer\n",
              world_rank);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }

  if (num_queries > 0) {
    bcast_bytes(queries, (size_t)num_queries * sizeof(Query), 0,
                MPI_COMM_WORLD);
  }

  for (int qi = 0; qi < num_queries; ++qi) {
    Buffer local_buf;
    buffer_init(&local_buf);
    const Query *q = &queries[qi];

    for (long long idx = 0; idx < local_count_ll; ++idx) {
      if (local_records && match_where(&local_records[idx], q->where_raw)) {
        if (!append_selected(&local_records[idx], q, &local_buf)) {
          fprintf(stderr, "Rank %d: Failed to append query result\n",
                  world_rank);
          MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
    }

    for (int src = 0; src < world_size; ++src) {
      MPI_Barrier(MPI_COMM_WORLD);
      if (src == world_rank && local_buf.len > 0) {
        fwrite(local_buf.data, 1, local_buf.len, stdout);
        fflush(stdout);
      }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    buffer_free(&local_buf);
  }

  if (world_rank == 0) {
    clock_t end = clock();
    double time = (double)(end - startTime) / CLOCKS_PER_SEC;

    printf("\nTiming summary (MPI_Wtime, max across ranks):\n");
    printf("  Total time: %.6f seconds\n", time);
    printf("  Number of processors: %d\n", world_size);
  }

  if (world_rank == 0) {
    free(queries);
    btree_free(tree);
  } else if (queries) {
    free(queries);
  }
  if (owns_local_records && local_records) {
    free(local_records);
  }
  if (world_rank == 0) {
    free(records);
  }
  MPI_Finalize();
  return 0;
}

/*
Comparison and database loading utilities
*/
int car_compare(const void *a, const void *b, void *udata) {
  (void)udata;
  const CarInventory *ca = (const CarInventory *)a;
  const CarInventory *cb = (const CarInventory *)b;

  if (ca->ID < cb->ID) {
    return -1;
  } else if (ca->ID > cb->ID) {
    return 1;
  } else {
    return 0;
  }
}

struct btree *load_database(const char *filename) {
  FILE *fp = fopen(filename, "r");
  if (!fp) {
    perror("fopen");
    return NULL;
  }

  struct btree *tree = btree_new(sizeof(CarInventory), 0, car_compare, NULL);
  if (!tree) {
    fclose(fp);
    return NULL;
  }

  char header_line[256];
  if (!fgets(header_line, sizeof(header_line), fp)) {
    fprintf(stderr, "Error: Failed to read header from %s\n", filename);
    btree_free(tree);
    fclose(fp);
    return NULL;
  }

  CarInventory car;
  while (1) {
    int scanned = fscanf(fp, "%d %19s %d %19s %d %19s", &car.ID, car.Model,
                         &car.YearMake, car.Color, &car.Price, car.Dealer);
    if (scanned == EOF) {
      break;
    }
    if (scanned != 6) {
      fprintf(stderr, "Warning: malformed line encountered in %s\n", filename);
      break;
    }
    if (btree_set(tree, &car) == NULL && btree_oom(tree)) {
      fprintf(stderr, "Error: out of memory inserting ID=%d\n", car.ID);
      btree_free(tree);
      fclose(fp);
      return NULL;
    }
  }

  fclose(fp);
  return tree;
}

bool print_iter(const void *item, void *udata) {
  (void)udata;
  const CarInventory *car = (const CarInventory *)item;
  printf("%d %s %d %s %d %s\n", car->ID, car->Model, car->YearMake, car->Color,
         car->Price, car->Dealer);
  return true;
}

void print_all_tuples(struct btree *tree) {
  btree_ascend(tree, NULL, print_iter, NULL);
}

/*
Query parsing helpers
*/
static const char *skip_ws(const char *s) {
  while (*s && isspace((unsigned char)*s)) {
    s++;
  }
  return s;
}

static void trim_trailing(char *s) {
  size_t len = strlen(s);
  while (len > 0 && isspace((unsigned char)s[len - 1])) {
    s[--len] = '\0';
  }
  if (len > 0 && s[len - 1] == ';') {
    s[--len] = '\0';
    trim_trailing(s);
  }
}

void load_queries(const char *filename, Query **queries, int *num_queries) {
  FILE *fp = fopen(filename, "r");
  if (!fp) {
    perror("fopen queries");
    *queries = NULL;
    *num_queries = 0;
    return;
  }

  int capacity = 4;
  Query *arr = malloc(sizeof(Query) * capacity);
  if (!arr) {
    fprintf(stderr, "Error: out of memory allocating queries\n");
    fclose(fp);
    *queries = NULL;
    *num_queries = 0;
    return;
  }

  char line[512];
  *num_queries = 0;

  while (fgets(line, sizeof(line), fp)) {
    if (line[0] == '\n' || line[0] == '\0') {
      continue;
    }

    Query q;
    memset(&q, 0, sizeof(q));

    char *select_pos = strstr(line, "SELECT");
    char *from_pos = strstr(line, "FROM");
    char *where_pos = strstr(line, "WHERE");

    if (!select_pos || !from_pos || !where_pos) {
      fprintf(stderr, "Warning: skipping malformed query: %s", line);
      continue;
    }

    select_pos += strlen("SELECT");
    while (*select_pos && isspace((unsigned char)*select_pos)) {
      select_pos++;
    }

    if (from_pos <= select_pos) {
      fprintf(stderr, "Warning: malformed SELECT clause: %s", line);
      continue;
    }

    char select_part[256];
    memset(select_part, 0, sizeof(select_part));
    strncpy(select_part, select_pos, (size_t)(from_pos - select_pos));
    trim_trailing(select_part);

    char *token = strtok(select_part, ",");
    int idx = 0;
    while (token && idx < 6) {
      token = (char *)skip_ws(token);
      trim_trailing(token);
      strncpy(q.select_attrs[idx], token, sizeof(q.select_attrs[idx]) - 1);
      idx++;
      token = strtok(NULL, ",");
    }
    q.num_select_attrs = idx;

    where_pos += strlen("WHERE");
    while (*where_pos && isspace((unsigned char)*where_pos)) {
      where_pos++;
    }
    strncpy(q.where_raw, where_pos, sizeof(q.where_raw) - 1);
    q.where_raw[sizeof(q.where_raw) - 1] = '\0';
    trim_trailing(q.where_raw);

    if (*num_queries >= capacity) {
      capacity *= 2;
      Query *tmp = realloc(arr, sizeof(Query) * capacity);
      if (!tmp) {
        fprintf(stderr, "Error: out of memory reallocating queries\n");
        break;
      }
      arr = tmp;
    }

    arr[*num_queries] = q;
    (*num_queries)++;
  }

  fclose(fp);
  *queries = arr;
}

typedef enum { VAL_INT, VAL_STR } ValueType;

typedef struct {
  ValueType type;
  int i;
  char s[64];
} Value;

static bool read_identifier(const char **p, char *out, size_t cap) {
  const char *s = *p;
  size_t i = 0;
  s = skip_ws(s);
  if (!*s) {
    return false;
  }
  while (*s && (isalnum((unsigned char)*s) || *s == '_')) {
    if (i + 1 < cap) {
      out[i++] = *s;
    }
    s++;
  }
  out[i] = '\0';
  *p = s;
  return i > 0;
}

static bool read_value(const char **p, Value *v) {
  const char *s = skip_ws(*p);
  if (*s == '"') {
    size_t i = 0;
    s++;
    while (*s && *s != '"' && i + 1 < sizeof(v->s)) {
      v->s[i++] = *s++;
    }
    v->s[i] = '\0';
    if (*s == '"') {
      s++;
    }
    v->type = VAL_STR;
  } else {
    char *endptr;
    v->i = (int)strtol(s, &endptr, 10);
    if (endptr == s) {
      return false;
    }
    s = endptr;
    v->type = VAL_INT;
  }
  *p = s;
  return true;
}

static int compare_attr_value(const CarInventory *car, const char *attr,
                              const Value *v) {
  if (strcasecmp(attr, "ID") == 0 || strcasecmp(attr, "YearMake") == 0 ||
      strcasecmp(attr, "Price") == 0) {
    int lhs = 0;
    if (strcasecmp(attr, "ID") == 0) {
      lhs = car->ID;
    } else if (strcasecmp(attr, "YearMake") == 0) {
      lhs = car->YearMake;
    } else if (strcasecmp(attr, "Price") == 0) {
      lhs = car->Price;
    }
    int rhs = (v->type == VAL_INT) ? v->i : atoi(v->s);
    if (lhs < rhs) {
      return -1;
    }
    if (lhs > rhs) {
      return 1;
    }
    return 0;
  }

  const char *lhs = NULL;
  if (strcasecmp(attr, "Model") == 0) {
    lhs = car->Model;
  } else if (strcasecmp(attr, "Color") == 0) {
    lhs = car->Color;
  } else if (strcasecmp(attr, "Dealer") == 0) {
    lhs = car->Dealer;
  } else {
    return 0;
  }

  const char *rhs = (v->type == VAL_STR) ? v->s : "";
  return strcasecmp(lhs, rhs);
}

static bool eval_comparison(const CarInventory *car, const char **p) {
  char attr[32];
  char op[3] = {0};
  Value v;
  const char *s = skip_ws(*p);

  if (!read_identifier(&s, attr, sizeof(attr))) {
    return false;
  }

  s = skip_ws(s);
  if (*s == '!' && *(s + 1) == '=') {
    strcpy(op, "!=");
    s += 2;
  } else if (*s == '>' && *(s + 1) == '=') {
    strcpy(op, ">=");
    s += 2;
  } else if (*s == '<' && *(s + 1) == '=') {
    strcpy(op, "<=");
    s += 2;
  } else if (*s == '>') {
    strcpy(op, ">");
    s++;
  } else if (*s == '<') {
    strcpy(op, "<");
    s++;
  } else if (*s == '=') {
    strcpy(op, "=");
    s++;
  } else {
    return false;
  }

  if (!read_value(&s, &v)) {
    return false;
  }

  *p = s;

  int cmp = compare_attr_value(car, attr, &v);

  if (strcmp(op, "=") == 0) {
    return cmp == 0;
  } else if (strcmp(op, "!=") == 0) {
    return cmp != 0;
  } else if (strcmp(op, ">") == 0) {
    return cmp > 0;
  } else if (strcmp(op, "<") == 0) {
    return cmp < 0;
  } else if (strcmp(op, ">=") == 0) {
    return cmp >= 0;
  } else if (strcmp(op, "<=") == 0) {
    return cmp <= 0;
  }

  return false;
}

static bool eval_expr(const CarInventory *car, const char **p);
static bool eval_factor(const CarInventory *car, const char **p);

static bool eval_term(const CarInventory *car, const char **p) {
  bool result = eval_factor(car, p);
  const char *s = skip_ws(*p);

  while (strncasecmp(s, "AND", 3) == 0) {
    s += 3;
    *p = s;
    bool rhs = eval_factor(car, p);
    result = result && rhs;
    s = skip_ws(*p);
  }

  *p = s;
  return result;
}

static bool eval_factor(const CarInventory *car, const char **p) {
  const char *s = skip_ws(*p);
  bool result;

  if (*s == '(') {
    s++;
    *p = s;
    result = eval_expr(car, p);
    s = skip_ws(*p);
    if (*s == ')') {
      s++;
    }
    *p = s;
  } else {
    result = eval_comparison(car, &s);
    *p = s;
  }

  return result;
}

static bool eval_expr(const CarInventory *car, const char **p) {
  bool result = eval_term(car, p);
  const char *s = skip_ws(*p);
  while (strncasecmp(s, "OR", 2) == 0) {
    s += 2;
    *p = s;
    bool rhs = eval_term(car, p);
    result = result || rhs;
    s = skip_ws(*p);
  }
  *p = s;
  return result;
}

int match_where(const CarInventory *car, const char *where_raw) {
  const char *p = skip_ws(where_raw);
  if (*p == '\0') {
    return 1;
  }
  return eval_expr(car, &p) ? 1 : 0;
}

static bool append_selected(const CarInventory *car, const Query *q,
                            Buffer *buf) {
  if (q->num_select_attrs == 0 ||
      (q->num_select_attrs == 1 && strcmp(q->select_attrs[0], "*") == 0)) {
    return buffer_appendf(buf, "%d %s %d %s %d %s\n", car->ID, car->Model,
                          car->YearMake, car->Color, car->Price, car->Dealer);
  }

  for (int i = 0; i < q->num_select_attrs; ++i) {
    if (i > 0 && !buffer_append(buf, " ", 1)) {
      return false;
    }
    const char *attr = q->select_attrs[i];
    if (strcasecmp(attr, "ID") == 0) {
      if (!buffer_appendf(buf, "%d", car->ID)) {
        return false;
      }
    } else if (strcasecmp(attr, "Model") == 0) {
      if (!buffer_appendf(buf, "%s", car->Model)) {
        return false;
      }
    } else if (strcasecmp(attr, "YearMake") == 0) {
      if (!buffer_appendf(buf, "%d", car->YearMake)) {
        return false;
      }
    } else if (strcasecmp(attr, "Color") == 0) {
      if (!buffer_appendf(buf, "%s", car->Color)) {
        return false;
      }
    } else if (strcasecmp(attr, "Price") == 0) {
      if (!buffer_appendf(buf, "%d", car->Price)) {
        return false;
      }
    } else if (strcasecmp(attr, "Dealer") == 0) {
      if (!buffer_appendf(buf, "%s", car->Dealer)) {
        return false;
      }
    }
  }
  return buffer_append(buf, "\n", 1);
}