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

typedef struct {
  CarInventory *arr;
  size_t index;
} ToArrayCtx;

typedef enum { VAL_INT, VAL_STR } ValueType;

typedef struct {
  ValueType type;
  int i;
  char s[64];
} Value;

enum {
  TAG_RECORD_COUNT = 1,
  TAG_RECORD_DATA = 2,
};

/*
Function prototypes
*/
static void buffer_init(Buffer *buf);
static void buffer_free(Buffer *buf);
static bool buffer_reserve(Buffer *buf, size_t needed);
static bool buffer_append(Buffer *buf, const char *data, size_t len);
static bool buffer_appendf(Buffer *buf, const char *fmt, ...);
static void bcast_bytes(void *data, size_t bytes, int root, MPI_Comm comm);
static void send_bytes(const void *data, size_t bytes, int dest, int tag,
                       MPI_Comm comm);
static void recv_bytes(void *data, size_t bytes, int src, int tag,
                       MPI_Comm comm);
static void compute_bounds(long long total, int size, int rank,
                           long long *start, long long *end);
static bool to_array_cb(const void *item, void *udata);
static CarInventory *btree_to_array(struct btree *tree, size_t *out_count);
int car_compare(const void *a, const void *b, void *udata);
struct btree *load_database(const char *filename);
bool print_iter(const void *item, void *udata);
void print_all_tuples(struct btree *tree);
static const char *skip_ws(const char *s);
static void trim_trailing(char *s);
void load_queries(const char *filename, Query **queries, int *num_queries);
static bool read_identifier(const char **p, char *out, size_t cap);
static bool read_value(const char **p, Value *v);
static int compare_attr_value(const CarInventory *car, const char *attr,
                              const Value *v);
static bool eval_comparison(const CarInventory *car, const char **p);
static bool eval_term(const CarInventory *car, const char **p);
static bool eval_factor(const CarInventory *car, const char **p);
static bool eval_expr(const CarInventory *car, const char **p);
int match_where(const CarInventory *car, const char *where_raw);
static bool append_selected(const CarInventory *car, const Query *q,
                            Buffer *buf);

/*
Name: main():
Parameters: int argc, char **argv
Return: int
Description:

Initializes MPI, loads the database and queries on rank 0, distributes records
to every rank, runs WHERE clause evaluation locally, and prints results in rank
order before reporting aggregate timing.
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
Buffer helpers
*/
/*
Name: buffer_init():
Parameters: Buffer *buf
Return: void
Description:

Initializes a Buffer by zeroing its metadata so subsequent append operations
can grow it dynamically.
*/
static void buffer_init(Buffer *buf) {
  buf->data = NULL;
  buf->len = 0;
  buf->cap = 0;
}

/*
Name: buffer_free():
Parameters: Buffer *buf
Return: void
Description:

Releases any allocated storage backing the buffer and resets the bookkeeping
fields.
*/
static void buffer_free(Buffer *buf) {
  free(buf->data);
  buf->data = NULL;
  buf->len = 0;
  buf->cap = 0;
}

/*
Name: buffer_reserve():
Parameters: Buffer *buf, size_t needed
Return: bool
Description:

Ensures the buffer has at least the requested capacity by reallocating in
doubling chunks; returns false on allocation failure.
*/
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

/*
Name: buffer_append():
Parameters: Buffer *buf, const char *data, size_t len
Return: bool
Description:

Appends raw bytes to the buffer, automatically reserving extra capacity and
maintaining a trailing null terminator.
*/
static bool buffer_append(Buffer *buf, const char *data, size_t len) {
  if (!buffer_reserve(buf, buf->len + len + 1)) {
    return false;
  }
  memcpy(buf->data + buf->len, data, len);
  buf->len += len;
  buf->data[buf->len] = '\0';
  return true;
}

/*
Name: buffer_appendf():
Parameters: Buffer *buf, const char *fmt, ...
Return: bool
Description:

Formats text using printf semantics directly into the buffer's append position,
growing the storage when required.
*/
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

/*
Name: bcast_bytes():
Parameters: void *data, size_t bytes, int root, MPI_Comm comm
Return: void
Description:

Broadcasts large buffers by chunking them so the MPI layer never exceeds INT_MAX
counts, ensuring every rank receives the serialized data.
*/
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

/*
Name: send_bytes():
Parameters: const void *data, size_t bytes, int dest, int tag, MPI_Comm comm
Return: void
Description:

Transmits arbitrary-sized buffers from the root rank by repeatedly sending
INT_MAX-sized chunks tagged appropriately.
*/
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

/*
Name: recv_bytes():
Parameters: void *data, size_t bytes, int src, int tag, MPI_Comm comm
Return: void
Description:

Receives arbitrarily large payloads piecewise, mirroring send_bytes() to
reconstruct the original buffer from the root.
*/
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

/*
Name: compute_bounds():
Parameters: long long total, int size, int rank, long long *start, long long *end
Return: void
Description:

Divides the global record count into contiguous ranges across MPI ranks,
ensuring a near-even distribution with at most one extra record per lower rank.
*/
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

/*
Name: to_array_cb():
Parameters: const void *item, void *udata
Return: bool
Description:

btree_ascend callback that copies each record into a linear array while
tracking the current insertion slot.
*/
static bool to_array_cb(const void *item, void *udata) {
  ToArrayCtx *ctx = (ToArrayCtx *)udata;
  ctx->arr[ctx->index++] = *(const CarInventory *)item;
  return true;
}

/*
Name: btree_to_array():
Parameters: struct btree *tree, size_t *out_count
Return: CarInventory *
Description:

Materializes the B-tree contents into a malloc'd array and returns both the
pointer and tuple count so MPI can scatter contiguous chunks to ranks.
*/
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
Name: car_compare():
Parameters: const void *a, const void *b, void *udata
Return: int
Description:

Comparison callback for the B-tree that orders CarInventory structures by ID,
ignoring the user data pointer.
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

/*
Name: load_database():
Parameters: const char *filename
Return: struct btree *
Description:

Opens the inventory file, loads each tuple into a B-tree keyed by ID, and
returns the populated structure or NULL if any error occurs.
*/
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

/*
Name: print_iter():
Parameters: const void *item, void *udata
Return: bool
Description:

btree iteration callback that prints a formatted CarInventory record and
continues traversal by returning true.
*/
bool print_iter(const void *item, void *udata) {
  (void)udata;
  const CarInventory *car = (const CarInventory *)item;
  printf("%d %s %d %s %d %s\n", car->ID, car->Model, car->YearMake, car->Color,
         car->Price, car->Dealer);
  return true;
}

/*
Name: print_all_tuples():
Parameters: struct btree *tree
Return: void
Description:

Walks the B-tree from smallest to largest ID and prints every tuple, useful on
small datasets to verify correctness before MPI distribution.
*/
void print_all_tuples(struct btree *tree) {
  btree_ascend(tree, NULL, print_iter, NULL);
}

/*
Name: skip_ws():
Parameters: const char *s
Return: const char *
Description:

Moves a pointer past leading whitespace characters and returns the first
non-space location within a SQL-like clause.
*/
static const char *skip_ws(const char *s) {
  while (*s && isspace((unsigned char)*s)) {
    s++;
  }
  return s;
}

/*
Name: trim_trailing():
Parameters: char *s
Return: void
Description:

Removes trailing whitespace and semicolons from a mutable string so later
comparisons operate on clean tokens.
*/
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

/*
Name: load_queries():
Parameters: const char *filename, Query **queries, int *num_queries
Return: void
Description:

Reads each query line from disk, extracts the SELECT attribute list and raw
WHERE clause, and stores them in a reallocating array of Query structures.
*/
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
/*
Name: read_identifier():
Parameters: const char **p, char *out, size_t cap
Return: bool
Description:

Parses an identifier token from the WHERE clause string, writing it into the
provided buffer and advancing the caller pointer when successful.
*/
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

/*
Name: read_value():
Parameters: const char **p, Value *v
Return: bool
Description:

Parses either a quoted string or an integer literal into a Value struct so
comparison logic can operate uniformly regardless of type.
*/
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

/*
Name: compare_attr_value():
Parameters: const CarInventory *car, const char *attr, const Value *v
Return: int
Description:

Extracts the named attribute from a car record, converts it if necessary, and
compares it to the provided literal returning -1, 0, or 1.
*/
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

/*
Name: eval_comparison():
Parameters: const CarInventory *car, const char **p
Return: bool
Description:

Parses a single comparison (attribute operator literal) from the WHERE clause
and evaluates it immediately against the provided record.
*/
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

/*
Name: eval_term():
Parameters: const CarInventory *car, const char **p
Return: bool
Description:

Evaluates AND-separated factors while advancing the parse pointer, combining the
results of eval_factor() calls.
*/
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

/*
Name: eval_factor():
Parameters: const CarInventory *car, const char **p
Return: bool
Description:

Handles parentheses or single comparisons when evaluating WHERE clauses, making
recursive calls to eval_expr() as needed.
*/
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

/*
Name: eval_expr():
Parameters: const CarInventory *car, const char **p
Return: bool
Description:

Evaluates OR-separated terms to compute the final truth value of the WHERE
clause for a record.
*/
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

/*
Name: match_where():
Parameters: const CarInventory *car, const char *where_raw
Return: int
Description:

Skips leading whitespace then invokes the recursive expression parser,
returning 1 if the record satisfies the WHERE predicate or 0 otherwise.
*/
int match_where(const CarInventory *car, const char *where_raw) {
  const char *p = skip_ws(where_raw);
  if (*p == '\0') {
    return 1;
  }
  return eval_expr(car, &p) ? 1 : 0;
}

/*
Name: append_selected():
Parameters: const CarInventory *car, const Query *q, Buffer *buf
Return: bool
Description:

Formats either all attributes or the requested subset into the provided Buffer,
appending a newline so rank-ordered output can be printed later.
*/
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
