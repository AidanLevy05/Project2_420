#include <ctype.h>
#include <omp.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

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
  CarInventory *arr;
  size_t index;
} ToArrayCtx;

typedef enum { VAL_INT, VAL_STR } ValueType;

typedef struct {
  ValueType type;
  int i;
  char s[64];
} Value;

static bool to_array_cb(const void *item, void *udata);
CarInventory *btree_to_array(struct btree *tree, size_t *out_count);
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
void print_selected(const CarInventory *car, Query *q);
void process_query(struct btree *tree, Query *q);

/*
Name: main():
Parameters: int argc, char **argv
Return: int
Description:

Initializes the OpenMP runtime, loads the database and queries, and uses a
parallel for loop to distribute query execution across threads while timing the
overall runtime.
*/
int main(int argc, char **argv) {
  double par_start = omp_get_wtime();

  const char *filename = "db/db.txt";
  const char *queryfile = "db/sql.txt";
  struct btree *tree;
  size_t count;

  int thread_num = atoi(argv[3]);
  omp_set_num_threads(thread_num);

  if (argc >= 2)
    filename = argv[1];
  if (argc >= 3)
    queryfile = argv[2];

  tree = load_database(filename);
  if (!tree) {
    fprintf(stderr, "Error: Failed to load database from %s\n", filename);
    return 1;
  }

  count = btree_count(tree);
  printf("Loaded %zu tuples from %s\n", count, filename);

  if (count <= 10) {
    printf("Printing all tuples for debugging:\n");
    print_all_tuples(tree);
  }

  Query *queries = NULL;
  int num_queries = 0;
  load_queries(queryfile, &queries, &num_queries);
  printf("Processing %d queries from %s\n", num_queries, queryfile);

/*

Parallel section: #pragma omp parallel for default(none) shared(tree, quieries,
num_queries)

Parallelizing the process_query function so different threads process different
queries

*/
#pragma omp parallel for default(none) shared(tree, queries, num_queries)
  for (int i = 0; i < num_queries; i++) {
    process_query(tree, &queries[i]);
  }

  free(queries);
  btree_free(tree);

  double totalPar = omp_get_wtime() - par_start;
  printf("\nTiming summary (omp_get_wtime):\n");
  printf("  Number of threads: %d\n", thread_num);
  printf("  Parallel time: %.6f seconds\n", totalPar);

  return 0;
}

/*
Name: to_array_cb():
Parameters: const void *item, void *udata
Return: bool
Description:

btree_ascend callback that copies each CarInventory snapshot into a flat array
and tracks the next insertion index stored inside ToArrayCtx.
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

Materializes the contents of the B-tree into a contiguous array so OpenMP
threads can process slices without holding tree locks; returns the array pointer
and count via out_count.
*/
CarInventory *btree_to_array(struct btree *tree, size_t *out_count) {
  size_t count = btree_count(tree);
  CarInventory *arr = malloc(count * sizeof(CarInventory));
  if (!arr)
    return NULL;
  ToArrayCtx ctx;
  ctx.arr = arr;
  ctx.index = 0;
  btree_ascend(tree, NULL, to_array_cb, &ctx);
  *out_count = count;
  return arr;
}

/*
Name: car_compare():
Parameters: const void *a, const void *b, void *udata
Return: int
Description:

Comparator passed to the B-tree that orders CarInventory records by ID, ignoring
user data.
*/
int car_compare(const void *a, const void *b, void *udata) {
  (void)udata;
  const CarInventory *ca = (const CarInventory *)a;
  const CarInventory *cb = (const CarInventory *)b;
  if (ca->ID < cb->ID)
    return -1;
  if (ca->ID > cb->ID)
    return 1;
  return 0;
}

/*
Name: load_database():
Parameters: const char *filename
Return: struct btree *
Description:

Reads the car inventory file, inserts each tuple into a B-tree keyed by ID, and
returns the populated tree or NULL if an error occurs.
*/
struct btree *load_database(const char *filename) {
  FILE *fp = fopen(filename, "r");
  if (!fp)
    return NULL;

  struct btree *tree = btree_new(sizeof(CarInventory), 0, car_compare, NULL);
  if (!tree) {
    fclose(fp);
    return NULL;
  }

  char header_line[256];
  if (!fgets(header_line, sizeof(header_line), fp)) {
    btree_free(tree);
    fclose(fp);
    return NULL;
  }

  CarInventory car;
  while (1) {
    int scanned = fscanf(fp, "%d %19s %d %19s %d %19s", &car.ID, car.Model,
                         &car.YearMake, car.Color, &car.Price, car.Dealer);
    if (scanned == EOF)
      break;
    if (scanned != 6)
      break;
    if (btree_set(tree, &car) == NULL && btree_oom(tree)) {
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

btree_ascend callback that prints a car record and returns true so the traversal
continues.
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

Walks the B-tree in ascending order and prints each record, which helps verify
small datasets prior to running parallel queries.
*/
void print_all_tuples(struct btree *tree) {
  btree_ascend(tree, NULL, print_iter, NULL);
}

/*
Name: skip_ws():
Parameters: const char *s
Return: const char *
Description:

Advances past leading whitespace characters while parsing SQL-like input and
returns the first non-whitespace position.
*/
static const char *skip_ws(const char *s) {
  while (*s && isspace((unsigned char)*s))
    s++;
  return s;
}

/*
Name: trim_trailing():
Parameters: char *s
Return: void
Description:

Removes trailing whitespace and semicolons from strings in-place to simplify
later comparisons.
*/
static void trim_trailing(char *s) {
  size_t len = strlen(s);
  while (len > 0 && isspace((unsigned char)s[len - 1]))
    s[--len] = '\0';
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

Parses SELECT attribute lists and WHERE clauses from the query file, storing the
results inside a dynamically resized array of Query structs.
*/
void load_queries(const char *filename, Query **queries, int *num_queries) {
  FILE *fp = fopen(filename, "r");
  if (!fp) {
    *queries = NULL;
    *num_queries = 0;
    return;
  }

  int capacity = 4;
  Query *arr = malloc(capacity * sizeof(Query));
  if (!arr) {
    fclose(fp);
    *queries = NULL;
    *num_queries = 0;
    return;
  }

  char line[512];
  *num_queries = 0;

  while (fgets(line, sizeof(line), fp)) {
    if (line[0] == '\n' || line[0] == '\0')
      continue;

    Query q;
    memset(&q, 0, sizeof(q));

    char *select_pos = strstr(line, "SELECT");
    char *from_pos = strstr(line, "FROM");
    char *where_pos = strstr(line, "WHERE");

    if (!select_pos || !from_pos || !where_pos)
      continue;

    select_pos += 6;
    while (*select_pos && isspace((unsigned char)*select_pos))
      select_pos++;

    if (from_pos <= select_pos)
      continue;

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

    where_pos += 5;
    while (*where_pos && isspace((unsigned char)*where_pos))
      where_pos++;
    strncpy(q.where_raw, where_pos, sizeof(q.where_raw) - 1);
    trim_trailing(q.where_raw);

    if (*num_queries >= capacity) {
      capacity *= 2;
      Query *tmp = realloc(arr, capacity * sizeof(Query));
      if (!tmp)
        break;
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

Consumes an identifier token from the SQL-like query string, writes it to the
output buffer, and advances the caller pointer.
*/
static bool read_identifier(const char **p, char *out, size_t cap) {
  const char *s = *p;
  size_t i = 0;
  s = skip_ws(s);
  if (!*s)
    return false;
  while (*s && (isalnum((unsigned char)*s) || *s == '_')) {
    if (i + 1 < cap)
      out[i++] = *s;
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

Parses the next literal (integer or quoted string) into a Value structure so
comparison logic can treat both uniformly.
*/
static bool read_value(const char **p, Value *v) {
  const char *s = skip_ws(*p);
  if (*s == '"') {
    size_t i = 0;
    s++;
    while (*s && *s != '"' && i + 1 < sizeof(v->s))
      v->s[i++] = *s++;
    v->s[i] = '\0';
    if (*s == '"')
      s++;
    v->type = VAL_STR;
  } else {
    char *endptr;
    v->i = (int)strtol(s, &endptr, 10);
    if (endptr == s)
      return false;
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

Retrieves the specified attribute from a car record, converts it as needed, and
compares it against the given literal returning -1/0/1.
*/
static int compare_attr_value(const CarInventory *car, const char *attr,
                              const Value *v) {
  if (strcasecmp(attr, "ID") == 0 || strcasecmp(attr, "YearMake") == 0 ||
      strcasecmp(attr, "Price") == 0) {

    int lhs = 0;
    if (strcasecmp(attr, "ID") == 0)
      lhs = car->ID;
    else if (strcasecmp(attr, "YearMake") == 0)
      lhs = car->YearMake;
    else if (strcasecmp(attr, "Price") == 0)
      lhs = car->Price;

    int rhs = (v->type == VAL_INT) ? v->i : atoi(v->s);
    if (lhs < rhs)
      return -1;
    if (lhs > rhs)
      return 1;
    return 0;
  }

  const char *lhs = NULL;
  if (strcasecmp(attr, "Model") == 0)
    lhs = car->Model;
  else if (strcasecmp(attr, "Color") == 0)
    lhs = car->Color;
  else if (strcasecmp(attr, "Dealer") == 0)
    lhs = car->Dealer;
  else
    return 0;

  const char *rhs = (v->type == VAL_STR) ? v->s : "";
  return strcasecmp(lhs, rhs);
}

/*
Name: eval_comparison():
Parameters: const CarInventory *car, const char **p
Return: bool
Description:

Parses a single comparison (attribute operator literal) from the WHERE clause
text and immediately evaluates it against the active record.
*/
static bool eval_comparison(const CarInventory *car, const char **p) {
  char attr[32];
  char op[3] = {0};
  Value v;

  const char *s = skip_ws(*p);
  if (!read_identifier(&s, attr, sizeof(attr)))
    return false;

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
  } else
    return false;

  if (!read_value(&s, &v))
    return false;
  *p = s;

  int cmp = compare_attr_value(car, attr, &v);

  if (strcmp(op, "=") == 0)
    return cmp == 0;
  if (strcmp(op, "!=") == 0)
    return cmp != 0;
  if (strcmp(op, ">") == 0)
    return cmp > 0;
  if (strcmp(op, "<") == 0)
    return cmp < 0;
  if (strcmp(op, ">=") == 0)
    return cmp >= 0;
  if (strcmp(op, "<=") == 0)
    return cmp <= 0;

  return false;
}

static bool eval_expr(const CarInventory *car, const char **p);
static bool eval_factor(const CarInventory *car, const char **p);

/*
Name: eval_term():
Parameters: const CarInventory *car, const char **p
Return: bool
Description:

Evaluates AND-connected factors, calling eval_factor recursively and combining
results until an OR or end of input is reached.
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

Handles parenthesized expressions or single comparisons when parsing WHERE
clauses, serving as the base of the recursive descent parser.
*/
static bool eval_factor(const CarInventory *car, const char **p) {
  const char *s = skip_ws(*p);
  bool result;

  if (*s == '(') {
    s++;
    *p = s;
    result = eval_expr(car, p);
    s = skip_ws(*p);
    if (*s == ')')
      s++;
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

Evaluates OR-connected terms, yielding the overall truthiness of the WHERE
clause text for a given record.
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

Trims the clause, invokes the recursive parser, and returns 1 when the record
satisfies the WHERE expression (0 otherwise).
*/
int match_where(const CarInventory *car, const char *where_raw) {
  const char *p = skip_ws(where_raw);
  if (*p == '\0')
    return 1;
  return eval_expr(car, &p) ? 1 : 0;
}

/*
Name: print_selected():
Parameters: const CarInventory *car, Query *q
Return: void
Description:

Prints either all fields or the subset requested in the query; since multiple
threads may print concurrently, the body executes under an OpenMP critical
section to serialize stdout access.
*/
void print_selected(const CarInventory *car, Query *q) {
/*

Parallel Section: #pragma omp critical(print_lock)

This section parallelizing the printing. It will be out of order
(compared to the sequential portion), but that does not matter
since the results are the same.

*/
#pragma omp critical(print_lock)
  {
    if (q->num_select_attrs == 0 ||
        (q->num_select_attrs == 1 && strcmp(q->select_attrs[0], "*") == 0)) {
      printf("%d %s %d %s %d %s\n", car->ID, car->Model, car->YearMake,
             car->Color, car->Price, car->Dealer);
    } else {
      bool first = true;
      for (int i = 0; i < q->num_select_attrs; i++) {
        const char *attr = q->select_attrs[i];
        if (!first)
          printf(" ");
        if (!strcasecmp(attr, "ID"))
          printf("%d", car->ID);
        else if (!strcasecmp(attr, "Model"))
          printf("%s", car->Model);
        else if (!strcasecmp(attr, "YearMake"))
          printf("%d", car->YearMake);
        else if (!strcasecmp(attr, "Color"))
          printf("%s", car->Color);
        else if (!strcasecmp(attr, "Price"))
          printf("%d", car->Price);
        else if (!strcasecmp(attr, "Dealer"))
          printf("%s", car->Dealer);
        first = false;
      }
      printf("\n");
    }
  }
}

/*
Name: process_query():
Parameters: struct btree *tree, Query *q
Return: void
Description:

Materializes the B-tree into an array and uses an OpenMP parallel for loop with
dynamic scheduling so threads evaluate disjoint record ranges concurrently.
*/
void process_query(struct btree *tree, Query *q) {
  size_t count = 0;
  CarInventory *arr = btree_to_array(tree, &count);
  if (!arr || count == 0)
    return;

/*

Parallel Section: #pragma omp parallel for schedule(dynamic)

Parallelizes the for loop and distributes the iterations
among multiple threads

*/
#pragma omp parallel for schedule(dynamic)
  for (size_t i = 0; i < count; i++) {
    if (match_where(&arr[i], q->where_raw)) {
      print_selected(&arr[i], q);
    }
  }

  free(arr);
}
