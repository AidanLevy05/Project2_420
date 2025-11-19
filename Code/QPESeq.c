/*

QPESeq.c

Authors:
    Aidan Levy
    Maddie Powell
    Nick Corcoran
    Austin Phalines
    Dean Bullock

Creation Date: 11-11-2025

Description:

This program loads the car inventory database
from the file: ../db/db.txt

All tuples are stored inside a B-tree (btree.c / btree.h)
keyed by the primary key: ID.

The following columns are:
- ID
- Model
- YearMake
- Color
- Price
- Dealer

If the database contains 10 or fewer tuples,
the program prints all tuples to the console
for easy debugging.

*/

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <strings.h>

#include "../btree/btree.h"

/*
Struct Definitions
*/
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

/*
Function Prototypes
*/
int car_compare(const void *a, const void *b, void *udata);
struct btree *load_database(const char *filename);
void print_all_tuples(struct btree *tree);
bool print_iter(const void *item, void *udata);
void load_queries(const char *filename, Query **queries, int *num_queries);
void process_query(struct btree *tree, Query *q);
int match_where(const CarInventory *car, const char *where_raw);
void print_selected(const CarInventory *car, Query *q);

int main(int argc, char **argv) {

  const char *filename = "../db/db.txt";
  const char *queryfile = "../db/sql.txt";
  struct btree *tree;
  size_t count;

  if (argc >= 2) {
    filename = argv[1];
  }
  if (argc >= 3) {
    queryfile = argv[2];
  }

  tree = load_database(filename);
  if (tree == NULL) {
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
  for (int i = 0; i < num_queries; i++) {
    process_query(tree, &queries[i]);
  }

  free(queries);
  btree_free(tree);

  return 0;
}

/*
Function: car_compare()
Parameters: const void *a, const void *b, void *udata
Return: int
Description:

Comparison function for the B-tree.

Compares two CarInventory records by ID only.
Returns:
  -1 if a.ID < b.ID
   0 if a.ID == b.ID
   1 if a.ID > b.ID

This ensures the B-tree is keyed on the primary key ID.
*/
int car_compare(const void *a, const void *b, void *udata) {
  const CarInventory *ca = (const CarInventory *)a;
  const CarInventory *cb = (const CarInventory *)b;

  (void)udata;

  if (ca->ID < cb->ID) {
    return -1;
  } else if (ca->ID > cb->ID) {
    return 1;
  } else {
    return 0;
  }
}

/*
Function: load_database()
Parameters: const char *filename
Return: struct btree *
Description:

Opens the given filename (expected format: db.txt),
reads all tuples, and inserts them into a B-tree
keyed by ID.

The B-tree uses CarInventory as the element type.

On success, returns a pointer to the B-tree.
On failure (e.g., fopen error), returns NULL.
*/
struct btree *load_database(const char *filename) {
  FILE *fp;
  struct btree *tree;
  CarInventory car;
  char header_line[256];
  int scanned;

  fp = fopen(filename, "r");
  if (fp == NULL) {
    perror("fopen");
    return NULL;
  }

  tree = btree_new(sizeof(CarInventory), 0, car_compare, NULL);
  if (tree == NULL) {
    fprintf(stderr, "Error: btree_new failed\n");
    fclose(fp);
    return NULL;
  }

  if (fgets(header_line, sizeof(header_line), fp) == NULL) {
    fprintf(stderr, "Error: Failed to read header from %s\n", filename);
    fclose(fp);
    btree_free(tree);
    return NULL;
  }

  while (1) {
    scanned = fscanf(fp, "%d %19s %d %19s %d %19s", &car.ID, car.Model,
                     &car.YearMake, car.Color, &car.Price, car.Dealer);

    if (scanned == EOF) {
      break;
    }

    if (scanned != 6) {
      fprintf(stderr, "Warning: Malformed line encountered in %s\n", filename);
      break;
    }

    if (btree_set(tree, &car) == NULL && btree_oom(tree)) {
      fprintf(stderr, "Error: Out of memory inserting ID=%d\n", car.ID);
      fclose(fp);
      btree_free(tree);
      return NULL;
    }
  }

  fclose(fp);
  return tree;
}

/*
Function: print_iter()
Parameters: const void *item, void *udata
Return: bool
Description:

Callback used by btree_ascend to print each tuple.

Returns true to continue iteration.
*/
bool print_iter(const void *item, void *udata) {
  const CarInventory *car = (const CarInventory *)item;

  (void)udata;

  printf("%d %s %d %s %d %s\n", car->ID, car->Model, car->YearMake, car->Color,
         car->Price, car->Dealer);

  return true;
}

/*
Function: print_all_tuples()
Parameters: struct btree *tree
Return: Void
Description:

Iterates through all tuples in ascending order of ID
and prints them to the console using btree_ascend().
*/
void print_all_tuples(struct btree *tree) {
  btree_ascend(tree, NULL, print_iter, NULL);
}

/*
Function: load_queries()
Parameters: const char *filename, Query **queries, int *num_queries
Return: Void

Opens the given filename, parses each query,
and stores reults in Query** queries.

For each query:
- Parses the SELECT attribute list into select_attrs and num_select_attrs.
- Copies the full WHERE condition into where_raw.
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
  char line[512];
  int capacity = 4;
  Query *arr;

  if (!fp) {
    perror("fopen queries");
    *queries = NULL;
    *num_queries = 0;
    return;
  }

  arr = malloc(sizeof(Query) * capacity);
  if (!arr) {
    fprintf(stderr, "Error: out of memory allocating queries\n");
    fclose(fp);
    *queries = NULL;
    *num_queries = 0;
    return;
  }

  *num_queries = 0;

  while (fgets(line, sizeof(line), fp) != NULL) {
    Query q;
    char *select_pos;
    char *from_pos;
    char *where_pos;
    char select_part[256];
    char *token;
    int idx = 0;

    if (line[0] == '\n' || line[0] == '\0') {
      continue;
    }

    memset(&q, 0, sizeof(Query));

    select_pos = strstr(line, "SELECT");
    from_pos = strstr(line, "FROM");
    where_pos = strstr(line, "WHERE");

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

    memset(select_part, 0, sizeof(select_part));
    strncpy(select_part, select_pos, (size_t)(from_pos - select_pos));
    trim_trailing(select_part);

    token = strtok(select_part, ",");
    while (token && idx < 6) {
      token = (char *)skip_ws(token);
      trim_trailing(token);
      strncpy(q.select_attrs[idx], token, sizeof(q.select_attrs[idx]) - 1);
      q.select_attrs[idx][sizeof(q.select_attrs[idx]) - 1] = '\0';
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
    if (lhs < rhs) return -1;
    if (lhs > rhs) return 1;
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
    /* evaluate full expression inside parentheses */
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
  const char *p = where_raw;
  p = skip_ws(p);
  if (*p == '\0') {
    return 1;
  }
  return eval_expr(car, &p) ? 1 : 0;
}

void print_selected(const CarInventory *car, Query *q) {
  int i;
  bool first = true;

  if (q->num_select_attrs == 0 ||
      (q->num_select_attrs == 1 && strcmp(q->select_attrs[0], "*") == 0)) {
    printf("%d %s %d %s %d %s\n", car->ID, car->Model, car->YearMake,
           car->Color, car->Price, car->Dealer);
    return;
  }

  for (i = 0; i < q->num_select_attrs; i++) {
    const char *attr = q->select_attrs[i];
    if (!first) {
      printf(" ");
    }
    if (strcasecmp(attr, "ID") == 0) {
      printf("%d", car->ID);
    } else if (strcasecmp(attr, "Model") == 0) {
      printf("%s", car->Model);
    } else if (strcasecmp(attr, "YearMake") == 0) {
      printf("%d", car->YearMake);
    } else if (strcasecmp(attr, "Color") == 0) {
      printf("%s", car->Color);
    } else if (strcasecmp(attr, "Price") == 0) {
      printf("%d", car->Price);
    } else if (strcasecmp(attr, "Dealer") == 0) {
      printf("%s", car->Dealer);
    }
    first = false;
  }
  printf("\n");
}

typedef struct {
  Query *q;
} ProcessCtx;

static bool process_iter_cb(const void *item, void *udata) {
  const CarInventory *car = (const CarInventory *)item;
  ProcessCtx *ctx = (ProcessCtx *)udata;
  if (match_where(car, ctx->q->where_raw)) {
    print_selected(car, ctx->q);
  }
  return true;
}

void process_query(struct btree *tree, Query *q) {
  ProcessCtx ctx = {.q = q};
  btree_ascend(tree, NULL, process_iter_cb, &ctx);
}
