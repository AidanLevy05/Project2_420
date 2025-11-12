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
// void load_queries(const char *filename, Query **queries, int *num_queries);
// void process_query(struct btree *tree, Query *q);
// int match_where(const CarInventory *car, const char *where_raw);
// void print_selected(const CarInventory *car, Query *q);

int main(int argc, char **argv) {

  const char *filename = "../db/db.txt";
  struct btree *tree;
  size_t count;

  if (argc >= 2) {
    filename = argv[1];
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

  (void)udata; /* unused */

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
