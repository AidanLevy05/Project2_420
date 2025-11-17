/*
QPEPar.c

Authors:
    Nick Corcoran
    Dean Bullock

Creation Date: 11-17-2025

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
#include <mpi.c>
#include "../btree/btree.h"

/* Struct Definitions */
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

/* Function Prototypes */
int car_compare(const void *a, const void *b, void *udata); //Dean Bullcock
struct btree *load_database(const char *filename); //Nick Cock
void print_all_tuples(struct btree *tree); //Dean
bool print_iter(const void *item, void *udata); //Nick
void load_queries(const char *filename, Query **queries, int *num_queries); //Dean
void process_query(struct btree *tree, Query *q); //Nick
int match_where(const CarInventory *car, const char *where_raw); //Dean
void print_selected(const CarInventory *car, Query *q); //Deam

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

