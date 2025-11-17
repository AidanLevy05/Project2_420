/*

dataGen.c

Authors:
    Aidan Levy
    Maddie Powell
    Nick Corcoran
    Austin Phalines
    Dean bullock

Creation Date: 11-11-2025

Description:

This program generates sample data
inside of the file: ../db/db.txt

The following colums are:
- ID
- Mode
- YearMake
- Color
- Price
- Dealer

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <omp.h>

/*
Function Prototypes
*/
void generate_data(const char *filename, long n);
int random_price(const char *model, int year);

int main(int argc, char **argv) {

  long n;
  const char *filename = "../db/db.txt";
  srand(time(NULL));

  if (argc < 2) {
    printf("Usage: ./dataGen <n>\n");
    return 1;
  }

  n = atoi(argv[1]);
  if (n < 1) {
    printf("Error: Number of tuples must be > 0\n");
    return 1;
  }

  generate_data(filename, n);

  return 0;
}

/*
Function: generate_data()
Parameters: const char *filename, long n
Return: Void
Description:

Generates n tuples and write them to filename.
If n <= 10, it also prints all tuples to consolse
for easy debugging.
*/
void generate_data(const char *filename, long n) 
{
	FILE *fp;
	long i;
	int printToConsole = 0;

	const char *models[] = {"Accord", "Corolla", "Civic",
                          "Maxima", "Focus",   "Camry"};

	const int numModels = 6;

	const int years[] = {2000, 2013, 2015, 2016, 2018, 2020, 2021, 2023};
	const int numYears = 8;

	const char *colors[] = {"Gray", "White", "Blue", "Red", "Green", "Black"};
	const int numColors = 6;

	const char *dealers[] = {"Pohanka", "AutoNation", "Mitsubishi",
                           "Sonic",   "Suburban",   "Atlantic",
                           "Ganley",  "Victory",    "GM"};
	const int numDealers = 9;

	fp = fopen(filename, "w");

	if (fp == NULL) 
	{
    		perror("fopen");
    		exit(1);
  	}

  	if (n <= 10) 
	{
    		printToConsole = 1;
  	}

  	
	fprintf(fp, "ID Model YearMake Color Price Dealer\n");
  	if (printToConsole) 
	{
    		printf("ID Model YearMake Color Price Dealer\n");
  	}
	#pragma omp parallel 
	{
		int numofThreads = omp_get_num_threads();
		int idtracker = omp_get_thread_num();
		#pragma omp parallel for shared (idtracker, n, numofThreads) private(i) 
				for (int i=0; i < n; i++) {
    				int id = 1000 + idtracker;
    				const char *model = models[rand() % numModels];
    				int year = years[rand() % numYears];
    				const char *color = colors[rand() % numColors];
    				const char *dealer = dealers[rand() % numDealers];
    				int price = random_price(model, year);	
    				fprintf(fp, "%d %s %d %s %d %s\n",id, model, year, color, price, dealer);
				//printf("Numer of threads %d\n", omp_get_num_threads());				
				#pragma omp atomic
			
				idtracker+=numofThreads;
    		if (printToConsole) {
     	 	printf("Tnum=%d, %d %s %d %s %d %s\n",idtracker, id, model, year, color, price, dealer);
    				}
    			}
	}
	fclose(fp);
}

/*
Function: random_price()
Parameters: const char *model, int year
Return: int
Description:

Estimated price generator based on model and year
*/
int random_price(const char *model, int year) {
  int base;

  if (strcmp(model, "Accord") == 0 || strcmp(model, "Camry") == 0) {
    base = 16000;
  } else if (strcmp(model, "Civic") == 0 || strcmp(model, "Corolla") == 0) {
    base = 15000;
  } else if (strcmp(model, "Maxima") == 0) {
    base = 17000;
  } else {
    base = 14000;
  }

  base += (year - 2010) * 500;
  base += (rand() % 4000) - 2000;
  if (base < 5000)
    base = 5000;

  return base;
}
