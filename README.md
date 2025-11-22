# Project2_420
By: Aidan James. L, Austin P, Dean B, Maddie P, Nicholas C
For: COSC 420 - Dr. Yaping Jing

# How to compile the programs

To compile `QPESeq.c`, run this at project `/`:

```{bash}
gcc -Wall Code/QPESeq.c btree/btree.c -Ibtree -o qpe_seq
```

To compile `QPEOMP.c`, run this at project `/`:

```{bash}
gcc -fopenmp -O2 -Wall Code/QPEOMP.c btree/btree.c -Ibtree -o qpe_omp
```

To compile `QPEMPI.c`, run this at project `/`:

```{bash}
Compilation command goes here
```

# How to run the programs

To run `QPESeq.c`, run this at project `/`:

```{bash}
./qpe_seq ./db/db.txt ./db/sql.txt
```

To run `QPEOMP.c`, run this at project `/`:

```{bash}
./qpe_omp ./db/db.txt ./db/sql.txt
```

To run `QPEMPI.c`, run this at project `/`:

```{bash}
./qpe_mpi ./db/db.txt ./db/sql.txt
```

# How to confirm outputs are the same

In order to confirm that the outputs are the same, we do not
want to go through and manually confirm outputs. So this bash
program does it. 

You need to enter a minimum of two files and it compares
the output of the files and sees if the text matches,
regardless of order.

To run `confirm.sh`, run this at project `/`:

```{bash}
./confirm.sh <program1> <program2> [program3 ...]
```

Example usage:

```{bash}
./confirm.sh ./qpe_seq ./qpe_omp
```

# How to generate data into `db.txt`

You have two choices- you can use `dataGen.c` or `dataGenParallel.c`.

To compile and run `dataGen.c`:

```{bash}
gcc -o dataGen dataGen.c
./dataGen <n>
```

To compile and run `dataGenParallel.c`:

```{bash}
gcc -fopenmp dataGenParallel.c -o dataGenParallel
```

# TODO
- Dean: 
	- ~~Implement dataGenParallel.c~~
	- Start working QPESeq.c -- Do not overwrite with Nick -- Clearly communicate when adding features to QPESeq.c
	- ~~Implemented QPEMPI.c~~
- Maddie: 
	- Create 20 test cases for sql.txt
	- Implemented QPEOMP.c
- Nick: 
	- Read and understand BTree Documentation
	- Start working QPESeq.c -- Do not overwrite with Dean -- Clearly communicate when adding features to QPESeq.c
	- Implemented QPEMPI.c
- Austin: 
	- Read and understand BTree Documentation
	- Implemented QPEMPI.c
- Aidan: 
	- Read and understand BTree Documentation
	- ~~Implemented QPEOMP.c~~
	- Wrote confirm.sh 
