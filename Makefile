CC := gcc
MPICC := /usr/lib64/openmpi/bin/mpicc
BTREE_SRC := btree/btree.c
BTREE_INC := -Ibtree

SEQ_SRC := Code/QPESeq.c
OMP_SRC := Code/QPEOMP.c
MPI_SRC := Code/QPEMPI.c

BINARIES := qpe_seq qpe_omp qpe_mpi

.PHONY: all clean

all: $(BINARIES)

qpe_seq: $(SEQ_SRC) $(BTREE_SRC)
	$(CC) -Wall $^ $(BTREE_INC) -o $@

qpe_omp: $(OMP_SRC) $(BTREE_SRC)
	$(CC) -fopenmp -O2 -Wall $^ $(BTREE_INC) -o $@

qpe_mpi: $(MPI_SRC) $(BTREE_SRC)
	$(MPICC) -Wall -Wextra -g $^ $(BTREE_INC) -o $@

clean:
	$(RM) $(BINARIES)
