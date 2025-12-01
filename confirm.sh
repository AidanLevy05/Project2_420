#!/bin/bash

DB="./db/db.txt"
SQL="./db/sql.txt"


if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <program1> <program2> [program3 ...]"
    echo "Requires at least 2 programs to compare."
    exit 1
fi

PROGRAMS=("$@")

i=0
for PROG in "${PROGRAMS[@]}"; do
    OUT="out_$i.txt"
    FILTERED="filtered_$i.txt"
    SORTED="sorted_$i.txt"

    echo "Running: $PROG"

    CMD=("$PROG" "$DB" "$SQL")
    if [[ $(basename "$PROG") == "qpe_mpi" ]]; then
        NP=${MPI_NP:-4}
        CMD=(mpirun -np "$NP" "$PROG" "$DB" "$SQL")
        echo "  (using mpirun with -np $NP)"
    fi

    "${CMD[@]}" > "$OUT"

    awk '/[Tt]iming[[:space:]][Ss]ummary/ {exit} {print}' "$OUT" > "$FILTERED"

    sort "$FILTERED" > "$SORTED"

    SORTED_OUTPUTS[$i]="$SORTED"
    ((i++))
done

echo "Comparing outputs..."

BASE="${SORTED_OUTPUTS[0]}"
PASS=true

for (( j=1; j<${#SORTED_OUTPUTS[@]}; j++ )); do
    diff "$BASE" "${SORTED_OUTPUTS[$j]}" > /dev/null
    if [ $? -ne 0 ]; then
        echo "ERROR: Output from ${PROGRAMS[$j]} differs from ${PROGRAMS[0]}!"
        echo "Differences:"
        diff "$BASE" "${SORTED_OUTPUTS[$j]}"
        PASS=false
    fi
done

if [ "$PASS" = true ]; then
    echo "SUCCESS: All programs produced identical results (order-independent)."
    rm out*.txt sorted*.txt filtered*.txt
else
    echo "ERROR: Outputs did NOT match."
fi
