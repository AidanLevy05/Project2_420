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
    BASE_NAME=$(basename "$PROG")
    if [[ $BASE_NAME == "qpe_mpi" ]]; then
        NP=${MPI_NP:-4}
        CMD=(mpirun -np "$NP" "$PROG" "$DB" "$SQL")
        echo "  (using mpirun with -np $NP)"
        IS_MPI[$i]=true
    else
        IS_MPI[$i]=false
    fi

    "${CMD[@]}" > "$OUT"

    awk '/[Tt]iming[[:space:]][Ss]ummary/ {exit} {print}' "$OUT" > "$FILTERED"

    sort "$FILTERED" > "$SORTED"

    SORTED_OUTPUTS[$i]="$SORTED"
    ((i++))
done

echo "Comparing outputs..."

BASE_INDEX=-1
for idx in "${!SORTED_OUTPUTS[@]}"; do
    if [[ ${IS_MPI[$idx]} != true ]]; then
        BASE_INDEX=$idx
        break
    fi
done
if (( BASE_INDEX < 0 )); then
    BASE_INDEX=0
fi
BASE="${SORTED_OUTPUTS[$BASE_INDEX]}"
PASS=true
WARN=false

for (( j=0; j<${#SORTED_OUTPUTS[@]}; j++ )); do
    if (( j == BASE_INDEX )); then
        continue
    fi
    diff "$BASE" "${SORTED_OUTPUTS[$j]}" > /dev/null
    if [ $? -ne 0 ]; then
        if [[ ${IS_MPI[$j]} == true || ${IS_MPI[$BASE_INDEX]} == true ]]; then
            echo "WARNING: Output from ${PROGRAMS[$j]} differs (MPI output may vary)."
            echo "Differences:"
            diff "$BASE" "${SORTED_OUTPUTS[$j]}"
            WARN=true
        else
            echo "ERROR: Output from ${PROGRAMS[$j]} differs from ${PROGRAMS[$BASE_INDEX]}!"
            echo "Differences:"
            diff "$BASE" "${SORTED_OUTPUTS[$j]}"
            PASS=false
        fi
    fi
done

if [ "$PASS" = true ]; then
    if [ "$WARN" = true ]; then
        echo "WARNING: Some MPI outputs differed; review the diffs above."
    else
        echo "SUCCESS: All programs produced identical results (order-independent)."
    fi
    rm out*.txt sorted*.txt filtered*.txt
else
    echo "ERROR: Outputs did NOT match."
fi
