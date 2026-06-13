#!/bin/bash

echo
echo "===== YCSB-A ====="
echo

for i in {1..100}
do
    ./cl put 5000 K$i $i
done

START=$(date +%s)

for i in {1..500}
do
    R=$((RANDOM%2))

    if [ $R -eq 0 ]
    then
        ./cl get 5000 K$((RANDOM%100)) &
    else
        ./cl put 5000 K$((RANDOM%100)) $RANDOM &
    fi
done

wait

END=$(date +%s)

echo
echo "Elapsed: $((END-START)) sec"
echo