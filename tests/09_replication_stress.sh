#!/bin/bash

echo
echo "===== REPLICATION STRESS ====="
echo

START=$(date +%s)

for i in {1..5000}
do
    ./cl put 5000 KEY$i $i &
done

wait

END=$(date +%s)

echo
echo "Elapsed: $((END-START)) sec"
echo

./cl get 5000 KEY1
./cl get 5000 KEY2500
./cl get 5000 KEY5000

echo
echo "Expected:"
echo "No lost writes"
echo