#!/bin/bash

echo
echo "===== UNIFORM WORKLOAD ====="
echo

START=$(date +%s)

for i in {1..1000}
do
    ./cl put 5000 KEY$i $i &
done

wait

END=$(date +%s)

echo
echo "Elapsed: $((END-START)) sec"
echo

./cl get 5000 KEY1
./cl get 5000 KEY500
./cl get 5000 KEY1000

echo
echo "Expected:"
echo "All keys retrievable"
echo