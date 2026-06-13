#!/bin/bash

echo
echo "===== INCARNATION ORDERING ====="
echo

for i in {1..50}
do
    ./cl put 5000 ACCOUNT $i &
done

for i in {51..100}
do
    ./cl put 5000 ACCOUNT $i &
done

wait

echo
echo "Result:"
echo

./cl get 5000 ACCOUNT

echo
echo "Expected:"
echo "Highest incarnation survives"
echo