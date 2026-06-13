#!/bin/bash

echo
echo "===== SKEWED WORKLOAD ====="
echo

START=$(date +%s)

for i in {1..1000}
do
    ./cl put 5000 HOTKEY $i &
done

wait

END=$(date +%s)

echo
echo "Elapsed: $((END-START)) sec"
echo

./cl get 5000 HOTKEY

echo
echo "Expected:"
echo "Newest incarnation survives"
echo "No corruption"
echo