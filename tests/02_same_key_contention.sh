#!/bin/bash

echo
echo "===== SAME KEY CONTENTION ====="
echo

for i in {1..100}
do
    ./cl put 5000 USER $i &
done

wait

echo
echo "Final value:"
echo

./cl get 5000 USER

echo
echo "Expected:"
echo "Many object entries"
echo "Newest incarnation visible"
echo