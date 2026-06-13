#!/bin/bash

echo
echo "===== LEADER FAILURE ====="
echo

for i in {1..500}
do
    ./cl put 5000 KEY$i $i &
done

wait

echo
echo "Kill leader manually:"
echo

echo "pkill -f 'leader 5000'"

echo
echo "After kill:"
echo "Inspect followers"
echo

echo "Expected:"
echo "Replicated objects still present"
echo