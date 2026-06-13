#!/bin/bash

echo
echo "===== SEGMENT LIFECYCLE ====="
echo

for i in {1..100}
do
    ./cl put 5000 KEY$i $i
done

echo
echo "Check leader.log"
echo

grep -i "segment" leader.log

echo
echo "Expected:"
echo "ACTIVE -> SEALED transitions"
echo