#!/bin/bash

echo
echo "===== THREAD SCALING ====="
echo

START=$(date +%s)

for i in {1..10000}
do
    ./cl put 5000 K$i $i &
done

wait

END=$(date +%s)

OPS=$((10000/(END-START+1)))

echo
echo "Approx Throughput:"
echo "$OPS ops/sec"
echo