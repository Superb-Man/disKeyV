#!/bin/bash

chmod +x *.sh

echo
echo "================================="
echo "LoLKV Evaluation Suite"
echo "================================="
echo

./01_basic_replication.sh

./02_same_key_contention.sh

./03_incarnation_ordering.sh

./05_skewed_workload.sh

./06_uniform_workload.sh

./07_ycsb_a.sh

./08_ycsb_b.sh

./09_replication_stress.sh

./11_thread_scaling.sh

echo
echo "================================="
echo "DONE"
echo "================================="