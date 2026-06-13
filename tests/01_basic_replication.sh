#!/bin/bash

# echo
# echo "===== BASIC REPLICATION TEST ====="
# echo

./cl put 5000 A 1
./cl put 5000 B 2
./cl put 5000 C 3

# echo
# echo "Reading values..."
# echo

./cl get 5000 A
./cl get 5000 B
./cl get 5000 C

# echo
# echo "Expected:"
# echo "A=1"
# echo "B=2"
# echo "C=3"
# echo