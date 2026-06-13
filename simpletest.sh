#!/bin/bash

./a follower 5001 &
F1=$!

./a follower 5002 &
F2=$!

sleep 1

./a leader 5000 5001 5002 &
L=$!

wait