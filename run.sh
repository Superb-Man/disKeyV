#!/bin/bash

BIN=./main   # change if your binary name is different

echo "Starting follower 1..."
gnome-terminal -- bash -c "$BIN follower 5001; exec bash"


echo "Starting follower 2..."
gnome-terminal -- bash -c "$BIN follower 5002; exec bash"

echo "Starting leader..."
gnome-terminal -- bash -c "$BIN leader 5000 5001 5002; exec bash"
