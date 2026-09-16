#!/bin/bash

BIN=./a.out   # change if your binary name is different

echo "Starting five-replica cluster (quorum 3)..."
gnome-terminal -- bash -c "DISKEYV_REPLICA_ID=2 $BIN follower 5001 5000 5002 5003 5004; exec bash"
gnome-terminal -- bash -c "DISKEYV_REPLICA_ID=3 $BIN follower 5002 5000 5001 5003 5004; exec bash"
gnome-terminal -- bash -c "DISKEYV_REPLICA_ID=4 $BIN follower 5003 5000 5001 5002 5004; exec bash"
gnome-terminal -- bash -c "DISKEYV_REPLICA_ID=5 $BIN follower 5004 5000 5001 5002 5003; exec bash"
gnome-terminal -- bash -c "DISKEYV_REPLICA_ID=1 $BIN leader 5000 5001 5002 5003 5004; exec bash"
