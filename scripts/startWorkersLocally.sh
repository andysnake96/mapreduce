#!/usr/bin/env bash
killall worker
cd ..
if [[ -n "$2" ]]; then
    make worker
    echo "worker rebuilded"
fi
WORKERS_NUM=5
SLEEP_LOCAL_PORTS_SYNC_SECS=0.2
if [[ -n "$1" ]]; then
    WORKERS_NUM=$1
else
    echo "USAGE <WORKER NUM TO START"
    exit
fi

for i in $( seq 1  $WORKERS_NUM ); do
    ./worker/worker &
    sleep $SLEEP_LOCAL_PORTS_SYNC_SECS
done
echo "started all workers"
