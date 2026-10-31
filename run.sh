#!/usr/bin/env bash
set -euo pipefail

python consumer/consumer.py &
consumer_pid=$!

python producer/producer.py &
producer_pid=$!

shutdown() {
    kill -TERM "$producer_pid" "$consumer_pid" 2>/dev/null || true
    wait "$producer_pid" "$consumer_pid" 2>/dev/null || true
}

trap shutdown TERM INT

status=0
wait -n "$producer_pid" "$consumer_pid" || status=$?
shutdown

exit "$status"
