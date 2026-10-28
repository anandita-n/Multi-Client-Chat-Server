#!/usr/bin/env bash
# run_capacity.sh - latency vs offered load (no injected loss), plus a
# many-connections run, for both servers.
#
# Runs in a private network namespace like run_netem.sh, so results are not
# affected by anything else using the host's loopback.
#
# usage: scripts/run_capacity.sh       (settings via env vars below)
set -euo pipefail

if [[ -z "${IN_NETNS:-}" ]]; then
    exec env IN_NETNS=1 unshare --user --map-root-user --net "$0" "$@"
fi

cd "$(dirname "$0")/.."
CLIENTS=${CLIENTS:-1000}
ROOMS=${ROOMS:-20}            # 50 users per room: each message fans out to 49
RATES=${RATES:-"250 500 1000 1500 2000 2500 3000"}
DURATION=${DURATION:-10}
CONC_CLIENTS=${CONC_CLIENTS:-10000}
OUT=${OUT:-results/capacity.csv}

make -s all
mkdir -p "$(dirname "$OUT")"
rm -f "$OUT"
ip link set lo up
ulimit -n "$(ulimit -Hn)"

run() {   # run PROTO CLIENTS ROOMS RATE LABEL
    local proto=$1 port pid
    if [[ $proto == tcp ]]; then
        port=9000; bin/server_tcp -b 127.0.0.1 -p $port 2>>results/server.log &
    else
        port=9001; bin/server_udp -b 127.0.0.1 -p $port 2>>results/server.log &
    fi
    pid=$!
    sleep 0.3
    bin/loadtest -P "$proto" -p $port -c "$2" -r "$3" -R "$4" -d "$DURATION" \
        -w 5 -l "$5" -o "$OUT"
    kill -TERM $pid
    wait $pid || true
}

: > results/server.log
for rate in $RATES; do
    for proto in tcp udp; do
        echo ">>> $proto, $CLIENTS clients, $rate msg/s"
        run "$proto" "$CLIENTS" "$ROOMS" "$rate" "load$rate"
    done
done

# Many concurrent connections at a modest message rate (10 users per room).
for proto in tcp udp; do
    echo ">>> $proto, $CONC_CLIENTS concurrent clients"
    run "$proto" "$CONC_CLIENTS" $((CONC_CLIENTS / 10)) 1000 "conc$CONC_CLIENTS"
done
echo "results written to $OUT"
