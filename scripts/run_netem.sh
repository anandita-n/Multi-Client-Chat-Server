#!/usr/bin/env bash
# run_netem.sh - TCP vs UDP under injected packet loss.
#
# For each loss rate, starts a fresh server, connects all simulated clients,
# then applies `tc netem` to the loopback interface and measures. Every run
# happens inside a private network namespace (unshare --user --net), so no
# sudo is needed and the host's real loopback is never touched.
#
# netem on `lo` affects every packet, and a chat message crosses `lo` twice
# (sender -> server, server -> receiver), so with p% loss a UDP message is
# expected to arrive with probability (1 - p)^2. TCP ACKs are also dropped.
#
# A small delay with jitter is applied at every loss level (including 0%):
# with no delay loopback has ~0 RTT, and netem only reorders packets when
# jitter lets a later packet get an earlier send time.
#
# usage: scripts/run_netem.sh            (settings via env vars below)
set -euo pipefail

if [[ -z "${IN_NETNS:-}" ]]; then
    exec env IN_NETNS=1 unshare --user --map-root-user --net "$0" "$@"
fi

cd "$(dirname "$0")/.."
CLIENTS=${CLIENTS:-200}
ROOMS=${ROOMS:-10}
RATE=${RATE:-1000}        # total chat messages per second
DURATION=${DURATION:-15}  # seconds of measured traffic per run
DRAIN=${DRAIN:-15}        # max seconds to wait for stragglers afterwards
LOSSES=${LOSSES:-"0 1 2 5 10"}
DELAY=${DELAY:-2ms}
JITTER=${JITTER:-1ms}
OUT=${OUT:-results/netem.csv}

make -s all
mkdir -p "$(dirname "$OUT")"
rm -f "$OUT"
ip link set lo up

run() {   # run PROTO LOSS
    local proto=$1 loss=$2 port pid
    tc qdisc del dev lo root 2>/dev/null || true
    if [[ $proto == tcp ]]; then
        port=9000; bin/server_tcp -b 127.0.0.1 -p $port 2>>results/server.log &
    else
        port=9001; bin/server_udp -b 127.0.0.1 -p $port 2>>results/server.log &
    fi
    pid=$!
    sleep 0.3
    bin/loadtest -P "$proto" -p $port -c "$CLIENTS" -r "$ROOMS" -R "$RATE" \
        -d "$DURATION" -w "$DRAIN" -l "loss${loss}" -o "$OUT" \
        -S "tc qdisc add dev lo root netem loss ${loss}% delay $DELAY $JITTER"
    tc qdisc del dev lo root 2>/dev/null || true
    kill -TERM $pid
    wait $pid || true
}

: > results/server.log
for loss in $LOSSES; do
    for proto in tcp udp; do
        echo ">>> $proto, loss ${loss}%, delay $DELAY ± $JITTER"
        run "$proto" "$loss"
    done
done
echo "results written to $OUT"
