#!/usr/bin/env bash
# Aletheia live demonstration: three full nodes and several light clients.
#
#   scripts/demo.sh local                   all three nodes on this machine
#   scripts/demo.sh ssh HOST1 HOST2 HOST3   one node per machine or VM, over ssh
#
# HOST is [user@]address, reachable with key-based ssh (no password prompt).
# The machine running the script builds both binaries, copies aletheia-node
# to each host, starts the nodes over ssh and runs the light clients itself.
# The hosts must accept TCP connections on DEMO_PORT from each other and from
# this machine, and must run a compatible Linux (same libc and libstdc++).
#
# Node 3 (DEMO_SLOW) is the weaker node. Across machines it should be the
# weakest VM; in local mode it is pinned to one CPU with taskset.
#
# The scenario runs unattended from a clean build and stops at the first
# failure (exit status 1). Steps:
#   1. start three nodes, one of them weaker
#   2. transfers with different fees; a higher fee is mined first
#   3. a client verifies a proof against its own headers: the O(log n) path
#   4. tampered proofs are rejected
#   5. bursts from several independent clients, ring-only then P2C: served
#      counts at the nodes and response times at the clients
#   6. the client's ring owner is stopped; the client fails over
#   7. a fork: the stopped node returns cut off, mines its own block, then
#      receives the longer chain, syncs by locator and replays it
#   8. every node reports the same tip
#
# Environment (defaults in brackets):
#   DEMO_PORT [8101]   node port (local mode: DEMO_PORT, +1, +2)
#   DEMO_DIR  [/tmp/aletheia-demo]   working directory on the hosts
#   DEMO_SLOW [3]      the weaker node (1..3)
#   DEMO_CLIENTS [8] DEMO_COUNT [25] DEMO_CONCURRENCY [4]   burst size
#   DEMO_PAUSE [0]     seconds to pause after each step
set -euo pipefail

PORT=${DEMO_PORT:-8101}
RDIR=${DEMO_DIR:-/tmp/aletheia-demo}
SLOW=${DEMO_SLOW:-3}
CLIENTS=${DEMO_CLIENTS:-8}
COUNT=${DEMO_COUNT:-25}
CONC=${DEMO_CONCURRENCY:-4}
PAUSE=${DEMO_PAUSE:-0}
WAIT=90s

usage() { sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

MODE=${1:-}
case "$MODE" in
local) [[ $# -eq 1 ]] || usage ;;
ssh)   [[ $# -eq 4 ]] || usage; HOSTS=("" "$2" "$3" "$4") ;;
*)     usage ;;
esac
[[ $SLOW =~ ^[123]$ ]] || { echo "DEMO_SLOW must be 1, 2 or 3" >&2; exit 2; }

cd "$(dirname "$0")/.."
LOGS=$(mktemp -d /tmp/aletheia-demo-logs.XXXXXX)
declare -a ADDR PID

step() { echo; echo "=== $*"; }
pause() { if [[ $PAUSE != 0 ]]; then sleep "$PAUSE"; fi; }

# Node i's address and how to start and inspect it.
for i in 1 2 3; do
	if [[ $MODE == local ]]; then
		ADDR[$i]=127.0.0.1:$((PORT + i - 1))
	else
		ADDR[$i]=${HOSTS[$i]#*@}:$PORT
	fi
done
ALL="${ADDR[1]},${ADDR[2]},${ADDR[3]}"

# CPU placement in local mode: the weaker node gets CPU 0 alone; the others
# share the remaining CPUs and leave some free for serving requests.
CPUS=$(nproc)
pin() {
	if [[ $MODE != local || $CPUS -lt 3 ]] || ! command -v taskset >/dev/null; then return; fi
	if [[ $1 == "$SLOW" ]]; then
		echo "taskset -c 0"
	else
		echo "taskset -c 1-$((CPUS - 1))"
	fi
}
workers() {
	if [[ $MODE == local && $1 != "$SLOW" ]]; then echo $(( (CPUS - 1) / 2 > 1 ? (CPUS - 1) / 2 - 1 : 1 )); else echo 0; fi
}

# start_node I PEERS: start node I with the given peer list ("" = none).
start_node() {
	local i=$1 peers=$2 args
	args="--listen 0.0.0.0:${ADDR[$i]##*:} --advertise ${ADDR[$i]} --workers $(workers "$i")"
	[[ -n $peers ]] && args="$args --peers $peers"
	if [[ $MODE == local ]]; then
		# shellcheck disable=SC2046,SC2086
		$(pin "$i") ./aletheia-node $args >>"$LOGS/node$i.log" 2>&1 &
		PID[$i]=$!
	else
		PID[$i]=$(ssh -n -o BatchMode=yes "${HOSTS[$i]}" \
			"cd $RDIR && { nohup ./aletheia-node $args >>node.log 2>&1 </dev/null & echo \$!; }")
	fi
	echo "node $i started at ${ADDR[$i]}${peers:+ with peers $peers}$([[ $i == "$SLOW" ]] && echo ' (weaker node)')"
}

stop_node() {
	local i=$1
	[[ -n ${PID[$i]:-} ]] || return 0
	if [[ $MODE == local ]]; then
		kill "${PID[$i]}" 2>/dev/null || true
		wait "${PID[$i]}" 2>/dev/null || true
	else
		ssh -n -o BatchMode=yes "${HOSTS[$i]}" \
			"kill ${PID[$i]} 2>/dev/null; while kill -0 ${PID[$i]} 2>/dev/null; do sleep 0.1; done" || true
	fi
	PID[$i]=
}

node_log() {
	if [[ $MODE == local ]]; then cat "$LOGS/node$1.log"; else ssh -n -o BatchMode=yes "${HOSTS[$1]}" "cat $RDIR/node.log"; fi
}

# served I: POST /tx requests node I has answered so far (its last status line).
served() { node_log "$1" | grep ' status ' | tail -n 1 | sed 's/.*served_tx=\([0-9]*\).*/\1/'; }

cleanup() {
	for i in 1 2 3; do stop_node "$i"; done
	echo "node logs: $LOGS (ssh mode: $RDIR/node.log on each host)"
}
trap cleanup EXIT

client() { ./aletheia-client --nodes "$ALL" "$@"; }

# wait_nodes: until every node answers.
wait_nodes() {
	for _ in $(seq 100); do
		if ! client status | grep -q unavailable; then return 0; fi
		sleep 0.2
	done
	echo "nodes did not come up" >&2; exit 1
}

# converge: until all nodes report the same tip. A tie at equal height lasts
# until the next block (no empty blocks), so a small transfer breaks it.
TIE_SEQ=0
converge() {
	local tries
	for tries in 1 2 3 4; do
		for _ in $(seq 25); do
			if [[ $(client status | grep -c "^node .*tip=") -eq 3 &&
				$(client status | grep "^node " | sed 's/.*tip=\([0-9a-f]*\).*/\1/' | sort -u | wc -l) -eq 1 ]]; then
				return 0
			fi
			sleep 0.2
		done
		echo "(nodes hold different tips of equal height; mining one more transfer to break the tie)"
		client send --from 6 --to 7 --amount 1 --fee 1 --seq $TIE_SEQ --wait $WAIT >/dev/null
		TIE_SEQ=$((TIE_SEQ + 1))
	done
	echo "nodes did not converge" >&2; exit 1
}

# ---------------------------------------------------------------------------

step "0. Build"
make node client
if [[ $MODE == ssh ]]; then
	for i in 1 2 3; do
		ssh -n -o BatchMode=yes "${HOSTS[$i]}" "mkdir -p $RDIR && rm -f $RDIR/node.log"
		scp -q -o BatchMode=yes aletheia-node "${HOSTS[$i]}:$RDIR/aletheia-node"
	done
fi

step "1. Three full nodes, node $SLOW weaker ($MODE mode)"
for i in 1 2 3; do start_node "$i" "$ALL"; done
if [[ $MODE == local && -z $(pin "$SLOW") ]]; then
	echo "(taskset or 3 CPUs unavailable: node $SLOW is not actually weaker)"
fi
wait_nodes
client status
pause

step "2. Transfers with different fees: within a block, the higher fee comes first"
# All three go to the same node (ring routing, one client name). The first
# starts a block; the other two wait in the fee-ordered mempool meanwhile.
client --route ring --id fees send --from 0 --to 7 --amount 10 --fee 1 --seq 0
client --route ring --id fees send --from 1 --to 7 --amount 10 --fee 2 --seq 0
client --route ring --id fees send --from 2 --to 7 --amount 10 --fee 50 --seq 0
for a in "0 1" "1 2" "2 50"; do
	set -- $a
	echo "--- account $1, fee $2:"
	client --id fees send --from "$1" --to 7 --amount 10 --fee "$2" --seq 0 --wait $WAIT | grep -v '^  '
done
pause

step "3. A light client proves a transfer against its own headers"
client prove --from 2 --to 7 --amount 10 --fee 50 --seq 0
pause

step "4. Tampered proofs are rejected"
client prove --from 2 --to 7 --amount 10 --fee 50 --seq 0 --tamper | grep -v '^  '
pause

step "5. Bursts from $CLIENTS independent clients: ring-only, then P2C"
# A node reports its load only after it has served a POST /tx, and P2C uses a
# node as the second choice only with a known estimate. So each node first
# measures one request: the confirmed transfer of step 2 again (rejected: seq).
for i in 1 2 3; do
	./aletheia-client --nodes "${ADDR[$i]}" burst --from 2 --to 7 --amount 10 --fee 50 --seq 0 --clients 1 --count 1 >/dev/null
done
for phase in "ring 0" "p2c 1"; do
	set -- $phase
	b1=$(served 1); b2=$(served 2); b3=$(served 3)
	client --route "$1" --id burst burst --from 3 --to 7 --amount 1 --fee 1 --seq "$2" \
		--clients "$CLIENTS" --count "$COUNT" --concurrency "$CONC" --wait $WAIT | grep -v '^  step\|^  own\|^  leaf'
	sleep 1.2 # one more status line from every node
	echo "served by the nodes during the $1 burst (node $SLOW is the weaker one):"
	echo "  node 1: $(( $(served 1) - b1 ))   node 2: $(( $(served 2) - b2 ))   node 3: $(( $(served 3) - b3 ))"
done
pause

step "6. The client's ring owner stops; requests fail over"
OWNER=$(client status | sed -n 's/.*owner=\([^ ]*\).*/\1/p')
for i in 1 2 3; do [[ ${ADDR[$i]} == "$OWNER" ]] && DOWN=$i; done
echo "owner of client \"client\" is node $DOWN ($OWNER): stopping it"
stop_node "$DOWN"
client send --from 4 --to 7 --amount 5 --fee 3 --seq 0 --wait $WAIT | grep -v '^  '
pause

step "7. A fork: node $DOWN returns cut off from its peers and mines alone"
start_node "$DOWN" ""
for _ in $(seq 50); do ./aletheia-client --nodes "$OWNER" status | grep -q unavailable || break; sleep 0.2; done
./aletheia-client --nodes "$OWNER" --id alone send --from 5 --to 6 --amount 9 --fee 1 --seq 0 --wait $WAIT | grep -v '^  '
client status | grep '^node'
echo "--- the other nodes spend account 5's sequence number 0 differently; their next block reaches node $DOWN:"
OTHERS=$(echo "$ALL" | tr ',' '\n' | grep -vx "$OWNER" | paste -sd,)
./aletheia-client --nodes "$OTHERS" send --from 5 --to 7 --amount 9 --fee 1 --seq 0 --wait $WAIT | grep -v '^  '
converge
node_log "$DOWN" | grep -E "mined block|accepted block|switched to" | tail -n 4 || true
pause

step "8. Every node reports the same tip"
converge
client status
echo
echo "Demonstration complete."
