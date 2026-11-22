# Aletheia

Aletheia is a small, tiered payment ledger built for a data structures and
algorithms course. **Full nodes** store the whole chain, mine blocks in
parallel, keep a fee-ordered pool of pending transactions and serve Merkle
inclusion proofs. **Light clients** keep only block headers, validate them
themselves, and verify in O(log n) that a transaction is in a block, without
trusting the node that answered.

Go handles concurrency, networking and routing; C++ holds the core data
structures. The two are linked with cgo against a static C++ library.

> Aletheia is a teaching project. It is **not secure for real use**: there
> are no signatures and no authentication (see [Limitations](#limitations)).

## Data structures

Each of these is written by hand; no library or standard container stands in
for any of them (`std::vector` and Go slices are used only as storage).

| Structure | Where | Used for |
|---|---|---|
| SHA-256 | `cpp/src/sha256.cpp` | every hash: txids, block hashes, Merkle nodes |
| Merkle tree with inclusion proofs | `cpp/src/merkle.cpp` | the block's transaction root; O(log n) proofs |
| Open-addressing hash map (linear probing, tombstones, resize at 0.7) | `cpp/include/aletheia/hash_map.h` | account balances and the transaction index |
| Hash-linked list of blocks with validation rules | `cpp/src/ledger.cpp`, `validation.cpp` | the chain; atomic, fully validated appends |
| Binary max-heap (fee, then txid) | `cpp/src/fee_queue.cpp` | the mempool's fee order |
| Parallel nonce search | `cpp/src/pow.cpp`, `internal/miner` | proof of work: goroutines over 65,536-nonce chunks, first solver stops the rest |
| Consistent-hash ring with virtual nodes | `internal/routing/ring.go` | each client's default node and failover order |

## Architecture

```
 light client (cmd/lightclient)                         full node (cmd/fullnode), one per machine
 ┌───────────────────────────────┐   HTTP/JSON      ┌──────────────────────────────────────────┐
 │ header chain (own validation) │ ───────────────▶ │ internal/api   handlers, load meter, relay│
 │ SPV proof verification        │  POST /tx        │ internal/node  assembly, fork choice      │
 │ selector: ring + P2C          │  GET /tip        │ internal/mempool  concurrent fee queue    │
 └───────────────────────────────┘  GET /headers    │ internal/miner    parallel PoW            │
                                    GET /proof/{id} │ internal/consensus  rules, block locator  │
                                                    │ internal/core   cgo bindings (only "C")   │
            peers ◀── POST /block, GET /blocks ───▶ │ C++ core: libaletheia_core.a              │
                                                    └──────────────────────────────────────────┘
```

- **Transactions** are 88-byte canonical encodings (sender, recipient,
  amount, fee, sequence number); the txid is their SHA-256. Fees are burned;
  there is no mining reward. Eight demonstration accounts hold 1,000,000
  each at genesis.
- **Mining:** a node mines only while its mempool holds transactions (blocks
  are never empty). It takes candidates in fee order, keeps each one only if
  the whole list still passes the ledger's checks, and searches nonces on all
  CPUs. Difficulty is fixed at 22 leading zero bits.
- **Forks:** the longest valid chain wins. A node that sees a longer chain
  sends a *block locator* (its block hashes at heights tip, tip−1, …, then
  doubling gaps down to genesis) and gets back the fork point and only the
  blocks after it, in one request. It then replays the new chain from genesis
  into a fresh ledger and switches only if every block is valid.
- **Light clients** synchronise headers the same way, check every header
  against the consensus rules (linkage, difficulty, proof of work), and
  verify a proof by recomputing the txid themselves and hashing up to the
  Merkle root in *their own* header. The proof carries no root.

## Routing

Each client sends a request to a node chosen by its **selector**:

1. The consistent-hash ring maps the client's name to a default node and a
   failover order over all nodes.
2. With `--route=p2c` (default) the selector also considers one random other
   node and takes it if its estimated response time is clearly lower (by more
   than 20%): **power of two choices**. The estimate is
   `RTT + (n + k + 1) · ŝ`, where each node reports its `POST /tx` service time
   ŝ and requests in flight n in the `X-Aletheia-Load` header, the client
   measures RTT with a probe (`GET /tip`) every second, and k counts the
   client's own sends since the last report. Reports older than 3 s are
   ignored. `--route=ring` always uses the ring's node, for comparison.
3. A node that fails twice in a row (no answer, timeout, or HTTP 5xx) is
   marked down until a probe succeeds; the request moves on to the next node.
   An answer such as "rejected: seq" is not a failure and is not retried
   elsewhere.

Fixed timeouts: connect 2 s, request 5 s, probe 1 s. The mempool size each
node reports is displayed only; routing never uses it.

**What routing does not do:** it measures load, latency and availability,
not honesty. A node that returns invalid headers or proofs is caught by the
client's own checks, but it is not ranked lower. Load is measured on
`POST /tx` only. A node that has not served any `POST /tx` yet sends no
report and is never chosen as the second candidate.

## Build and test

Development and testing are supported on one machine: Ubuntu 26.04 (x86_64)
with g++ 15.2, Go 1.26.4 and GNU Make 4.4.1.

```sh
make            # C++ core library
make test       # C++ unit and property tests
make go-test    # go vet, then go test -race -count=1 ./... (includes end-to-end)
make check      # both
make node client   # ./aletheia-node and ./aletheia-client
```

Always build Go through `make`: the targets pass a hash of the C++ library to
the linker, so Go never reuses a build linked against an older library.

The end-to-end tests (`test/e2e`) run three nodes on real sockets with light
clients: submit, mine and prove; failover with the owner node stopped; a
partition and its resolution; several independent clients against a slowed
node (ring-only vs P2C, counted at the nodes); concurrent senders with
parallel mining. `TestExecutables` also builds and runs the two binaries.
`go test -short` skips that one.

## Running

Full node, on each machine:

```sh
./aletheia-node --listen 0.0.0.0:8101 --advertise 192.168.1.10:8101 \
                --peers 192.168.1.11:8101,192.168.1.12:8101
```

`--listen` must be reachable from the other machines (not only loopback);
`--advertise` is the address peers use to fetch blocks from this node
(default: `--listen`). `--workers` sets the mining goroutines (default: one
per CPU). Every second the node prints
`status height=… tip=… mempool=… served_tx=…`; `served_tx` counts the
`POST /tx` requests it has answered. A node that starts late catches up from
its peers. Stop it with Ctrl-C or SIGTERM.

Light client, anywhere:

```sh
N=192.168.1.10:8101,192.168.1.11:8101,192.168.1.12:8101
./aletheia-client --nodes $N status
./aletheia-client --nodes $N send  --from 0 --to 1 --amount 100 --fee 5 --seq 0 --wait 60s
./aletheia-client --nodes $N prove --from 0 --to 1 --amount 100 --fee 5 --seq 0 --tamper
./aletheia-client --nodes $N --route ring burst --from 2 --to 3 --amount 1 --fee 1 --seq 0 --clients 8
```

Accounts are given by index (0–7 hold the genesis balances). `--seq` is the
sender's number of confirmed transfers. `send --wait` re-sends the identical
transaction every few seconds until it is proven; that is always safe, since
a transaction is applied at most once. `--id` names the client (its routing
key). At start the client asks every node for its genesis block and ignores
nodes of another network.

## Demonstration

```sh
scripts/demo.sh local                   # three nodes on this machine
scripts/demo.sh ssh HOST1 HOST2 HOST3   # one node per machine or VM
```

The script builds both binaries and runs the whole scenario unattended,
stopping at the first failure:

1. three nodes start, node 3 weaker;
2. three transfers with fees 1, 2 and 50: within a block, the higher fee
   comes first;
3. a light client verifies a proof against its own header and prints the
   path;
4. tampered proofs (transaction, sibling, direction, height) are rejected;
5. bursts from 8 independent clients, ring-only then P2C, with the requests
   each node served;
6. the client's ring owner is stopped and the client fails over;
7. that node returns cut off from its peers, mines its own block, then
   receives the longer chain and switches (locator sync, replay from genesis);
8. all nodes report the same tip.

**ssh mode:** the machine running the script needs key-based ssh to each
`[user@]host` and copies `aletheia-node` to `DEMO_DIR` (default
`/tmp/aletheia-demo`) there, so the hosts need a compatible Linux (same libc
and libstdc++). Open TCP port `DEMO_PORT` (default 8101) between all hosts
and from the control machine. Make the weaker node (`DEMO_SLOW`, default 3)
a VM with fewer or slower CPUs.

**local mode:** nodes use ports 8101–8103; with `taskset` and at least three
CPUs, node 3 is pinned to CPU 0 and the others share the rest. On one
machine the nodes differ little, and P2C follows the load it measures, not
the label "weaker": step 5 may show little difference, or another node may
happen to be the busier one in a given run. Use separate machines with a
clearly weaker VM to show the effect live. The end-to-end test
`TestSlowNodeRouting` shows it reliably with a node slowed by 100 ms per
request (ring-only: 40 of 120 requests to it; P2C: none).

Other settings: `DEMO_CLIENTS`, `DEMO_COUNT`, `DEMO_CONCURRENCY` (burst
size) and `DEMO_PAUSE` (seconds after each step).

## Limitations

Security (deliberate, for scope):

- **No signatures, no authentication.** An account is a 32-byte identifier;
  anyone who names an account can spend from it. Any client can post a block
  to a node and make it fetch blocks from an arbitrary `host:port`.
- **Not production-secure.** Nodes validate every block and clients check
  every header and proof, but there is no defence against denial of service,
  and nodes are not scored for honesty.

Scope:

- **No persistence:** chains and mempools live in memory; a restarted node
  starts from genesis and catches up from its peers.
- **No peer discovery:** peers are given with `--peers`.
- **One pending transaction per sender** at each node: a sender's next
  transfer is accepted after the previous one is mined.
- **Equal-height forks persist while nobody mines** (first seen wins, and
  blocks are never empty); the next transaction resolves them. The demo
  sends one if needed.
- **No confirmation depth:** a proof is accepted at any height the client
  holds; after a reorganisation, prove again.
- After a reorganisation, only the first of a sender's abandoned transfers
  returns to the mempool; clients re-send the rest (`send --wait` does).
- A peer sync must finish within the 5 s request timeout (ample at
  demonstration scale).
- A node that lags behind may answer "not found" for a proof or reject a
  sequence number another node would accept; the client does not ask a
  second node, so retry later.
- Fixed difficulty, no retargeting; fixed genesis parameters.

## Repository layout

```
cpp/include, cpp/src     C++ core: SHA-256, Merkle, hash map, block, ledger, rules, fee queue, PoW
cpp/capi                 C interfaces for cgo (int32 status codes, no exceptions across)
cpp/tests                C++ tests (make test)
internal/core            cgo bindings; the only package importing "C"
internal/mempool         concurrent mempool
internal/miner           parallel mining coordinator
internal/consensus       parameters, header rules, fork choice, block locator
internal/node            full node: assembly, relay hooks, chain selection and replay
internal/routing         consistent-hash ring, load meter, selector
internal/api             HTTP API, client, peer transport
internal/client          light client: header chain, SPV verification
cmd/fullnode, cmd/lightclient   executables
test/e2e                 end-to-end tests
scripts/demo.sh          demonstration
```

## Team

| Area | Owner |
|---|---|
| Cryptography and ledger state (C++): SHA-256, Merkle trees, hash map, blocks, ledger and validation | Lalit Sandeep Girgaonkar |
| Mempool and consensus: build, cgo bindings, fee queue, mempool, proof of work, mining, consensus, block locator, node | Aryman Pathak |
| Network, routing and light client: ring, selector, HTTP API, light client, executables, end-to-end tests, demo | Numair Muzzafar Wani |

## License

GNU General Public License v3.0; see `LICENSE`.
