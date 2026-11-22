// Package e2e tests the whole system end to end over real HTTP: full nodes
// (node, mempool, miner, HTTP API, peer relay) on TCP listeners, and light
// clients (selector with probes, locator header sync, SPV verification)
// reaching them over the network.
//
// Most tests run the nodes in this process, on real sockets, so they can
// slow a node down (a test-only wrapper around api.Backend, the same
// mechanism as the API tests), cut it off from block relay, and read its
// server-side served counts. TestExecutables builds and runs the real
// aletheia-node and aletheia-client binaries.
package e2e

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Anon-HP/Aletheia/internal/api"
	"github.com/Anon-HP/Aletheia/internal/client"
	"github.com/Anon-HP/Aletheia/internal/consensus"
	"github.com/Anon-HP/Aletheia/internal/core"
	"github.com/Anon-HP/Aletheia/internal/node"
	"github.com/Anon-HP/Aletheia/internal/routing"
)

// testDifficulty keeps in-process mining fast: 2^18 hashes per block on
// average, a few chunks of a few workers.
const testDifficulty = 18

// tieAccount is reserved for tie-breaking transactions (see settle).
const tieAccount = 7

func testParams() consensus.Params {
	p := consensus.Default()
	p.Difficulty = testDifficulty
	p.Genesis.Difficulty = testDifficulty
	return p
}

func acct(i int) core.Hash { return consensus.DemoAccount(i) }

// ---- In-process cluster ----

// testNode is one full node serving HTTP on its own TCP listener.
type testNode struct {
	addr string
	n    *node.Node
	pt   *api.PeerTransport
	hs   *http.Server

	served   atomic.Int64 // POST /tx requests that reached the node
	delay    atomic.Int64 // added to every POST /tx (nanoseconds): a slower node
	isolated atomic.Bool  // no block leaves or reaches the node

	stopOnce sync.Once
}

// backend is the API's view of the node, with the test hooks.
type backend struct {
	*node.Node
	tn *testNode
}

func (b backend) SubmitTx(tx core.Tx) (core.Hash, error) {
	b.tn.served.Add(1)
	if d := b.tn.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	return b.Node.SubmitTx(tx)
}

func (b backend) ReceiveBlock(blk core.Block, from string) error {
	if b.tn.isolated.Load() {
		return nil // partitioned: the block is lost on the way
	}
	return b.Node.ReceiveBlock(blk, from)
}

// gate is the node's Broadcaster: the HTTP PeerTransport, silenced while the
// node is isolated.
type gate struct{ tn *testNode }

func (g gate) BroadcastBlock(b core.Block) {
	if !g.tn.isolated.Load() {
		g.tn.pt.BroadcastBlock(b)
	}
}

func (g gate) FetchBlocks(peer string, loc []consensus.LocatorEntry) (uint64, []core.Block, error) {
	return g.tn.pt.FetchBlocks(peer, loc)
}

// stop shuts the node down like a killed process: its port refuses
// connections afterwards.
func (tn *testNode) stop() {
	tn.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), routing.RequestTimeout)
		defer cancel()
		tn.hs.Shutdown(ctx)
		tn.n.Stop()
		tn.pt.Wait()
	})
}

// startCluster starts k peered full nodes on 127.0.0.1.
func startCluster(t *testing.T, p consensus.Params, k int) []*testNode {
	t.Helper()
	lns := make([]net.Listener, k)
	addrs := make([]string, k)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i], addrs[i] = ln, ln.Addr().String()
	}
	nodes := make([]*testNode, k)
	for i := range nodes {
		tn := &testNode{addr: addrs[i]}
		tn.pt = api.NewPeerTransport(api.NewClient(), addrs[i], addrs, nil)
		n, err := node.New(p, gate{tn}, node.Options{Workers: 2})
		if err != nil {
			t.Fatal(err)
		}
		tn.n = n
		tn.hs = &http.Server{Handler: api.NewServer(backend{n, tn}, nil), ReadHeaderTimeout: routing.RequestTimeout}
		go tn.hs.Serve(lns[i])
		n.Start(context.Background())
		nodes[i] = tn
		t.Cleanup(tn.stop)
	}
	return nodes
}

func addrsOf(nodes []*testNode) []string {
	out := make([]string, len(nodes))
	for i, tn := range nodes {
		out[i] = tn.addr
	}
	return out
}

// newClient creates a light client with its own selector over nodes and
// runs its probes until the test ends, after one completed probe round.
func newClient(t *testing.T, p consensus.Params, nodes []string, id string, strategy routing.Strategy) *client.LightClient {
	t.Helper()
	ac := api.NewClient()
	sel := routing.NewSelector(routing.New(150, nodes...), routing.Options{Strategy: strategy})
	ctx, cancel := context.WithCancel(context.Background())
	sel.ProbeOnce(ctx, ac.Probe)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sel.Run(ctx, ac.Probe)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		ac.CloseIdleConnections()
	})
	return client.New(id, client.NewHeaderChain(p), sel, ac)
}

// confirm submits tx and waits until the client proves it against its own
// headers, sending the identical transaction again now and then (always
// harmless; recovers a copy lost by a reorganisation). A rejection that
// cannot change with time is an error.
func confirm(ctx context.Context, lc *client.LightClient, tx core.Tx, timeout time.Duration) (client.Verified, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last error
	var lastSend time.Time
	for {
		if time.Since(lastSend) >= time.Second {
			_, err := lc.Submit(ctx, tx)
			if errors.Is(err, core.ErrBalance) || errors.Is(err, core.ErrOverflow) || errors.Is(err, api.ErrMalformed) {
				return client.Verified{}, fmt.Errorf("submission of %s rejected: %w", core.TxID(tx), err)
			}
			lastSend = time.Now()
		}
		if _, err := lc.Sync(ctx); err != nil {
			last = err
		} else if v, err := lc.Prove(ctx, tx); err != nil {
			last = err
		} else {
			return v, nil
		}
		select {
		case <-ctx.Done():
			return client.Verified{}, fmt.Errorf("%s not proven: %v", core.TxID(tx), last)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func mustConfirm(t *testing.T, lc *client.LightClient, tx core.Tx) client.Verified {
	t.Helper()
	v, err := confirm(context.Background(), lc, tx, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// sameTip reports whether all nodes have the same tip.
func sameTip(nodes []*testNode) bool {
	_, first := nodes[0].n.Tip()
	for _, tn := range nodes[1:] {
		if _, hd := tn.n.Tip(); hd != first {
			return false
		}
	}
	return true
}

// settle waits until every node has the same tip. Two nodes that mine
// blocks at the same height keep their own until a longer chain appears
// (first seen wins, no empty blocks), so if the nodes stay split a
// tie-breaking transaction is mined: tieAccount pays itself, which costs
// only the fee and changes no account that a test checks.
func settle(t *testing.T, nodes []*testNode, lc *client.LightClient, tieSeq *uint64) {
	t.Helper()
	for attempt := 0; attempt < 6; attempt++ {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if sameTip(nodes) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		tx := core.Tx{Sender: acct(tieAccount), Recipient: acct(tieAccount), Amount: 1, Fee: 1, Seq: *tieSeq}
		mustConfirm(t, lc, tx)
		*tieSeq++
		t.Logf("tie-breaker %d mined", *tieSeq)
	}
	t.Fatal("nodes did not converge on one tip")
}

// ---- Tests ----

// TestSubmitMineProve covers flows F1–F4 over HTTP: two clients submit
// through the selector, the nodes mine and relay, every node ends in the
// same state, and each client verifies its transaction in O(log n) against
// the headers it synchronised and validated itself.
func TestSubmitMineProve(t *testing.T) {
	p := testParams()
	nodes := startCluster(t, p, 3)
	alice := newClient(t, p, addrsOf(nodes), "alice", routing.StrategyP2C)
	bob := newClient(t, p, addrsOf(nodes), "bob", routing.StrategyP2C)

	txA := core.Tx{Sender: acct(0), Recipient: acct(1), Amount: 100, Fee: 5, Seq: 0}
	txB := core.Tx{Sender: acct(1), Recipient: acct(2), Amount: 50, Fee: 9, Seq: 0}
	var wg sync.WaitGroup
	var vA, vB client.Verified
	var errA, errB error
	wg.Add(2)
	go func() { defer wg.Done(); vA, errA = confirm(context.Background(), alice, txA, 60*time.Second) }()
	go func() { defer wg.Done(); vB, errB = confirm(context.Background(), bob, txB, 60*time.Second) }()
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatal(errA, errB)
	}
	var tie uint64
	settle(t, nodes, alice, &tie)

	want := map[int][2]uint64{ // account: balance, next seq
		0: {consensus.DemoBalance - 105, 1},
		1: {consensus.DemoBalance + 100 - 59, 1},
		2: {consensus.DemoBalance + 50, 0},
	}
	for _, tn := range nodes {
		for a, w := range want {
			if bal, seq := tn.n.Account(acct(a)); bal != w[0] || seq != w[1] {
				t.Errorf("node %s account %d: balance %d seq %d, want %d %d", tn.addr, a, bal, seq, w[0], w[1])
			}
		}
	}

	// The light client stores exactly the nodes' headers (and nothing else).
	if _, err := alice.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	full := nodes[0].n.Headers(0)
	if got := alice.Headers().Headers(); len(got) != len(full) || got[len(got)-1] != full[len(full)-1] {
		t.Fatalf("client headers: %d, tip %v; node: %d", len(got), got[len(got)-1], len(full))
	}

	// Proof length is ceil(log2 n) for a block of n transactions.
	ac := api.NewClient()
	for _, c := range []struct {
		tx core.Tx
		v  client.Verified
	}{{txA, vA}, {txB, vB}} {
		pr, err := ac.Proof(context.Background(), nodes[0].addr, c.v.TxID)
		if err != nil {
			t.Fatal(err)
		}
		n := len(nodes[0].n.Blocks(pr.Height)[0].Txs)
		if want := int(math.Ceil(math.Log2(float64(n)))); len(pr.Proof) != want {
			t.Errorf("block of %d txs: proof has %d steps, want %d", n, len(pr.Proof), want)
		}
		if err := client.VerifyProof(alice.Headers(), c.tx, pr); err != nil {
			t.Errorf("proof of %s: %v", c.v.TxID, err)
		}
	}
}

// TestFailoverOwnerDown stops the client's ring owner: submission, header
// synchronisation and the proof request all go to another node.
func TestFailoverOwnerDown(t *testing.T) {
	p := testParams()
	nodes := startCluster(t, p, 3)
	const id = "carol"
	owner, err := routing.New(150, addrsOf(nodes)...).Owner(id)
	if err != nil {
		t.Fatal(err)
	}
	lc := newClient(t, p, addrsOf(nodes), id, routing.StrategyP2C)
	for _, tn := range nodes {
		if tn.addr == owner {
			tn.stop()
		}
	}

	tx := core.Tx{Sender: acct(2), Recipient: acct(3), Amount: 7, Fee: 2, Seq: 0}
	sub, err := lc.Submit(context.Background(), tx)
	if err != nil {
		t.Fatalf("submit with the owner down: %v", err)
	}
	if sub.Node == owner {
		t.Fatalf("submission answered by the stopped owner %s", owner)
	}
	v := mustConfirm(t, lc, tx)
	if v.Node == owner {
		t.Fatalf("proof served by the stopped owner %s", owner)
	}
	t.Logf("owner %s down: submitted to %s, proof from %s at height %d", owner, sub.Node, v.Node, v.Height)
}

// TestForkResolution partitions one node, lets both sides mine conflicting
// transactions, then heals the partition: the minority node fetches only the
// blocks after the fork point by locator, replays from genesis onto the
// longer chain and drops its conflicting transaction; a light client that
// followed the minority switches with one locator request.
func TestForkResolution(t *testing.T) {
	p := testParams()
	nodes := startCluster(t, p, 3)
	a, b, c := nodes[0], nodes[1], nodes[2]
	c.isolated.Store(true)

	onC := newClient(t, p, []string{c.addr}, "minority", routing.StrategyRing)
	onAB := newClient(t, p, []string{a.addr, b.addr}, "majority", routing.StrategyP2C)

	// The two sides spend account 3's sequence number 0 differently.
	x := core.Tx{Sender: acct(3), Recipient: acct(4), Amount: 30, Fee: 1, Seq: 0}
	y1 := core.Tx{Sender: acct(3), Recipient: acct(5), Amount: 40, Fee: 1, Seq: 0}
	y2 := core.Tx{Sender: acct(4), Recipient: acct(5), Amount: 1, Fee: 1, Seq: 0}
	mustConfirm(t, onC, x)
	mustConfirm(t, onAB, y1)
	mustConfirm(t, onAB, y2)
	var tie uint64
	settle(t, []*testNode{a, b}, onAB, &tie)
	hc, _ := c.n.Tip()
	ha, _ := a.n.Tip()
	if hc >= ha {
		t.Fatalf("partition: minority height %d, majority %d", hc, ha)
	}

	// A light client that follows the minority.
	ac := api.NewClient()
	follower := client.NewHeaderChain(p)
	if res, err := follower.Sync(context.Background(), ac, c.addr); err != nil || follower.Height() != hc {
		t.Fatalf("follower sync: %+v %v", res, err)
	}

	// Heal: the next majority block reaches c, which syncs from its sender.
	c.isolated.Store(false)
	mustConfirm(t, onAB, core.Tx{Sender: acct(5), Recipient: acct(6), Amount: 1, Fee: 1, Seq: 0})
	settle(t, nodes, onAB, &tie)

	if _, err := c.n.Proof(core.TxID(x)); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("minority transaction still in c's chain: %v", err)
	}
	if bal, seq := c.n.Account(acct(5)); seq != 1 || bal != consensus.DemoBalance+40+1-2 {
		t.Errorf("c account 5: balance %d seq %d", bal, seq)
	}
	if size := c.n.MempoolSize(); size != 0 {
		t.Errorf("c mempool holds %d transactions; the conflicting one must be dropped", size)
	}
	res, err := follower.Sync(context.Background(), ac, a.addr)
	if err != nil || !res.Adopted || res.Fork != 0 {
		t.Fatalf("follower switch: %+v %v", res, err)
	}
	if _, hd := follower.Tip(); hd != func() core.Header { _, h := a.n.Tip(); return h }() {
		t.Fatal("follower tip differs from the nodes' tip")
	}
}

// idsPerOwner returns client names such that every node is the ring owner of
// exactly per of them, so ring-only routing spreads the clients evenly.
func idsPerOwner(t *testing.T, nodes []string, per int) []string {
	t.Helper()
	ring := routing.New(150, nodes...)
	count := map[string]int{}
	var ids []string
	for i := 0; len(ids) < per*len(nodes); i++ {
		if i > 10000 {
			t.Fatal("no balanced client names found")
		}
		id := fmt.Sprintf("client-%d", i)
		if o, _ := ring.Owner(id); count[o] < per {
			count[o]++
			ids = append(ids, id)
		}
	}
	return ids
}

// burst gives every name in ids a new light client with its own selector
// and the given strategy, and lets all of them send `requests` POST /tx
// requests each, at the same time. It returns how many requests reached the
// node slow, and the mean latency.
func burst(t *testing.T, p consensus.Params, nodes []*testNode, slow *testNode, ids []string, requests int, strategy routing.Strategy) (int64, time.Duration) {
	t.Helper()
	lcs := make([]*client.LightClient, len(ids))
	for i, id := range ids {
		lcs[i] = newClient(t, p, addrsOf(nodes), id, strategy)
	}
	before := slow.served.Load()
	var total atomic.Int64
	var wg sync.WaitGroup
	for i, lc := range lcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := core.Tx{Sender: acct(i % 7), Recipient: acct(6), Amount: 1, Fee: 1, Seq: 1 << 40}
			for r := 0; r < requests; r++ {
				start := time.Now()
				if _, err := lc.Submit(context.Background(), tx); !errors.Is(err, core.ErrSeq) {
					t.Errorf("client %s: want a seq rejection, got %v", ids[i], err)
				}
				total.Add(int64(time.Since(start)))
			}
		}()
	}
	wg.Wait()
	return slow.served.Load() - before, time.Duration(total.Load() / int64(len(ids)*requests))
}

// TestSlowNodeRouting runs several independent clients (each with its own
// selector) against three nodes, one of them slowed down, first with
// ring-only routing and then with P2C, and compares the server-side counts
// of POST /tx requests at the slow node (DR-13; acceptance item 5a). Every
// request carries a transaction the nodes reject (its sequence number is far
// ahead): the node does the full admission work for each, nothing is mined,
// and both phases see the same state, so only the routing differs. The
// client names are the same in both phases, so are their ring owners.
//
// The slow node takes 100 ms more per request. P2C is load-aware, not
// "avoid the slow node": when the fast nodes become busy enough that their
// estimate exceeds the slow node's, it rightly uses the slow node too. Under
// the race detector a fast node needs a few milliseconds per request, so a
// slowdown of a few tens of milliseconds would make that happen now and
// then; 100 ms keeps the slow node clearly the worse choice.
func TestSlowNodeRouting(t *testing.T) {
	p := testParams()
	nodes := startCluster(t, p, 3)
	slow := nodes[2]
	slow.delay.Store(int64(100 * time.Millisecond))
	const perOwner, requests = 2, 20
	ids := idsPerOwner(t, addrsOf(nodes), perOwner)

	ringSlow, ringMean := burst(t, p, nodes, slow, ids, requests, routing.StrategyRing)
	p2cSlow, p2cMean := burst(t, p, nodes, slow, ids, requests, routing.StrategyP2C)
	all := len(ids) * requests
	t.Logf("slow node served: ring-only %d of %d, p2c %d of %d; mean latency ring-only %v, p2c %v",
		ringSlow, all, p2cSlow, all, ringMean, p2cMean)

	// Ring-only: every client uses its owner, so the slow node serves exactly
	// the requests of the clients it owns, however slow it is.
	if want := int64(perOwner * requests); ringSlow != want {
		t.Errorf("ring-only: slow node served %d, want exactly %d", ringSlow, want)
	}
	if p2cSlow*4 > ringSlow {
		t.Errorf("p2c: slow node served %d, ring-only %d; want under a quarter", p2cSlow, ringSlow)
	}
	if p2cMean >= ringMean {
		t.Errorf("mean latency: p2c %v, ring-only %v", p2cMean, ringMean)
	}
}

// TestConcurrentStress has seven independent clients, one per sender, each
// confirm a sequence of transfers through the selector while three nodes
// mine in parallel, relay and resolve forks (acceptance item 6). Afterwards
// every node holds the same chain, in which every transaction appears
// exactly once: each sender's next sequence number equals the number of its
// transfers, and each balance matches.
func TestConcurrentStress(t *testing.T) {
	p := testParams()
	nodes := startCluster(t, p, 3)
	const senders, perSender = 7, 4
	fee := func(i int) uint64 { return uint64(i + 1) }

	lcs := make([]*client.LightClient, senders)
	for i := range lcs {
		lcs[i] = newClient(t, p, addrsOf(nodes), fmt.Sprintf("stress-%d", i), routing.StrategyP2C)
	}
	errs := make([]error, senders)
	var wg sync.WaitGroup
	start := time.Now()
	for i, lc := range lcs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := 0; s < perSender; s++ {
				tx := core.Tx{Sender: acct(i), Recipient: acct((i + 1) % senders), Amount: 10, Fee: fee(i), Seq: uint64(s)}
				if _, err := confirm(context.Background(), lc, tx, 90*time.Second); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	var tie uint64
	settle(t, nodes, lcs[0], &tie)

	for _, tn := range nodes {
		for i := 0; i < senders; i++ {
			// Each sender paid perSender·(10 + fee) and received perSender·10.
			want := uint64(consensus.DemoBalance) - perSender*fee(i)
			if bal, seq := tn.n.Account(acct(i)); seq != perSender || bal != want {
				t.Errorf("node %s account %d: balance %d seq %d, want %d %d", tn.addr, i, bal, seq, want, perSender)
			}
			for s := 0; s < perSender; s++ {
				tx := core.Tx{Sender: acct(i), Recipient: acct((i + 1) % senders), Amount: 10, Fee: fee(i), Seq: uint64(s)}
				if _, err := tn.n.Proof(core.TxID(tx)); err != nil {
					t.Errorf("node %s: transaction %d/%d: %v", tn.addr, i, s, err)
				}
			}
		}
	}
	h, _ := nodes[0].n.Tip()
	t.Logf("%d transfers confirmed in %v; chain height %d; %d tie-breakers", senders*perSender, time.Since(start).Round(time.Millisecond), h, tie)
}

// ---- Real executables ----

// process is a running aletheia-node.
type process struct {
	cmd  *exec.Cmd
	addr string
	log  string
	done chan struct{}
	err  error
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// buildBinaries builds both executables with the race detector. The go
// command runs with this test's environment, so under `make go-test` it
// links the freshly built C++ library (CGO_LDFLAGS).
func buildBinaries(t *testing.T) (nodeBin, clientBin string) {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs the executables")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go command not found")
	}
	dir := t.TempDir()
	nodeBin, clientBin = filepath.Join(dir, "aletheia-node"), filepath.Join(dir, "aletheia-client")
	for bin, pkg := range map[string]string{nodeBin: "../../cmd/fullnode", clientBin: "../../cmd/lightclient"} {
		out, err := exec.Command(goBin, "build", "-race", "-o", bin, pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("go build %s: %v\n%s", pkg, err, out)
		}
	}
	return nodeBin, clientBin
}

func startNodeProcess(t *testing.T, bin, addr string, peers []string) *process {
	t.Helper()
	pr := &process{addr: addr, log: filepath.Join(t.TempDir(), "node.log"), done: make(chan struct{})}
	f, err := os.Create(pr.log)
	if err != nil {
		t.Fatal(err)
	}
	pr.cmd = exec.Command(bin, "--listen", addr, "--peers", strings.Join(peers, ","), "--workers", "2", "--status", "200ms")
	pr.cmd.Stdout, pr.cmd.Stderr = f, f
	if err := pr.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		pr.err = pr.cmd.Wait()
		f.Close()
		close(pr.done)
	}()
	t.Cleanup(func() {
		pr.cmd.Process.Kill()
		<-pr.done
	})
	return pr
}

// terminate stops the node with SIGTERM and checks that it exits cleanly.
func (pr *process) terminate(t *testing.T) string {
	t.Helper()
	pr.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-pr.done:
	case <-time.After(30 * time.Second):
		t.Fatalf("node %s did not stop", pr.addr)
	}
	out, _ := os.ReadFile(pr.log)
	if pr.err != nil {
		t.Errorf("node %s exited with %v:\n%s", pr.addr, pr.err, out)
	}
	return string(out)
}

func runClient(t *testing.T, bin string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("aletheia-client %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	if strings.Contains(string(out), "DATA RACE") {
		t.Fatalf("race in aletheia-client %s:\n%s", strings.Join(args, " "), out)
	}
	return string(out)
}

// fieldValue returns the value of the first "key=value" field in out.
func fieldValue(out, key string) string {
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		for _, f := range strings.Fields(sc.Text()) {
			if v, ok := strings.CutPrefix(f, key+"="); ok {
				return v
			}
		}
	}
	return ""
}

// TestExecutables runs three aletheia-node processes (demonstration
// parameters, difficulty 22) and drives them with aletheia-client: a
// transfer is submitted and proven, tampered proofs are rejected, the
// status shows every node, a burst from several clients confirms one
// transfer, and after the client's ring owner is stopped with SIGTERM a
// further transfer fails over to another node. Both binaries are built with
// the race detector; every node must exit cleanly.
func TestExecutables(t *testing.T) {
	nodeBin, clientBin := buildBinaries(t)
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	procs := make([]*process, len(addrs))
	for i, a := range addrs {
		procs[i] = startNodeProcess(t, nodeBin, a, addrs)
	}
	ac := api.NewClient()
	for _, a := range addrs {
		deadline := time.Now().Add(20 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := ac.Tip(ctx, a)
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not come up: %v", a, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	nodesFlag := "--nodes=" + strings.Join(addrs, ",")

	out := runClient(t, clientBin, nodesFlag, "send", "--from=0", "--to=1", "--amount=100", "--fee=5", "--seq=0", "--wait=90s")
	if !strings.Contains(out, "verified txid=") {
		t.Fatalf("send: %s", out)
	}
	out = runClient(t, clientBin, nodesFlag, "prove", "--from=0", "--to=1", "--amount=100", "--fee=5", "--seq=0", "--tamper")
	if strings.Count(out, "tampered proof rejected") < 2 {
		t.Fatalf("prove --tamper: %s", out)
	}
	out = runClient(t, clientBin, nodesFlag, "--route=ring", "burst", "--from=2", "--to=3", "--amount=1", "--fee=1", "--seq=0",
		"--clients=3", "--count=5", "--wait=90s")
	if !strings.Contains(out, "burst route=ring requests=15") || !strings.Contains(out, "verified txid=") {
		t.Fatalf("burst: %s", out)
	}
	out = runClient(t, clientBin, nodesFlag, "status")
	owner := fieldValue(out, "owner")
	if strings.Count(out, "mempool=") != len(addrs) || owner == "" {
		t.Fatalf("status: %s", out)
	}
	h, _ := nodeHeight(out, owner)
	if h < 2 {
		t.Fatalf("status: owner %s at height %d, want at least 2:\n%s", owner, h, out)
	}

	// Stop the owner; the next transfer goes elsewhere.
	var rest []*process
	for _, pr := range procs {
		if pr.addr == owner {
			log := pr.terminate(t)
			if !strings.Contains(log, "shutting down") || !strings.Contains(log, "served_tx=") {
				t.Errorf("owner log:\n%s", log)
			}
		} else {
			rest = append(rest, pr)
		}
	}
	out = runClient(t, clientBin, nodesFlag, "send", "--from=0", "--to=1", "--amount=1", "--fee=1", "--seq=1", "--wait=90s")
	if node := fieldValue(out, "node"); node == "" || node == owner {
		t.Fatalf("send after stopping %s: %s", owner, out)
	}
	for _, pr := range rest {
		if log := pr.terminate(t); strings.Contains(log, "DATA RACE") {
			t.Errorf("race in node %s:\n%s", pr.addr, log)
		}
	}
}

// nodeHeight reads "node <addr>: height=<h>" from status output.
func nodeHeight(out, addr string) (uint64, bool) {
	for _, line := range strings.Split(out, "\n") {
		if rest, ok := strings.CutPrefix(line, "node "+addr+": "); ok {
			var h uint64
			if _, err := fmt.Sscanf(fieldValue(rest, "height"), "%d", &h); err == nil {
				return h, true
			}
		}
	}
	return 0, false
}
