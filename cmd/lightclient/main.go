// Command aletheia-client is an Aletheia light client.
//
// It keeps only block headers, which it validates itself with the consensus
// rules from its own genesis, and verifies Merkle inclusion proofs against
// its own headers. It reaches full nodes through the routing selector: the
// consistent-hash ring gives the default node and the failover order, and
// with --route=p2c (the default) the power of two choices on the nodes'
// reported POST /tx load and the measured round-trip time picks between two
// candidates (DR-13).
//
//	aletheia-client --nodes a:8101,b:8101,c:8101 [--route p2c|ring] [--id NAME] COMMAND [flags]
//
// Commands:
//
//	status   synchronise headers; show the client's tip and every node's
//	         tip, mempool size (display only), load report and health
//	send     submit one transfer; with --wait, wait until it is proven
//	prove    prove a transfer (with --tamper, also show tampered proofs fail)
//	burst    several independent clients send many POST /tx requests;
//	         report where they went and how long they took
//
// Accounts are given by index: account i is consensus.DemoAccount(i), and
// accounts 0..7 hold the genesis balances. There are no signatures (README,
// security limitations). The sender's sequence number is given with --seq.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Anon-HP/Aletheia/internal/api"
	"github.com/Anon-HP/Aletheia/internal/client"
	"github.com/Anon-HP/Aletheia/internal/consensus"
	"github.com/Anon-HP/Aletheia/internal/core"
	"github.com/Anon-HP/Aletheia/internal/mempool"
	"github.com/Anon-HP/Aletheia/internal/node"
	"github.com/Anon-HP/Aletheia/internal/routing"
)

// Client settings.
const (
	// vnodes is the number of virtual nodes per full node on the ring.
	vnodes = 150
	// pollEvery is the pause between attempts while waiting for a proof.
	pollEvery = 250 * time.Millisecond
	// resendEvery: while waiting, the identical transaction is sent again
	// this often. That is always harmless (same txid, same sequence number;
	// applied at most once) and recovers a transaction dropped by a
	// reorganisation or lost by an ambiguous submission (D3 technical notes).
	resendEvery = 3 * time.Second
)

const usage = `usage: aletheia-client --nodes host:port,... [--route p2c|ring] [--id NAME] COMMAND [flags]

commands:
  status                                                    headers, node tips, mempool sizes, load
  send  --from I --to J --amount A --fee F --seq S [--wait D]   submit a transfer
  prove --from I --to J --amount A --fee F --seq S [--tamper]   verify a confirmed transfer
  burst --from I --to J --amount A --fee F --seq S [--clients K] [--count N]
        [--concurrency C] [--wait D]                        load from K independent clients

Run "aletheia-client COMMAND -h" for the flags of a command.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "aletheia-client:", err)
		os.Exit(1)
	}
}

// globals are the flags before the command.
type globals struct {
	nodes    []string
	strategy routing.Strategy
	id       string
}

func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("aletheia-client", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() { fmt.Fprint(out, usage); fs.PrintDefaults() }
	nodes := fs.String("nodes", "", "comma-separated `host:port` list of full nodes (required)")
	route := fs.String("route", "p2c", "routing strategy: p2c (load- and latency-aware) or ring")
	id := fs.String("id", "client", "client name: the routing key that the ring maps to a default node")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var g globals
	var err error
	if g.nodes, err = parseAddrs(*nodes); err != nil {
		return fmt.Errorf("--nodes: %v", err)
	}
	if len(g.nodes) == 0 {
		return errors.New("--nodes is required")
	}
	switch *route {
	case "p2c":
		g.strategy = routing.StrategyP2C
	case "ring":
		g.strategy = routing.StrategyRing
	default:
		return fmt.Errorf("--route must be p2c or ring, not %q", *route)
	}
	g.id = *id
	if fs.NArg() == 0 {
		fs.Usage()
		return errors.New("no command given")
	}
	cmd, rest := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "status":
		return cmdStatus(ctx, g, rest, out)
	case "send":
		return cmdSend(ctx, g, rest, out)
	case "prove":
		return cmdProve(ctx, g, rest, out)
	case "burst":
		return cmdBurst(ctx, g, rest, out)
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// parseAddrs splits a comma-separated host:port list; empty entries are
// skipped and duplicates removed.
func parseAddrs(s string) ([]string, error) {
	var out []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a == "" || slices.Contains(out, a) {
			continue
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// ---- Setup ----

// env is what every command uses: the network parameters, the HTTP client
// and the nodes that passed the genesis check.
type env struct {
	p     consensus.Params
	ac    *api.Client
	nodes []string
	out   io.Writer
}

// newEnv checks every node's genesis. A node that answers with another
// genesis belongs to another network and is not used (it would stay
// healthy for probes and answer every header request with 409). A node that
// does not answer now is kept: it may come up later, and the selector
// handles unavailable nodes.
func newEnv(ctx context.Context, g globals, out io.Writer) (*env, error) {
	e := &env{p: consensus.Default(), ac: api.NewClient(), out: out}
	genesis := []consensus.LocatorEntry{{Height: 0, Hash: core.HeaderHash(e.p.Genesis)}}
	errs := make([]error, len(g.nodes))
	var wg sync.WaitGroup
	for i, n := range g.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, routing.RequestTimeout)
			defer cancel()
			_, _, errs[i] = e.ac.HeadersAfter(cctx, n, genesis)
		}()
	}
	wg.Wait()
	for i, n := range g.nodes {
		switch err := errs[i]; {
		case err == nil:
			e.nodes = append(e.nodes, n)
		case errors.Is(err, consensus.ErrGenesisMismatch):
			fmt.Fprintf(out, "warning: %s has a different genesis block; not used\n", n)
		default:
			fmt.Fprintf(out, "warning: %s did not answer the genesis check (%v); kept\n", n, err)
			e.nodes = append(e.nodes, n)
		}
	}
	if len(e.nodes) == 0 {
		return nil, errors.New("no node shares this client's genesis block")
	}
	return e, nil
}

// lightClient is one client with its own selector, ring and header chain.
type lightClient struct {
	*client.LightClient
	id   string
	ring *routing.Ring
	sel  *routing.Selector
	ac   *api.Client
}

// newClient creates a light client named id and starts its probes, which
// run until ctx ends. One probe round completes first, so the first request
// already has RTTs and any load reports (otherwise P2C would route like the
// ring for one probe interval).
func (e *env) newClient(ctx context.Context, id string, strategy routing.Strategy) *lightClient {
	ring := routing.New(vnodes, e.nodes...)
	sel := routing.NewSelector(ring, routing.Options{Strategy: strategy})
	sel.ProbeOnce(ctx, e.ac.Probe)
	go sel.Run(ctx, e.ac.Probe)
	return &lightClient{
		LightClient: client.New(id, client.NewHeaderChain(e.p), sel, e.ac),
		id:          id, ring: ring, sel: sel, ac: e.ac,
	}
}

// txFlags adds the flags describing one transfer to fs.
func txFlags(fs *flag.FlagSet) func() (core.Tx, error) {
	from := fs.Int("from", -1, "sender account index (0..7 hold genesis balances)")
	to := fs.Int("to", -1, "recipient account index")
	amount := fs.Uint64("amount", 0, "amount to transfer")
	fee := fs.Uint64("fee", 0, "fee (burned); higher fees are mined first")
	seq := fs.Uint64("seq", 0, "the sender's sequence number: its number of confirmed transfers")
	return func() (core.Tx, error) {
		if *from < 0 || *to < 0 {
			return core.Tx{}, errors.New("--from and --to are required account indices")
		}
		return core.Tx{
			Sender:    consensus.DemoAccount(*from),
			Recipient: consensus.DemoAccount(*to),
			Amount:    *amount,
			Fee:       *fee,
			Seq:       *seq,
		}, nil
	}
}

// newFlagSet returns a flag set for a command that prints to out.
func newFlagSet(name string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	return fs
}

func parseCommand(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

func short(h core.Hash) string { return h.String()[:16] }

// ---- status ----

func cmdStatus(ctx context.Context, g globals, args []string, out io.Writer) error {
	if err := parseCommand(newFlagSet("status", out), args); err != nil {
		return err
	}
	e, err := newEnv(ctx, g, out)
	if err != nil {
		return err
	}
	lc := e.newClient(ctx, g.id, g.strategy)
	owner, _ := lc.ring.Owner(lc.id)
	fmt.Fprintf(out, "client %s: route=%s owner=%s\n", lc.id, g.strategy, owner)
	res, err := lc.Sync(ctx)
	if err != nil {
		fmt.Fprintf(out, "header sync failed: %v\n", err)
	} else {
		fmt.Fprintf(out, "header sync from %s: fork=%d received=%d adopted=%t\n", res.Node, res.Fork, res.Received, res.Adopted)
	}
	h, hd := lc.Headers().Tip()
	fmt.Fprintf(out, "own headers: height=%d tip=%s\n", h, short(core.HeaderHash(hd)))
	for _, n := range e.nodes {
		pctx, cancel := context.WithTimeout(ctx, routing.ProbeTimeout)
		start := time.Now()
		info, err := e.ac.Tip(pctx, n)
		rtt := time.Since(start)
		cancel()
		if err != nil {
			fmt.Fprintf(out, "node %s: unavailable (%v)\n", n, err)
			continue
		}
		load := "none yet"
		if info.HasReport {
			load = info.Report.String()
		}
		fmt.Fprintf(out, "node %s: height=%d tip=%s mempool=%d load=%s rtt=%s\n",
			n, info.Height, short(core.HeaderHash(info.Header)), info.MempoolSize, load, rtt.Round(time.Microsecond))
	}
	fmt.Fprintln(out, "(mempool sizes are shown for information only; routing never uses them)")
	return nil
}

// ---- send ----

func cmdSend(ctx context.Context, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("send", out)
	txOf := txFlags(fs)
	wait := fs.Duration("wait", 0, "wait up to this long until the transfer is proven (0: do not wait)")
	if err := parseCommand(fs, args); err != nil {
		return err
	}
	tx, err := txOf()
	if err != nil {
		return err
	}
	e, err := newEnv(ctx, g, out)
	if err != nil {
		return err
	}
	lc := e.newClient(ctx, g.id, g.strategy)
	fmt.Fprintf(out, "txid %s\n", core.TxID(tx))
	sub, err := lc.Submit(ctx, tx)
	if err := reportSubmit(out, sub, err); err != nil && *wait == 0 {
		return err
	} else if err != nil && !retryable(err) {
		return err
	}
	if *wait == 0 {
		return nil
	}
	return lc.confirm(ctx, tx, *wait, out)
}

// reportSubmit prints the outcome of one submission and returns its error.
func reportSubmit(out io.Writer, sub client.Submission, err error) error {
	var ae *api.APIError
	switch {
	case err == nil:
		fmt.Fprintf(out, "accepted by %s\n", sub.Node)
	case errors.Is(err, mempool.ErrDuplicate):
		fmt.Fprintf(out, "already pending at %s\n", sub.Node)
	case errors.Is(err, core.ErrSeq):
		fmt.Fprintf(out, "rejected by %s: seq (already confirmed, or a wrong sequence number)\n", sub.Node)
	case errors.As(err, &ae):
		fmt.Fprintf(out, "rejected by %s: %s\n", sub.Node, ae.Code)
	default:
		fmt.Fprintf(out, "not delivered: %v\n", err)
	}
	return err
}

// retryable reports whether waiting can still lead to a confirmation after
// a submission error: the transaction may already be pending or confirmed
// (duplicate, seq), another of the sender's transactions is pending
// (sender_pending), or no node answered.
func retryable(err error) bool {
	return errors.Is(err, mempool.ErrDuplicate) || errors.Is(err, core.ErrSeq) ||
		errors.Is(err, mempool.ErrSenderPending) || errors.Is(err, client.ErrNoNode)
}

// confirm waits until tx is proven against the client's own headers. Each
// round synchronises headers and asks for the proof; the identical
// transaction is sent again every resendEvery. Answers that may change with
// time (not yet mined, a node or the client behind, a fork) are retried
// until the deadline.
func (lc *lightClient) confirm(ctx context.Context, tx core.Tx, wait time.Duration, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	lastSend := time.Now()
	var last error
	for {
		if _, err := lc.Sync(ctx); err != nil {
			last = err
		} else if v, err := lc.Prove(ctx, tx); err != nil {
			last = err
		} else {
			return lc.printProof(ctx, tx, v, out)
		}
		if time.Since(lastSend) >= resendEvery {
			lc.Submit(ctx, tx)
			lastSend = time.Now()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("not proven within %s: %v", wait, last)
		case <-time.After(pollEvery):
		}
	}
}

// fetchProof fetches the proof of tx from addr and verifies it against the
// client's own headers.
func (lc *lightClient) fetchProof(ctx context.Context, tx core.Tx, addr string) (node.ProofResult, error) {
	pr, err := lc.ac.Proof(ctx, addr, core.TxID(tx))
	if err != nil {
		return pr, err
	}
	return pr, client.VerifyProof(lc.Headers(), tx, pr)
}

// printProof prints a verified proof: its height, the transaction's position
// in the block and the O(log n) path that was checked.
func (lc *lightClient) printProof(ctx context.Context, tx core.Tx, v client.Verified, out io.Writer) error {
	pr, err := lc.fetchProof(ctx, tx, v.Node)
	if err != nil {
		// The proof verified a moment ago; the node's chain moved since.
		fmt.Fprintf(out, "verified txid=%s height=%d node=%s\n", v.TxID, v.Height, v.Node)
		return nil
	}
	fmt.Fprintf(out, "verified txid=%s height=%d index=%d steps=%d node=%s\n",
		v.TxID, v.Height, leafIndex(pr.Proof), len(pr.Proof), v.Node)
	_, hd := lc.Headers().Tip()
	h, _ := lc.Headers().Header(pr.Height)
	fmt.Fprintf(out, "  own header %d: merkle_root=%s (client tip %s)\n", pr.Height, short(h.MerkleRoot), short(core.HeaderHash(hd)))
	fmt.Fprintf(out, "  leaf = txid computed by the client: %s\n", short(v.TxID))
	for i, s := range pr.Proof {
		side := "right"
		if s.Side == core.Left {
			side = "left"
		}
		fmt.Fprintf(out, "  step %d: sibling %s on the %s\n", i+1, short(s.Sibling), side)
	}
	return nil
}

// leafIndex is the transaction's position in its block, read from the proof:
// at step i the running hash is the right child (bit i = 1) exactly when its
// sibling is on the left.
func leafIndex(p core.Proof) uint64 {
	var idx uint64
	for i, s := range p {
		if s.Side == core.Left {
			idx |= 1 << i
		}
	}
	return idx
}

// ---- prove ----

func cmdProve(ctx context.Context, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("prove", out)
	txOf := txFlags(fs)
	tamper := fs.Bool("tamper", false, "also check that tampered versions of the proof are rejected")
	if err := parseCommand(fs, args); err != nil {
		return err
	}
	tx, err := txOf()
	if err != nil {
		return err
	}
	e, err := newEnv(ctx, g, out)
	if err != nil {
		return err
	}
	lc := e.newClient(ctx, g.id, g.strategy)
	if _, err := lc.Sync(ctx); err != nil {
		return fmt.Errorf("header sync: %w", err)
	}
	v, err := lc.Prove(ctx, tx)
	if err != nil {
		return fmt.Errorf("proof of %s: %w", core.TxID(tx), err)
	}
	if err := lc.printProof(ctx, tx, v, out); err != nil {
		return err
	}
	if !*tamper {
		return nil
	}
	pr, err := lc.fetchProof(ctx, tx, v.Node)
	if err != nil {
		return fmt.Errorf("refetching the proof: %w", err)
	}
	return checkTampered(lc.Headers(), tx, pr, out)
}

// checkTampered shows that the client rejects modified versions of a genuine
// proof, as it would if a node sent them.
func checkTampered(hc *client.HeaderChain, tx core.Tx, pr node.ProofResult, out io.Writer) error {
	type tampering struct {
		name string
		pr   node.ProofResult
	}
	var cases []tampering
	clone := func() node.ProofResult {
		c := pr
		c.Proof = append(core.Proof(nil), pr.Proof...)
		return c
	}
	c := clone()
	c.Tx.Amount++
	cases = append(cases, tampering{"transaction amount changed", c})
	if len(pr.Proof) > 0 {
		c = clone()
		c.Proof[0].Sibling[0] ^= 1
		cases = append(cases, tampering{"one bit of a sibling hash flipped", c})
		c = clone()
		c.Proof[0].Side ^= 1
		cases = append(cases, tampering{"direction of a step swapped", c})
	} else {
		fmt.Fprintln(out, "  (the block holds one transaction, so the path is empty: no sibling to tamper with)")
	}
	c = clone()
	c.Height--
	cases = append(cases, tampering{"claimed block height changed", c})
	for _, t := range cases {
		err := client.VerifyProof(hc, tx, t.pr)
		if err == nil {
			return fmt.Errorf("tampered proof (%s) was accepted", t.name)
		}
		fmt.Fprintf(out, "tampered proof rejected (%s): %v\n", t.name, err)
	}
	return nil
}

// ---- burst ----

// outcome is the result of one request of a burst.
type outcome struct {
	node    string
	latency time.Duration
	err     error
}

func cmdBurst(ctx context.Context, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("burst", out)
	txOf := txFlags(fs)
	clients := fs.Int("clients", 8, "independent clients, each with its own selector and name <id>-<i>")
	count := fs.Int("count", 25, "requests per client")
	concurrency := fs.Int("concurrency", 4, "requests in flight per client")
	wait := fs.Duration("wait", 0, "afterwards, wait up to this long until the transfer is proven (0: do not wait)")
	if err := parseCommand(fs, args); err != nil {
		return err
	}
	tx, err := txOf()
	if err != nil {
		return err
	}
	if *clients < 1 || *count < 1 || *concurrency < 1 {
		return errors.New("--clients, --count and --concurrency must be at least 1")
	}
	e, err := newEnv(ctx, g, out)
	if err != nil {
		return err
	}
	lcs := make([]*lightClient, *clients)
	for i := range lcs {
		lcs[i] = e.newClient(ctx, fmt.Sprintf("%s-%d", g.id, i+1), g.strategy)
	}

	// Every request carries the same transaction: the first copy a node
	// receives is admitted, later ones are answered "duplicate" (and "seq"
	// once it is mined). So the burst confirms one transfer whatever the
	// routing, and each request still costs the node a full POST /tx.
	results := make([][]outcome, len(lcs))
	start := time.Now()
	var wg sync.WaitGroup
	for i, lc := range lcs {
		results[i] = make([]outcome, *count)
		jobs := make(chan int)
		for w := 0; w < *concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					t0 := time.Now()
					sub, err := lc.Submit(ctx, tx)
					results[i][j] = outcome{node: sub.Node, latency: time.Since(t0), err: err}
				}
			}()
		}
		go func() {
			for j := 0; j < *count; j++ {
				jobs <- j
			}
			close(jobs)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []outcome
	for _, r := range results {
		all = append(all, r...)
	}
	printBurst(out, g.strategy, e.nodes, all, elapsed)
	if *wait == 0 {
		return nil
	}
	return lcs[0].confirm(ctx, tx, *wait, out)
}

// printBurst summarises a burst: requests per node (as the clients saw them),
// answers by kind, and latency percentiles.
func printBurst(out io.Writer, strategy routing.Strategy, nodes []string, all []outcome, elapsed time.Duration) {
	perNode := make([]int, len(nodes))
	kinds := []string{"accepted", "duplicate", "seq", "other rejection", "not delivered"}
	perKind := make([]int, len(kinds))
	lat := make([]time.Duration, 0, len(all))
	for _, o := range all {
		if i := slices.Index(nodes, o.node); i >= 0 {
			perNode[i]++
		}
		var ae *api.APIError
		switch {
		case o.err == nil:
			perKind[0]++
		case errors.Is(o.err, mempool.ErrDuplicate):
			perKind[1]++
		case errors.Is(o.err, core.ErrSeq):
			perKind[2]++
		case errors.As(o.err, &ae):
			perKind[3]++
		default:
			perKind[4]++
		}
		lat = append(lat, o.latency)
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))].Round(time.Microsecond) }
	fmt.Fprintf(out, "burst route=%s requests=%d elapsed=%s\n", strategy, len(all), elapsed.Round(time.Millisecond))
	for i, n := range nodes {
		fmt.Fprintf(out, "  node %s: %d requests\n", n, perNode[i])
	}
	for i, k := range kinds {
		if perKind[i] > 0 {
			fmt.Fprintf(out, "  %s: %d\n", k, perKind[i])
		}
	}
	fmt.Fprintf(out, "  latency p50=%s p95=%s max=%s\n", pct(0.5), pct(0.95), lat[len(lat)-1].Round(time.Microsecond))
}
