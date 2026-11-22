// Command aletheia-node runs one Aletheia full node.
//
// The node mines blocks in parallel, keeps the fee-ordered mempool, relays
// every new block to its peers and serves the HTTP API that light clients
// and peers use (internal/api). Nodes may run on separate machines, so the
// node listens on a network interface and every request it makes has a
// fixed timeout (connect 2 s, request 5 s).
//
//	aletheia-node --listen 0.0.0.0:8101 --advertise 192.168.1.10:8101 \
//	              --peers 192.168.1.11:8101,192.168.1.12:8101
//
// The node starts from the demonstration genesis (consensus.Default) and
// keeps everything in memory. At start it synchronises with each peer that
// is already running (block locator, DR-14). Once per --status interval it
// prints one line:
//
//	status height=<h> tip=<hash> mempool=<pending txs> served_tx=<POST /tx served>
//
// served_tx counts every POST /tx request the node has answered, accepted
// or rejected; the demonstration compares it between routing strategies.
// SIGINT or SIGTERM stops the node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Anon-HP/Aletheia/internal/api"
	"github.com/Anon-HP/Aletheia/internal/consensus"
	"github.com/Anon-HP/Aletheia/internal/core"
	"github.com/Anon-HP/Aletheia/internal/node"
	"github.com/Anon-HP/Aletheia/internal/routing"
)

// idleTimeout closes keep-alive connections that carry no request.
const idleTimeout = 60 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "aletheia-node:", err)
		os.Exit(1)
	}
}

// config is the parsed command line.
type config struct {
	listen  string        // address to listen on
	self    string        // address peers reach this node at
	peers   []string      // the other full nodes
	workers int           // mining goroutines; < 1 means one per CPU
	status  time.Duration // status line interval; 0 disables
}

func parseFlags(args []string, out io.Writer) (config, error) {
	fs := flag.NewFlagSet("aletheia-node", flag.ContinueOnError)
	fs.SetOutput(out)
	listen := fs.String("listen", "", "`host:port` to listen on: a network interface or 0.0.0.0, so that machines elsewhere can connect (required)")
	advertise := fs.String("advertise", "", "`host:port` at which peers reach this node (default: --listen)")
	peers := fs.String("peers", "", "comma-separated `host:port` list of the other full nodes")
	workers := fs.Int("workers", 0, "mining goroutines (0: one per CPU)")
	status := fs.Duration("status", time.Second, "interval of the status line (0: none)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *listen == "" {
		return config{}, errors.New("--listen is required")
	}
	if _, _, err := net.SplitHostPort(*listen); err != nil {
		return config{}, fmt.Errorf("--listen: %v", err)
	}
	c := config{listen: *listen, self: *advertise, workers: *workers, status: *status}
	if c.self == "" {
		c.self = *listen
	}
	// Peers fetch missing blocks from the address a block arrives with, so it
	// must name a reachable host.
	if host, _, err := net.SplitHostPort(c.self); err != nil {
		return config{}, fmt.Errorf("--advertise: %v", err)
	} else if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		return config{}, fmt.Errorf("listening on %q: give --advertise with an address peers can reach", c.self)
	}
	var err error
	if c.peers, err = parseAddrs(*peers); err != nil {
		return config{}, fmt.Errorf("--peers: %v", err)
	}
	if c.status < 0 {
		return config{}, errors.New("--status must not be negative")
	}
	return c, nil
}

// parseAddrs splits a comma-separated host:port list; empty entries are
// skipped.
func parseAddrs(s string) ([]string, error) {
	var out []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// run starts the node and serves until ctx ends or the listener fails.
func run(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseFlags(args, out)
	if err != nil {
		return err
	}
	logger := log.New(out, "", log.LstdFlags|log.Lmicroseconds)

	p := consensus.Default()
	ac := api.NewClient()
	pt := api.NewPeerTransport(ac, cfg.self, cfg.peers, logger.Printf)
	n, err := node.New(p, pt, node.Options{Workers: cfg.workers, Logf: logger.Printf})
	if err != nil {
		return err
	}
	srv := api.NewServer(n, nil)

	// served counts the POST /tx requests answered, outside the API package:
	// the count is for the demonstration only and is never used for routing.
	var served atomic.Uint64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.ServeHTTP(w, r)
		if r.Method == http.MethodPost && r.URL.Path == "/tx" {
			served.Add(1)
		}
	})
	hs := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: routing.RequestTimeout,
		ReadTimeout:       routing.RequestTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          logger,
	}
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		n.Stop()
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- hs.Serve(ln) }()
	n.Start(ctx)
	logger.Printf("node listening on %s as %s, peers %v, genesis %s, difficulty %d",
		ln.Addr(), cfg.self, pt.Peers(), core.HeaderHash(p.Genesis), p.Difficulty)

	var bg sync.WaitGroup
	// Catch up with peers that already run (a late joiner, DR-14).
	for _, peer := range pt.Peers() {
		bg.Add(1)
		go func() {
			defer bg.Done()
			switch err := n.Sync(peer); {
			case err == nil:
				logger.Printf("caught up with %s", peer)
			case errors.Is(err, node.ErrStale):
				logger.Printf("chain of %s is not longer; nothing to catch up", peer)
			default:
				logger.Printf("catch-up with %s failed: %v", peer, err)
			}
		}()
	}
	printStatus := func() {
		h, hd := n.Tip()
		logger.Printf("status height=%d tip=%s mempool=%d served_tx=%d",
			h, core.HeaderHash(hd), n.MempoolSize(), served.Load())
	}
	if cfg.status > 0 {
		bg.Add(1)
		go func() {
			defer bg.Done()
			t := time.NewTicker(cfg.status)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					printStatus()
				}
			}
		}()
	}

	var result error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		result = err
	}
	cancel()
	logger.Printf("shutting down")
	sctx, scancel := context.WithTimeout(context.Background(), routing.RequestTimeout)
	defer scancel()
	hs.Shutdown(sctx) // let requests in progress finish
	bg.Wait()
	printStatus()
	n.Stop()
	pt.Wait()
	return result
}
