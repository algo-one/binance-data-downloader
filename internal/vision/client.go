package vision

import (
	"net/http"
	"sync"
	"time"
)

// defaultClient is the client [NewLister] and [NewDownloader] use when handed a
// nil one, shared by every caller that does not supply their own.
//
// # Why not http.DefaultClient
//
// Because http.DefaultClient is the bug. It wraps http.DefaultTransport, whose
// MaxIdleConnsPerHost is 2 — so a pool of eight workers keeps two connections
// and re-handshakes TCP and TLS for the other six on every chunk. Defaulting to
// it made correctness requirement 8 conditional on the caller knowing to pass
// [NewHTTPClient] instead, which is to say the documented default was the
// failure the requirement names.
//
// # Why sync.OnceValue
//
// One client for the process is the entire point, so building a fresh one per
// constructor would restore the per-pool fragmentation in a subtler form:
// several clients, each with a correct-looking transport, none of them sharing
// a connection. OnceValue takes a func() T and returns a func() T that runs it
// at most once, caching the result — the concise spelling of a package-level
// value that must not be built at init time. Doing this lazily rather than as a
// plain var matters: a var would build a transport, read the proxy environment
// and start a dialer in every program that merely imports this package.
//
// The returned func is safe to call from multiple goroutines, and so is the
// *http.Client it yields — one value genuinely serves the whole worker pool.
//
// Listing and downloading go to different hosts, and they share this client on
// purpose. A Transport keeps its idle connections in a per-host pool, so one
// client holds separate pools for S3 and for data.binance.vision rather than
// mixing them.
var defaultClient = sync.OnceValue(NewHTTPClient)

// NewHTTPClient returns the *http.Client this library uses for every request in
// a process.
//
// # One client, not one per call
//
// An http.Client is not a connection. It is a handle onto a Transport, and the
// Transport is what holds the pool of live TCP connections. Creating a client
// per request therefore creates a pool per request, uses one connection from
// it, and throws the pool away — so every single download pays a fresh TCP
// handshake and a fresh TLS handshake, roughly 100–200 ms of round trips before
// a byte of payload moves. Over a few hundred archives that is minutes spent
// re-introducing ourselves to a server we never left.
//
// That is bug 8 of the ten this library exists to fix, and it is the reason
// both [NewLister] and [NewDownloader] take a client rather than making one:
// sharing is only possible if the layer above owns it.
//
// http.Client is safe for concurrent use by multiple goroutines — that is a
// documented guarantee, not an accident — so one value serves the whole worker
// pool.
func NewHTTPClient() *http.Client {
	// Clone the default transport rather than building a Transport from
	// scratch. The default carries settings worth keeping that are easy to
	// forget: proxy configuration from the environment, the dual-stack dialer,
	// HTTP/2 negotiation. A hand-built &http.Transport{} silently drops all of
	// them, which is how a client stops honouring HTTPS_PROXY for no visible
	// reason.
	tr, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		// Defensive: DefaultTransport is documented to be an *http.Transport,
		// but a program that replaced it globally would land here rather than
		// panicking on the type assertion.
		return &http.Client{}
	}

	t := tr.Clone()

	// The default is 2, which is the single most consequential number here.
	// Every request this library makes goes to the same host, so with the
	// default a pool of eight workers keeps two connections and re-dials for
	// the other six on every chunk. Raising it to comfortably exceed any
	// concurrency limit we would set makes the pool actually pool.
	t.MaxIdleConnsPerHost = 64
	t.MaxIdleConns = 64

	// How long an unused connection is kept before being closed. The default
	// is 90 s; keeping it means a second backtest run started a minute later
	// reuses the connections from the first.
	t.IdleConnTimeout = 90 * time.Second

	// Bound the phases that can hang without producing data. These are not an
	// overall time limit — see below — they are limits on silence.
	t.TLSHandshakeTimeout = 10 * time.Second
	t.ResponseHeaderTimeout = 30 * time.Second
	t.ExpectContinueTimeout = 1 * time.Second

	return &http.Client{
		Transport: t,

		// Client.Timeout is deliberately left at zero, and this is the one
		// setting here that looks like an omission.
		//
		// It bounds the entire exchange — dial, request, response headers, and
		// the whole body read — with a single deadline. A 1s monthly archive
		// is 93 MB, so any value large enough not to kill that transfer on a
		// slow link is far too large to catch a hung connection, and any value
		// small enough to catch a hang will cancel a legitimate download that
		// was streaming perfectly well. One number cannot do both jobs.
		//
		// The jobs are split instead: ResponseHeaderTimeout above catches a
		// server that accepts a connection and then says nothing, and the
		// per-call context.Context bounds the operation as a whole. The
		// context is the better tool anyway, because the caller sets it and a
		// cancelled backtest stops mid-transfer rather than at a fixed
		// deadline.
	}
}
