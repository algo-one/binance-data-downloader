package vision

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestNewHTTPClientTuning pins the three settings that are not defaults, each
// of which would be invisible if it silently reverted.
func TestNewHTTPClientTuning(t *testing.T) {
	t.Parallel()

	c := NewHTTPClient()

	// Client.Timeout bounds the whole exchange including the body read. A 1s
	// monthly archive is 93 MB, so any value that does not kill that transfer
	// is too large to catch a hang, and any value that catches a hang kills the
	// transfer. Bounding belongs to ResponseHeaderTimeout and the context.
	if c.Timeout != 0 {
		t.Errorf("Client.Timeout = %v, want 0 — see the comment in client.go", c.Timeout)
	}

	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport is %T, want *http.Transport", c.Transport)
	}

	// The default is 2. Every request this library makes goes to one host, so
	// leaving it there means a pool of eight workers re-dials for six of them
	// on every chunk — the connection reuse of correctness requirement 8,
	// present in name only.
	if tr.MaxIdleConnsPerHost < 8 {
		t.Errorf("MaxIdleConnsPerHost = %d, want comfortably above any concurrency limit", tr.MaxIdleConnsPerHost)
	}

	if tr.ResponseHeaderTimeout == 0 {
		t.Error("ResponseHeaderTimeout is unset; a server that accepts a connection and says nothing would hang forever")
	}

	// Cloning http.DefaultTransport rather than building one from scratch is
	// what keeps proxy support, the dual-stack dialer and HTTP/2. A
	// hand-written &http.Transport{} drops all three, and the proxy one fails
	// only for the people behind a proxy.
	if tr.Proxy == nil {
		t.Error("Proxy is nil; the transport was built from scratch rather than cloned from the default")
	}

	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 is false; the clone lost the default's HTTP/2 negotiation")
	}
}

// TestOneClientReusesConnections is correctness requirement 8 stated as a
// measurement: several requests through one client must share one connection.
//
// The bug it guards against — a fresh http.Client per request — passes every
// functional test there is. It costs a TCP and TLS handshake per archive, which
// only shows up as a stopwatch reading, so the test counts connections instead
// of asserting on behaviour that cannot distinguish the two.
func TestOneClientReusesConnections(t *testing.T) {
	t.Parallel()

	var conns atomic.Int64

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("archive bytes"))
	}))

	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}

	srv.Start()
	t.Cleanup(srv.Close)

	// The server is plain HTTP, so NewHTTPClient's own transport can be used
	// directly; srv.Client() would only matter for TLS.
	dl := NewDownloader(srv.URL, NewHTTPClient(), testPolicy())

	for i := range 5 {
		var dst strings.Builder

		if _, err := dl.Download(t.Context(), "data/spot/x.zip", &dst); err != nil {
			t.Fatalf("download %d: %v", i, err)
		}
	}

	if got := conns.Load(); got != 1 {
		t.Errorf("five downloads opened %d connections, want 1", got)
	}
}
