package binancedata

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewLoaderDefaults pins what a caller who passes nothing gets. Every line
// here is a decision recorded in options.go, and a default that drifts without
// anyone noticing is the failure this catches.
func TestNewLoaderDefaults(t *testing.T) {
	t.Parallel()

	// Offline hosts, not because this test fetches anything, but because
	// leaving them unset means the real Binance endpoints — and CLAUDE.md's
	// "no test may touch Binance" is worth keeping true by construction rather
	// than by everyone remembering not to add a Fetch here later. The default
	// cache dir is deliberately left alone, since that is one of the defaults
	// under test.
	l, err := NewLoader(withOfflineHosts())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	if got := cap(l.sem); got != defaultConcurrency {
		t.Errorf("concurrency is %d, want %d", got, defaultConcurrency)
	}

	if l.logger == nil {
		t.Error("logger is nil; a nil *slog.Logger panics on first use")
	}

	if l.progress != nil {
		t.Error("a progress callback was installed without being asked for")
	}

	if l.now == nil {
		t.Fatal("clock is nil")
	}

	// The clock must be UTC. Everything downstream requires it, and a local
	// one would be rejected several layers away with a confusing message.
	if loc := l.now().Location(); loc != time.UTC {
		t.Errorf("the default clock reads %s, want UTC", loc)
	}

	// The cache root defaults under the OS cache directory and is absolute:
	// a relative one would follow the process's working directory, so a
	// program calling os.Chdir would end up with two caches without being
	// told.
	if !filepath.IsAbs(l.cache.root) {
		t.Errorf("cache root %q is not absolute", l.cache.root)
	}
}

// TestNewLoaderTouchesNothing is the constructor's other promise. A program
// that builds a Loader at startup must not fail to start because Binance is
// down, and must not leave a directory tree behind for a run that then fetched
// nothing.
func TestNewLoaderTouchesNothing(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "not-created-yet")

	if _, err := NewLoader(withOfflineHosts(), WithCacheDir(dir)); err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the constructor created %q; it should be created on first write", dir)
	}
}

func TestNewLoaderRejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
	}{
		{
			// Not "use the default": a config file with a missing key would
			// otherwise write to somewhere its author never chose.
			name: "an empty cache dir",
			opt:  WithCacheDir(""),
		},
		{
			name: "zero concurrency",
			opt:  WithConcurrency(0),
		},
		{
			name: "negative concurrency",
			opt:  WithConcurrency(-1),
		},
		{
			name: "a nil http client",
			opt:  WithHTTPClient(nil),
		},
		{
			name: "a nil progress function",
			opt:  WithProgress(nil),
		},
		{
			name: "a nil logger",
			opt:  WithLogger(nil),
		},
		{
			// The shape of a conditional that returned nothing on one branch.
			// Saying so beats a nil dereference inside the constructor.
			name: "a nil option",
			opt:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l, err := NewLoader(tt.opt, withOfflineHosts())
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("got error %v, want one wrapping ErrInvalidRequest", err)
			}

			if l != nil {
				t.Error("got a Loader alongside the error")
			}
		})
	}
}

// TestEveryOptionIsHonoured is the project's rule about options stated as a
// test: an accepted-and-ignored setting is a defect, not a stub. Each case
// applies one option and then looks at where it should have landed.
func TestEveryOptionIsHonoured(t *testing.T) {
	t.Parallel()

	t.Run("cache dir", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()

		l, err := NewLoader(withOfflineHosts(), WithCacheDir(dir))
		if err != nil {
			t.Fatalf("NewLoader: %v", err)
		}

		if got := l.cache.root; got != dir {
			t.Errorf("cache root is %q, want %q", got, dir)
		}
	})

	t.Run("concurrency", func(t *testing.T) {
		t.Parallel()

		l, err := NewLoader(withOfflineHosts(), WithConcurrency(3))
		if err != nil {
			t.Fatalf("NewLoader: %v", err)
		}

		if got := cap(l.sem); got != 3 {
			t.Errorf("the pool holds %d permits, want 3", got)
		}
	})

	t.Run("http client", func(t *testing.T) {
		t.Parallel()

		// Checking that a field was assigned would prove nothing useful — the
		// client has to reach all three transports, and the way to know it did
		// is to count what went through it. A counting RoundTripper wrapping
		// the test server's own is the whole test.
		f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
		f.start(t)

		var trips atomic.Int64

		client := &http.Client{Transport: countingTransport{n: &trips, next: http.DefaultTransport}}

		l, err := NewLoader(
			WithCacheDir(t.TempDir()),
			WithHTTPClient(client),
			withTestHosts(f.listURL, f.archiveURL, f.restURL),
			withClock(func() time.Time { return utc(2026, 8, 20) }),
			withPolicy(fastPolicy()),
		)
		if err != nil {
			t.Fatalf("NewLoader: %v", err)
		}

		if _, err := l.Fetch(t.Context(), Request{
			Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
			Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
		}); err != nil {
			t.Fatalf("Fetch: %v", err)
		}

		// Two listings and two archive requests, all of them through the
		// client the caller supplied.
		if got := trips.Load(); got != 4 {
			t.Errorf("%d requests went through the supplied client, want 4", got)
		}
	})

	t.Run("progress", func(t *testing.T) {
		t.Parallel()

		called := false

		l, err := NewLoader(withOfflineHosts(), WithProgress(func(Progress) { called = true }))
		if err != nil {
			t.Fatalf("NewLoader: %v", err)
		}

		if l.progress == nil {
			t.Fatal("the progress callback was not installed")
		}

		l.report(Progress{})

		if !called {
			t.Error("report did not reach the installed callback")
		}
	})

	t.Run("logger", func(t *testing.T) {
		t.Parallel()

		logger := slog.New(slog.DiscardHandler)

		l, err := NewLoader(withOfflineHosts(), WithLogger(logger))
		if err != nil {
			t.Fatalf("NewLoader: %v", err)
		}

		if l.logger != logger {
			t.Error("the logger was not installed")
		}
	})
}

// TestReportWithoutACallbackIsSafe: the nil check is the whole of it, and it
// runs on every chunk of every fetch.
func TestReportWithoutACallbackIsSafe(t *testing.T) {
	t.Parallel()

	l, err := NewLoader(withOfflineHosts())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	l.report(Progress{}) // must not panic
}

// countingTransport counts the requests that pass through it and forwards them
// unchanged. It is how "the supplied client was actually used" becomes a
// measurement rather than an assertion about a struct field.
type countingTransport struct {
	n    *atomic.Int64
	next http.RoundTripper
}

func (c countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)

	return c.next.RoundTrip(r)
}
