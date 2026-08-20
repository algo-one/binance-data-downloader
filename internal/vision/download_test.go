package vision

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newTestDownloader starts an httptest.Server running handler and returns a
// Downloader aimed at it, with the retry policy that does not sleep.
func newTestDownloader(t *testing.T, handler http.HandlerFunc) *Downloader {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return NewDownloader(srv.URL, srv.Client(), testPolicy())
}

// sha256Hex is what Binance writes into a .CHECKSUM sidecar.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:])
}

func TestDownloadStreamsAndHashes(t *testing.T) {
	t.Parallel()

	// Not a real archive: this test is about the transfer, and the real
	// archives are exercised end to end by the root package's download_test.go
	// against their genuine published checksums.
	payload := bytes.Repeat([]byte("PK\x03\x04 not really a zip "), 5000)

	var gotPath string

	dl := newTestDownloader(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write(payload)
	})

	const key = "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-15.zip"

	var dst bytes.Buffer

	res, err := dl.Download(t.Context(), key, &dst)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	if gotPath != "/"+key {
		t.Errorf("requested %q, want %q", gotPath, "/"+key)
	}

	if !bytes.Equal(dst.Bytes(), payload) {
		t.Errorf("destination holds %d bytes, want the %d served", dst.Len(), len(payload))
	}

	// The hash must be of the bytes that were written, computed in the same
	// pass rather than by reading anything back.
	if want := sha256Hex(payload); res.SHA256 != want {
		t.Errorf("SHA256 = %s, want %s", res.SHA256, want)
	}

	if res.Size != int64(len(payload)) {
		t.Errorf("Size = %d, want %d", res.Size, len(payload))
	}
}

// TestRateLimitErrorCarriesTheServersHint covers the number that used to be
// read and thrown away.
//
// The retry loop consults Retry-After to schedule its own backoff, and within
// one request that is all it needs. But a 429 that outlives every attempt is
// not a fact about one request — it says the pipeline as a whole is going too
// fast, and only the layer owning the worker pool can slow that down. Reporting
// a bare "rate limited" to that layer tells it to back off without telling it
// for how long, leaving it to guess at a number the server had already supplied
// one frame below.
func TestRateLimitErrorCarriesTheServersHint(t *testing.T) {
	t.Parallel()

	// The clock is injected so that the date form is exact rather than
	// dependent on how long the suite took to get here.
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"delta seconds", "30", 30 * time.Second},
		{"an http date", "Tue, 18 Aug 2026 12:00:45 GMT", 45 * time.Second},
		// No header is 0, not an error and not a guess. "The server did not say"
		// is information in its own right, and the pool applies its own policy.
		{"no header", "", 0},
		{"a header that parses as neither", "soon please", 0},
		// Deliberately far above maxRetryAfter. That constant bounds what this
		// package will wait *inside* one request; how long a whole pipeline
		// should pause is the pool's policy, and silently rounding a
		// misconfigured proxy's day down to 30 s here would hide the
		// misconfiguration instead of surfacing it.
		{"an absurd hint is reported unclamped", "86400", 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.header != "" {
					w.Header().Set("Retry-After", tt.header)
				}

				w.WriteHeader(http.StatusTooManyRequests)
			}))
			t.Cleanup(srv.Close)

			p := testPolicy()
			p.Now = func() time.Time { return now }

			dl := NewDownloader(srv.URL, srv.Client(), p)

			_, err := dl.Download(t.Context(), "some/key.zip", &bytes.Buffer{})
			if err == nil {
				t.Fatal("got nil, want an error")
			}

			// A caller that only branches on the condition never learns the
			// type exists. This must keep working through RateLimitError's
			// Unwrap, or every existing 429 check breaks silently.
			if !errors.Is(err, ErrRateLimited) {
				t.Fatalf("got %v, want an error wrapping ErrRateLimited", err)
			}

			// errors.As wants **RateLimitError: Error has a pointer receiver,
			// so *RateLimitError is what implements error.
			var rl *RateLimitError
			if !errors.As(err, &rl) {
				t.Fatalf("got %T, want an error unwrapping to *RateLimitError", err)
			}

			if rl.RetryAfter != tt.want {
				t.Errorf("RetryAfter = %v, want %v", rl.RetryAfter, tt.want)
			}

			if rl.Key != "some/key.zip" {
				t.Errorf("Key = %q, want the object that was throttled", rl.Key)
			}
		})
	}
}

func TestDownloadStatusErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    int
		body      string
		wantErr   error
		wantHits  int64
		wantInMsg string
	}{
		{
			// The important one. An archive Binance never published is a fact
			// about the calendar — a month not yet released, a day before the
			// symbol was listed — and the caller branches on it rather than
			// failing. It is also not retried: see the classification test.
			name:   "404 is a fact, not a failure",
			status: http.StatusNotFound, wantErr: ErrNotFound, wantHits: 1,
		},
		{
			// Reached only after the policy has already backed off and asked
			// again, so it means the whole pipeline is going too fast.
			name:   "429 that outlives the retries",
			status: http.StatusTooManyRequests, wantErr: ErrRateLimited, wantHits: 4,
		},
		{
			// No sentinel: an unexpected status is exactly the case where the
			// body is the only thing that explains anything.
			name:     "403 quotes the body",
			status:   http.StatusForbidden,
			body:     "<Error><Code>AccessDenied</Code></Error>",
			wantHits: 1, wantInMsg: "AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64

			dl := newTestDownloader(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})

			_, err := dl.Download(t.Context(), "some/key.zip", &bytes.Buffer{})
			if err == nil {
				t.Fatal("got nil, want an error")
			}

			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v, want an error wrapping %v", err, tt.wantErr)
			}

			if tt.wantInMsg != "" && !strings.Contains(err.Error(), tt.wantInMsg) {
				t.Errorf("error %q does not mention %q", err, tt.wantInMsg)
			}

			// The key belongs in every message: a bare "object not found"
			// during a fan-out over thirty days says nothing.
			if !strings.Contains(err.Error(), "some/key.zip") {
				t.Errorf("error %q does not name the key", err)
			}

			if got := calls.Load(); got != tt.wantHits {
				t.Errorf("server saw %d requests, want %d", got, tt.wantHits)
			}
		})
	}
}

// TestDownloadReportsATruncatedTransfer covers the case that makes streaming
// worth being careful about: a connection that dies part way through delivers a
// file that is well-formed as far as it goes.
//
// Nothing about those bytes is detectably wrong on their own — this is what the
// checksum exists for — but the transfer itself knows, and saying so here turns
// a mysterious corrupt-archive error two layers up into a network error where
// it happened.
func TestDownloadReportsATruncatedTransfer(t *testing.T) {
	t.Parallel()

	dl := newTestDownloader(t, func(w http.ResponseWriter, _ *http.Request) {
		// Hijack the connection to declare more than is sent. Going through
		// the normal ResponseWriter would have net/http notice and correct the
		// mismatch, which is exactly the behaviour under test.
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}

		conn, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
		_, _ = buf.WriteString("only the first few bytes")
		_ = buf.Flush()
	})

	var dst bytes.Buffer

	_, err := dl.Download(t.Context(), "some/key.zip", &dst)
	if err == nil {
		t.Fatal("got nil, want a truncated-transfer error")
	}

	// The byte count is in the message because "reset after 0 bytes" and
	// "reset after 90 MB" are different problems.
	if !strings.Contains(err.Error(), "after 24 bytes") {
		t.Errorf("error %q does not say how far it got", err)
	}

	// The documented contract: dst holds whatever arrived. The caller discards
	// it — the cache does that for free by writing to a temp file it only
	// renames into place on success.
	if got := dst.String(); got != "only the first few bytes" {
		t.Errorf("destination holds %q; the partial write should be visible", got)
	}
}

func TestChecksum(t *testing.T) {
	t.Parallel()

	const (
		key  = "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-15.zip"
		want = "ea666c96c344145155ba118c469805b34455156c8f1da0234bd7f2546874d99a"
	)

	var gotPath string

	dl := newTestDownloader(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		// Exactly the shape Binance publishes: no trailing newline.
		_, _ = fmt.Fprintf(w, "%s  BTCUSDT-1h-2024-01-15.zip", want)
	})

	got, err := dl.Checksum(t.Context(), key)
	if err != nil {
		t.Fatalf("Checksum: %v", err)
	}

	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// The suffix is appended here so no caller has to remember it.
	if wantPath := "/" + key + ChecksumSuffix; gotPath != wantPath {
		t.Errorf("requested %q, want %q", gotPath, wantPath)
	}
}

// TestChecksumRejectsASidecarForAnotherFile pins the check that turns a
// misrouted request into an honest error.
//
// A mirror serving one directory's sidecar for another file, or a proxy with a
// stale cache, hands back a perfectly valid SHA-256 for the wrong archive. Used
// unchecked it fails later as a checksum mismatch — sending whoever
// investigates hunting for a corruption that never happened.
func TestChecksumRejectsASidecarForAnotherFile(t *testing.T) {
	t.Parallel()

	dl := newTestDownloader(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "%s  BTCUSDT-1h-2024-01-16.zip", strings.Repeat("ab", 32))
	})

	_, err := dl.Checksum(t.Context(), "x/BTCUSDT-1h-2024-01-15.zip")
	if err == nil {
		t.Fatal("got nil, want an error naming both files")
	}

	for _, want := range []string{"2024-01-16", "2024-01-15"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestChecksumRejectsAnOversizedSidecar(t *testing.T) {
	t.Parallel()

	dl := newTestDownloader(t, func(w http.ResponseWriter, _ *http.Request) {
		// A proxy's HTML error page, served with a 200. A sidecar is 91 bytes;
		// anything approaching a kilobyte is not one.
		_, _ = w.Write(bytes.Repeat([]byte("<html>not a checksum</html>"), 1000))
	})

	_, err := dl.Checksum(t.Context(), "x/BTCUSDT-1h-2024-01-15.zip")
	if err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("got %v, want a size-limit error", err)
	}
}

func TestParseChecksum(t *testing.T) {
	t.Parallel()

	const (
		hash = "ea666c96c344145155ba118c469805b34455156c8f1da0234bd7f2546874d99a"
		name = "BTCUSDT-1h-2024-01-15.zip"
	)

	tests := []struct {
		name     string
		body     string
		wantName string
		want     string
		wantErr  bool
	}{
		{
			// The real thing, byte for byte: two spaces, no trailing newline.
			name: "as Binance publishes it",
			body: hash + "  " + name, wantName: name, want: hash,
		},
		{
			name: "trailing newline",
			body: hash + "  " + name + "\n", wantName: name, want: hash,
		},
		{
			name: "CRLF line ending",
			body: hash + "  " + name + "\r\n", wantName: name, want: hash,
		},
		{
			name: "a single space",
			body: hash + " " + name, wantName: name, want: hash,
		},
		{
			// coreutils marks a file hashed in binary mode with an asterisk.
			name: "binary-mode marker",
			body: hash + " *" + name, wantName: name, want: hash,
		},
		{
			// Hex is case-insensitive; the comparison downstream is not, so
			// normalising here is what keeps == a valid check.
			name: "uppercase hex is normalised",
			body: strings.ToUpper(hash) + "  " + name, wantName: name, want: hash,
		},
		{
			name: "no file name at all",
			body: hash, wantName: name, want: hash,
		},
		{
			name: "caller does not care about the name",
			body: hash + "  something-else.zip", wantName: "", want: hash,
		},
		{
			name: "names a different file",
			body: hash + "  BTCUSDT-1h-2024-01-16.zip", wantName: name, wantErr: true,
		},
		{
			name: "empty", body: "", wantName: name, wantErr: true,
		},
		{
			name: "whitespace only", body: "  \n  ", wantName: name, wantErr: true,
		},
		{
			// A hash of the wrong length is a different algorithm, and
			// comparing against it would fail as a mismatch — a corruption
			// report for what is really a format change.
			name: "sha1 rather than sha256",
			body: strings.Repeat("ab", 20) + "  " + name, wantName: name, wantErr: true,
		},
		{
			name: "right length, not hexadecimal",
			body: strings.Repeat("zz", 32) + "  " + name, wantName: name, wantErr: true,
		},
		{
			name: "an HTML error page", body: "<html><body>404</body></html>", wantName: name, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseChecksum([]byte(tt.body), tt.wantName)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %q, want an error", got)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseChecksum: %v", err)
			}

			if got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

// TestDownloadJoinsPathsCleanly covers the configuration mistake that would
// otherwise only appear as a 404: a base URL written with a trailing slash.
func TestDownloadJoinsPathsCleanly(t *testing.T) {
	t.Parallel()

	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte("x"))
	}))
	t.Cleanup(srv.Close)

	for _, base := range []string{srv.URL, srv.URL + "/"} {
		dl := NewDownloader(base, srv.Client(), testPolicy())

		if _, err := dl.Download(t.Context(), "data/spot/x.zip", &bytes.Buffer{}); err != nil {
			t.Fatalf("Download with base %q: %v", base, err)
		}

		if want := "/data/spot/x.zip"; gotPath != want {
			t.Errorf("base %q requested %q, want %q", base, gotPath, want)
		}
	}
}

func TestNewDownloaderDefaults(t *testing.T) {
	t.Parallel()

	dl := NewDownloader("", nil, Policy{})

	if dl.baseURL != DefaultDownloadBaseURL {
		t.Errorf("baseURL = %q, want %q", dl.baseURL, DefaultDownloadBaseURL)
	}

	// Not http.DefaultClient. Its transport keeps two idle connections per
	// host, so defaulting to it would hand a caller who passed nil the exact
	// connection churn correctness requirement 8 names — from the constructor
	// the documentation advertises. The tuned client is the default; supplying
	// your own is the opt-out.
	if dl.client == http.DefaultClient {
		t.Fatal("client defaulted to http.DefaultClient, whose MaxIdleConnsPerHost is 2 — that is bug 8")
	}

	if dl.client != defaultClient() {
		t.Error("client should default to the process-wide client from NewHTTPClient")
	}

	// The zero Policy must arrive filled in, or MaxAttempts of 0 would mean a
	// loop that never runs.
	if dl.policy.MaxAttempts != DefaultPolicy().MaxAttempts {
		t.Errorf("MaxAttempts = %d, want the default %d", dl.policy.MaxAttempts, DefaultPolicy().MaxAttempts)
	}

	if dl.policy.After == nil || dl.policy.Jitter == nil || dl.policy.Now == nil {
		t.Error("the zero Policy must arrive with its functions filled in")
	}
}
