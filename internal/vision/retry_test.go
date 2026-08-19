package vision

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testPolicy is the production retry policy with the waiting taken out.
//
// Only two fields change: the timer fires immediately, and the jitter is the
// identity so a recorded delay is the delay the code computed rather than a
// random draw from it. Everything that decides *whether* to retry is left
// exactly as it ships, which is the point — a test policy that also changed
// MaxAttempts would be testing a configuration nobody runs.
func testPolicy() Policy {
	p := DefaultPolicy()
	p.Jitter = func(d time.Duration) time.Duration { return d }
	p.After = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}

		return ch
	}

	return p
}

// recordingPolicy is testPolicy with the requested delays captured, so a test
// can assert on the backoff schedule without spending it.
func recordingPolicy() (Policy, *[]time.Duration) {
	delays := &[]time.Duration{}

	p := testPolicy()
	p.After = func(d time.Duration) <-chan time.Time {
		*delays = append(*delays, d)

		ch := make(chan time.Time, 1)
		ch <- time.Time{}

		return ch
	}

	return p, delays
}

// newTestRequest builds a GET aimed at url. Every request this package makes is
// a bodyless GET, which is what makes retrying it sound.
func newTestRequest(t *testing.T, ctx context.Context, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}

	return req
}

func TestDoWithRetryRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The first two attempts fail with a status the policy retries; the
		// third succeeds. This is the shape of a load balancer restarting.
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), testPolicy())
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("server saw %d requests, want 3", got)
	}
}

func TestDoWithRetryReturnsTheLastResponseWhenAttemptsRunOut(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("still down"))
	}))
	t.Cleanup(srv.Close)

	p := testPolicy()

	resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), p)
	// A response, not an error. Exhausting the retries is not a failure to
	// obtain an answer — the answer is a 503, and the caller is the one who
	// knows what a 503 means for what it was asking. Synthesising an error
	// here would throw away the body, which is the only diagnostic there is.
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}

	if got := calls.Load(); got != int64(p.MaxAttempts) {
		t.Errorf("server saw %d requests, want %d", got, p.MaxAttempts)
	}

	// The body must still be readable: nothing in the retry loop may consume
	// the response it hands back. bodySnippet returns it quoted, which is what
	// keeps a body that is not text from reaching a terminal raw.
	if s := bodySnippet(resp); s != `"still down"` {
		t.Errorf("body = %s, want %q", s, "still down")
	}
}

func TestDoWithRetryStatusClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   int
		wantHits int64
	}{
		// Retried: transient by definition.
		{"request timeout", http.StatusRequestTimeout, 4},
		{"too early", http.StatusTooEarly, 4},
		{"too many requests", http.StatusTooManyRequests, 4},
		{"internal server error", http.StatusInternalServerError, 4},
		{"bad gateway", http.StatusBadGateway, 4},
		{"service unavailable", http.StatusServiceUnavailable, 4},
		{"gateway timeout", http.StatusGatewayTimeout, 4},

		// Not retried. 404 is the one that matters: an unpublished archive is
		// the single most common non-200 this library sees, and retrying it
		// would multiply the cost of every gap in the calendar by four while
		// looking, in a log, exactly like a network problem.
		{"not found", http.StatusNotFound, 1},
		{"forbidden", http.StatusForbidden, 1},
		{"moved permanently", http.StatusMovedPermanently, 1},
		{"bad request", http.StatusBadRequest, 1},
		{"ok", http.StatusOK, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(srv.Close)

			// A redirect status would otherwise be followed by the client
			// rather than returned, and this test is about the retry loop.
			client := *srv.Client()
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

			resp, err := doWithRetry(t.Context(), &client, newTestRequest(t, t.Context(), srv.URL), testPolicy())
			if err != nil {
				t.Fatalf("doWithRetry: %v", err)
			}
			drainAndClose(resp)

			if got := calls.Load(); got != tt.wantHits {
				t.Errorf("server saw %d requests, want %d", got, tt.wantHits)
			}
		})
	}
}

func TestBackoffSchedule(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	p, delays := recordingPolicy()

	resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), p)
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	drainAndClose(resp)

	// Four attempts means three waits, doubling from BaseDelay. The last
	// attempt is not followed by a wait — waiting after the final try is a
	// half-second of latency added to every failure for no benefit, and it is
	// an easy off-by-one to write.
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}

	if len(*delays) != len(want) {
		t.Fatalf("waited %v, want %v", *delays, want)
	}

	for i, d := range *delays {
		if d != want[i] {
			t.Errorf("wait %d = %v, want %v", i+1, d, want[i])
		}
	}
}

func TestBackoffCapsAndCannotOverflow(t *testing.T) {
	t.Parallel()

	p := testPolicy()

	// Attempt numbers far beyond any policy: the shift is what overflows a
	// time.Duration into a negative number, and a negative delay is a timer
	// that fires immediately — a retry storm produced by the code meant to
	// prevent one.
	for _, attempt := range []int{-1, 0, 1, 5, 40, 100, 1000} {
		got := p.backoff(attempt)

		if got < 0 {
			t.Errorf("backoff(%d) = %v, must never be negative", attempt, got)
		}

		if got > p.MaxDelay {
			t.Errorf("backoff(%d) = %v, want at most %v", attempt, got, p.MaxDelay)
		}
	}
}

func TestFullJitterStaysInRange(t *testing.T) {
	t.Parallel()

	const ceiling = time.Second

	// Jitter is random, so the assertion is on the bounds rather than the
	// value, and on the spread: a "jitter" that always returned the ceiling
	// would satisfy the bounds while doing nothing about a thundering herd.
	seen := map[time.Duration]bool{}

	for range 100 {
		d := fullJitter(ceiling)

		if d < 0 || d >= ceiling {
			t.Fatalf("fullJitter(%v) = %v, want [0, %v)", ceiling, d, ceiling)
		}

		seen[d] = true
	}

	if len(seen) < 50 {
		t.Errorf("only %d distinct delays in 100 draws; jitter is not spreading", len(seen))
	}

	if got := fullJitter(0); got != 0 {
		t.Errorf("fullJitter(0) = %v, want 0", got)
	}
}

func TestRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   time.Duration
		wantOK bool
	}{
		{"absent", "", 0, false},
		{"delta seconds", "120", 2 * time.Minute, true},
		{"zero seconds", "0", 0, true},
		{"negative seconds", "-5", 0, false},
		{"http date in the future", "Tue, 18 Aug 2026 12:00:30 GMT", 30 * time.Second, true},
		// A date already past means "now", not "in the past". A negative
		// duration would reach Policy.wait, which treats anything at or below
		// zero as no wait at all — so the outcome is the same either way, but
		// only one of the two is a delay this code deliberately chose.
		{"http date in the past", "Tue, 18 Aug 2026 11:59:00 GMT", 0, true},
		{"nonsense", "soon please", 0, false},
		{"empty-ish", "   ", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := retryAfter(tt.header, now)

			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}

			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDoWithRetryHonoursRetryAfter(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	p, delays := recordingPolicy()

	resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), p)
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	drainAndClose(resp)

	// The server's instruction replaces the computed backoff — it knows when it
	// will be ready and we do not — but it is a floor, not the last word.
	// Jitter is added on top, so nobody retries earlier than the server asked
	// and forty workers holding the same header do not all re-fire on the same
	// millisecond.
	//
	// testPolicy's jitter is the identity, so the addition here is exactly one
	// BaseDelay and the delays are checkable.
	want := 3*time.Second + p.BaseDelay

	for i, d := range *delays {
		if d != want {
			t.Errorf("wait %d = %v, want the header's 3s plus jitter (%v)", i+1, d, want)
		}
	}
}

// TestDoWithRetryDecorrelatesAConcurrentHerd is the thundering herd stated as a
// measurement.
//
// A shared Retry-After is the worst case for lockstep retries, because it is
// the one input that makes every worker's backoff *identical* by construction —
// the jitter that would otherwise spread them has been replaced by a number the
// server handed to all of them at once. Forty workers then re-fire on the same
// millisecond and recreate the overload they are backing off from.
//
// The assertion is on the spread rather than on any value: a policy that
// honoured the header literally produces one distinct delay across every
// worker, which is precisely what must not happen.
func TestDoWithRetryDecorrelatesAConcurrentHerd(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	const workers = 40

	var (
		mu   sync.Mutex
		seen = map[time.Duration]bool{}
		wg   sync.WaitGroup
	)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// The real jitter, not the identity: this test is about the
			// randomness, so substituting it away would test nothing.
			p := DefaultPolicy()
			p.MaxAttempts = 2
			p.After = func(d time.Duration) <-chan time.Time {
				mu.Lock()
				seen[d] = true
				mu.Unlock()

				ch := make(chan time.Time, 1)
				ch <- time.Time{}

				return ch
			}

			resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), p)
			if err != nil {
				return
			}

			drainAndClose(resp)
		}()
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(seen) < workers/2 {
		t.Errorf("%d workers produced %d distinct delays; the herd is still in lockstep", workers, len(seen))
	}

	// Spreading must never mean retrying before the server said it would be
	// ready. The jitter is added to the header's value, not drawn from it.
	for d := range seen {
		if d < time.Second {
			t.Errorf("waited %v, which is earlier than the header's 1s", d)
		}
	}
}

// TestDoWithRetryStillBacksOffWhenRetryAfterIsZero covers the reading of the
// header that turns backing off into not backing off at all.
//
// retryAfter reports (0, true) for a literal "Retry-After: 0" and for any
// HTTP-date already in the past — which a clock a couple of seconds fast
// produces from an entirely ordinary "two seconds from now". Taken at face
// value that is a zero delay, and Policy.wait short-circuits on anything at or
// below zero, so all four attempts would fire back to back at a server that had
// just reported it was overloaded.
func TestDoWithRetryStillBacksOffWhenRetryAfterIsZero(t *testing.T) {
	t.Parallel()

	// The clock is injected, so "in the past" is exact rather than dependent on
	// how long the suite takes to reach this line.
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
	}{
		{"literal zero", "0"},
		{"an http date that has already passed", "Tue, 18 Aug 2026 11:59:00 GMT"},
		{"an http date one second in the past", "Tue, 18 Aug 2026 11:59:59 GMT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int64

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Retry-After", tt.header)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			t.Cleanup(srv.Close)

			p, delays := recordingPolicy()
			p.Now = func() time.Time { return now }

			resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), p)
			if err != nil {
				t.Fatalf("doWithRetry: %v", err)
			}
			drainAndClose(resp)

			if len(*delays) != p.MaxAttempts-1 {
				t.Fatalf("waited %d times for %d attempts, want %d", len(*delays), p.MaxAttempts, p.MaxAttempts-1)
			}

			// Every wait must be a real one. Policy.wait treats anything at or
			// below zero as no wait at all, so a zero here is four requests
			// with nothing between them.
			for i, d := range *delays {
				if d <= 0 {
					t.Errorf("wait %d = %v; a zero Retry-After became no backoff at all", i+1, d)
				}

				if d < p.BaseDelay {
					t.Errorf("wait %d = %v, want at least BaseDelay (%v)", i+1, d, p.BaseDelay)
				}
			}
		})
	}
}

func TestDoWithRetryCapsAnAbsurdRetryAfter(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "86400") // a misconfigured proxy: one day
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	p, delays := recordingPolicy()

	resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), p)
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	drainAndClose(resp)

	// The cap binds, and the jitter is added after it: a cap that every worker
	// lands on to the millisecond is the same herd in slower motion, so the
	// bound the constant states is on what the *header* can ask for rather than
	// on the final delay.
	want := maxRetryAfter + p.BaseDelay

	for i, d := range *delays {
		if d != want {
			t.Errorf("wait %d = %v, want the %v cap plus jitter (%v)", i+1, d, maxRetryAfter, want)
		}
	}
}

func TestDoWithRetryStopsWhenTheContextIsCancelled(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(t.Context())

	// A timer that cancels instead of firing: this is the moment a backtest is
	// interrupted while a worker sits in its backoff. Without the select on
	// ctx.Done in Policy.wait, the wait would complete and a fourth request
	// would go out after the caller had already given up.
	p := testPolicy()
	p.After = func(time.Duration) <-chan time.Time {
		cancel()

		return make(chan time.Time) // never fires
	}

	resp, err := doWithRetry(ctx, srv.Client(), newTestRequest(t, ctx, srv.URL), p)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}

	// Nothing to close: an error return means no response was obtained, and a
	// caller deferring a close on it would panic. That contract is asserted
	// rather than assumed.
	if resp != nil {
		_ = resp.Body.Close()

		t.Error("an error return must not also carry a response")
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("server saw %d requests, want 1", got)
	}
}

func TestDoWithRetryReportsATransportFailure(t *testing.T) {
	t.Parallel()

	// A server that is closed before the request: connecting fails at the
	// transport, which is the class of failure retrying exists for.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	client := srv.Client()
	srv.Close()

	p := testPolicy()

	resp, err := doWithRetry(t.Context(), client, newTestRequest(t, t.Context(), url), p)
	if err == nil {
		t.Fatal("got nil, want a transport error")
	}

	if resp != nil {
		_ = resp.Body.Close()

		t.Error("an error return must not also carry a response")
	}

	// The attempt count belongs in the message: "connection refused" and
	// "connection refused, four times, over three seconds" are different
	// diagnoses.
	if !strings.Contains(err.Error(), "after 4 attempts") {
		t.Errorf("error %q does not report the attempts spent", err)
	}
}

// TestDoWithRetryReplaysARequestBody covers the gap between what the guard in
// doWithRetry checks and what the loop needs.
//
// The guard admits any request whose GetBody is non-nil. The loop then retried
// via req.Clone, and Clone deep-copies everything *except* the body: it copies
// the Body field, so both requests hold the same io.ReadCloser. After one round
// trip that reader is at EOF and net/http has closed it, so attempt two sent a
// zero-length payload — a request the server rejects for reasons that look
// entirely like a server bug.
//
// Nothing in this package sends a body today; every request is a bodyless GET.
// The guard exists precisely for the day that changes, which makes a guard
// stating the wrong requirement worse than none: it reads as a check that has
// been thought about.
func TestDoWithRetryReplaysARequestBody(t *testing.T) {
	t.Parallel()

	const payload = "the request body"

	var (
		mu     sync.Mutex
		bodies []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)

		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()

		// Fail the first two attempts, so the body has to survive being sent
		// twice before it is finally accepted.
		if len(bodies) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	// http.NewRequestWithContext installs GetBody automatically for the body
	// types it can rewind — *bytes.Reader, *bytes.Buffer, *strings.Reader. That
	// is what makes this the realistic shape of a future POST rather than a
	// contrivance built to pass.
	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, srv.URL, strings.NewReader(payload))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}

	if req.GetBody == nil {
		t.Fatal("NewRequestWithContext did not install GetBody; the test is not testing what it claims")
	}

	resp, err := doWithRetry(t.Context(), srv.Client(), req, testPolicy())
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	drainAndClose(resp)

	mu.Lock()
	defer mu.Unlock()

	if len(bodies) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(bodies))
	}

	// Every attempt, not just the first. An empty string here is the bug: the
	// reader reached EOF on attempt one and was never rewound.
	for i, got := range bodies {
		if got != payload {
			t.Errorf("attempt %d sent %q, want %q", i+1, got, payload)
		}
	}
}

// TestDoWithRetryRejectsAnUnreplayableBody is the other half: a body that
// cannot be rewound must be refused up front rather than retried into a
// zero-length request.
func TestDoWithRetryRejectsAnUnreplayableBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}

	// A reader net/http cannot rewind: io.NopCloser hides the concrete type, so
	// NewRequest installs no GetBody. This is what a streaming upload looks
	// like, and it is the case the guard exists for.
	req.Body = io.NopCloser(strings.NewReader("one shot"))

	resp, err := doWithRetry(t.Context(), srv.Client(), req, testPolicy())
	if err == nil {
		drainAndClose(resp)

		t.Fatal("got nil, want a refusal to retry an unreplayable body")
	}

	if resp != nil {
		_ = resp.Body.Close()

		t.Error("an error return must not also carry a response")
	}

	if !strings.Contains(err.Error(), "cannot be replayed") {
		t.Errorf("error %q does not say why the request was refused", err)
	}
}

// TestRetriesReuseOneConnection is the regression test for a bug that no
// functional test would ever catch: every non-200 response whose body is closed
// without being read costs the connection it arrived on.
//
// net/http can only return a connection to the idle pool once its body has been
// read to EOF. Close on a partially read body therefore closes the TCP
// connection, so a burst of 503s — exactly when the server is least able to
// afford it — makes this library re-handshake TCP and TLS for every single
// retry. Nothing fails, everything works, and correctness requirement 8 is
// quietly false.
//
// Counting connections rather than requests is what makes it visible.
func TestRetriesReuseOneConnection(t *testing.T) {
	t.Parallel()

	var conns atomic.Int64

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		// A body large enough that skipping the drain is not something the
		// transport papers over.
		_, _ = w.Write([]byte(strings.Repeat("service unavailable\n", 200)))
	}))

	// ConnState fires on every state change of every connection; StateNew is
	// one fresh TCP connection accepted. It has to be set before Start.
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}

	srv.Start()
	t.Cleanup(srv.Close)

	resp, err := doWithRetry(t.Context(), srv.Client(), newTestRequest(t, t.Context(), srv.URL), testPolicy())
	if err != nil {
		t.Fatalf("doWithRetry: %v", err)
	}
	drainAndClose(resp)

	// Four attempts over one connection. Without the drain this is 4.
	if got := conns.Load(); got != 1 {
		t.Errorf("opened %d connections for 4 attempts, want 1", got)
	}
}
