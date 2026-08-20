package vision

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// oneRow is a real BTCUSDT 1h row, captured from the live endpoint on
// 2026-08-20. Real rather than invented, because the point of several tests
// below is that the exact characters Binance sends survive the decode.
const oneRow = `[1704067200000,"42283.58000000","42554.57000000","42261.02000000",` +
	`"42475.23000000","1271.68108000",1704070799999,"53957248.97378900",47134,` +
	`"682.57581000","28957416.81964500","0"]`

// newTestAPI starts an httptest.Server running handler and returns an API aimed
// at it, with a retry policy that does not sleep and a limiter that never
// binds.
//
// The limiter is deliberately loose here. Its own behaviour is pinned in
// limiter_test.go; letting it also pace these tests would make every assertion
// about URLs and status codes depend on a rate calculation as well.
func newTestAPI(t *testing.T, handler http.HandlerFunc) *API {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return NewAPI(srv.URL, srv.Client(), testPolicy(), NewLimiter(1e6, 1e6))
}

// jsonHandler answers every request with body and the given status.
func jsonHandler(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// TestKlinesBuildsTheDocumentedQuery pins the two off-by-one conversions that
// nothing else would catch.
//
// The endpoint's startTime and endTime are both inclusive and in whole
// milliseconds, while this project's ranges are half-open with nanosecond
// precision. Each mismatch can move exactly one candle across the boundary,
// which is the kind of error that shows up as a duplicated or missing bar at
// the seam between two chunks and nowhere else.
func TestKlinesBuildsTheDocumentedQuery(t *testing.T) {
	t.Parallel()

	day := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		start, end  time.Time
		limit       int
		wantStartMs string
		wantEndMs   string
		wantLimit   string
	}{
		{
			name:  "millisecond-aligned bounds",
			start: day,
			end:   day.AddDate(0, 0, 1),
			limit: 1000,
			// End is exclusive, so the last instant asked for is the
			// millisecond before midnight.
			wantStartMs: "1705276800000",
			wantEndMs:   "1705363199999",
			wantLimit:   "1000",
		},
		{
			name:  "sub-millisecond start rounds up",
			start: day.Add(500 * time.Microsecond),
			end:   day.AddDate(0, 0, 1),
			limit: 1000,
			// Truncating downwards would ask for the candle opening at
			// midnight, which is before the range and which the decoder would
			// then correctly reject as outside the period.
			wantStartMs: "1705276800001",
			wantEndMs:   "1705363199999",
			wantLimit:   "1000",
		},
		{
			name:  "sub-millisecond end truncates",
			start: day,
			end:   day.AddDate(0, 0, 1).Add(500 * time.Microsecond),
			limit: 1000,
			// The largest whole millisecond strictly below End.
			wantStartMs: "1705276800000",
			wantEndMs:   "1705363200000",
			wantLimit:   "1000",
		},
		{
			name:        "a zero limit is left to the endpoint's default",
			start:       day,
			end:         day.AddDate(0, 0, 1),
			limit:       0,
			wantStartMs: "1705276800000",
			wantEndMs:   "1705363199999",
			wantLimit:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got url.Values

			api := newTestAPI(t, func(w http.ResponseWriter, r *http.Request) {
				got = r.URL.Query()

				if r.URL.Path != "/"+klinesPath {
					t.Errorf("path = %q, want %q", r.URL.Path, "/"+klinesPath)
				}

				_, _ = w.Write([]byte("[]"))
			})

			_, err := api.Klines(t.Context(), KlineQuery{
				Symbol:   "BTCUSDT",
				Interval: "1h",
				Start:    tc.start,
				End:      tc.end,
				Limit:    tc.limit,
			})
			if err != nil {
				t.Fatalf("Klines: %v", err)
			}

			for param, want := range map[string]string{
				"symbol":    "BTCUSDT",
				"interval":  "1h",
				"startTime": tc.wantStartMs,
				"endTime":   tc.wantEndMs,
				"limit":     tc.wantLimit,
			} {
				if g := got.Get(param); g != want {
					t.Errorf("%s = %q, want %q", param, g, want)
				}
			}
		})
	}
}

// TestKlinesKeepsDigitsExactly is the test this whole file exists for.
//
// The obvious decode of a kline row is into []any, which turns every unquoted
// number into a float64 — and would do the same to a price if the shape ever
// changed. A real BTCUSDT monthly quote volume is 118661604939.99255335, which
// float64 cannot hold. So the rows are lifted out as raw characters and never
// pass through a numeric type in this package at all.
func TestKlinesKeepsDigitsExactly(t *testing.T) {
	t.Parallel()

	// Twenty significant digits, the worst real value measured across 1.75
	// million archive rows. Written as the quote-volume column.
	const huge = "118661604939.99255335"

	row := fmt.Sprintf(`[1704067200000,"1","2","0.5","1.5","10",1704070799999,%q,47134,"1","1","0"]`, huge)

	api := newTestAPI(t, jsonHandler(http.StatusOK, "["+row+"]"))

	page, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})
	if err != nil {
		t.Fatalf("Klines: %v", err)
	}

	if len(page.Klines) != 1 {
		t.Fatalf("got %d rows, want 1", len(page.Klines))
	}

	got := page.Klines[0]

	// Column 7 is the quote volume; the constants naming it live in the root
	// package, so this test names the index it is asserting on directly.
	if got[7] != huge {
		t.Errorf("quote volume = %q, want %q", got[7], huge)
	}

	// The unquoted columns keep their literal text too, digits unchanged.
	if got[0] != "1704067200000" {
		t.Errorf("open time = %q, want %q", got[0], "1704067200000")
	}

	if got[8] != "47134" {
		t.Errorf("trades = %q, want %q", got[8], "47134")
	}
}

func TestKlinesRejectsMalformedRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "too few fields",
			body: `[[1704067200000,"1","2","0.5","1.5","10",1704070799999,"1",47134,"1","1"]]`,
			want: "has 11 fields",
		},
		{
			name: "too many fields",
			body: `[[1704067200000,"1","2","0.5","1.5","10",1704070799999,"1",47134,"1","1","0","extra"]]`,
			want: "has 13 fields",
		},
		{
			name: "a null where a value belongs",
			body: `[[1704067200000,"1","2","0.5","1.5","10",1704070799999,null,47134,"1","1","0"]]`,
			want: "expected a number or a string",
		},
		{
			name: "a nested object where a value belongs",
			body: `[[1704067200000,"1","2","0.5","1.5","10",1704070799999,{"a":1},47134,"1","1","0"]]`,
			want: "expected a number or a string",
		},
		{
			name: "not JSON at all",
			body: `<!DOCTYPE html><html><body>maintenance</body></html>`,
			want: "decoding response",
		},
		{
			name: "an object where the array belongs",
			body: `{"code":-1121,"msg":"Invalid symbol."}`,
			want: "decoding response",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			api := newTestAPI(t, jsonHandler(http.StatusOK, tc.body))

			_, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})
			if err == nil {
				t.Fatal("Klines accepted a malformed body, want an error")
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestKlinesEmptyPageIsNotAnError covers a range Binance simply has no data
// for — before the pair was listed, most often. That is a fact rather than a
// failure, and reporting it as an error would make the pagination loop above
// treat "the range ran out" as something to retry.
func TestKlinesEmptyPageIsNotAnError(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, jsonHandler(http.StatusOK, "[]"))

	page, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})
	if err != nil {
		t.Fatalf("Klines: %v", err)
	}

	if len(page.Klines) != 0 {
		t.Errorf("got %d rows, want 0", len(page.Klines))
	}
}

// TestKlinesBoundsTheBody stops a misrouted request from streaming an unbounded
// body into memory — a redirect to a CDN's video file, or an endpoint that
// changed shape.
func TestKlinesBoundsTheBody(t *testing.T) {
	t.Parallel()

	// Comfortably past maxKlinesResponse, built from valid rows so that the
	// only thing wrong with it is the size.
	huge := "[" + strings.TrimSuffix(strings.Repeat(oneRow+",", (maxKlinesResponse/len(oneRow))+64), ",") + "]"

	if len(huge) <= maxKlinesResponse {
		t.Fatalf("test body is %d bytes, needs to exceed the %d-byte cap", len(huge), maxKlinesResponse)
	}

	api := newTestAPI(t, jsonHandler(http.StatusOK, huge))

	if _, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"}); err == nil {
		t.Fatal("Klines read a body past the cap without complaint, want an error")
	}
}

// TestKlinesStatusErrors covers every status this endpoint answers with, and
// the sentinel each one has to carry. The 418 case is the one worth the most:
// it is a ban rather than a throttle, and a caller that cannot tell them apart
// will retry into a longer ban.
func TestKlinesStatusErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      int
		body        string
		retryAfter  string
		wantIs      []error
		wantIsNot   []error
		wantMessage string
	}{
		{
			name:      "429 is a throttle",
			status:    http.StatusTooManyRequests,
			body:      `{"code":-1003,"msg":"Too much request weight used"}`,
			wantIs:    []error{ErrRateLimited},
			wantIsNot: []error{ErrIPBanned},
		},
		{
			name:   "418 is a ban and is also a throttle",
			status: http.StatusTeapot,
			body:   `{"code":-1003,"msg":"IP banned until 1659146400000"}`,
			// Both, so a caller that only asks "should I slow down?" still
			// gets a yes, while one that can tell the difference can.
			wantIs: []error{ErrRateLimited, ErrIPBanned},
		},
		{
			name:   "400 carries Binance's own code",
			status: http.StatusBadRequest,
			body:   `{"code":-1121,"msg":"Invalid symbol."}`,
			wantIs: []error{ErrBadRequest},
			// Not a rate limit, and above all not a 404 — an unknown symbol
			// must not be reported as "Binance has not published this yet".
			wantIsNot:   []error{ErrRateLimited, ErrNotFound},
			wantMessage: "Invalid symbol.",
		},
		{
			name:      "404 is a missing object",
			status:    http.StatusNotFound,
			body:      "",
			wantIs:    []error{ErrNotFound},
			wantIsNot: []error{ErrBadRequest},
		},
		{
			name:   "an unexplained status quotes what arrived",
			status: http.StatusBadGateway,
			body:   "<html><title>502 Bad Gateway</title></html>",
			// Quoted by strconv.Quote, so the server's words are delimited
			// from ours.
			wantMessage: `"<html><title>502 Bad Gateway</title></html>"`,
			// A 502 is Binance's failure whether or not the thing that
			// answered bothered to explain itself in JSON.
			wantIs:    []error{ErrServerError},
			wantIsNot: []error{ErrBadRequest},
		},
		{
			// The finding this case exists for. Binance describes its own
			// failures in the same {"code","msg"} document it uses for a
			// refusal, so reading the body without reading the status reports
			// an outage as the caller's bug — and the root package maps
			// ErrBadRequest onto ErrInvalidRequest, which it documents as
			// always the caller's to fix.
			name:        "a 5xx with Binance's own document is still Binance's failure",
			status:      http.StatusServiceUnavailable,
			body:        `{"code":-1001,"msg":"Internal error; unable to process your request."}`,
			wantIs:      []error{ErrServerError},
			wantIsNot:   []error{ErrBadRequest, ErrNotFound, ErrRateLimited},
			wantMessage: "Internal error; unable to process your request.",
		},
		{
			// A 4xx nobody explained. The status is the whole diagnosis, and
			// it is enough of one: whose fault a refusal is depends on the
			// class of the status, not on whether the server chose to say
			// anything. This used to arrive untyped, so a caller could branch
			// on "this is my bug" only when Binance felt like sending JSON.
			name:      "an unexplained 4xx is still the caller's",
			status:    http.StatusForbidden,
			body:      "<html>blocked</html>",
			wantIs:    []error{ErrBadRequest},
			wantIsNot: []error{ErrServerError},
		},
		{
			// The parse reads a document-sized prefix rather than a
			// snippet-sized one. Read at the snippet's 204 bytes, a message
			// this long is cut mid-document, fails json.Unmarshal, and loses
			// both its code and its sentinel — so whether a caller could tell
			// that this was their own bug came down to how many characters
			// Binance put in msg.
			name:   "a long explanation still parses",
			status: http.StatusBadRequest,
			body: `{"code":-1102,"msg":"Mandatory parameter 'symbol' was not sent, was empty/null, ` +
				`or malformed. Reference the API documentation for the parameter list of this ` +
				`endpoint, its accepted spellings, and the constraints applied to each of them ` +
				`before retrying the request with a corrected parameter set."}`,
			wantIs:      []error{ErrBadRequest},
			wantMessage: "before retrying the request with a corrected parameter set.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			api := newTestAPI(t, func(w http.ResponseWriter, _ *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}

				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})

			_, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})
			if err == nil {
				t.Fatalf("status %d produced no error", tc.status)
			}

			for _, want := range tc.wantIs {
				if !errors.Is(err, want) {
					t.Errorf("error %v is not %v", err, want)
				}
			}

			for _, notWant := range tc.wantIsNot {
				if errors.Is(err, notWant) {
					t.Errorf("error %v should not be %v", err, notWant)
				}
			}

			if tc.wantMessage != "" && !strings.Contains(err.Error(), tc.wantMessage) {
				t.Errorf("error = %v, want it to mention %q", err, tc.wantMessage)
			}
		})
	}
}

// TestKlinesSpendsQuotaPerRequestNotPerCall is the accounting the limiter
// exists for.
//
// One call is one reservation only while nothing goes wrong. A retryable status
// turns it into as many as MaxAttempts requests, and it does so precisely when
// the budget matters — 429 and every retryable 5xx are the statuses that
// trigger the extra attempts, so the failure mode is that the limiter which
// exists to pre-empt an IP ban is what permits the burst that earns one.
//
// The bucket here holds exactly four requests' worth of weight and refills at
// one unit a second, so the test spends no real time and the reading afterwards
// is the whole spend: empty means four attempts were counted, and three
// quarters full means the old behaviour is back.
func TestKlinesSpendsQuotaPerRequestNotPerCall(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	t.Cleanup(srv.Close)

	lim := NewLimiter(1, 4*KlinesWeight)

	api := NewAPI(srv.URL, srv.Client(), testPolicy(), lim)

	_, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})
	if !errors.Is(err, ErrServerError) {
		t.Fatalf("error %v is not %v", err, ErrServerError)
	}

	if want := int32(DefaultPolicy().MaxAttempts); calls.Load() != want {
		t.Fatalf("the server saw %d requests, want %d", calls.Load(), want)
	}

	// Tokens() reports what is left, including whatever trickled back in while
	// the test ran — at one unit a second over a few milliseconds, well under
	// one. Anything above that is weight that was spent without being counted.
	if left := lim.Tokens(); left >= 1 {
		t.Errorf("%.1f of %d weight units left after %d requests, so %d of them went uncounted",
			left, 4*KlinesWeight, calls.Load(), calls.Load()-1)
	}
}

// TestKlinesRateLimitCarriesTheServersHint checks that the number Binance
// supplies survives up to the layer that can act on it. The retry loop reads
// Retry-After for its own backoff and discards it; a 429 that outlives every
// attempt is a fact about the pipeline, and the pool is the only thing that can
// slow the pipeline down.
func TestKlinesRateLimitCarriesTheServersHint(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTeapot)
	})

	_, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})

	var rl *RateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("error %v is not a *RateLimitError", err)
	}

	if !rl.Banned {
		t.Error("Banned = false for a 418, want true")
	}

	if want := 17 * time.Second; rl.RetryAfter != want {
		t.Errorf("RetryAfter = %s, want %s", rl.RetryAfter, want)
	}
}

// TestKlinesReportsUsedWeight covers the header that tells the layer above when
// the local accounting has lost track — a second process on the same address is
// the usual reason, and no amount of local bookkeeping can see that.
func TestKlinesReportsUsedWeight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
		want   int
	}{
		{name: "present", header: "482", want: 482},
		{name: "absent", header: "", want: 0},
		{name: "not a number", header: "lots", want: 0},
		{name: "negative", header: "-5", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			api := newTestAPI(t, func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("X-MBX-USED-WEIGHT-1M", tc.header)
				}

				_, _ = w.Write([]byte("[]"))
			})

			page, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"})
			if err != nil {
				t.Fatalf("Klines: %v", err)
			}

			if page.UsedWeight != tc.want {
				t.Errorf("UsedWeight = %d, want %d", page.UsedWeight, tc.want)
			}
		})
	}
}

// TestKlinesValidatesBeforeSpendingARequest checks that a query which cannot
// describe a page is refused locally. A request spent discovering a caller's
// typo is a request against the quota and a round trip, both for nothing.
func TestKlinesValidatesBeforeSpendingARequest(t *testing.T) {
	t.Parallel()

	day := time.Date(2024, time.January, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		q    KlineQuery
	}{
		{name: "no symbol", q: KlineQuery{Interval: "1h"}},
		{name: "no interval", q: KlineQuery{Symbol: "BTCUSDT"}},
		{name: "limit past the maximum", q: KlineQuery{Symbol: "BTCUSDT", Interval: "1h", Limit: MaxKlinesLimit + 1}},
		{name: "negative limit", q: KlineQuery{Symbol: "BTCUSDT", Interval: "1h", Limit: -1}},
		{name: "end before start", q: KlineQuery{Symbol: "BTCUSDT", Interval: "1h", Start: day, End: day.Add(-time.Hour)}},
		{name: "empty range", q: KlineQuery{Symbol: "BTCUSDT", Interval: "1h", Start: day, End: day}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var called bool

			api := newTestAPI(t, func(w http.ResponseWriter, _ *http.Request) {
				called = true

				_, _ = w.Write([]byte("[]"))
			})

			if _, err := api.Klines(t.Context(), tc.q); err == nil {
				t.Fatal("Klines accepted an invalid query, want an error")
			}

			if called {
				t.Error("a request was sent for a query that should have been refused locally")
			}
		})
	}
}

// TestKlinesPacesItselfAgainstTheLimiter checks the wiring rather than the
// arithmetic: the pacing itself is pinned in limiter_test.go, and what matters
// here is that Klines actually consults the limiter — and consults it *before*
// sending, not after.
//
// The bug this catches is the accepted-and-ignored setting: a limiter passed to
// a constructor, stored, and never asked. docs/architecture.md names that as a
// defect rather than a stub for exactly this reason.
//
// No bubble here, deliberately. This test drives a real httptest.Server, and a
// goroutine blocked on a real socket is never "durably blocked" in synctest's
// sense — it can be woken from outside the bubble — so the fake clock would not
// advance and the two clocks would fight. The assertion is on the request count
// instead of on a duration, which needs no clock at all.
func TestKlinesPacesItselfAgainstTheLimiter(t *testing.T) {
	t.Parallel()

	// One call's worth of bucket, refilling once an hour: the second call has
	// a wait ahead of it that no test would sit through.
	lim := NewLimiter(1.0/3600, KlinesWeight)

	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)

		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(srv.Close)

	api := NewAPI(srv.URL, srv.Client(), testPolicy(), lim)

	if _, err := api.Klines(t.Context(), KlineQuery{Symbol: "BTCUSDT", Interval: "1h"}); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Given a context that expires long before the bucket refills, rate.Limiter
	// works out that the wait cannot fit and refuses immediately rather than
	// sleeping — so this costs no real time.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if _, err := api.Klines(ctx, KlineQuery{Symbol: "BTCUSDT", Interval: "1h"}); err == nil {
		t.Fatal("the second call was not paced at all")
	}

	// The count is the real assertion: one request reached the server, so the
	// limiter refused the second before it was sent rather than after.
	if got := calls.Load(); got != 1 {
		t.Errorf("the server saw %d requests, want 1", got)
	}
}

// TestKlinesDefaultsAreShared pins the two defaults that are load-bearing
// rather than convenient.
//
// One client per process is correctness requirement 8. One limiter per process
// is the same argument applied to the quota: it is enforced per IP address, so
// two limiters each allowing the documented rate permit twice it — each correct
// alone and wrong together.
//
// The limiter is not a field on API — it is captured by the Reserve closure the
// constructor installs on the policy — so sharing is asserted where it actually
// happens, on the sync.OnceValue that produces it, and the constructor is
// checked for having wired pacing in at all.
func TestKlinesDefaultsAreShared(t *testing.T) {
	t.Parallel()

	a := NewAPI("", nil, Policy{}, nil)
	b := NewAPI("", nil, Policy{}, nil)

	if a.client != b.client {
		t.Error("two APIs hold different http.Clients, so they do not share a connection pool")
	}

	// Two calls into two variables, rather than comparing the calls directly:
	// staticcheck reads `f() != f()` as a mistake, and here it is the point.
	first, second := defaultLimiter(), defaultLimiter()
	if first != second {
		t.Error("the process-wide limiter is rebuilt per call, so limiters together exceed the quota each respects alone")
	}

	if a.policy.Reserve == nil || b.policy.Reserve == nil {
		t.Error("an API was built with no reservation on its policy, so its requests are unpaced")
	}

	if a.baseURL != DefaultAPIBaseURL {
		t.Errorf("baseURL = %q, want %q", a.baseURL, DefaultAPIBaseURL)
	}
}
