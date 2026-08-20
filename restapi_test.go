package binancedata

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// newTestRESTFetcher starts an httptest.Server running handler and returns a
// fetcher aimed at it.
//
// The limiter is built enormous on purpose. Its behaviour is pinned in
// internal/vision/limiter_test.go; letting it also pace these tests would make
// every assertion about pagination depend on a rate calculation too.
func newTestRESTFetcher(t *testing.T, handler http.HandlerFunc) restFetcher {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return restFetcher{api: vision.NewAPI(srv.URL, srv.Client(), fastPolicy(), vision.NewLimiter(1e6, 1e6))}
}

// fakeKline is one generated candle: the instant it opens, and the JSON row the
// endpoint would send for it.
type fakeKline struct {
	open time.Time
	json string
}

// makeKlines generates n consecutive candles of the given length, with prices
// that satisfy checkValues — low ≤ open, close ≤ high, and taker volumes inside
// their totals — so that a test failing here has failed at what it was aiming
// at rather than at the row verification.
func makeKlines(start time.Time, iv time.Duration, n int) []fakeKline {
	out := make([]fakeKline, 0, n)

	for i := range n {
		open := start.Add(time.Duration(i) * iv)

		// Binance's close time is inclusive: one millisecond before the next
		// candle opens, in the millisecond era the REST API still uses.
		close := open.Add(iv).Add(-time.Millisecond)

		out = append(out, fakeKline{
			open: open,
			json: fmt.Sprintf(`[%d,"100","110","90","105","1",%d,"105",7,"0.5","52.5","0"]`,
				open.UnixMilli(), close.UnixMilli()),
		})
	}

	return out
}

// serveKlines answers requests the way the real endpoint does: it honours
// startTime and endTime as inclusive millisecond bounds and caps the result at
// limit.
//
// Faithfulness matters more here than in most mocks. The pagination loop is
// built entirely around those three parameters, so a handler that ignored them
// and returned everything would let a cursor that never advanced pass as
// working.
func serveKlines(rows []fakeKline, calls *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}

		q := r.URL.Query()

		startMs := parseMs(q.Get("startTime"), -1)
		endMs := parseMs(q.Get("endTime"), 1<<62)

		limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil || limit <= 0 {
			limit = 500
		}

		var page []string

		for _, row := range rows {
			ms := row.open.UnixMilli()
			if ms < startMs || ms > endMs {
				continue
			}

			page = append(page, row.json)

			if len(page) == limit {
				break
			}
		}

		_, _ = w.Write([]byte("[" + strings.Join(page, ",") + "]"))
	}
}

func parseMs(s string, fallback int64) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fallback
	}

	return v
}

// TestRESTAgreesWithTheArchive is the strongest test in this file, and the
// reason a REST fixture was captured for a day an archive fixture already
// covers.
//
// The two sources are different formats served by different hosts — a zipped
// CSV from the bucket, a JSON array from the API — and this library treats them
// as interchangeable. That is only true if they decode to the same candles, and
// nothing checks it except this. Both fixtures are real: 24 hours of BTCUSDT on
// 2024-01-15, fetched from Binance rather than constructed here.
func TestRESTAgreesWithTheArchive(t *testing.T) {
	t.Parallel()

	start, end := dayRange(2024, 1, 15)

	archive, size := readFixture(t, "BTCUSDT-1h-2024-01-15.zip")

	want, err := decodeArchiveAll(t.Context(), archive, size,
		decodeSpec{Interval: Interval1h, Start: start, End: end})
	if err != nil {
		t.Fatalf("decoding the archive fixture: %v", err)
	}

	body, err := os.ReadFile(filepath.Join("testdata", "BTCUSDT-1h-2024-01-15.klines.json"))
	if err != nil {
		t.Fatalf("reading the REST fixture: %v", err)
	}

	f := newTestRESTFetcher(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	})

	// now is exactly the end of the range. The last candle opens at 23:00 and
	// its interval closes at midnight, so this is the boundary case for the
	// partial-candle rule: a candle whose interval ends exactly now has
	// closed, and must be kept.
	got, err := f.klines(t.Context(), restRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
	}, end)
	if err != nil {
		t.Fatalf("klines: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("REST returned %d candles, the archive has %d", len(got), len(want))
	}

	if len(want) != 24 {
		t.Fatalf("the archive fixture holds %d candles, want 24 — has testdata changed?", len(want))
	}

	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Errorf("candle %d differs:\n  REST    %+v\n  archive %+v", i, got[i], want[i])
		}
	}
}

// TestRESTPaginatesWholeRange covers the loop itself: more candles than one
// page holds, fetched without a gap and without a duplicate at either seam.
//
// The seams are the whole risk. startTime is inclusive, so a cursor advanced by
// nothing returns the last candle again — and one advanced too far skips the
// next one, which is a silent hole of exactly one bar.
func TestRESTPaginatesWholeRange(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)

	// 2,500 one-minute candles: three pages at the endpoint's maximum, the
	// last one short.
	const count = 2500

	rows := makeKlines(start, time.Minute, count)
	end := start.Add(count * time.Minute)

	var calls atomic.Int32

	f := newTestRESTFetcher(t, serveKlines(rows, &calls))

	got, err := f.klines(t.Context(), restRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1m, Start: start, End: end,
	}, end)
	if err != nil {
		t.Fatalf("klines: %v", err)
	}

	if len(got) != count {
		t.Fatalf("got %d candles, want %d", len(got), count)
	}

	if want := int32(3); calls.Load() != want {
		t.Errorf("made %d requests, want %d (1000 + 1000 + 500)", calls.Load(), want)
	}

	// Every candle in order, each exactly one interval after the last. This is
	// what catches a duplicated or dropped bar at a page boundary, which a
	// count alone would not: 2,500 candles with one duplicate and one missing
	// still counts to 2,500.
	for i, k := range got {
		if want := start.Add(time.Duration(i) * time.Minute); !k.OpenTime.Equal(want) {
			t.Fatalf("candle %d opens at %s, want %s", i, k.OpenTime.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

// TestRESTStopsAtTheLastClosedCandle is the partial-candle rule.
//
// The endpoint always returns the interval in progress, whose volume and close
// price are still moving. Everything downstream assumes a candle is settled
// once seen — the Parquet tier caches it, Stage 7 merges on open time — so
// admitting one makes two identical requests seconds apart disagree, with
// neither being wrong.
func TestRESTStopsAtTheLastClosedCandle(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	rows := makeKlines(start, time.Hour, 5)

	// Half past the fifth candle's hour: four have closed, the fifth is still
	// forming.
	now := start.Add(4*time.Hour + 30*time.Minute)
	end := start.Add(5 * time.Hour)

	tests := []struct {
		name           string
		includePartial bool
		wantCandles    int
	}{
		{name: "dropped by default", includePartial: false, wantCandles: 4},
		{name: "kept when asked for", includePartial: true, wantCandles: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newTestRESTFetcher(t, serveKlines(rows, nil))
			f.includePartial = tc.includePartial

			got, err := f.klines(t.Context(), restRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
			}, now)
			if err != nil {
				t.Fatalf("klines: %v", err)
			}

			if len(got) != tc.wantCandles {
				t.Fatalf("got %d candles, want %d", len(got), tc.wantCandles)
			}

			// The last one kept must be the one expected, not merely the right
			// count of them.
			wantLast := start.Add(time.Duration(tc.wantCandles-1) * time.Hour)
			if last := got[len(got)-1].OpenTime; !last.Equal(wantLast) {
				t.Errorf("last candle opens at %s, want %s",
					last.Format(time.RFC3339), wantLast.Format(time.RFC3339))
			}
		})
	}
}

// TestRESTRejectsBadPages covers the responses that are well-formed HTTP and
// wrong about the data. Each one is quietly plausible, which is why each is
// checked rather than trusted.
func TestRESTRejectsBadPages(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	end := start.Add(5 * time.Hour)

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "a repeated candle",
			// Strict increase subsumes ordering and duplication at once. A
			// page that repeats its last row would also stall the cursor,
			// since the cursor is derived from it.
			body: `[[1705276800000,"100","110","90","105","1",1705280399999,"105",7,"0.5","52.5","0"],` +
				`[1705276800000,"100","110","90","105","1",1705280399999,"105",7,"0.5","52.5","0"]]`,
			want: "does not follow the previous candle",
		},
		{
			name: "candles running backwards",
			body: `[[1705280400000,"100","110","90","105","1",1705283999999,"105",7,"0.5","52.5","0"],` +
				`[1705276800000,"100","110","90","105","1",1705280399999,"105",7,"0.5","52.5","0"]]`,
			want: "does not follow the previous candle",
		},
		{
			name: "a candle from outside the range",
			// One day early. Nothing about the row is malformed; it simply
			// answers a different question than the one asked, which is what
			// a misrouted or cached response looks like.
			body: `[[1705190400000,"100","110","90","105","1",1705193999999,"105",7,"0.5","52.5","0"]]`,
			want: "outside the period",
		},
		{
			name: "a candle off the interval grid",
			// Opens at 00:30 on a 1h grid.
			body: `[[1705278600000,"100","110","90","105","1",1705282199999,"105",7,"0.5","52.5","0"]]`,
			want: "not aligned",
		},
		{
			name: "prices that contradict each other",
			// A low above the high: the signature of a column shifted by one,
			// which parses perfectly and is simply wrong.
			body: `[[1705276800000,"100","90","110","105","1",1705280399999,"105",7,"0.5","52.5","0"]]`,
			want: "prices are inconsistent",
		},
		{
			name: "a taker volume larger than its total",
			body: `[[1705276800000,"100","110","90","105","1",1705280399999,"105",7,"9","52.5","0"]]`,
			want: "exceeds volume",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newTestRESTFetcher(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			})

			_, err := f.klines(t.Context(), restRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
			}, end)
			if err == nil {
				t.Fatal("klines accepted the page, want an error")
			}

			if !errors.Is(err, ErrCorruptArchive) {
				t.Errorf("error %v does not wrap ErrCorruptArchive", err)
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestRESTEmptyRangeIsNotAnError covers a range Binance has no data for, which
// is routinely the correct answer — before the pair was listed, most often.
// Reporting it as a failure would make the planner treat "there is nothing
// here" as something to retry or fall back from.
func TestRESTEmptyRangeIsNotAnError(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	end := start.Add(5 * time.Hour)

	var calls atomic.Int32

	f := newTestRESTFetcher(t, serveKlines(nil, &calls))

	got, err := f.klines(t.Context(), restRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
	}, end)
	if err != nil {
		t.Fatalf("klines: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("got %d candles, want 0", len(got))
	}

	if calls.Load() != 1 {
		t.Errorf("made %d requests, want 1 — an empty first page ends the range", calls.Load())
	}
}

// TestRESTUsesTheRESTIntervalSpelling covers the one interval whose two names
// differ, and differ in a way that is hostile to a typo: "1m" is a minute and
// "1M" is a month, so a stray case fold turns a month of candles into a minute
// of them without any error at all.
func TestRESTUsesTheRESTIntervalSpelling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		interval Interval
		want     string
	}{
		{interval: Interval1mo, want: "1M"},
		{interval: Interval1m, want: "1m"},
		{interval: Interval1h, want: "1h"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()

			var got string

			f := newTestRESTFetcher(t, func(w http.ResponseWriter, r *http.Request) {
				got = r.URL.Query().Get("interval")

				_, _ = w.Write([]byte("[]"))
			})

			start := utc(2024, 1, 1)

			if _, err := f.klines(t.Context(), restRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: tc.interval,
				Start: start, End: start.AddDate(0, 2, 0),
			}, start.AddDate(0, 2, 0)); err != nil {
				t.Fatalf("klines: %v", err)
			}

			if got != tc.want {
				t.Errorf("interval parameter = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRESTValidatesBeforeSpendingARequest checks that a ref which cannot
// describe a range is refused locally, without a round trip or a unit of quota.
//
// The market case is the one that matters most. The endpoint serves spot only,
// so a futures ref would come back with spot candles under a futures label —
// numbers that look entirely reasonable and are for the wrong instrument.
func TestRESTValidatesBeforeSpendingARequest(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	end := start.Add(5 * time.Hour)

	tests := []struct {
		name string
		ref  restRef
	}{
		{
			name: "unset market",
			ref:  restRef{Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end},
		},
		{
			name: "unset interval",
			ref:  restRef{Market: MarketSpot, Symbol: "BTCUSDT", Start: start, End: end},
		},
		{
			name: "unnormalised symbol",
			ref:  restRef{Market: MarketSpot, Symbol: "BTC/USDT", Interval: Interval1h, Start: start, End: end},
		},
		{
			name: "empty symbol",
			ref:  restRef{Market: MarketSpot, Interval: Interval1h, Start: start, End: end},
		},
		{
			name: "end before start",
			ref:  restRef{Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: end, End: start},
		},
		{
			name: "empty range",
			ref:  restRef{Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: start},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var called atomic.Bool

			f := newTestRESTFetcher(t, func(w http.ResponseWriter, _ *http.Request) {
				called.Store(true)

				_, _ = w.Write([]byte("[]"))
			})

			_, err := f.klines(t.Context(), tc.ref, end)
			if err == nil {
				t.Fatal("klines accepted an invalid ref, want an error")
			}

			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error %v does not wrap ErrInvalidRequest", err)
			}

			if called.Load() {
				t.Error("a request was sent for a ref that should have been refused locally")
			}
		})
	}
}

// TestRESTTranslatesTransportErrors checks the seam between internal/vision's
// vocabulary and this package's. A caller branches on the sentinels in
// errors.go and should never need to know internal/vision exists.
func TestRESTTranslatesTransportErrors(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	end := start.Add(5 * time.Hour)

	tests := []struct {
		name      string
		status    int
		body      string
		want      error
		wantNotIs error
	}{
		{
			name:   "404 is a fact about the calendar",
			status: http.StatusNotFound,
			want:   ErrNotAvailable,
		},
		{
			name:   "429 asks the pipeline to slow down",
			status: http.StatusTooManyRequests,
			want:   ErrRateLimited,
		},
		{
			name:   "418 is a ban, and still a rate limit",
			status: http.StatusTeapot,
			want:   ErrRateLimited,
		},
		{
			name:   "an unknown symbol is the caller's bug",
			status: http.StatusBadRequest,
			body:   `{"code":-1121,"msg":"Invalid symbol."}`,
			want:   ErrInvalidRequest,
			// Above all not ErrNotAvailable: "Binance has not published this"
			// would send whoever is debugging to the wrong place entirely.
			wantNotIs: ErrNotAvailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newTestRESTFetcher(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})

			_, err := f.klines(t.Context(), restRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
			}, end)

			if !errors.Is(err, tc.want) {
				t.Errorf("error %v does not wrap %v", err, tc.want)
			}

			if tc.wantNotIs != nil && errors.Is(err, tc.wantNotIs) {
				t.Errorf("error %v should not wrap %v", err, tc.wantNotIs)
			}
		})
	}
}

// TestRESTStopsOnCancellation checks that a cancelled backtest stops rather
// than paginating on. The context reaches the request through
// http.NewRequestWithContext, which is what makes this immediate rather than
// something that takes effect at the next page boundary.
func TestRESTStopsOnCancellation(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	rows := makeKlines(start, time.Minute, 2500)
	end := start.Add(2500 * time.Minute)

	ctx, cancel := context.WithCancel(t.Context())

	var calls atomic.Int32

	serve := serveKlines(rows, &calls)

	f := newTestRESTFetcher(t, func(w http.ResponseWriter, r *http.Request) {
		// Cancel while the first page is being served, so the second page is
		// the one that has to notice.
		cancel()

		serve(w, r)
	})

	_, err := f.klines(ctx, restRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1m, Start: start, End: end,
	}, end)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("klines = %v, want context.Canceled", err)
	}

	if calls.Load() > 2 {
		t.Errorf("made %d requests after cancellation, want at most 2", calls.Load())
	}
}
