package binancedata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

	return restFetcher{
		api: vision.NewAPI(srv.URL, srv.Client(), fastPolicy(), vision.NewLimiter(1e6, 1e6)),
		// Discarding rather than nil. A nil *slog.Logger panics the moment a
		// handler sends the used-weight header, which is a failure this helper
		// would hand to whichever test happened to add one — so the default is
		// the same safe one defaultLoaderConfig picks. A test that wants to
		// read the records assigns its own over this.
		logger: slog.New(slog.DiscardHandler),
	}
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
		want      []error
		wantNotIs []error
	}{
		{
			// The bucket is a static file server, where a missing object is a
			// fact about the calendar. This endpoint is not: asked for a range
			// it has nothing in, it answers 200 and an empty array. So a 404
			// here is a misconfigured base URL or path, and calling it
			// ErrNotAvailable would have Stage 7 degrade the whole REST tail to
			// nothing and report success.
			name:      "404 is a misconfiguration, not a calendar fact",
			status:    http.StatusNotFound,
			wantNotIs: []error{ErrNotAvailable, ErrInvalidRequest},
		},
		{
			name:   "429 asks the pipeline to slow down",
			status: http.StatusTooManyRequests,
			want:   []error{ErrRateLimited},
			// A throttle is not a ban. Reporting one as the other would have a
			// caller that can stop, stop — for a condition that clears in
			// seconds.
			wantNotIs: []error{ErrIPBanned},
		},
		{
			name:   "418 is a ban, and still a rate limit",
			status: http.StatusTeapot,
			want:   []error{ErrRateLimited, ErrIPBanned},
		},
		{
			name:   "an unknown symbol is the caller's bug",
			status: http.StatusBadRequest,
			body:   `{"code":-1121,"msg":"Invalid symbol."}`,
			want:   []error{ErrInvalidRequest},
			// Above all not ErrNotAvailable: "Binance has not published this"
			// would send whoever is debugging to the wrong place entirely.
			wantNotIs: []error{ErrNotAvailable},
		},
		{
			// The finding this test exists for. Binance answers a 5xx with the
			// same {"code","msg"} document it uses for a 400, so a reading that
			// looks only at the body reports its own outage as the caller's
			// bug — and ErrInvalidRequest is documented as always the caller's
			// to fix, so a worker pool told that would refuse to retry the one
			// failure retrying was made for.
			name:      "a 5xx outage is nobody's bug but Binance's",
			status:    http.StatusServiceUnavailable,
			body:      `{"code":-1001,"msg":"Internal error; unable to process your request."}`,
			wantNotIs: []error{ErrInvalidRequest, ErrNotAvailable, ErrCorruptArchive},
		},
		{
			// The same status with a body from something that is not Binance at
			// all — a proxy in the way. The verdict comes from the status class
			// either way; only the detail in the message changes.
			name:      "a 5xx from a proxy is judged the same way",
			status:    http.StatusBadGateway,
			body:      "<html><title>502 Bad Gateway</title></html>",
			wantNotIs: []error{ErrInvalidRequest, ErrNotAvailable},
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

			if err == nil {
				t.Fatal("a non-200 produced no error at all")
			}

			for _, want := range tc.want {
				if !errors.Is(err, want) {
					t.Errorf("error %v does not wrap %v", err, want)
				}
			}

			for _, notWant := range tc.wantNotIs {
				if errors.Is(err, notWant) {
					t.Errorf("error %v should not wrap %v", err, notWant)
				}
			}
		})
	}
}

// serveKlinesContaining answers the way the endpoint would if its inclusive
// startTime selected the kline whose *interval contains* the timestamp, rather
// than the first kline opening at or after it.
//
// # Why a second handler exists
//
// Because the first one cannot test this. [serveKlines] filters on
// row.open.UnixMilli(), which is one of the two readings of Binance's
// documented "inclusive startTime" — and a handler written here necessarily
// encodes whichever reading its author assumed, so a cursor that depends on
// that assumption passes against it no matter what Binance actually does.
//
// This is the other reading, and it is the unforgiving one: a cursor landing
// one millisecond past a candle's open is still inside that candle, so the page
// begins by repeating the candle the previous page ended with, and
// appendPage's strict-increase check fails the whole fetch. A cursor landing
// exactly on the next candle's open is correct under both, which is why
// klines advances by intervalEnd and why this test can pass at the same time as
// the one above.
func serveKlinesContaining(rows []fakeKline, iv time.Duration, calls *atomic.Int32) http.HandlerFunc {
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
			openMs := row.open.UnixMilli()

			// The whole difference: a row qualifies when startTime falls
			// anywhere inside its interval, not only on its open.
			closeMs := row.open.Add(iv).UnixMilli() - 1
			if closeMs < startMs || openMs > endMs {
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

// TestRESTPaginatesUnderEitherStartTimeReading is the regression test for the
// pagination cursor.
//
// Binance documents startTime as inclusive and does not say inclusive of what.
// The cursor used to be the last candle's open plus one millisecond, which is
// an instant *inside* that candle and therefore only safe under one of the two
// readings. Under the other, every fetch longer than one page fails — loudly,
// but on the first real multi-page range anyone asks for.
//
// Advancing to the next candle's open instead removes the dependency rather
// than betting on the answer, and this test is what says so: the same range,
// the same assertions, a handler implementing the other reading.
func TestRESTPaginatesUnderEitherStartTimeReading(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)

	const count = 2500

	rows := makeKlines(start, time.Minute, count)
	end := start.Add(count * time.Minute)

	var calls atomic.Int32

	f := newTestRESTFetcher(t, serveKlinesContaining(rows, time.Minute, &calls))

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

	for i, k := range got {
		if want := start.Add(time.Duration(i) * time.Minute); !k.OpenTime.Equal(want) {
			t.Fatalf("candle %d opens at %s, want %s", i, k.OpenTime.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

// TestRESTRequiresAClock covers the one input to klines that fails silently.
//
// Every other field is validated, and a zero time.Time passes through to the
// partial-candle rule as an instant that every candle ever published closes
// after — so appendPage stops on the first row of the first page and the fetch
// returns no candles and no error. A failed read wearing the shape of an empty
// range is the conflation the typed errors in errors.go exist to prevent.
func TestRESTRequiresAClock(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	rows := makeKlines(start, time.Hour, 24)
	end := start.AddDate(0, 0, 1)

	var calls atomic.Int32

	f := newTestRESTFetcher(t, serveKlines(rows, &calls))

	got, err := f.klines(t.Context(), restRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
	}, time.Time{})

	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("error %v does not wrap %v", err, ErrInvalidRequest)
	}

	if got != nil {
		t.Errorf("got %d candles alongside the error, want none", len(got))
	}

	if calls.Load() != 0 {
		t.Errorf("made %d requests for a call that could not succeed, want 0", calls.Load())
	}
}

// TestRESTMalformedResponseIsACorruptArchive checks that a body which is not
// klines reaches the caller as the sentinel for "Binance sent bytes this
// library cannot understand".
//
// The condition is one condition however the bytes were packaged: a CSV row
// with the wrong column count and a JSON array that is not an array of arrays
// are the same problem, and a caller should not have to discover which layer
// noticed in order to know what to do. The two used to disagree — a bad decimal
// inside a well-formed row was ErrCorruptArchive, while a body that would not
// parse at all arrived untyped.
func TestRESTMalformedResponseIsACorruptArchive(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)
	end := start.Add(5 * time.Hour)

	bodies := map[string]string{
		"not JSON at all":          "<html>the wrong service answered</html>",
		"an object, not a page":    `{"klines":[]}`,
		"a row of the wrong width": `[[1705276800000,"1","2","3","4"]]`,
		"a column that is a boolean": `[[1705276800000,true,"2","1","2","10",` +
			`1705280399999,"20",5,"5","10","0"]]`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newTestRESTFetcher(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			})

			_, err := f.klines(t.Context(), restRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
			}, end)

			if !errors.Is(err, ErrCorruptArchive) {
				t.Errorf("error %v does not wrap %v", err, ErrCorruptArchive)
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

// logRecord is one captured slog record, decoded far enough to assert on.
type logRecord struct {
	Level      string `json:"level"`
	Msg        string `json:"msg"`
	UsedWeight int    `json:"used_weight"`
}

// captureLogs returns a logger recording everything at debug and above, and a
// function that decodes what it collected.
//
// A JSON handler into a buffer rather than a hand-written slog.Handler: the
// assertions here are about which records appeared and what one attribute said,
// and a handler implementation would be more code to maintain than the two
// lines of decoding it saves.
func captureLogs(t *testing.T) (*slog.Logger, func() []logRecord) {
	t.Helper()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	return logger, func() []logRecord {
		t.Helper()

		var out []logRecord

		for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}

			var rec logRecord
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("decoding log line %q: %v", line, err)
			}

			out = append(out, rec)
		}

		return out
	}
}

// withUsedWeight wraps a handler so every response carries the quota header.
//
// A value of zero sends no header at all, which is the real endpoint's own
// behaviour on the paths that do not meter — and the case that has to stay
// distinguishable from "nothing has been spent", since one request always costs
// vision.KlinesWeight.
func withUsedWeight(next http.HandlerFunc, used int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if used > 0 {
			w.Header().Set("X-MBX-USED-WEIGHT-1M", strconv.Itoa(used))
		}

		next(w, r)
	}
}

// TestRESTReportsUsedWeight covers the quota reading being surfaced rather than
// dropped.
//
// X-MBX-USED-WEIGHT-1M is the only visibility this library has into a budget
// that is shared per IP address rather than per process, so a second backtest
// or a live trading bot on the same machine is invisible to every other
// measurement here. Decoding the header and then ignoring it — which is what
// this did before — is the accepted-and-ignored defect docs/architecture.md
// names, in the one place where the ignored value is the only evidence there is.
func TestRESTReportsUsedWeight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		used      int
		wantDebug int
		wantWarn  int
	}{
		{
			name: "no header is not a measurement",
			used: 0,
			// Zero is "absent or unreadable", not "nothing spent" — this very
			// request cost vision.KlinesWeight — so nothing is claimed.
			wantDebug: 0,
			wantWarn:  0,
		},
		{
			name:      "an ordinary reading is debug only",
			used:      120,
			wantDebug: 1,
			wantWarn:  0,
		},
		{
			name: "one under the threshold does not warn",
			used: restWeightWarnThreshold - 1,
			// The boundary is checked from below as well as on it, because a
			// >= written as > is a bug no round number would expose.
			wantDebug: 1,
			wantWarn:  0,
		},
		{
			name:      "the threshold itself warns",
			used:      restWeightWarnThreshold,
			wantDebug: 1,
			wantWarn:  1,
		},
		{
			name:      "past the threshold warns",
			used:      vision.WeightLimitPerMinute - 10,
			wantDebug: 1,
			wantWarn:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start := utc(2024, 1, 15)
			rows := makeKlines(start, time.Hour, 4)
			end := start.Add(4 * time.Hour)

			f := newTestRESTFetcher(t, withUsedWeight(serveKlines(rows, nil), tc.used))

			logger, records := captureLogs(t)
			f.logger = logger

			got, err := f.klines(t.Context(), restRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Start: start, End: end,
			}, end)
			if err != nil {
				t.Fatalf("klines: %v", err)
			}

			// The candles come back either way. That is the whole reason this
			// needs its own test: reporting the quota is invisible in the
			// result, so a version that dropped the header again would pass
			// every other test in this file.
			if len(got) != 4 {
				t.Fatalf("got %d candles, want 4", len(got))
			}

			var debug, warn int

			for _, rec := range records() {
				switch rec.Level {
				case "DEBUG":
					debug++

					if rec.UsedWeight != tc.used {
						t.Errorf("debug record says used_weight %d, want %d", rec.UsedWeight, tc.used)
					}
				case "WARN":
					warn++
				}
			}

			if debug != tc.wantDebug {
				t.Errorf("got %d debug records, want %d", debug, tc.wantDebug)
			}

			if warn != tc.wantWarn {
				t.Errorf("got %d warn records, want %d", warn, tc.wantWarn)
			}
		})
	}
}

// TestRESTWarnsOncePerFetch pins the warning to the fetch rather than the page.
//
// The condition is a property of the minute, not of the page that observed it:
// a range crossing the threshold on its first page crosses it on every
// subsequent one, so a per-page warning turns a real signal into a hundred
// lines nobody reads. The debug records stay per page, because that is the
// level whose job is detail.
func TestRESTWarnsOncePerFetch(t *testing.T) {
	t.Parallel()

	start := utc(2024, 1, 15)

	// 2,500 one-minute candles: three pages, as in TestRESTPaginatesWholeRange.
	const count = 2500

	rows := makeKlines(start, time.Minute, count)
	end := start.Add(count * time.Minute)

	var calls atomic.Int32

	f := newTestRESTFetcher(t, withUsedWeight(serveKlines(rows, &calls), vision.WeightLimitPerMinute-1))

	logger, records := captureLogs(t)
	f.logger = logger

	if _, err := f.klines(t.Context(), restRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1m, Start: start, End: end,
	}, end); err != nil {
		t.Fatalf("klines: %v", err)
	}

	if want := int32(3); calls.Load() != want {
		t.Fatalf("made %d requests, want %d — the rest of this test assumes three pages", calls.Load(), want)
	}

	var debug, warn int

	for _, rec := range records() {
		switch rec.Level {
		case "DEBUG":
			debug++
		case "WARN":
			warn++
		}
	}

	if debug != 3 {
		t.Errorf("got %d debug records, want 3 — one per page", debug)
	}

	if warn != 1 {
		t.Errorf("got %d warn records, want exactly 1 for the whole fetch", warn)
	}
}
