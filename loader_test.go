package binancedata

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/plan"
	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// Every test in this file runs against three httptest.Servers standing in for
// the three hosts this library talks to, and against the real archives in
// testdata/. Nothing reaches Binance.
//
// The archives being real is what makes these end-to-end rather than
// self-confirming: the checksums verified along the way are the ones Binance
// published, so a decoder or cache that quietly mangled a candle would fail
// here even though this file never mentions a price.

// fakeBinance is the three servers, their contents, and a count of what was
// asked of each.
//
// The counts are the point of several tests. "The plan avoided a request" is
// only a testable claim if the requests are counted, and a test that merely
// checks the candles came back passes just as happily when the pipeline made
// sixty-two pointless round trips on the way.
type fakeBinance struct {
	// months and days are the archive file names the bucket listing reports.
	// A name that is not here is a month or day Binance never published, and
	// the archive server 404s it — the two must agree, exactly as they do in
	// the real bucket.
	months, days []string

	// rest is what the REST endpoint serves. Nil means an empty array, which
	// is what Binance answers for a range it has no data for.
	rest []fakeKline

	// mutate can corrupt or fail an archive response; nil serves the fixture
	// unchanged. Same shape as newFixtureServer's, in download_test.go.
	mutate func(name string, body []byte) ([]byte, int)

	// restHandler replaces the default REST handler outright, for tests about
	// what a failing endpoint does.
	restHandler http.HandlerFunc

	listCalls, archiveCalls, restCalls atomic.Int64

	listURL, archiveURL, restURL string
}

// start brings up the three servers and fills in their URLs.
func (f *fakeBinance) start(t *testing.T) {
	t.Helper()

	listing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.listCalls.Add(1)

		q := r.URL.Query()
		prefix, marker := q.Get("prefix"), q.Get("marker")

		names := f.months
		if strings.Contains(prefix, "/daily/") {
			names = f.days
		}

		// The marker is honoured rather than ignored, because the index seeks
		// with one and a fake that returned everything regardless would hide a
		// seek that landed in the wrong place.
		var listed []string

		for _, n := range names {
			if key := prefix + n; marker == "" || key > marker {
				listed = append(listed, n)
			}
		}

		_, _ = w.Write([]byte(listingOf(prefix, listed...)))
	}))
	t.Cleanup(listing.Close)

	archives := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.archiveCalls.Add(1)

		name := path.Base(r.URL.Path)

		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			http.NotFound(w, r)

			return
		}

		status := http.StatusOK
		if f.mutate != nil {
			body, status = f.mutate(name, body)
		}

		if status != http.StatusOK {
			w.WriteHeader(status)

			return
		}

		_, _ = w.Write(body)
	}))
	t.Cleanup(archives.Close)

	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.restCalls.Add(1)

		if f.restHandler != nil {
			f.restHandler(w, r)

			return
		}

		serveKlines(f.rest, nil)(w, r)
	}))
	t.Cleanup(rest.Close)

	f.listURL, f.archiveURL, f.restURL = listing.URL, archives.URL, rest.URL
}

// loader builds a Loader aimed at the three servers, with its clock frozen at
// now and its cache in a fresh temporary directory.
//
// The retry policy is the production one with the waiting removed (fastPolicy,
// in download_test.go) and the rate limiter is built enormous: both are pinned
// by their own tests in internal/vision, and letting them also pace these would
// make every assertion here depend on a backoff calculation.
func (f *fakeBinance) loader(t *testing.T, now time.Time, opts ...Option) *Loader {
	t.Helper()

	f.start(t)

	base := []Option{
		WithCacheDir(t.TempDir()),
		withTestHosts(f.listURL, f.archiveURL, f.restURL),
		withClock(func() time.Time { return now }),
		withPolicy(fastPolicy()),
		withLimiter(vision.NewLimiter(1e6, 1e6)),
	}

	l, err := NewLoader(append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	return l
}

// archiveNames turns a run of dates into the daily archive file names for them,
// which is what the bucket listing is built from.
func archiveNames(symbol string, iv Interval, agg aggregation, from time.Time, n int) []string {
	out := make([]string, 0, n)

	for i := range n {
		when := from.AddDate(0, 0, i)
		if agg == aggMonthly {
			when = from.AddDate(0, i, 0)
		}

		name, err := archiveName(symbol, iv, agg, when)
		if err != nil {
			panic(err) // a test helper handed an invalid interval
		}

		out = append(out, name)
	}

	return out
}

// warm fetches req once so that a later fetch of the same range does no I/O.
//
// Several tests below need it, and always for the same reason. A failing chunk
// cancels its siblings, and the cache deliberately does *not* stop a download
// it has already started — whoever else is waiting still wants it, and if
// nobody is, it finishes and populates the cache for the next run. That is the
// right policy for a directory that outlives the process, and it races
// t.TempDir's cleanup, which runs the instant the test returns.
//
// Warming first means the cancelled path has nothing left to write, so the test
// measures what it is about instead of that race. See the note on cache.klines,
// and the "what an error leaves behind" section on Loader.Fetch.
func warm(t *testing.T, l *Loader, req Request) {
	t.Helper()

	if _, err := l.Fetch(t.Context(), req); err != nil {
		t.Fatalf("warming the cache for %s %s: %v", req.Symbol, req.Interval, err)
	}
}

// collect drains an iterator into a slice and its first error, which is what
// every Stream test wants and none of them should spell out.
func collect(seq func(func(Kline, error) bool)) ([]Kline, error) {
	var out []Kline

	for k, err := range seq {
		if err != nil {
			return out, err
		}

		out = append(out, k)
	}

	return out, nil
}

// ---------------------------------------------------------------------------
// The happy path
// ---------------------------------------------------------------------------

// TestFetchOneDailyArchive is the whole pipeline at its smallest: one request,
// one chunk, one real archive, 24 real candles.
func TestFetchOneDailyArchive(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTC/USDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 24 {
		t.Fatalf("got %d candles, want 24", len(klines))
	}

	if got, want := klines[0].OpenTime, utc(2024, 1, 15); !got.Equal(want) {
		t.Errorf("first candle opens at %s, want %s", got, want)
	}

	if got, want := klines[23].OpenTime, utc(2024, 1, 15, 23); !got.Equal(want) {
		t.Errorf("last candle opens at %s, want %s", got, want)
	}

	// One listing per granularity, and the archive plus its sidecar. Nothing
	// else: no REST call for a range the archives cover in full.
	if got := f.listCalls.Load(); got != 2 {
		t.Errorf("made %d listing requests, want 2", got)
	}

	if got := f.archiveCalls.Load(); got != 2 {
		t.Errorf("made %d archive requests, want 2 (sidecar then zip)", got)
	}

	if got := f.restCalls.Load(); got != 0 {
		t.Errorf("made %d REST requests, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// The closed range
// ---------------------------------------------------------------------------

// TestEndIsInclusive is the defining test for [Request]'s closed range, and the
// one that fails outright against the half-open rule this replaced.
//
// It works by moving the boundary one nanosecond and watching exactly one
// candle appear. That is a stronger claim than "the right number came back":
// a nanosecond is finer than any resolution Binance has ever published in —
// milliseconds in the archives, microseconds since 2025 — so there is nothing
// in the gap that could account for a difference of anything but the single
// candle whose open time the boundary crossed.
func TestEndIsInclusive(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20))

	// The open time of the final candle in the 2024-01-15 archive.
	last := utc(2024, 1, 15, 23)

	fetch := func(end time.Time) []Kline {
		t.Helper()

		klines, err := l.Fetch(t.Context(), Request{
			Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
			Start: utc(2024, 1, 15), End: end,
		})
		if err != nil {
			t.Fatalf("Fetch with end %s: %v", end.Format(time.RFC3339Nano), err)
		}

		return klines
	}

	// An End sitting exactly on a candle's open time includes that candle.
	// Under the old rule this returned 23.
	got := fetch(last)

	if len(got) != 24 {
		t.Fatalf("end on the last open time returned %d candles, want 24", len(got))
	}

	if openTime := got[len(got)-1].OpenTime; !openTime.Equal(last) {
		t.Errorf("last candle opens at %s, want %s", openTime, last)
	}

	// One nanosecond earlier excludes it, and nothing else.
	got = fetch(last.Add(-time.Nanosecond))

	if len(got) != 23 {
		t.Fatalf("end one nanosecond earlier returned %d candles, want 23", len(got))
	}

	if openTime, want := got[len(got)-1].OpenTime, utc(2024, 1, 15, 22); !openTime.Equal(want) {
		t.Errorf("last candle opens at %s, want %s", openTime, want)
	}
}

// TestASingleInstantIsOneCandle pins down the spelling Validate used to reject.
//
// Start == End is an empty range under the half-open rule and a one-instant
// range under the closed one, so it is the case where the two conventions
// disagree most sharply: the old code returned an error, and the new code has
// to return exactly one candle rather than the whole day the archive it came
// from actually holds.
func TestASingleInstantIsOneCandle(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20))

	noon := utc(2024, 1, 15, 12)

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: noon, End: noon,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 1 {
		t.Fatalf("got %d candles, want 1", len(klines))
	}

	if openTime := klines[0].OpenTime; !openTime.Equal(noon) {
		t.Errorf("candle opens at %s, want %s", openTime, noon)
	}
}

// TestProgressReportsTheEndTheCallerWrote guards the boundary between the two
// conventions against the refactor most likely to blur it.
//
// Converting inside resolve would be tempting and would pass every test above,
// because everything downstream wants the exclusive bound anyway. What it would
// also do is put the converted value into Progress.Request, so a caller's
// progress display would quietly report an End one nanosecond past the one they
// asked for. The conversion therefore lives in a method rather than in resolve,
// and this is what says so.
func TestProgressReportsTheEndTheCallerWrote(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}

	var (
		mu   sync.Mutex
		ends []time.Time
	)

	l := f.loader(t, utc(2026, 8, 20), WithProgress(func(p Progress) {
		mu.Lock()
		defer mu.Unlock()

		ends = append(ends, p.Request.End)
	}))

	end := utc(2024, 1, 15, 23)

	if _, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: end,
	}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(ends) == 0 {
		t.Fatal("no progress was reported")
	}

	for i, got := range ends {
		if !got.Equal(end) {
			t.Errorf("progress %d reported end %s, want %s",
				i, got.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
		}
	}
}

// TestFetchIsSortedDeduplicatedAndTrimmed checks the reduce phase against a
// range that needs all three of its jobs at once.
//
// The two fixtures are the last day of the millisecond era and the first day of
// the microsecond one — 2024-12-31 and 2025-01-01 — so the candles also have to
// survive the timestamp-unit switch to line up. The requested range starts and
// ends inside a day, so both archives overshoot it and both must be trimmed.
func TestFetchIsSortedDeduplicatedAndTrimmed(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: []string{
		"BTCUSDT-1h-2024-12-31.zip",
		"BTCUSDT-1h-2025-01-01.zip",
	}}
	l := f.loader(t, utc(2026, 8, 20))

	start, end := utc(2024, 12, 31, 20), utc(2025, 1, 1, 4)

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: start, End: upTo(end),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Four hours of 2024 and four of 2025.
	if len(klines) != 8 {
		t.Fatalf("got %d candles, want 8", len(klines))
	}

	for i, k := range klines {
		if k.OpenTime.Before(start) || !k.OpenTime.Before(end) {
			t.Errorf("candle %d opens at %s, outside the requested [%s,%s)",
				i, k.OpenTime, start, end)
		}

		if i > 0 && !k.OpenTime.After(klines[i-1].OpenTime) {
			t.Errorf("candle %d opens at %s, which does not follow %s",
				i, k.OpenTime, klines[i-1].OpenTime)
		}
	}

	if got, want := klines[0].OpenTime, utc(2024, 12, 31, 20); !got.Equal(want) {
		t.Errorf("first candle opens at %s, want %s", got, want)
	}

	if got, want := klines[7].OpenTime, utc(2025, 1, 1, 3); !got.Equal(want) {
		t.Errorf("last candle opens at %s, want %s", got, want)
	}
}

// TestFetchJoinsArchivesToTheRESTTail covers the seam the whole REST fetcher
// exists for: a range that runs past the last published archive.
func TestFetchJoinsArchivesToTheRESTTail(t *testing.T) {
	t.Parallel()

	// The archives stop after the 15th; the 16th has to come from the API.
	f := &fakeBinance{
		days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1),
		rest: makeKlines(utc(2024, 1, 16), time.Hour, 24),
	}

	// Frozen well after the range, so every candle in it has closed.
	l := f.loader(t, utc(2024, 1, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 17)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 48 {
		t.Fatalf("got %d candles, want 48 (24 archived, 24 from REST)", len(klines))
	}

	// The seam itself: the archive's last candle and the API's first must be
	// adjacent, with nothing missing and nothing repeated between them.
	if got, want := klines[23].OpenTime, utc(2024, 1, 15, 23); !got.Equal(want) {
		t.Errorf("last archived candle opens at %s, want %s", got, want)
	}

	if got, want := klines[24].OpenTime, utc(2024, 1, 16); !got.Equal(want) {
		t.Errorf("first REST candle opens at %s, want %s", got, want)
	}

	if got := f.restCalls.Load(); got == 0 {
		t.Error("the tail past the archives was never fetched from REST")
	}
}

// TestStreamYieldsTheSameCandlesAsFetch pins the two APIs to each other. They
// share an implementation today; this is what notices if they stop.
func TestStreamYieldsTheSameCandlesAsFetch(t *testing.T) {
	t.Parallel()

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 31), End: upTo(utc(2025, 1, 2)),
	}
	days := []string{"BTCUSDT-1h-2024-12-31.zip", "BTCUSDT-1h-2025-01-01.zip"}

	f1 := &fakeBinance{days: days}
	fetched, err := f1.loader(t, utc(2026, 8, 20)).Fetch(t.Context(), req)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	f2 := &fakeBinance{days: days}
	streamed, err := collect(f2.loader(t, utc(2026, 8, 20)).Stream(t.Context(), req))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if len(fetched) != 48 {
		t.Fatalf("Fetch returned %d candles, want 48", len(fetched))
	}

	if len(streamed) != len(fetched) {
		t.Fatalf("Stream yielded %d candles, Fetch returned %d", len(streamed), len(fetched))
	}

	for i := range fetched {
		if !fetched[i].Equal(streamed[i]) {
			t.Fatalf("candle %d differs:\n fetch: %+v\nstream: %+v", i, fetched[i], streamed[i])
		}
	}
}

// TestStreamStopsOnBreak checks that breaking out of the iterator stops it and
// winds the pipeline down.
//
// Two things are under test, and it is worth being clear which. The first is the
// yield contract: a `break` makes yield return false, and a stream that ignored
// that would keep pushing candles at a consumer that has left. The second is
// that the shutdown completes at all — the producer goroutine, the workers
// blocked mid-send and the errgroup are all torn down by the deferred cancel,
// and a mistake there is a test that hangs rather than one that fails.
//
// What is deliberately *not* claimed is that an in-flight download stops. The
// cache does not stop work it has already started, on purpose — see the note on
// Loader.Fetch — so the range is warmed first and the streaming pass does no
// network I/O at all. Leaving it cold made this test race t.TempDir's cleanup
// perhaps one run in ten.
func TestStreamStopsOnBreak(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: []string{
		"BTCUSDT-1h-2024-12-31.zip",
		"BTCUSDT-1h-2025-01-01.zip",
	}}
	l := f.loader(t, utc(2026, 8, 20))

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 31), End: upTo(utc(2025, 1, 2)),
	}

	warm(t, l, req)

	before := f.archiveCalls.Load()

	n := 0

	for _, err := range l.Stream(t.Context(), req) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}

		n++

		if n == 3 {
			break
		}
	}

	if n != 3 {
		t.Fatalf("consumed %d candles before breaking, want 3", n)
	}

	// Served entirely from the cache, so the break cannot have been masked by
	// a slow download that happened to end the range anyway.
	if got := f.archiveCalls.Load(); got != before {
		t.Errorf("the streaming pass made %d archive requests, want none", got-before)
	}
}

// ---------------------------------------------------------------------------
// Correctness requirement 4: what a 404 does
// ---------------------------------------------------------------------------

// TestMissingMonthlyArchiveFallsBackToDailies is the requirement-4 case, and
// the real one: BTCUSDT's 1mo archive for March 2024 does not exist while
// February and April both do. Here the same shape is played out with 1h data,
// where dailies exist to fall back to.
//
// The listing is what makes it cheap. The month is known to be missing before
// anything is fetched, so the fallback costs no 404 at all.
func TestMissingMonthlyArchiveFallsBackToDailies(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{
		// The monthly listing is empty: no month was published.
		days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1),
	}
	l := f.loader(t, utc(2026, 8, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 24 {
		t.Fatalf("got %d candles, want 24", len(klines))
	}

	// Two: the sidecar and the archive. Not four, which is what a 404 on the
	// month first would have cost.
	if got := f.archiveCalls.Load(); got != 2 {
		t.Errorf("made %d archive requests, want 2", got)
	}
}

// TestOneMissingDayDegradesThatDayOnly is correctness requirement 4 stated
// exactly: a day that no archive covers must not cost the rest of the range.
func TestOneMissingDayDegradesThatDayOnly(t *testing.T) {
	t.Parallel()

	// Three days of dailies published, but the middle one is missing from the
	// bucket. The REST endpoint has it.
	f := &fakeBinance{
		days: []string{
			"BTCUSDT-1h-2024-12-31.zip",
			// 2025-01-01 deliberately absent from the listing.
		},
		rest: makeKlines(utc(2025, 1, 1), time.Hour, 24),
	}
	l := f.loader(t, utc(2026, 8, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 31), End: upTo(utc(2025, 1, 2)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 48 {
		t.Fatalf("got %d candles, want 48 — the archived day plus the recovered one", len(klines))
	}

	if got, want := klines[24].OpenTime, utc(2025, 1, 1); !got.Equal(want) {
		t.Errorf("the recovered day starts at %s, want %s", got, want)
	}
}

// TestAnEmptySpanIsAnError is the other half of the requirement-4 policy, and
// the decision recorded in docs/architecture.md: a range Binance has nothing
// for fails rather than coming back quietly short.
//
// The failure mode being avoided is specific. A backtest handed 22 candles for
// a 31-day month cannot tell "the market was quiet" from "nine days are
// missing", and every risk number computed from the second is wrong with no
// sign that it is.
func TestAnEmptySpanIsAnError(t *testing.T) {
	t.Parallel()

	// Nothing published, and the REST endpoint has nothing either — the shape
	// of a range that precedes the pair's listing date.
	f := &fakeBinance{days: []string{"BTCUSDT-1h-2024-01-15.zip"}}
	l := f.loader(t, utc(2026, 8, 20))

	// The 15th exists and the 14th does not, so the range needs both and only
	// one of them can be served. Warmed first — see [warm].
	warm(t, l, Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	})

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 14), End: upTo(utc(2024, 1, 16)),
	})

	if !errors.Is(err, ErrNotAvailable) {
		t.Fatalf("got error %v, want one wrapping ErrNotAvailable", err)
	}

	// Nothing alongside the error. Half a range plus an error is two things
	// for a caller to check, and the ones who check one check the wrong one.
	if klines != nil {
		t.Errorf("got %d candles alongside the error, want none", len(klines))
	}

	// The message must name the span that is missing, since that is the only
	// thing the caller can act on.
	if !strings.Contains(err.Error(), "2024-01-14") {
		t.Errorf("error does not name the missing day: %v", err)
	}
}

// TestAnUnfinishedTailIsNotAnError is the exemption that keeps the rule above
// usable. Every request that ends at "now" ends part-way through a candle that
// has not closed, and the fetcher drops unclosed candles — so the final chunk
// legitimately produces nothing and must not be read as missing data.
func TestAnUnfinishedTailIsNotAnError(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{
		days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1),
		rest: makeKlines(utc(2024, 1, 16), time.Hour, 24),
	}

	// 20 past midnight on the 16th: the archives cover through the 15th, and
	// the REST tail is 20 minutes into an hourly candle that will not close
	// for another 40.
	now := utc(2024, 1, 16, 0, 20)
	l := f.loader(t, now)

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), // End left zero: "now, at call time"
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 24 {
		t.Fatalf("got %d candles, want the 24 settled ones", len(klines))
	}
}

// ---------------------------------------------------------------------------
// Requirement 5: the pool never lets the same archive be fetched twice
// ---------------------------------------------------------------------------

// TestConcurrentRequestsFetchAnArchiveOnce is correctness requirement 5 as the
// loader can demonstrate it, and it is arranged rather than hoped for.
//
// Eight overlapping requests all want the same day. The archive server blocks
// the first request until every caller has queued behind it, which is what
// makes the test meaningful: a test that merely starts eight goroutines and
// counts requests passes just as happily when the deduplication is missing,
// because the first fetch usually finishes before the second one starts.
//
// The concurrency limit is set to 8 so the pool is saturated at the moment of
// the check, which is the exact condition the ported implementation got wrong —
// it registered its deduplication entry after taking a permit, so a full pool
// let several tasks past the check before any of them registered.
func TestConcurrentRequestsFetchAnArchiveOnce(t *testing.T) {
	t.Parallel()

	const callers = 8

	var (
		release = make(chan struct{})
		once    sync.Once
	)

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	f.mutate = func(name string, body []byte) ([]byte, int) {
		if strings.HasSuffix(name, ".zip") {
			// Hold the first archive request open until every caller has
			// finished both of its listings, which is the last thing each does
			// before entering the cache.
			//
			// The barrier used to be a WaitGroup the callers signalled *before*
			// calling Fetch, which is satisfied the moment eight goroutines
			// exist and guarantees nothing at all about where they have got to.
			// It happened to work, because the leader makes two more round
			// trips after its own listings and the others use that time — but
			// "happened to" is not what a barrier is for.
			once.Do(func() {
				// Spun here rather than through waitFor, which reports a
				// timeout with t.Fatal — and t.Fatal from a server goroutine
				// runs runtime.Goexit on the wrong stack. On timeout this just
				// proceeds and lets the request count fail the assertion.
				deadline := time.Now().Add(10 * time.Second)
				for f.listCalls.Load() < callers*2 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}

				// A short settle so the callers that have just finished listing
				// are inside cache.klines and registered on the singleflight,
				// rather than merely about to be. Nothing observable from out
				// here marks that moment; the group exposes no waiter count.
				time.Sleep(20 * time.Millisecond)

				close(release)
			})
			<-release
		}

		return body, http.StatusOK
	}

	l := f.loader(t, utc(2026, 8, 20), WithConcurrency(callers))

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	}

	results := make([][]Kline, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup

	for i := range callers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			results[i], errs[i] = l.Fetch(t.Context(), req)
		}()
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}

		if len(results[i]) != 24 {
			t.Fatalf("caller %d got %d candles, want 24", i, len(results[i]))
		}
	}

	// One sidecar and one archive, however many callers asked.
	if got := f.archiveCalls.Load(); got != 2 {
		t.Errorf("made %d archive requests for one day across %d callers, want 2", got, callers)
	}
}

// TestEachCallerOwnsItsCandles is the other half of deduplication. The reduce
// phase trims ranges in place, so two callers handed the same backing array
// would silently truncate each other.
func TestEachCallerOwnsItsCandles(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20))

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	}

	first, err := l.Fetch(t.Context(), req)
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}

	second, err := l.Fetch(t.Context(), req)
	if err != nil {
		t.Fatalf("second Fetch: %v", err)
	}

	if &first[0] == &second[0] {
		t.Fatal("two calls share one backing array; a trim in one would be visible in the other")
	}

	want := first[0].Close

	second[0].Close = want.Neg()

	if !first[0].Close.Equal(want) {
		t.Error("writing through the second result changed the first")
	}
}

// ---------------------------------------------------------------------------
// FetchAll
// ---------------------------------------------------------------------------

func TestFetchAll(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: []string{
		"BTCUSDT-1h-2024-12-31.zip",
		"BTCUSDT-1h-2025-01-01.zip",
	}}
	l := f.loader(t, utc(2026, 8, 20))

	// Two overlapping ranges. Between them they want both days, and the day
	// they share must be fetched once.
	first := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 31), End: upTo(utc(2025, 1, 2)),
	}
	second := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2025, 1, 1), End: upTo(utc(2025, 1, 2)),
	}

	got, err := l.FetchAll(t.Context(), []Request{first, second})
	if err != nil {
		t.Fatalf("FetchAll: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}

	// Keyed by the request as passed in, which is the only key a caller has.
	if n := len(got[first]); n != 48 {
		t.Errorf("first request got %d candles, want 48", n)
	}

	if n := len(got[second]); n != 24 {
		t.Errorf("second request got %d candles, want 24", n)
	}

	// Four archive requests: a sidecar and a zip for each of the two days. The
	// day both ranges wanted was downloaded once.
	if n := f.archiveCalls.Load(); n != 4 {
		t.Errorf("made %d archive requests, want 4", n)
	}
}

func TestFetchAllFailsOnTheFirstError(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20))

	good := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	}
	bad := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 10), End: upTo(utc(2024, 1, 11)), // never published
	}

	// Fail-fast cancels the sibling request mid-flight; warming first is what
	// keeps that from racing t.TempDir's cleanup. See [warm].
	warm(t, l, good)

	got, err := l.FetchAll(t.Context(), []Request{good, bad})
	if !errors.Is(err, ErrNotAvailable) {
		t.Fatalf("got error %v, want one wrapping ErrNotAvailable", err)
	}

	if got != nil {
		t.Errorf("got a map of %d results alongside the error, want nil", len(got))
	}

	// The error must say which request failed. With twenty in flight, one that
	// only says "no candles" sends whoever is debugging to read all twenty.
	if !strings.Contains(err.Error(), "BTCUSDT") {
		t.Errorf("error does not name the request: %v", err)
	}
}

func TestFetchAllOfNothing(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{}
	l := f.loader(t, utc(2026, 8, 20))

	got, err := l.FetchAll(t.Context(), nil)
	if err != nil {
		t.Fatalf("FetchAll: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("got %d results for no requests, want 0", len(got))
	}

	if n := f.listCalls.Load(); n != 0 {
		t.Errorf("made %d requests for no requests, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Routing: what the listing saves
// ---------------------------------------------------------------------------

// TestRouteSkipsAnArchiveTheListingDoesNotHave is the routing decision measured
// end to end: a day inside the published range that the bucket does not have
// must cost no archive request at all.
//
// The listing has the 31st and the 1st but not the 30th, so the plan names three
// daily archives and only two of them exist. Without the listing being consulted
// first, the 30th costs a sidecar 404 before anything falls back.
func TestRouteSkipsAnArchiveTheListingDoesNotHave(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{
		days: []string{
			// 2024-12-30 deliberately absent.
			"BTCUSDT-1h-2024-12-31.zip",
			"BTCUSDT-1h-2025-01-01.zip",
		},
		rest: makeKlines(utc(2024, 12, 30), time.Hour, 24),
	}
	l := f.loader(t, utc(2026, 8, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 30), End: upTo(utc(2025, 1, 2)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 72 {
		t.Fatalf("got %d candles, want 72 — one recovered day and two archived ones", len(klines))
	}

	if got, want := klines[0].OpenTime, utc(2024, 12, 30); !got.Equal(want) {
		t.Errorf("the recovered day starts at %s, want %s", got, want)
	}

	// Four, not six: a sidecar and a zip for each of the two days that exist,
	// and nothing at all spent discovering that the third does not.
	if got := f.archiveCalls.Load(); got != 4 {
		t.Errorf("made %d archive requests, want 4 — the missing day should cost none", got)
	}
}

// TestRouteRerouteAndCoalesce tests the routing decision directly, because the
// case that decided its design cannot be built from the committed fixtures.
//
// That case is real: BTCUSDT's 1mo archive for March 2024 does not exist while
// February and April both do. 1mo has no daily archives, so a missing month can
// only go to the API — and two missing months in a row must become one REST
// range rather than two, since that endpoint is the one with a quota.
func TestRouteRerouteAndCoalesce(t *testing.T) {
	t.Parallel()

	l, err := NewLoader(withOfflineHosts())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	month := func(m time.Month) plan.Chunk {
		start := utc(2024, m, 1)

		return plan.Chunk{Kind: plan.KindMonthlyArchive, Start: start, End: start.AddDate(0, 1, 0)}
	}

	// January and April were published; February and March were not.
	index := archiveIndex{
		months:  map[time.Time]bool{utc(2024, 1, 1): true, utc(2024, 4, 1): true},
		days:    map[time.Time]bool{},
		through: utc(2024, 5, 1),
	}

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1mo, Market: MarketSpot,
		Start: utc(2024, 1, 1), End: upTo(utc(2024, 5, 1)),
	}

	got := l.route(t.Context(),
		[]plan.Chunk{month(time.January), month(time.February), month(time.March), month(time.April)},
		index, req)

	want := []plan.Chunk{
		month(time.January),
		{Kind: plan.KindRESTRange, Start: utc(2024, 2, 1), End: utc(2024, 4, 1)},
		month(time.April),
	}

	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestRouteFansAMissingMonthOutIntoTheDaysThatExist is the same decision at an
// interval that has daily archives: the ladder's middle rung.
func TestRouteFansAMissingMonthOutIntoTheDaysThatExist(t *testing.T) {
	t.Parallel()

	l, err := NewLoader(withOfflineHosts())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	// A three-day February in a toy calendar is not possible, so use a real
	// one: the month is missing, and only two of its days were published.
	days := map[time.Time]bool{}
	for d := 1; d <= 29; d++ {
		days[utc(2024, 2, d)] = d == 3 || d == 4
	}

	index := archiveIndex{
		months:  map[time.Time]bool{},
		days:    days,
		through: utc(2024, 3, 1),
	}

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 2, 1), End: upTo(utc(2024, 3, 1)),
	}

	got := l.route(t.Context(),
		[]plan.Chunk{{Kind: plan.KindMonthlyArchive, Start: utc(2024, 2, 1), End: utc(2024, 3, 1)}},
		index, req)

	want := []plan.Chunk{
		{Kind: plan.KindRESTRange, Start: utc(2024, 2, 1), End: utc(2024, 2, 3)},
		{Kind: plan.KindDailyArchive, Start: utc(2024, 2, 3), End: utc(2024, 2, 4)},
		{Kind: plan.KindDailyArchive, Start: utc(2024, 2, 4), End: utc(2024, 2, 5)},
		{Kind: plan.KindRESTRange, Start: utc(2024, 2, 5), End: utc(2024, 3, 1)},
	}

	if !slices.Equal(got, want) {
		t.Errorf("got %v,\nwant %v", got, want)
	}

	// The whole point: twenty-seven unpublished days became two REST ranges,
	// not twenty-seven.
	rest := 0

	for _, c := range got {
		if c.Kind == plan.KindRESTRange {
			rest++
		}
	}

	if rest != 2 {
		t.Errorf("produced %d REST chunks, want 2", rest)
	}
}

func TestCoalesceREST(t *testing.T) {
	t.Parallel()

	day := func(d int) time.Time { return utc(2024, 1, d) }

	tests := []struct {
		name string
		in   []plan.Chunk
		want []plan.Chunk
	}{
		{
			name: "adjacent rest ranges join",
			in: []plan.Chunk{
				{Kind: plan.KindRESTRange, Start: day(1), End: day(2)},
				{Kind: plan.KindRESTRange, Start: day(2), End: day(3)},
				{Kind: plan.KindRESTRange, Start: day(3), End: day(4)},
			},
			want: []plan.Chunk{{Kind: plan.KindRESTRange, Start: day(1), End: day(4)}},
		},
		{
			name: "an archive between them is a wall",
			in: []plan.Chunk{
				{Kind: plan.KindRESTRange, Start: day(1), End: day(2)},
				{Kind: plan.KindDailyArchive, Start: day(2), End: day(3)},
				{Kind: plan.KindRESTRange, Start: day(3), End: day(4)},
			},
			want: []plan.Chunk{
				{Kind: plan.KindRESTRange, Start: day(1), End: day(2)},
				{Kind: plan.KindDailyArchive, Start: day(2), End: day(3)},
				{Kind: plan.KindRESTRange, Start: day(3), End: day(4)},
			},
		},
		{
			// Not adjacent, so not joined — joining them would invent coverage
			// for the day between, which is the one mistake this must not make.
			name: "a gap is not closed",
			in: []plan.Chunk{
				{Kind: plan.KindRESTRange, Start: day(1), End: day(2)},
				{Kind: plan.KindRESTRange, Start: day(3), End: day(4)},
			},
			want: []plan.Chunk{
				{Kind: plan.KindRESTRange, Start: day(1), End: day(2)},
				{Kind: plan.KindRESTRange, Start: day(3), End: day(4)},
			},
		},
		{
			name: "archives alone are untouched",
			in: []plan.Chunk{
				{Kind: plan.KindDailyArchive, Start: day(1), End: day(2)},
				{Kind: plan.KindDailyArchive, Start: day(2), End: day(3)},
			},
			want: []plan.Chunk{
				{Kind: plan.KindDailyArchive, Start: day(1), End: day(2)},
				{Kind: plan.KindDailyArchive, Start: day(2), End: day(3)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := coalesceREST(tt.in)

			if !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

func TestProgressReportsEveryChunkOnce(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: []string{
		"BTCUSDT-1h-2024-12-31.zip",
		"BTCUSDT-1h-2025-01-01.zip",
	}}

	var (
		mu     sync.Mutex
		events []Progress
	)

	l := f.loader(t, utc(2026, 8, 20), WithProgress(func(p Progress) {
		// No mutex is needed here — the loader serialises these — but the
		// test takes one anyway because it reads the slice from a different
		// goroutine afterwards, and -race is entitled to say so.
		mu.Lock()
		defer mu.Unlock()

		events = append(events, p)
	}))

	if _, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 31), End: upTo(utc(2025, 1, 2)),
	}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(events) != 2 {
		t.Fatalf("got %d progress events, want 2 (one per chunk)", len(events))
	}

	seen := map[int]bool{}

	for _, p := range events {
		if p.Total != 2 {
			t.Errorf("event reports Total %d, want 2", p.Total)
		}

		if p.Done < 1 || p.Done > 2 {
			t.Errorf("event reports Done %d, outside 1..2", p.Done)
		}

		if seen[p.Done] {
			t.Errorf("two events both report Done %d", p.Done)
		}

		seen[p.Done] = true

		if p.Source != SourceDailyArchive {
			t.Errorf("event reports source %s, want %s", p.Source, SourceDailyArchive)
		}

		if p.Klines != 24 {
			t.Errorf("event reports %d candles, want 24", p.Klines)
		}

		if p.Err != nil {
			t.Errorf("event carries an error: %v", p.Err)
		}

		// The resolved request, so a FetchAll callback can tell which of
		// twenty requests it is being told about.
		if p.Request.Symbol != "BTCUSDT" {
			t.Errorf("event carries symbol %q, want the resolved BTCUSDT", p.Request.Symbol)
		}
	}
}

func TestProgressIsSerialised(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: []string{
		"BTCUSDT-1h-2024-12-31.zip",
		"BTCUSDT-1h-2025-01-01.zip",
	}}

	// A deliberately unsafe counter. If two goroutines ever enter the callback
	// at once, -race reports it — which is the only way to test this claim,
	// since a mutex-free counter that happens not to race proves nothing on
	// its own.
	unguarded := 0

	l := f.loader(t, utc(2026, 8, 20),
		WithConcurrency(8),
		WithProgress(func(Progress) { unguarded++ }))

	if _, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 12, 31), End: upTo(utc(2025, 1, 2)),
	}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if unguarded != 2 {
		t.Errorf("callback ran %d times, want 2", unguarded)
	}
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

func TestFetchRejectsABadRequestWithoutTouchingTheNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "no symbol",
			req: Request{
				Interval: Interval1h, Market: MarketSpot,
				Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
			},
		},
		{
			name: "no interval",
			req: Request{
				Symbol: "BTCUSDT", Market: MarketSpot,
				Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
			},
		},
		{
			name: "no market",
			req: Request{
				Symbol: "BTCUSDT", Interval: Interval1h,
				Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
			},
		},
		{
			name: "start not UTC",
			req: Request{
				Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
				Start: time.Date(2024, 1, 15, 0, 0, 0, 0, time.FixedZone("CET", 3600)),
				End:   upTo(utc(2024, 1, 16)),
			},
		},
		{
			name: "end before start",
			req: Request{
				Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
				Start: utc(2024, 1, 16), End: upTo(utc(2024, 1, 15)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fakeBinance{}
			l := f.loader(t, utc(2026, 8, 20))

			if _, err := l.Fetch(t.Context(), tt.req); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("got error %v, want one wrapping ErrInvalidRequest", err)
			}

			// The cheapest request is the one never made. A request that
			// cannot possibly succeed must not cost a round trip.
			if n := f.listCalls.Load(); n != 0 {
				t.Errorf("made %d listing requests for an invalid request, want 0", n)
			}
		})
	}
}

// TestFetchFailsWhenTheListingFails is the rule the whole availability design
// rests on: a listing that could not be read is never treated as an empty one.
//
// The failure it prevents is the quiet one. An unreadable listing read as
// "Binance has published nothing" sends a multi-year range to the REST API one
// page at a time, and the answer that comes back looks like success.
func TestFetchFailsWhenTheListingFails(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{}
	f.start(t)

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(broken.Close)

	l, err := NewLoader(
		WithCacheDir(t.TempDir()),
		withTestHosts(broken.URL, f.archiveURL, f.restURL),
		withClock(func() time.Time { return utc(2026, 8, 20) }),
		withPolicy(fastPolicy()),
		withLimiter(vision.NewLimiter(1e6, 1e6)),
	)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	})
	if err == nil {
		t.Fatalf("got %d candles and no error from a failed listing", len(klines))
	}

	if n := f.archiveCalls.Load(); n != 0 {
		t.Errorf("fetched %d archives after the listing failed, want 0", n)
	}
}

// TestFetchHonoursCancellation checks that an interrupted run stops rather than
// finishing the work nobody is waiting for.
func TestFetchHonoursCancellation(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := l.Fetch(ctx, Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got error %v, want one wrapping context.Canceled", err)
	}
}

// TestAnIPBanIsNotWaitedOut pins the one rate-limit response that must not be
// a pause. HTTP 418 means the address is barred for anything from two minutes
// to three days; a pipeline that waits it out hangs, and one that retries earns
// the next, longer ban.
func TestAnIPBanIsNotWaitedOut(t *testing.T) {
	t.Parallel()

	var restHits atomic.Int64

	f := &fakeBinance{
		restHandler: func(w http.ResponseWriter, _ *http.Request) {
			restHits.Add(1)
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte(`{"code":-1003,"msg":"banned"}`))
		},
	}
	l := f.loader(t, utc(2024, 1, 20))

	start := time.Now()

	_, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	})

	if !errors.Is(err, ErrIPBanned) {
		t.Fatalf("got error %v, want one wrapping ErrIPBanned", err)
	}

	// Also rate limited, so a caller asking only "should I slow down?" gets a
	// yes without needing to know the distinction exists.
	if !errors.Is(err, ErrRateLimited) {
		t.Errorf("a ban does not report as rate limited: %v", err)
	}

	// The Retry-After said 120 seconds. Waiting even one of them would mean
	// the pipeline treated a ban as a throttle.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %s, so the ban was waited on rather than reported", elapsed)
	}
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

// TestGatePausesEveryWaiter checks the shared pause in isolation, inside a
// synctest bubble so the durations are exact and cost no real time.
func TestGatePausesEveryWaiter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var g gate

		g.pause(time.Second)

		start := time.Now()

		var wg sync.WaitGroup

		for range 4 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				if err := g.wait(t.Context()); err != nil {
					t.Errorf("wait: %v", err)
				}
			}()
		}

		wg.Wait()

		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("waited %s, want exactly 1s", elapsed)
		}
	})
}

// TestGateNeverShortensAPause pins the rule that keeps two workers hitting the
// same 429 from talking each other down to the smaller delay.
func TestGateNeverShortensAPause(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var g gate

		g.pause(10 * time.Second)
		g.pause(time.Second) // must not win

		start := time.Now()

		if err := g.wait(t.Context()); err != nil {
			t.Fatalf("wait: %v", err)
		}

		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Errorf("waited %s, want the longer 10s pause", elapsed)
		}
	})
}

func TestGateStopsWaitingWhenCancelled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var g gate

		g.pause(time.Hour)

		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			time.Sleep(time.Second)
			cancel()
		}()

		start := time.Now()

		if err := g.wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("got error %v, want context.Canceled", err)
		}

		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("waited %s, want to stop at the cancellation after 1s", elapsed)
		}
	})
}

// TestGateChecksTheContextEvenWhenOpen: an already-cancelled caller must not
// buy one more chunk's worth of work just because there was nothing to wait
// for. Same shape as Policy.wait in internal/vision.
func TestGateChecksTheContextEvenWhenOpen(t *testing.T) {
	t.Parallel()

	var g gate

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := g.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// expectsCandles
// ---------------------------------------------------------------------------

func TestExpectsCandles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		chunk plan.Chunk
		iv    Interval
		now   time.Time
		want  bool
	}{
		{
			name:  "a settled day",
			chunk: plan.Chunk{Kind: plan.KindDailyArchive, Start: utc(2024, 1, 15), End: utc(2024, 1, 16)},
			iv:    Interval1h,
			now:   utc(2026, 8, 20),
			want:  true,
		},
		{
			// The live tail: 20 minutes into an hourly candle that will not
			// close for another 40. Nothing here is missing; it has not
			// happened yet.
			name:  "a tail inside an unclosed candle",
			chunk: plan.Chunk{Kind: plan.KindRESTRange, Start: utc(2024, 1, 16), End: utc(2024, 1, 16, 0, 20)},
			iv:    Interval1h,
			now:   utc(2024, 1, 16, 0, 20),
			want:  false,
		},
		{
			// The same span at 1m, where twenty candles have closed inside it.
			name:  "a tail with settled minutes in it",
			chunk: plan.Chunk{Kind: plan.KindRESTRange, Start: utc(2024, 1, 16), End: utc(2024, 1, 16, 0, 20)},
			iv:    Interval1m,
			now:   utc(2024, 1, 16, 0, 20),
			want:  true,
		},
		{
			// Exactly one candle, closing exactly now. Closed is closed.
			name:  "a candle that closes at this instant",
			chunk: plan.Chunk{Kind: plan.KindRESTRange, Start: utc(2024, 1, 16), End: utc(2024, 1, 16, 1)},
			iv:    Interval1h,
			now:   utc(2024, 1, 16, 1),
			want:  true,
		},
		{
			// A weekly candle opens on Monday. A Tuesday-to-Thursday span
			// contains no open time at all, so it holds nothing by right.
			name:  "no grid point inside the span",
			chunk: plan.Chunk{Kind: plan.KindRESTRange, Start: utc(2024, 1, 2), End: utc(2024, 1, 4)},
			iv:    Interval1w,
			now:   utc(2026, 8, 20),
			want:  false,
		},
		{
			name:  "an unset interval has no grid",
			chunk: plan.Chunk{Kind: plan.KindDailyArchive, Start: utc(2024, 1, 15), End: utc(2024, 1, 16)},
			iv:    Interval(0),
			now:   utc(2026, 8, 20),
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := expectsCandles(tt.chunk.Start, tt.chunk.End, tt.iv, tt.now); got != tt.want {
				t.Errorf("expectsCandles(%s, %s, %s) = %v, want %v",
					tt.chunk, tt.iv, tt.now.Format(time.RFC3339), got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The semaphore
// ---------------------------------------------------------------------------

func TestSemaphoreBoundsConcurrency(t *testing.T) {
	t.Parallel()

	const (
		limit   = 3
		workers = 20
	)

	s := newSemaphore(limit)

	var (
		live, peak atomic.Int64
		wg         sync.WaitGroup
	)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := s.acquire(t.Context()); err != nil {
				t.Errorf("acquire: %v", err)

				return
			}
			defer s.release()

			n := live.Add(1)
			defer live.Add(-1)

			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}

			time.Sleep(time.Millisecond)
		}()
	}

	wg.Wait()

	if got := peak.Load(); got > limit {
		t.Errorf("%d workers ran at once, want at most %d", got, limit)
	}

	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrency was %d, so the test never exercised the limit", got)
	}
}

func TestSemaphoreStopsWaitingWhenCancelled(t *testing.T) {
	t.Parallel()

	s := newSemaphore(1)

	if err := s.acquire(t.Context()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := s.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

func TestSourceString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src  Source
		want string
	}{
		{SourceMonthlyArchive, "monthly archive"},
		{SourceDailyArchive, "daily archive"},
		{SourceRESTAPI, "rest api"},
		{Source(0), "Source(0)"},
	}

	for _, tt := range tests {
		if got := tt.src.String(); got != tt.want {
			t.Errorf("Source(%d).String() = %q, want %q", uint8(tt.src), got, tt.want)
		}
	}
}

func TestSourceForCoversEveryKind(t *testing.T) {
	t.Parallel()

	// Every kind the planner can emit must map onto a public Source. A new
	// kind that nobody mapped would be reported as Source(0) through the
	// progress callback, which is a lie rather than an omission.
	for _, k := range []plan.Kind{plan.KindMonthlyArchive, plan.KindDailyArchive, plan.KindRESTRange} {
		if got := sourceFor(k); got == 0 {
			t.Errorf("chunk kind %s has no Source", k)
		}
	}

	if got := sourceFor(plan.Kind(0)); got != 0 {
		t.Errorf("the unset kind maps to %s, want the zero Source", got)
	}
}

// ---------------------------------------------------------------------------
// Regressions from the Stage 7 code review
// ---------------------------------------------------------------------------

// TestCancellationMidStreamIsReported is review finding 1.
//
// errgroup records only errors that a function passed to g.Go returned, so a
// cancellation of the caller's context sets none by itself. With the producer
// blocked acquiring a permit and every worker launched so far already finished,
// Wait reported nil, the consumer stopped part-way through the range, and Fetch
// returned a short result and no error — the silent truncation this package's
// whole error contract exists to prevent.
//
// stream is driven directly rather than through Fetch, because the condition has
// to be arranged rather than waited for: every permit taken *before* any worker
// starts. Reaching that through Fetch would mean racing the plan phase, which
// now takes a permit of its own. In production the same state is ordinary — one
// FetchAll request's producer sits in acquire for as long as its siblings hold
// every permit.
func TestCancellationMidStreamIsReported(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 15), 1)}
	l := f.loader(t, utc(2026, 8, 20), WithConcurrency(1))

	// Hold the only permit, so no worker can ever start.
	if err := l.sem.acquire(t.Context()); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer l.sem.release()

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
	}
	chunks := []plan.Chunk{{Kind: plan.KindDailyArchive, Start: req.Start, End: req.End}}

	ctx, cancel := context.WithCancel(t.Context())

	var (
		gotErr  error
		yielded int
		done    = make(chan struct{})
	)

	go func() {
		defer close(done)

		l.stream(ctx, req, chunks, utc(2026, 8, 20), func(_ Kline, err error) bool {
			if err != nil {
				gotErr = err

				return false
			}

			yielded++

			return true
		})
	}()

	cancel()
	<-done

	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("stream yielded %d candles and error %v, want one wrapping context.Canceled",
			yielded, gotErr)
	}
}

// TestAChunkWithNothingInTheRequestedPartIsAnError is review finding 2, the
// lenient direction — and the one that made a short range look like a success.
//
// The shape is a delisted pair. Its final monthly archive exists and holds the
// first ten days of the month; a caller asks for the second half. The chunk is
// not empty, so a check made on the chunk's own extent passes — and then the
// reduce step trims every candle away and the call returns an empty slice and a
// nil error. Consolidation made whole-month chunks the normal way to serve a
// partly-wanted month, so this stopped being exotic.
//
// Tested against checkGap directly rather than end to end: no committed fixture
// is a monthly archive that stops part-way through its month, and inventing one
// would mean re-zipping a real archive, which testdata/README.md forbids.
func TestAChunkWithNothingInTheRequestedPartIsAnError(t *testing.T) {
	t.Parallel()

	// Ten days of hourly candles, then nothing: the pair stopped trading.
	var klines []Kline
	for h := range 10 * 24 {
		klines = append(klines, Kline{OpenTime: utc(2024, 3, 1).Add(time.Duration(h) * time.Hour)})
	}

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 3, 16), End: upTo(utc(2024, 4, 1)),
	}
	chunk := plan.Chunk{Kind: plan.KindMonthlyArchive, Start: utc(2024, 3, 1), End: utc(2024, 4, 1)}

	err := checkGap(klines, req, chunk, utc(2026, 8, 20))
	if !errors.Is(err, ErrNotAvailable) {
		t.Fatalf("got error %v, want one wrapping ErrNotAvailable — the chunk holds %d candles "+
			"but none of them is inside the requested range", err, len(klines))
	}

	// And the same chunk when the candles *do* reach the request is fine.
	klines = append(klines, Kline{OpenTime: utc(2024, 3, 20)})

	if err := checkGap(klines, req, chunk, utc(2026, 8, 20)); err != nil {
		t.Errorf("a chunk with a candle inside the request was reported as a gap: %v", err)
	}
}

// TestAWindowWithNoCandleOpeningInItIsNotAGap guards the other reading of the
// same check.
//
// A request is a filter on open time, so [2024-01-15, 2024-02-01) at 1mo asks
// for monthly candles opening in the second half of January — of which there
// are none, by definition, since monthly candles open on the 1st. That is an
// empty answer rather than missing data, and it must stay a nil error however
// strict the gap check becomes.
func TestAWindowWithNoCandleOpeningInItIsNotAGap(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{months: []string{"BTCUSDT-1mo-2024-01.zip"}}
	l := f.loader(t, utc(2026, 8, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1mo, Market: MarketSpot,
		Start: utc(2024, 1, 15), End: upTo(utc(2024, 2, 1)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 0 {
		t.Errorf("got %d candles, want none — no monthly candle opens in that window", len(klines))
	}
}

// TestAChunkOutsideTheRequestIsNotAGap is review finding 2, the strict
// direction — and the one that would fail a request whose own range is entirely
// available.
//
// Consolidation and the no-daily-archives rule both produce chunks that begin
// before the request. Substituting one produces daily chunks that lie wholly
// before it, and those have no data because the pair had not listed yet. They
// must not fail the call, and the message must never name a span whose end
// precedes its start.
func TestAChunkOutsideTheRequestIsNotAGap(t *testing.T) {
	t.Parallel()

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 20), End: upTo(utc(2024, 2, 1)),
	}

	// A daily chunk a fortnight before the request begins: no overlap at all.
	before := plan.Chunk{Kind: plan.KindDailyArchive, Start: utc(2024, 1, 5), End: utc(2024, 1, 6)}

	if err := checkGap(nil, req, before, utc(2026, 8, 20)); err != nil {
		t.Errorf("a chunk entirely before the request was reported as a gap: %v", err)
	}

	// And one after it.
	after := plan.Chunk{Kind: plan.KindDailyArchive, Start: utc(2024, 2, 5), End: utc(2024, 2, 6)}

	if err := checkGap(nil, req, after, utc(2026, 8, 20)); err != nil {
		t.Errorf("a chunk entirely after the request was reported as a gap: %v", err)
	}

	// A chunk that does overlap, and has nothing, is still a gap — and names
	// only the overlapping part.
	overlap := plan.Chunk{Kind: plan.KindMonthlyArchive, Start: utc(2024, 1, 1), End: utc(2024, 2, 1)}

	err := checkGap(nil, req, overlap, utc(2026, 8, 20))
	if !errors.Is(err, ErrNotAvailable) {
		t.Fatalf("got error %v, want one wrapping ErrNotAvailable", err)
	}

	if !strings.Contains(err.Error(), "2024-01-20") {
		t.Errorf("error should name the requested part of the chunk, got: %v", err)
	}

	if strings.Contains(err.Error(), "2024-01-01") {
		t.Errorf("error names dates outside the request, got: %v", err)
	}
}

// TestNoConsolidationOntoAnUnpublishedMonth is review finding 3.
//
// Monthly archives lag real time by up to a month plus a day, dailies by about
// a day, so for most of every month there are dailies for a month whose monthly
// archive does not exist yet. Consolidating onto that month is the worst of both
// outcomes: the chunk 404s and the substitution fans out the *whole* month
// rather than the days that were wanted.
//
// Here February's dailies are published and its monthly archive is not, and 20
// of 29 days are wanted — comfortably over the threshold.
func TestNoConsolidationOntoAnUnpublishedMonth(t *testing.T) {
	t.Parallel()

	l, err := NewLoader(withOfflineHosts())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	days := map[time.Time]bool{}
	for d := 1; d <= 29; d++ {
		days[utc(2024, 2, d)] = true
	}

	index := archiveIndex{
		// No monthly archive for February; the frontier is nonetheless past it,
		// because the daily archives reach further.
		months:  map[time.Time]bool{utc(2024, 1, 1): true},
		days:    days,
		through: utc(2024, 3, 1),
	}

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 2, 10), End: upTo(utc(2024, 3, 1)),
	}

	chunks, err := plan.Expand(plan.Spec{
		Start: req.Start, End: req.End,
		ArchivesThrough: index.through, HasDaily: true, HasMonthly: true,
	})
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}

	got := l.route(t.Context(), chunks, index, req)

	// The twenty days that were asked for, and not one more.
	if len(got) != 20 {
		t.Fatalf("got %d chunks, want the 20 daily archives that were wanted", len(got))
	}

	for _, c := range got {
		if c.Kind != plan.KindDailyArchive {
			t.Fatalf("got a %s chunk (%s); consolidating onto an unpublished month "+
				"costs a 404 and a fan-out across the whole month", c.Kind, c)
		}
	}

	if got, want := got[0].Start, utc(2024, 2, 10); !got.Equal(want) {
		t.Errorf("first chunk starts at %s, want %s", got, want)
	}
}

// TestConsolidationHappensWhenTheMonthIsThere is the other half of finding 3:
// the threshold must still fire when the listing does have the month, or the
// fix would have quietly disabled the feature it was protecting.
func TestConsolidationHappensWhenTheMonthIsThere(t *testing.T) {
	t.Parallel()

	l, err := NewLoader(withOfflineHosts())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	days := map[time.Time]bool{}
	for d := 1; d <= 29; d++ {
		days[utc(2024, 2, d)] = true
	}

	index := archiveIndex{
		months:  map[time.Time]bool{utc(2024, 2, 1): true},
		days:    days,
		through: utc(2024, 3, 1),
	}

	req := Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 2, 10), End: upTo(utc(2024, 3, 1)),
	}

	chunks, err := plan.Expand(plan.Spec{
		Start: req.Start, End: req.End,
		ArchivesThrough: index.through, HasDaily: true, HasMonthly: true,
	})
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}

	got := l.route(t.Context(), chunks, index, req)

	want := []plan.Chunk{{Kind: plan.KindMonthlyArchive, Start: utc(2024, 2, 1), End: utc(2024, 3, 1)}}

	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestFetchAllBoundsTheListingPhase is review finding 4.
//
// The semaphore was taken per chunk, inside the execute phase, so the plan phase
// — which is itself two concurrent listings — ran entirely outside the limit.
// FetchAll over N requests opened 2N simultaneous listings whatever
// WithConcurrency said.
func TestFetchAllBoundsTheListingPhase(t *testing.T) {
	t.Parallel()

	const (
		requests = 12
		limit    = 2
	)

	var live, peak atomic.Int64

	listing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := live.Add(1)

		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}

		// Held open so that overlap, if there is any, is observable rather than
		// something the test has to catch in flight.
		time.Sleep(5 * time.Millisecond)
		live.Add(-1)

		prefix := r.URL.Query().Get("prefix")

		var names []string
		if strings.Contains(prefix, "/daily/") {
			names = []string{"BTCUSDT-1h-2024-01-15.zip"}
		}

		_, _ = w.Write([]byte(listingOf(prefix, names...)))
	}))
	t.Cleanup(listing.Close)

	f := &fakeBinance{days: []string{"BTCUSDT-1h-2024-01-15.zip"}}
	f.start(t)

	l, err := NewLoader(
		WithCacheDir(t.TempDir()),
		WithConcurrency(limit),
		withTestHosts(listing.URL, f.archiveURL, f.restURL),
		withClock(func() time.Time { return utc(2026, 8, 20) }),
		withPolicy(fastPolicy()),
		withLimiter(vision.NewLimiter(1e6, 1e6)),
	)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	// Identical requests. FetchAll launches one goroutine per element whatever
	// they contain, so each still runs its own plan phase — which is the thing
	// being counted. Varying them instead would mean varying the range, and a
	// range ending a nanosecond later grows a REST tail that this fake has no
	// data for.
	reqs := make([]Request, 0, requests)
	for range requests {
		reqs = append(reqs, Request{
			Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
			Start: utc(2024, 1, 15), End: upTo(utc(2024, 1, 16)),
		})
	}

	if _, err := l.FetchAll(t.Context(), reqs); err != nil {
		t.Fatalf("FetchAll: %v", err)
	}

	// One permit covers a request's plan phase, and a plan phase is two
	// listings — so at most twice the limit, never twice the request count.
	if got, max := peak.Load(), int64(limit*2); got > max {
		t.Errorf("%d listings ran at once for %d requests against a limit of %d; want at most %d",
			got, requests, limit, max)
	}
}

// TestPauseDecidesAndClamps is review finding 7: the rate-limit decision itself
// had no test, only the negative branch of it.
//
// Run inside a synctest bubble so the gate's deadline can be compared exactly
// rather than within a tolerance — the gate reads the real clock deliberately,
// and a bubble is what makes real-clock code assertable.
func TestPauseDecidesAndClamps(t *testing.T) {
	t.Parallel()

	// The errors arrive at this layer already translated, so they are built the
	// same way here: through translateVisionError, with the two %w verbs
	// errors.As has to walk to reach the internal type.
	rateLimited := func(after time.Duration, banned bool) error {
		return translateVisionError(&vision.RateLimitError{
			Key: "some/key", RetryAfter: after, Banned: banned,
		})
	}

	tests := []struct {
		name  string
		err   error
		want  bool
		pause time.Duration
	}{
		{
			name: "an ordinary failure is not a reason to pause",
			err:  ErrNotAvailable,
			want: false,
		},
		{
			// A ban is not waited out: two minutes to three days, and retrying
			// earns the next, longer one.
			name: "a ban is reported rather than waited out",
			err:  rateLimited(120*time.Second, true),
			want: false,
		},
		{
			// Inside the clamp, so it passes through untouched.
			name:  "the server's own Retry-After is honoured",
			err:   rateLimited(45*time.Second, false),
			want:  true,
			pause: 45 * time.Second,
		},
		{
			// The case options.go spends two paragraphs justifying. Binance
			// sending no header, or an HTTP-date a fast clock reads as already
			// elapsed, arrives here as zero — and a zero pause is no pause,
			// which re-fires every worker at a server that just said stop.
			name:  "a zero Retry-After is raised to the floor",
			err:   rateLimited(0, false),
			want:  true,
			pause: 30 * time.Second,
		},
		{
			// Somebody else's number. A misconfigured proxy must not be able to
			// hang a backtest until tomorrow.
			name:  "an absurd Retry-After is cut to the ceiling",
			err:   rateLimited(24*time.Hour, false),
			want:  true,
			pause: 90 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				l, err := NewLoader(
					withOfflineHosts(),
					withPauseBounds(30*time.Second, 90*time.Second),
				)
				if err != nil {
					t.Fatalf("NewLoader: %v", err)
				}

				if got := l.pause(t.Context(), tt.err); got != tt.want {
					t.Fatalf("pause = %v, want %v", got, tt.want)
				}

				if !tt.want {
					if !l.gate.until.IsZero() {
						t.Errorf("the gate was closed for an error that is not a reason to pause")
					}

					return
				}

				if got := time.Until(l.gate.until); got != tt.pause {
					t.Errorf("gate closed for %s, want %s", got, tt.pause)
				}
			})
		})
	}
}

// TestRateLimitPausesThenSucceeds is the loop the pause exists to drive: a 429
// that outlives internal/vision's four retries stops the whole pipeline for a
// moment and the chunk is tried again.
func TestRateLimitPausesThenSucceeds(t *testing.T) {
	t.Parallel()

	// vision retries a 429 up to MaxAttempts times inside one call, so the
	// first work attempt burns four requests before this layer sees anything.
	const attemptsPerCall = 4

	var seen atomic.Int64

	f := &fakeBinance{
		restHandler: func(w http.ResponseWriter, r *http.Request) {
			if seen.Add(1) <= attemptsPerCall {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"code":-1003,"msg":"too many requests"}`))

				return
			}

			serveKlines(makeKlines(utc(2024, 1, 16), time.Hour, 24), nil)(w, r)
		},
	}

	l := f.loader(t, utc(2024, 1, 20),
		withPauseBounds(time.Millisecond, 5*time.Millisecond))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 16), End: upTo(utc(2024, 1, 17)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 24 {
		t.Fatalf("got %d candles, want 24 — the chunk should have been retried after the pause", len(klines))
	}

	if got := l.gate.until; got.IsZero() {
		t.Error("the pipeline was never paused, so the 429 was absorbed somewhere it should not have been")
	}
}

// TestRateLimitGivesUpRatherThanHanging is the other end of that loop. A server
// answering 429 forever must produce an error the caller can see, not a
// pipeline that waits on it indefinitely.
func TestRateLimitGivesUpRatherThanHanging(t *testing.T) {
	t.Parallel()

	var seen atomic.Int64

	f := &fakeBinance{
		restHandler: func(w http.ResponseWriter, _ *http.Request) {
			seen.Add(1)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":-1003,"msg":"too many requests"}`))
		},
	}

	l := f.loader(t, utc(2024, 1, 20),
		withPauseBounds(time.Millisecond, 5*time.Millisecond))

	_, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 16), End: upTo(utc(2024, 1, 17)),
	})

	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("got error %v, want one wrapping ErrRateLimited", err)
	}

	if errors.Is(err, ErrIPBanned) {
		t.Error("a 429 was reported as a ban")
	}

	// One initial attempt plus maxPipelinePauses retries, each of which is
	// vision's four. A loop that kept pausing would blow straight past this.
	if got, want := seen.Load(), int64(4*(maxPipelinePauses+1)); got != want {
		t.Errorf("made %d requests, want %d — the pipeline pause is bounded at %d retries",
			got, want, maxPipelinePauses)
	}
}

// TestLadderRecoversAnArchiveThatVanished is the runtime fallback, which routing
// makes rare but cannot make impossible: the listing said the day was there and
// the download 404'd anyway.
func TestLadderRecoversAnArchiveThatVanished(t *testing.T) {
	t.Parallel()

	f := &fakeBinance{
		// Listed, so routing keeps it as a daily archive chunk...
		days: archiveNames("BTCUSDT", Interval1h, aggDaily, utc(2024, 1, 16), 1),
		rest: makeKlines(utc(2024, 1, 16), time.Hour, 24),
	}

	// ...but no such fixture exists, so the archive server answers 404 — a
	// bucket that pruned the object between the listing and the download.
	l := f.loader(t, utc(2024, 1, 20))

	klines, err := l.Fetch(t.Context(), Request{
		Symbol: "BTCUSDT", Interval: Interval1h, Market: MarketSpot,
		Start: utc(2024, 1, 16), End: upTo(utc(2024, 1, 17)),
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(klines) != 24 {
		t.Fatalf("got %d candles, want 24 recovered from the API", len(klines))
	}

	if f.restCalls.Load() == 0 {
		t.Error("the fallback never reached the REST API")
	}
}
