package binancedata

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

func TestArchivePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		market   Market
		symbol   string
		interval Interval
		agg      aggregation
		want     string
		wantErr  bool
	}{
		{
			name: "spot monthly", market: MarketSpot, symbol: "BTCUSDT",
			interval: Interval1h, agg: aggMonthly,
			want: "data/spot/monthly/klines/BTCUSDT/1h/",
		},
		{
			name: "spot daily", market: MarketSpot, symbol: "ETHUSDT",
			interval: Interval1m, agg: aggDaily,
			want: "data/spot/daily/klines/ETHUSDT/1m/",
		},
		{
			// The archive spelling, not the REST one. A prefix built with "1M"
			// lists nothing and looks exactly like a symbol that never traded.
			name: "monthly candles use the archive spelling", market: MarketSpot, symbol: "BTCUSDT",
			interval: Interval1mo, agg: aggMonthly,
			want: "data/spot/monthly/klines/BTCUSDT/1mo/",
		},
		{
			name: "an unset market is rejected", market: 0, symbol: "BTCUSDT",
			interval: Interval1h, agg: aggMonthly,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := archivePrefix(tt.market, tt.symbol, tt.interval, tt.agg)

			if tt.wantErr {
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("got %v, want an error wrapping ErrInvalidRequest", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("archivePrefix: %v", err)
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}

			// Without the trailing slash the prefix means "starts with these
			// characters", so a listing for 1h would also return 1h30m if such
			// an interval were ever added.
			if !strings.HasSuffix(got, "/") {
				t.Errorf("prefix %q must end in a slash", got)
			}
		})
	}
}

func TestArchiveName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		symbol   string
		interval Interval
		agg      aggregation
		at       time.Time
		want     string
	}{
		{
			name: "monthly", symbol: "BTCUSDT", interval: Interval1h,
			agg: aggMonthly, at: utc(2024, 1, 1), want: "BTCUSDT-1h-2024-01.zip",
		},
		{
			name: "daily", symbol: "BTCUSDT", interval: Interval1h,
			agg: aggDaily, at: utc(2024, 1, 15), want: "BTCUSDT-1h-2024-01-15.zip",
		},
		{
			name: "monthly candles", symbol: "BTCUSDT", interval: Interval1mo,
			agg: aggMonthly, at: utc(2024, 3, 1), want: "BTCUSDT-1mo-2024-03.zip",
		},
		{
			// A monthly name is built from the month, so any day within it
			// produces the same file — which is what lets a chunk's start
			// instant be passed straight in.
			name: "a mid-month instant still names the month", symbol: "BTCUSDT", interval: Interval1h,
			agg: aggMonthly, at: utc(2024, 1, 31, 23, 59), want: "BTCUSDT-1h-2024-01.zip",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := archiveName(tt.symbol, tt.interval, tt.agg, tt.at)
			if err != nil {
				t.Fatalf("archiveName: %v", err)
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseArchiveDate(t *testing.T) {
	t.Parallel()

	const dir = "data/spot/monthly/klines/BTCUSDT/1h/"

	tests := []struct {
		name   string
		key    string
		agg    aggregation
		want   time.Time
		wantOK bool
	}{
		{
			name: "a monthly archive", key: dir + "BTCUSDT-1h-2024-01.zip",
			agg: aggMonthly, want: utc(2024, 1, 1), wantOK: true,
		},
		{
			name: "a daily archive", key: "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-15.zip",
			agg: aggDaily, want: utc(2024, 1, 15), wantOK: true,
		},
		{
			// Every .zip has one of these beside it, which is why a 1000-key
			// S3 page holds only 500 days. Skipping them is the single most
			// load-bearing line in the parser.
			name: "a checksum sidecar is not an archive", key: dir + "BTCUSDT-1h-2024-01.zip.CHECKSUM",
			agg: aggMonthly,
		},
		{
			name: "a different symbol is skipped", key: dir + "ETHUSDT-1h-2024-01.zip",
			agg: aggMonthly,
		},
		{
			name: "a different interval is skipped", key: dir + "BTCUSDT-4h-2024-01.zip",
			agg: aggMonthly,
		},
		{
			// 1m and 1mo share a prefix, so a sloppy match would read a minute
			// archive as a month archive and place it a year off.
			name: "a lookalike interval is skipped", key: dir + "BTCUSDT-1mo-2024-01.zip",
			agg: aggMonthly,
		},
		{
			name: "a daily name read as monthly is skipped", key: dir + "BTCUSDT-1h-2024-01-15.zip",
			agg: aggMonthly,
		},
		{
			name: "an unparseable date is skipped", key: dir + "BTCUSDT-1h-latest.zip",
			agg: aggMonthly,
		},
		{
			name: "an impossible date is skipped", key: dir + "BTCUSDT-1h-2024-13.zip",
			agg: aggMonthly,
		},
		{
			name: "an unrelated file is skipped", key: dir + "index.html",
			agg: aggMonthly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseArchiveDate(tt.key, "BTCUSDT", Interval1h, tt.agg)

			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (got %s)", ok, tt.wantOK, got)
			}

			if ok && !got.Equal(tt.want) {
				t.Errorf("got %s, want %s", got, tt.want)
			}

			if ok && got.Location() != time.UTC {
				t.Errorf("location = %s, want UTC — these become map keys", got.Location())
			}
		})
	}
}

// TestArchiveNameRoundTrips checks that the two halves agree, across a year of
// dates at both granularities. A builder and a parser that drift apart produce
// an index that is silently missing everything.
func TestArchiveNameRoundTrips(t *testing.T) {
	t.Parallel()

	for _, iv := range []Interval{Interval1s, Interval1m, Interval1h, Interval1d, Interval1mo} {
		for _, agg := range []aggregation{aggMonthly, aggDaily} {
			for d := utc(2024, 1, 1); d.Before(utc(2025, 1, 1)); d = d.AddDate(0, 0, 1) {
				name, err := archiveName("BTCUSDT", iv, agg, d)
				if err != nil {
					t.Fatalf("archiveName: %v", err)
				}

				got, ok := parseArchiveDate("some/dir/"+name, "BTCUSDT", iv, agg)
				if !ok {
					t.Fatalf("parseArchiveDate(%q) failed", name)
				}

				want := d
				if agg == aggMonthly {
					want = time.Date(d.Year(), d.Month(), 1, 0, 0, 0, 0, time.UTC)
				}

				if !got.Equal(want) {
					t.Fatalf("%s round-tripped to %s, want %s", name, got, want)
				}
			}
		}
	}
}

// listingOf renders a ListBucketResult holding the given file names, so a test
// can state what the bucket contains as a list of archives.
//
// The XML parsing itself is covered against real captured responses in
// internal/vision; these tests are about interpreting the keys, so generating
// the envelope here keeps each case to one readable line.
func listingOf(prefix string, names ...string) string {
	var b strings.Builder

	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>` +
		`<IsTruncated>false</IsTruncated>`)

	for _, n := range names {
		fmt.Fprintf(&b, `<Contents><Key>%s%s</Key><Size>1024</Size></Contents>`, prefix, n)
		// Every archive has a checksum sidecar beside it in a real listing.
		// Including them means the tests exercise the filtering rather than a
		// tidied-up world where it is unnecessary.
		fmt.Fprintf(&b, `<Contents><Key>%s%s.CHECKSUM</Key><Size>82</Size></Contents>`, prefix, n)
	}

	b.WriteString(`</ListBucketResult>`)

	return b.String()
}

// newIndexServer serves one listing per aggregation, chosen by the prefix in
// the request, and records the queries it received.
//
// The recorder is mutex-guarded because fetchArchiveIndex issues its two
// listings concurrently, so net/http runs this handler in two goroutines at
// once. An unguarded append is a genuine data race, and -race fails the suite
// on it — which is the race detector doing exactly what it is for: the change
// that made the listings parallel is what made this test helper unsafe, and
// nothing else would have said so.
//
// The guard is returned as a snapshot function rather than as a *[]string, so
// that reading is locked too. Handing back the slice made the protection
// one-sided: writes took the mutex, reads did not, and the tests were left
// leaning on net/http happening to establish a happens-before edge between the
// handler's append and the client's read of the response. It does, today,
// through the transport's own internal synchronisation — which is a fact about
// an implementation nothing promises, not a guarantee this test holds. A
// snapshot taken under the same mutex needs no such argument.
func newIndexServer(t *testing.T, bodies map[aggregation]string) (*vision.Lister, func() []string) {
	t.Helper()

	var (
		mu      sync.Mutex
		queries []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")

		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		mu.Unlock()

		agg := aggMonthly
		if strings.Contains(prefix, "/daily/") {
			agg = aggDaily
		}

		body, ok := bodies[agg]
		if !ok {
			body = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>` +
				`<IsTruncated>false</IsTruncated></ListBucketResult>`
		}

		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	// fastPolicy (download_test.go) retries exactly as production does but
	// without spending the wall-clock time — TestFetchArchiveIndexPropagatesErrors
	// serves a 500, which is a status the policy retries.
	//
	// slices.Clone rather than returning queries itself: the caller would
	// otherwise hold the same backing array the handler appends to, and the
	// mutex would have guarded the copy while the caller read the original.
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()

		return slices.Clone(queries)
	}

	return vision.NewLister(srv.URL, srv.Client(), fastPolicy()), snapshot
}

func TestFetchArchiveIndex(t *testing.T) {
	t.Parallel()

	monthlyPrefix := "data/spot/monthly/klines/BTCUSDT/1h/"
	dailyPrefix := "data/spot/daily/klines/BTCUSDT/1h/"

	lister, queries := newIndexServer(t, map[aggregation]string{
		aggMonthly: listingOf(monthlyPrefix,
			"BTCUSDT-1h-2026-05.zip", "BTCUSDT-1h-2026-06.zip", "BTCUSDT-1h-2026-07.zip"),
		aggDaily: listingOf(dailyPrefix,
			"BTCUSDT-1h-2026-08-14.zip", "BTCUSDT-1h-2026-08-15.zip", "BTCUSDT-1h-2026-08-16.zip"),
	})

	ix, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", Interval1h, time.Time{})
	if err != nil {
		t.Fatalf("fetchArchiveIndex: %v", err)
	}

	if len(queries()) != 2 {
		t.Errorf("made %d listings, want 2 (one per granularity)", len(queries()))
	}

	for _, m := range []time.Time{utc(2026, 5, 1), utc(2026, 6, 1), utc(2026, 7, 1)} {
		if !ix.has(aggMonthly, m) {
			t.Errorf("month %s should be present", m.Format("2006-01"))
		}
	}

	if ix.has(aggMonthly, utc(2026, 8, 1)) {
		t.Error("August was not listed and must not be present")
	}

	if !ix.has(aggDaily, utc(2026, 8, 16)) {
		t.Error("2026-08-16 should be present")
	}

	// The frontier comes from the finest granularity available, because that
	// is the one reaching closest to the present. Taking it from the monthly
	// listing would push two weeks of published daily archives onto the REST
	// path.
	if want := utc(2026, 8, 17); !ix.through.Equal(want) {
		t.Errorf("through = %s, want %s (the day after the newest daily archive)", ix.through, want)
	}

	// The sidecars must not have been counted as archives.
	if len(ix.months) != 3 || len(ix.days) != 3 {
		t.Errorf("index holds %d months and %d days, want 3 and 3 — .CHECKSUM files leaked in",
			len(ix.months), len(ix.days))
	}
}

// TestFetchArchiveIndexRecordsHoles is the availability half of the finding
// that ruled out a calendar heuristic: BTCUSDT's 1mo archive for March 2024 is
// missing while February and April exist.
func TestFetchArchiveIndexRecordsHoles(t *testing.T) {
	t.Parallel()

	prefix := "data/spot/monthly/klines/BTCUSDT/1mo/"

	lister, queries := newIndexServer(t, map[aggregation]string{
		aggMonthly: listingOf(prefix,
			"BTCUSDT-1mo-2024-01.zip", "BTCUSDT-1mo-2024-02.zip",
			"BTCUSDT-1mo-2024-04.zip", "BTCUSDT-1mo-2024-05.zip"),
	})

	ix, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", Interval1mo, time.Time{})
	if err != nil {
		t.Fatalf("fetchArchiveIndex: %v", err)
	}

	// 1mo has no daily archives, so only one listing should have been made.
	// Asking for a directory that cannot exist is a wasted round trip on every
	// request for these three intervals.
	if len(queries()) != 1 {
		t.Errorf("made %d listings for a monthly-only interval, want 1", len(queries()))
	}

	if ix.has(aggMonthly, utc(2024, 3, 1)) {
		t.Error("March 2024 must be absent — that hole is the reason availability is probed, not computed")
	}

	for _, m := range []time.Time{utc(2024, 2, 1), utc(2024, 4, 1)} {
		if !ix.has(aggMonthly, m) {
			t.Errorf("%s should be present", m.Format("2006-01"))
		}
	}

	// A hole is not the frontier. through stays at the newest archive plus one
	// period; the gap in the middle is discovered on fetch and handled by
	// plan.Substitute.
	if want := utc(2024, 6, 1); !ix.through.Equal(want) {
		t.Errorf("through = %s, want %s", ix.through, want)
	}
}

// TestFetchArchiveIndexEmptyIsNotAnError covers the trap the live bucket sets:
// an unknown symbol answers HTTP 200 with an empty listing, not a 404.
//
// The right result is an empty index and a nil error, and a zero frontier —
// which flows into plan.Spec.ArchivesThrough as "no archives, use the API for
// everything". A symbol listed this week produces exactly this, with no special
// case anywhere in the planner.
func TestFetchArchiveIndexEmptyIsNotAnError(t *testing.T) {
	t.Parallel()

	lister, _ := newIndexServer(t, nil)

	ix, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "NOSUCHSYM", Interval1h, time.Time{})
	if err != nil {
		t.Fatalf("an empty listing must not be an error, got: %v", err)
	}

	if len(ix.months) != 0 || len(ix.days) != 0 {
		t.Errorf("index should be empty, holds %d months and %d days", len(ix.months), len(ix.days))
	}

	if !ix.through.IsZero() {
		t.Errorf("through = %s, want the zero time (no archives at all)", ix.through)
	}
}

// TestFetchArchiveIndexSeeks pins the pagination optimisation. Listings cap at
// 1000 keys, and archives come in .zip/.CHECKSUM pairs, so one page is only 500
// days — the full daily history of BTCUSDT is seven round trips. Seeking with a
// marker makes the cost proportional to the range asked about instead.
func TestFetchArchiveIndexSeeks(t *testing.T) {
	t.Parallel()

	lister, queries := newIndexServer(t, nil)

	since := utc(2024, 6, 1)

	if _, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", Interval1h, since); err != nil {
		t.Fatalf("fetchArchiveIndex: %v", err)
	}

	var monthlyQuery, dailyQuery string

	for _, q := range queries() {
		if strings.Contains(q, "monthly") {
			monthlyQuery = q
		} else {
			dailyQuery = q
		}
	}

	// marker is exclusive, so it must stop just short of the first wanted
	// archive: "...2024-06" rather than "...2024-06.zip", which would skip
	// June itself.
	for _, tc := range []struct{ query, want string }{
		{monthlyQuery, "BTCUSDT-1h-2024-06"},
		{dailyQuery, "BTCUSDT-1h-2024-06-01"},
	} {
		if !strings.Contains(tc.query, "marker") {
			t.Errorf("query %q sent no marker, so it would list from 2017", tc.query)

			continue
		}

		// The query is URL-encoded, so compare against the encoded form.
		if !strings.Contains(tc.query, strings.ReplaceAll(tc.want, "/", "%2F")) {
			t.Errorf("query %q does not seek to %q", tc.query, tc.want)
		}

		if strings.Contains(tc.query, tc.want+".zip") {
			t.Errorf("query %q includes the .zip suffix, which would skip the first archive", tc.query)
		}
	}
}

func TestFetchArchiveIndexPropagatesErrors(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>boom</Message></Error>`))
	}))
	t.Cleanup(srv.Close)

	lister := vision.NewLister(srv.URL, srv.Client(), fastPolicy())

	_, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", Interval1h, time.Time{})
	if err == nil {
		t.Fatal("want an error, got nil")
	}

	// A failed listing must never look like an empty one. "We do not know" and
	// "there is nothing" lead to opposite actions, and conflating them is how
	// a range silently returns zero candles.
	if !strings.Contains(err.Error(), "InternalError") {
		t.Errorf("error %v should name the underlying failure", err)
	}
}

func TestFetchArchiveIndexRejectsUnknownMarket(t *testing.T) {
	t.Parallel()

	lister, _ := newIndexServer(t, nil)

	_, err := fetchArchiveIndex(t.Context(), lister, Market(99), "BTCUSDT", Interval1h, time.Time{})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("got %v, want an error wrapping ErrInvalidRequest", err)
	}
}

func TestAggregationString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		agg  aggregation
		want string
	}{
		{aggMonthly, "monthly"},
		{aggDaily, "daily"},
		{aggregation(0), "aggregation(0)"},
	}

	for _, tt := range tests {
		if got := tt.agg.String(); got != tt.want {
			t.Errorf("aggregation(%d).String() = %q, want %q", uint8(tt.agg), got, tt.want)
		}
	}
}

// TestFetchArchiveIndexFrontierSurvivesAnEmptyDailyListing is the regression
// test for a frontier taken from the daily map alone.
//
// The daily listing being empty while the monthly one is full looks impossible
// until it happens: Binance pruning old dailies, the daily prefix moving, or a
// marker seek landing past the end all produce it. Reading the frontier from
// the empty map alone yields the zero time, which means "no archives exist at
// all" — and a multi-year request then goes to the REST API one page at a time,
// slowly and correctly enough that nobody notices for a while.
//
// The frontier is the later of the two, so the monthly answer stands when the
// daily one has nothing to say.
func TestFetchArchiveIndexFrontierSurvivesAnEmptyDailyListing(t *testing.T) {
	t.Parallel()

	monthlyPrefix := "data/spot/monthly/klines/BTCUSDT/1h/"

	lister, _ := newIndexServer(t, map[aggregation]string{
		aggMonthly: listingOf(monthlyPrefix,
			"BTCUSDT-1h-2026-05.zip", "BTCUSDT-1h-2026-06.zip", "BTCUSDT-1h-2026-07.zip"),
		// aggDaily is absent, so the server answers with a well-formed empty
		// listing — the bucket saying "nothing here", not an error.
	})

	ix, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", Interval1h, time.Time{})
	if err != nil {
		t.Fatalf("fetchArchiveIndex: %v", err)
	}

	if want := utc(2026, 8, 1); !ix.through.Equal(want) {
		t.Errorf("through = %s, want %s (the month after the newest monthly archive)", ix.through, want)
	}

	if ix.through.IsZero() {
		t.Error("a zero frontier would send the whole range to the REST API")
	}
}

// TestFetchArchiveIndexPrefersTheDailyFrontier is the other side of the same
// rule: in the ordinary case dailies reach closer to the present, and the
// monthly frontier must not drag the answer backwards.
func TestFetchArchiveIndexPrefersTheDailyFrontier(t *testing.T) {
	t.Parallel()

	lister, _ := newIndexServer(t, map[aggregation]string{
		aggMonthly: listingOf("data/spot/monthly/klines/BTCUSDT/1h/",
			"BTCUSDT-1h-2026-06.zip", "BTCUSDT-1h-2026-07.zip"),
		aggDaily: listingOf("data/spot/daily/klines/BTCUSDT/1h/",
			"BTCUSDT-1h-2026-08-15.zip", "BTCUSDT-1h-2026-08-16.zip"),
	})

	ix, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", Interval1h, time.Time{})
	if err != nil {
		t.Fatalf("fetchArchiveIndex: %v", err)
	}

	// August 1st (monthly) vs August 17th (daily): taking the earlier would
	// discard sixteen days of published archives.
	if want := utc(2026, 8, 17); !ix.through.Equal(want) {
		t.Errorf("through = %s, want %s", ix.through, want)
	}
}

// TestArchivePrefixRejectsAnUnsetAggregation covers a validation gap whose
// failure mode is a successful request that returns nothing.
//
// fmt.Stringer's fallback renders an unset aggregation as "aggregation(0)",
// which is a perfectly valid path segment. The prefix it produces lists
// successfully and matches no keys — indistinguishable, from the planner's
// side, from a symbol that never traded.
func TestArchivePrefixRejectsAnUnsetAggregation(t *testing.T) {
	t.Parallel()

	for _, agg := range []aggregation{0, aggregation(99)} {
		got, err := archivePrefix(MarketSpot, "BTCUSDT", Interval1h, agg)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("archivePrefix(agg=%d) = %q, %v; want an error wrapping ErrInvalidRequest", agg, got, err)
		}
	}

	// Its neighbour validates the same value via the layout lookup. Both are
	// checked here so the pair cannot drift apart again.
	if _, err := archiveName("BTCUSDT", Interval1h, 0, utc(2024, 1, 1)); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("archiveName with an unset aggregation: got %v, want ErrInvalidRequest", err)
	}
}

// TestArchivePrefixValidatesEveryParameter is the aggregation check above
// generalised to the two parameters it originally left out.
//
// Each of these formats into something that looks like a legal path segment, so
// none of them produces an error from path.Join. They produce a *different*
// well-formed key, which 404s, which the root package then reports as
// ErrNotAvailable — Binance's calendar taking the blame for a caller's bug.
func TestArchivePrefixValidatesEveryParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		symbol   string
		interval Interval
		// why records what the unvalidated value would have produced, so that
		// the test states the failure it prevents rather than only the error it
		// now expects.
		why string
	}{
		{
			name: "an unset interval", symbol: "BTCUSDT", interval: 0,
			why: `fmt.Stringer's fallback gives .../BTCUSDT/Interval(0)/`,
		},
		{
			name: "an out-of-range interval", symbol: "BTCUSDT", interval: Interval(200),
			why: `likewise .../BTCUSDT/Interval(200)/`,
		},
		{
			name: "an empty symbol", symbol: "", interval: Interval1h,
			why: `path.Join drops the empty segment, so the interval slides into the symbol's slot: data/spot/daily/klines/1h/`,
		},
		{
			name: "a symbol that escapes its directory", symbol: "../ETHUSDT", interval: Interval1h,
			why: `path.Clean eats the klines segment: data/spot/daily/ETHUSDT/1h/`,
		},
		{
			// NormalizeSymbol accepts this and returns "BTCUSDT" — but the path
			// builders assert rather than normalise, because a value normalised
			// in two places is a value that gets normalised in one of them.
			// Two spellings reaching the cache are two entries for one pair.
			name: "an unnormalised symbol", symbol: "BTC/USDT", interval: Interval1h,
			why: `the caller skipped Request.resolve; normalising here would hide that`,
		},
		{
			name: "a lowercase symbol", symbol: "btcusdt", interval: Interval1h,
			why: `S3 keys are case-sensitive, so this lists nothing at all`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := archivePrefix(MarketSpot, tt.symbol, tt.interval, aggDaily)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("archivePrefix(%q, %s) = %q, %v; want an error wrapping ErrInvalidRequest (%s)",
					tt.symbol, tt.interval, got, err, tt.why)
			}
		})
	}
}

// TestFetchArchiveIndexRejectsAnIntervalWithNoArchives covers the one input
// that made this function answer without asking anything.
//
// Both HasMonthlyArchives and HasDailyArchives begin with i.IsValid(), so an
// interval that fails validation reported false to both, launched zero
// goroutines, and let g.Wait return nil over an empty errgroup. The result was
// an empty index and a nil error — "Binance has published nothing for this
// symbol" — produced without a single request being made.
//
// That is the "failed lookup read as an empty one" conflation the whole package
// is arranged around, and it is the reason the guard is on the granularities
// rather than on IsValid: the day an interval exists that Binance publishes at
// neither, this must still say so instead of reporting silence.
func TestFetchArchiveIndexRejectsAnIntervalWithNoArchives(t *testing.T) {
	t.Parallel()

	for _, iv := range []Interval{0, Interval(200)} {
		lister, queries := newIndexServer(t, nil)

		ix, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "BTCUSDT", iv, time.Time{})
		if !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("fetchArchiveIndex(iv=%d) = %+v, %v; want an error wrapping ErrInvalidRequest", iv, ix, err)
		}

		// The failure must be reported before anything is asked, not inferred
		// from an empty answer.
		if n := len(queries()); n != 0 {
			t.Errorf("made %d listings for an interval that has none, want 0", n)
		}
	}

	// The contrast that makes the point: a valid interval over an empty bucket
	// is the same empty index with a nil error, and that one is a fact worth
	// acting on. The two must never be spelled the same way.
	lister, _ := newIndexServer(t, nil)

	if _, err := fetchArchiveIndex(t.Context(), lister, MarketSpot, "NOSUCHSYM", Interval1h, time.Time{}); err != nil {
		t.Errorf("an empty bucket must not be an error: %v", err)
	}
}
