package binancedata

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
func newIndexServer(t *testing.T, bodies map[aggregation]string) (*vision.Lister, *[]string) {
	t.Helper()

	var queries []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		queries = append(queries, r.URL.RawQuery)

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

	return vision.NewLister(srv.URL, srv.Client()), &queries
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

	if len(*queries) != 2 {
		t.Errorf("made %d listings, want 2 (one per granularity)", len(*queries))
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
	if len(*queries) != 1 {
		t.Errorf("made %d listings for a monthly-only interval, want 1", len(*queries))
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

	for _, q := range *queries {
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

	lister := vision.NewLister(srv.URL, srv.Client())

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
