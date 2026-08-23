package binancedata

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// aggregation is which of the two bulk archive granularities a path refers to.
//
// It is unexported because it is not a choice a caller makes — the library
// picks per chunk — but it is a named type rather than a bool so that
// archivePrefix(m, s, i, aggDaily) reads as itself, where
// archivePrefix(m, s, i, true) would need the reader to go and look.
type aggregation uint8

const (
	aggMonthly aggregation = iota + 1
	aggDaily
)

// String implements fmt.Stringer, and doubles as the path segment Binance uses.
// The two being the same string is convenient rather than coincidental: these
// names come from the bucket layout.
func (a aggregation) String() string {
	switch a {
	case aggMonthly:
		return "monthly"
	case aggDaily:
		return "daily"
	default:
		return fmt.Sprintf("aggregation(%d)", uint8(a))
	}
}

// marketPath is the path segment identifying a market inside the bucket.
//
// This function is the first of the three futures extension points named in
// docs/architecture.md. Adding USD-M futures is a case returning
// "futures/um", not a new code path — everything downstream is already
// parameterised on the result.
func marketPath(m Market) (string, error) {
	switch m {
	case MarketSpot:
		return "spot", nil
	default:
		return "", fmt.Errorf("market %d: %w", uint8(m), ErrInvalidRequest)
	}
}

// archivePrefix builds the bucket path holding every archive for one symbol,
// interval and granularity — the directory, not any single file:
//
//	data/spot/monthly/klines/BTCUSDT/1h/
//
// The trailing slash is required. It is what makes the prefix mean "inside this
// directory" rather than "beginning with these characters", and without it a
// listing for .../1h/ would also return everything under .../1h30m/ if such a
// thing ever existed.
func archivePrefix(m Market, symbol string, iv Interval, agg aggregation) (string, error) {
	mp, err := marketPath(m)
	if err != nil {
		return "", err
	}

	// The aggregation is validated for the same reason the market is, and it
	// was missed here originally while its neighbour archiveName validated it
	// via the layout lookup. The failure mode is quiet: an unset aggregation
	// formats through fmt.Stringer's fallback as "aggregation(0)", producing
	// the syntactically valid prefix data/spot/aggregation(0)/klines/BTCUSDT/1h/,
	// which lists successfully and returns nothing — indistinguishable from a
	// symbol that never traded.
	if !agg.valid() {
		return "", fmt.Errorf("aggregation %s: %w", agg, ErrInvalidRequest)
	}

	// The interval and the symbol are checked for the same reason, and they are
	// the two that were still missing when the aggregation check above was
	// added. Every parameter this function interpolates into a path must be
	// validated here, because every one of them formats into something that
	// looks like a legal path segment and therefore fails silently rather than
	// loudly:
	//
	//	Interval(0)   fmt.Stringer's fallback, a perfectly valid directory name
	//	""            path.Join drops an empty segment, so the interval slides
	//	              into the symbol's slot and the key names another object
	//	"../ETHUSDT"  path.Clean eats the preceding segment, likewise
	//
	// None of these produce an error from path.Join. They produce a key that
	// 404s, which translateVisionError then labels ErrNotAvailable — "Binance
	// has not published this" reported for what is a caller's bug. Validation
	// upstream in Request.Validate does catch all of them today; this is the
	// belt to that braces, at the boundary where the values become a URL.
	if !iv.IsValid() {
		return "", fmt.Errorf("interval %s: %w", iv, ErrInvalidRequest)
	}

	if err := checkNormalizedSymbol(symbol); err != nil {
		return "", err
	}

	// path.Join, not filepath.Join. These are URL paths and always use forward
	// slashes; filepath uses the host separator, which would silently produce
	// backslashes on Windows and a 404 that only reproduces there.
	return path.Join("data", mp, agg.String(), dataTypeKlines.String(), symbol, iv.String()) + "/", nil
}

// archiveDateLayouts are the time layouts Binance names archives with, one per
// granularity.
//
// Go's time layouts are a reference instant — Mon Jan 2 15:04:05 MST 2006 —
// written in the target format, rather than the strftime-style % codes used
// almost everywhere else. So "2006-01" means a four-digit year, a hyphen and a
// two-digit month. The digits are not arbitrary: 1 is the month, 2 the day,
// 15 the hour, and using any other number produces a layout that silently does
// something else.
var archiveDateLayouts = map[aggregation]string{
	aggMonthly: "2006-01",
	aggDaily:   "2006-01-02",
}

// valid reports whether a is one of the two aggregations that exist.
//
// It consults archiveDateLayouts rather than listing the constants again, so
// that the map is the single registry of what an aggregation can be: adding a
// third granularity means adding one entry, not finding every switch.
func (a aggregation) valid() bool {
	_, ok := archiveDateLayouts[a]

	return ok
}

// archiveName is the file name of one archive, without any directory:
//
//	BTCUSDT-1h-2024-01.zip        (monthly)
//	BTCUSDT-1h-2024-01-15.zip     (daily)
//
// The interval uses [Interval.String], the archive spelling, which is "1mo" for
// monthly candles. The REST spelling "1M" would 404 here, which is exactly why
// the two spellings live on the type rather than in the code that builds paths.
func archiveName(symbol string, iv Interval, agg aggregation, t time.Time) (string, error) {
	layout, ok := archiveDateLayouts[agg]
	if !ok {
		return "", fmt.Errorf("aggregation %s: %w", agg, ErrInvalidRequest)
	}

	return fmt.Sprintf("%s-%s-%s.zip", symbol, iv, t.UTC().Format(layout)), nil
}

// parseArchiveDate recovers the period an archive key covers.
//
// It is the inverse of [archiveName], and it is written as a strict parse
// rather than a regular expression or a substring search: everything except the
// date must match exactly what this library would have generated, or the key is
// not one of ours and is skipped.
//
// That strictness is doing real work, because listings are not homogeneous.
// Every .zip has a .zip.CHECKSUM sidecar beside it — which is why one 1000-key
// S3 page holds only 500 days — and Binance has published stray files in these
// directories before. Anything that does not parse is silently not an archive,
// which is the correct reading.
func parseArchiveDate(key, symbol string, iv Interval, agg aggregation) (time.Time, bool) {
	layout, ok := archiveDateLayouts[agg]
	if !ok {
		return time.Time{}, false
	}

	// A listing returns full keys, so reduce to the file name first.
	name := path.Base(key)

	// CutSuffix returns the string with the suffix removed and whether it was
	// there, in one call — so the check and the trim cannot disagree. This is
	// what rejects the .CHECKSUM sidecars.
	name, ok = strings.CutSuffix(name, ".zip")
	if !ok {
		return time.Time{}, false
	}

	stem := symbol + "-" + iv.String() + "-"

	name, ok = strings.CutPrefix(name, stem)
	if !ok {
		return time.Time{}, false
	}

	// time.Parse with no location produces a UTC time, which is what every
	// other time in this package is. ParseInLocation would be needed only if
	// the layout carried no zone and the value was meant to be local; it is
	// not.
	t, err := time.Parse(layout, name)
	if err != nil {
		return time.Time{}, false
	}

	return t, true
}

// archiveIndex is what the bucket says exists for one symbol and interval.
//
// It is a snapshot, and a snapshot of two different kinds of fact. A month that
// is present will be present forever — published archives are immutable, which
// is the premise the whole cache rests on. A month that is absent may be absent
// only for now, because it has not been published yet. Those two want different
// cache lifetimes, and conflating them gives you a process that decided at
// 00:05 that today has no data and believes it until restarted.
type archiveIndex struct {
	// months and days hold the start instant of every published period. A
	// time.Time is safe as a map key here in a way it would not be generally:
	// every one of these is built by time.Parse from an archive name, so it
	// carries no monotonic reading and its location is the shared time.UTC
	// value. See the note on [Request] for why that matters.
	months map[time.Time]bool
	days   map[time.Time]bool

	// through is the first instant no archive covers — what [plan.Spec]'s
	// ArchivesThrough field wants. Everything at or after it must come from
	// the REST API.
	through time.Time
}

// has reports whether the archive covering t at this granularity was listed.
func (ix archiveIndex) has(agg aggregation, t time.Time) bool {
	switch agg {
	case aggMonthly:
		return ix.months[t]
	case aggDaily:
		return ix.days[t]
	default:
		return false
	}
}

// fetchArchiveIndex asks the bucket what exists for one symbol and interval.
//
// It costs one listing request per granularity — two for most intervals, one
// for 3d, 1w and 1mo which have no daily archives. Both listings seek with a
// marker derived from since, so the cost is set by the range being requested
// rather than by how long the symbol has been trading.
//
// An empty result is returned as an empty index and a nil error. That is the
// honest answer: the bucket said there is nothing here. It is the caller's job
// not to turn it into an empty candle slice without also saying why — see the
// three-outcome note on [vision.Lister.List].
//
// An empty index therefore always means the bucket answered. An interval that
// cannot be served from archives at all is rejected before any request is made,
// because "we never asked" and "we asked and there is nothing" must not arrive
// looking identical.
func fetchArchiveIndex(
	ctx context.Context,
	lister *vision.Lister,
	m Market,
	symbol string,
	iv Interval,
	since time.Time,
) (archiveIndex, error) {
	// Neither granularity means no goroutine below runs, g.Wait returns nil,
	// and this function would hand back an empty index and a nil error — the
	// "failed lookup read as an empty one" conflation the whole package is
	// arranged to prevent, arriving without even a request having been made.
	//
	// The check is written on the granularities rather than on iv.IsValid
	// because the two are not quite the same question, and this is the one that
	// matters. Every interval in the table today publishes monthly archives, so
	// in practice zero granularities *is* an invalid interval — but the day an
	// interval is added that Binance publishes at neither, this still says so
	// instead of reporting a symbol that never traded.
	//
	// Note the asymmetry it removes: a bad market or aggregation already fails
	// loudly, because load reaches archivePrefix and archivePrefix rejects
	// them. Three of the four parameters were reported; the fourth returned
	// silence that reads as a fact.
	if !iv.HasMonthlyArchives() && !iv.HasDailyArchives() {
		return archiveIndex{}, fmt.Errorf(
			"interval %s: published at no archive granularity: %w", iv, ErrInvalidRequest)
	}

	ix := archiveIndex{
		months: map[time.Time]bool{},
		days:   map[time.Time]bool{},
	}

	// errgroup.WithContext gives a WaitGroup that also collects the first
	// error and cancels the other goroutines when one fails. The two listings
	// are independent round trips to the same host, so running them
	// concurrently halves the latency of building an index for the thirteen
	// intervals that have both granularities — one round trip's worth of
	// waiting instead of two, at the cost of nothing but this call.
	//
	// The context it returns must be the one the goroutines use, or a failed
	// monthly listing would leave the daily one running to completion for an
	// answer nobody will read.
	g, gctx := errgroup.WithContext(ctx)

	if iv.HasMonthlyArchives() {
		// g.Go takes a func() error and runs it in a new goroutine. The
		// closure captures ix, and the two goroutines then write to the same
		// struct concurrently — which is safe here, and only here, because
		// each writes to a different map inside it and neither writes the
		// struct's own fields. Add a third granularity that shares a map and
		// this becomes a data race the -race detector would catch.
		g.Go(func() error {
			return ix.load(gctx, lister, m, symbol, iv, aggMonthly, since)
		})
	}

	if iv.HasDailyArchives() {
		g.Go(func() error {
			return ix.load(gctx, lister, m, symbol, iv, aggDaily, since)
		})
	}

	// Wait blocks until every goroutine has returned and yields the first
	// error any of them reported. Returning the zero archiveIndex alongside it
	// matters: a partially loaded index is exactly the "failed listing read as
	// an empty one" that the design forbids, and handing one back would let a
	// caller ignoring the error plan around half a bucket.
	if err := g.Wait(); err != nil {
		return archiveIndex{}, err
	}

	// The frontier is the later of the two granularities' frontiers.
	//
	// In the ordinary case that is the daily one, because dailies reach
	// closest to the present: they lag real time by about a day, monthly
	// archives by as much as a month plus that day. Taking the later of the
	// two rather than the daily one outright is what handles the case that
	// looks impossible until it happens — a populated monthly listing beside
	// an empty daily one, whether because Binance pruned old dailies, because
	// the daily prefix moved, or because the marker seek landed past the end.
	// Reading the frontier from the empty map alone yields the zero time,
	// which means "no archives at all" and sends a multi-year range to the
	// REST API one page at a time.
	//
	// Holes deliberately play no part in this. through is the frontier, not a
	// claim that everything before it exists; a missing month in the middle is
	// found when it is fetched and handled by plan.Substitute.
	monthlyThrough := nextAfterLatest(ix.months, func(t time.Time) time.Time { return t.AddDate(0, 1, 0) })
	dailyThrough := nextAfterLatest(ix.days, func(t time.Time) time.Time { return t.AddDate(0, 0, 1) })

	ix.through = monthlyThrough
	if dailyThrough.After(ix.through) {
		ix.through = dailyThrough
	}

	return ix, nil
}

// load runs one listing and records what it found.
//
// The receiver is a value, not a pointer, and it still mutates the index —
// which looks like a bug and is not. A map value is a small header pointing at
// a shared hash table, so copying the struct copies the header and both copies
// address the same table. Writing ix.months[t] in here is therefore visible to
// the caller, while assigning ix.months itself would not be. The rule worth
// carrying away: maps, slices and channels are reference-like even inside a
// value receiver; every other field is a copy.
//
// A pointer receiver would be the safer habit in general. It is avoided here so
// that this comment can exist at the one place the distinction actually bites.
func (ix archiveIndex) load(
	ctx context.Context,
	lister *vision.Lister,
	m Market,
	symbol string,
	iv Interval,
	agg aggregation,
	since time.Time,
) error {
	prefix, err := archivePrefix(m, symbol, iv, agg)
	if err != nil {
		return err
	}

	// Seek to the range being asked about instead of listing from 2017. Keys
	// sort lexicographically and the dates in them are ISO, so lexicographic
	// order is chronological order and the key a range starts at is a valid
	// marker for it. Measured 2026-08-18: listing BTCUSDT 1m dailies from the
	// beginning is seven round trips; seeking to 2024-06 is one.
	//
	// marker is exclusive, so the key naming the first period wanted must not
	// be used directly or that period would be skipped. Trimming back to the
	// prefix of the name is enough — "BTCUSDT-1m-2024-06-01" sorts before
	// "BTCUSDT-1m-2024-06-01.zip" and after every earlier date.
	var marker string

	if !since.IsZero() {
		name, err := archiveName(symbol, iv, agg, since)
		if err != nil {
			return err
		}

		marker = prefix + strings.TrimSuffix(name, ".zip")
	}

	objects, err := lister.List(ctx, prefix, marker)
	if err != nil {
		return err
	}

	for _, o := range objects {
		t, ok := parseArchiveDate(o.Key, symbol, iv, agg)
		if !ok {
			continue // a .CHECKSUM sidecar, or something that is not ours
		}

		switch agg {
		case aggMonthly:
			ix.months[t] = true
		case aggDaily:
			ix.days[t] = true
		}
	}

	return nil
}

// nextAfterLatest returns advance(latest key in set), or the zero time for an
// empty set.
//
// The zero time is a meaningful answer rather than a missing one: it flows into
// plan.Spec.ArchivesThrough, where it means "no archives, fetch the whole range
// from the API". A symbol listed this week produces exactly that, with no
// special case anywhere.
//
// Map iteration order in Go is deliberately randomised — the runtime perturbs
// the starting bucket on every range — so there is no "last" key to read and
// the maximum has to be found by scanning. That randomisation is a feature: it
// stops code accruing a dependence on an order the language never promised.
func nextAfterLatest(set map[time.Time]bool, advance func(time.Time) time.Time) time.Time {
	var latest time.Time

	for t := range set {
		if t.After(latest) {
			latest = t
		}
	}

	if latest.IsZero() {
		return time.Time{}
	}

	return advance(latest)
}

// ---------------------------------------------------------------------------
// The public view: what Binance publishes
// ---------------------------------------------------------------------------

// AvailabilityQuery names what to ask the bucket about. It is the input to
// [Loader.Available].
//
// It is a struct rather than four parameters for the same reason [Request] is:
// three of the fields are required and one is not, and a struct says which is
// which at the call site. It is a *separate* struct from Request because Request
// carries a Start and an End that this question has no use for, and a type whose
// fields are silently ignored is the kind of API that teaches callers to stop
// trusting the ones that are not.
type AvailabilityQuery struct {
	// Symbol is the trading pair, in any spelling [NormalizeSymbol] accepts.
	Symbol string

	// Interval is the candle period. Required; the zero value is invalid.
	Interval Interval

	// Market selects the Binance market. Required; the zero value is invalid.
	Market Market

	// Since bounds the answer, and bounds its cost. The bucket listing is
	// seeked with a marker built from it, so asking about 2024 onwards is one
	// round trip where asking about everything is seven for a pair that has
	// traded since 2017. Optional; the zero value means the whole history.
	//
	// It is not a filter applied afterwards. Archives before it are not
	// listed, so they are absent from the result rather than excluded from it,
	// and [Availability.ArchivesThrough] is the only field a Since cannot
	// affect — the frontier is at the far end.
	Since time.Time
}

// validate checks the query and returns the normalised symbol.
//
// Returning the normalised value rather than writing it back into the receiver
// is the same shape [Request.resolve] uses, and for the same reason: a value
// normalised in two places is one that gets normalised in only one of them.
func (q AvailabilityQuery) validate() (string, error) {
	symbol, err := NormalizeSymbol(q.Symbol)
	if err != nil {
		return "", err // already wraps ErrInvalidRequest and names the symbol
	}

	if !q.Interval.IsValid() {
		return "", fmt.Errorf("interval: %w", ErrInvalidRequest)
	}

	if !q.Market.IsValid() {
		return "", fmt.Errorf("market: %w", ErrInvalidRequest)
	}

	if !q.Since.IsZero() {
		if err := requireUTC("since", q.Since); err != nil {
			return "", err
		}
	}

	return symbol, nil
}

// Availability is what Binance publishes for one symbol and interval: which
// archives exist, and where the archives stop and the REST API takes over.
//
// It answers the question a caller cannot answer from a calendar — "how far
// back does this pair go, and is anything missing in the middle?" — and it
// answers it by asking the bucket rather than by inferring. Binance is not
// contractually bound to publish on any schedule, and it demonstrably has
// holes: BTCUSDT-1mo-2024-03.zip does not exist while 2024-02 and 2024-04 both
// do. No date arithmetic predicts that.
type Availability struct {
	// Symbol, Interval and Market echo the query, with the symbol normalised.
	// They are here so that a result can be passed around, logged or printed
	// without its question having to travel beside it.
	Symbol   string
	Interval Interval
	Market   Market

	// Monthly and Daily hold the start instant of every archive the bucket
	// listed, ascending: midnight UTC on the 1st for a month, midnight UTC for
	// a day. Both are nil for a symbol that never traded — which is a fact
	// about Binance, not an error, and is why [Loader.Available] returns it
	// with a nil error.
	//
	// Daily is always nil for the three intervals Binance publishes monthly
	// only: 3d, 1w and 1mo.
	Monthly []time.Time
	Daily   []time.Time

	// ArchivesThrough is the first instant no archive covers. Everything at or
	// after it has to come from the REST API, and it is the frontier the
	// planner uses. The zero time means there are no archives at all.
	ArchivesThrough time.Time
}

// MonthlyGaps returns the months missing between the first and last monthly
// archive. DailyGaps does the same for days.
//
// Only the interior counts. Periods after the last archive are not gaps — they
// are the part of history that has not been published yet, which is what
// [Availability.ArchivesThrough] is for — and there is nothing before the first
// archive to have a hole in.
//
// A non-empty result is the case that breaks calendar-based implementations,
// and it is why this library lists the bucket at all.
func (a Availability) MonthlyGaps() []time.Time {
	return gaps(a.Monthly, func(t time.Time) time.Time { return t.AddDate(0, 1, 0) })
}

// DailyGaps returns the days missing between the first and last daily archive.
// See [Availability.MonthlyGaps].
func (a Availability) DailyGaps() []time.Time {
	return gaps(a.Daily, func(t time.Time) time.Time { return t.AddDate(0, 0, 1) })
}

// gaps walks the grid from the first published period to the last and collects
// the ones that are absent.
//
// It rebuilds a set from the slice rather than taking one, because the slice is
// what the caller has and rebuilding it is O(n) against an O(n) walk. advance
// supplies the grid step, which is what lets months and days share the walk:
// AddDate(0, 1, 0) on the 1st of a month is always the 1st of the next one, and
// no duration constant can say that.
func gaps(published []time.Time, advance func(time.Time) time.Time) []time.Time {
	if len(published) < 2 {
		return nil // nothing can be missing between fewer than two things
	}

	set := make(map[time.Time]bool, len(published))
	for _, t := range published {
		set[t] = true
	}

	first, last := published[0], published[len(published)-1]

	var out []time.Time

	for t := advance(first); t.Before(last); t = advance(t) {
		if !set[t] {
			out = append(out, t)
		}
	}

	return out
}

// Available reports what Binance publishes for one symbol and interval.
//
// It is the library half of `bmd list`, and it costs one bucket listing per
// granularity — two for most intervals, one for the three that have no daily
// archives. Nothing is downloaded and no candle is parsed.
//
// An empty result with a nil error is the honest answer for a symbol that never
// traded: the bucket answered, and it said there is nothing there. That is a
// different outcome from an error, which means the bucket did not answer, and
// the two must not arrive looking the same — see the note on the bucket
// lister's three outcomes in docs/architecture.md.
//
// The call takes a permit from the Loader's concurrency limit for the same
// reason the plan phase does: it is I/O, and a limit that only covers downloads
// is a limit that a hundred concurrent list calls walk straight past.
func (l *Loader) Available(ctx context.Context, q AvailabilityQuery) (Availability, error) {
	symbol, err := q.validate()
	if err != nil {
		return Availability{}, err
	}

	if err := l.sem.acquire(ctx); err != nil {
		return Availability{}, err
	}
	defer l.sem.release()

	index, err := fetchArchiveIndex(ctx, l.lister, q.Market, symbol, q.Interval, q.Since)
	if err != nil {
		return Availability{}, err
	}

	l.logger.DebugContext(ctx, "availability",
		"symbol", symbol, "interval", q.Interval.String(),
		"monthly", len(index.months), "daily", len(index.days),
		"archivesThrough", index.through)

	return Availability{
		Symbol:          symbol,
		Interval:        q.Interval,
		Market:          q.Market,
		Monthly:         sortedTimes(index.months),
		Daily:           sortedTimes(index.days),
		ArchivesThrough: index.through,
	}, nil
}

// sortedTimes turns one of the index's sets into an ascending slice.
//
// Go randomises map iteration order deliberately, so the sort is not tidying up
// after a coincidence — it is the only thing that makes two calls agree with
// each other. A nil slice for an empty set is intentional: len and range treat
// nil and empty identically, and nil is what a caller gets from a zero
// Availability, so the two spellings never both need handling.
func sortedTimes(set map[time.Time]bool) []time.Time {
	if len(set) == 0 {
		return nil
	}

	out := make([]time.Time, 0, len(set))
	for t := range set {
		out = append(out, t)
	}

	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })

	return out
}
