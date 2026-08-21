// This file holds the package's runnable examples. They are the first thing a
// reader sees under each identifier on pkg.go.dev, and `go test` compiles every
// one of them, so an example that names a field this package no longer has
// fails the build rather than misleading somebody quietly.
//
// # Two kinds of example, and why
//
// `go test` *runs* an example only when it ends in an "// Output:" comment and
// compares what the example printed against it. Everything else is compiled and
// then discarded.
//
// That splits this file cleanly in two, and the split is forced rather than
// chosen:
//
//   - Examples for the pure API — parsing an interval, normalising a symbol,
//     finding the holes in an Availability — carry an "// Output:" block. They
//     execute on every run and their printed answers are assertions.
//
//   - Examples that hold a [binancedata.Loader] do not, because running one
//     would mean fetching from Binance, and this project's rule is that no test
//     touches Binance. The compiler still checks them, which catches the
//     failure that actually happens to documentation: a renamed field or a
//     changed signature.
//
// Pointing the second group at an httptest.Server would make them executable —
// [binancedata.WithHTTPClient] is public, so a fake transport is reachable from
// out here — and it is deliberately not done. The example source is what
// pkg.go.dev prints, and a page of test-server plumbing teaches a reader
// nothing about how to call Fetch. Documentation first; the coverage is already
// bought by loader_test.go, which stands up three fake hosts and counts what
// each is asked for.
//
// # Why package binancedata_test
//
// Go allows a second package in the same directory, named with a _test suffix,
// and examples belong in it. Its files import the library the way a consumer
// does, so they can only reach exported identifiers — which means an example
// cannot accidentally lean on something a real caller has no access to. Given
// that half this repository lives under internal/, that is worth having the
// compiler check rather than remembering.
package binancedata_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/quagmt/udecimal"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// ---------------------------------------------------------------------------
// Executed examples: pure logic, no I/O, output checked on every run.
// ---------------------------------------------------------------------------

// ExampleParseInterval shows the case-sensitivity that matters most.
func ExampleParseInterval() {
	// Binance spells one month two ways, depending on which endpoint is
	// asking: "1mo" in the archive paths, "1M" in the REST API. Both parse to
	// the same value, so a caller never has to know which one they were
	// handed.
	archive, err := binancedata.ParseInterval("1mo")
	if err != nil {
		log.Fatal(err)
	}

	rest, err := binancedata.ParseInterval("1M")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(archive == rest, archive)

	// The other half of that: "1m" is one *minute*. Matching is exact and
	// case-sensitive precisely so this pair cannot be confused, because the
	// confusion is silent — monthly candles for a request that wanted minutes
	// still look like candles.
	minute, err := binancedata.ParseInterval("1m")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(minute)

	// An unknown spelling wraps ErrInvalidRequest, so it is tested with
	// errors.Is rather than by reading the message.
	_, err = binancedata.ParseInterval("1 hour")
	fmt.Println(errors.Is(err, binancedata.ErrInvalidRequest))

	// Output:
	// true 1mo
	// 1m
	// true
}

// ExampleNormalizeSymbol shows the three spellings that mean one pair.
func ExampleNormalizeSymbol() {
	// A human writes BTC/USDT, a config file often has BTC-USDT, and Binance's
	// own URLs use BTCUSDT. All three are accepted so that a caller never has
	// to reformat, and all three normalise to the form the API and the archive
	// paths expect.
	for _, s := range []string{"BTC/USDT", "btc-usdt", "BTCUSDT"} {
		norm, err := binancedata.NormalizeSymbol(s)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(norm)
	}

	// Anything outside ASCII letters and digits is rejected rather than
	// stripped, because a symbol quietly rewritten into a different valid
	// symbol is worse than one that fails.
	_, err := binancedata.NormalizeSymbol("BTC USDT")
	fmt.Println(errors.Is(err, binancedata.ErrInvalidRequest))

	// Output:
	// BTCUSDT
	// BTCUSDT
	// BTCUSDT
	// true
}

// ExampleInterval_HasDailyArchives shows how to tell which archives exist for
// an interval before asking for any.
func ExampleInterval_HasDailyArchives() {
	// Binance publishes daily archives for most intervals but not for the
	// three coarsest: a 1w candle spans more than a day, so a daily file could
	// not hold a whole one. The planner already knows this; the method is here
	// for a caller building their own UI over the same rules.
	for _, i := range []binancedata.Interval{
		binancedata.Interval1h,
		binancedata.Interval1d,
		binancedata.Interval1w,
		binancedata.Interval1mo,
	} {
		fmt.Printf("%-4s monthly=%-5t daily=%t\n", i, i.HasMonthlyArchives(), i.HasDailyArchives())
	}

	// Output:
	// 1h   monthly=true  daily=true
	// 1d   monthly=true  daily=true
	// 1w   monthly=true  daily=false
	// 1mo  monthly=true  daily=false
}

// ExampleInterval_Duration shows the interval whose candles have no fixed
// length, and why that is a second return value rather than an approximation.
func ExampleInterval_Duration() {
	d, ok := binancedata.Interval1h.Duration()
	fmt.Println(d, ok)

	// A calendar month is 28, 29, 30 or 31 days depending on where it falls,
	// so there is no duration to return. The false is the useful part: a
	// caller computing "how many candles should this range hold?" is told the
	// arithmetic does not apply, instead of being handed 30*24h and being
	// wrong eleven months a year.
	d, ok = binancedata.Interval1mo.Duration()
	fmt.Println(d, ok)

	// Output:
	// 1h0m0s true
	// 0s false
}

// ExampleRequest_Validate shows a request being rejected before any I/O.
func ExampleRequest_Validate() {
	req := binancedata.Request{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1h,
		Market:   binancedata.MarketSpot,
		Start:    time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		// Local time, not UTC. Every instant in this library is UTC, and a
		// zone that happens to be UTC+0 today is not the same promise.
		End: time.Date(2024, 3, 2, 0, 0, 0, 0, time.Local),
	}

	err := req.Validate()
	fmt.Println(errors.Is(err, binancedata.ErrInvalidRequest))

	// Fetch calls Validate itself, so this method is for a caller who wants to
	// check a request early — while a form is being filled in, or before
	// queueing a batch — rather than at the point of use.
	req.End = time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC)
	fmt.Println(req.Validate())

	// A single instant is a legal request for exactly one candle: the range is
	// closed, so Start and End are both included.
	req.End = req.Start
	fmt.Println(req.Validate())

	// Output:
	// true
	// <nil>
	// <nil>
}

// ExampleAvailability_MonthlyGaps shows the case that breaks every
// calendar-based implementation.
func ExampleAvailability_MonthlyGaps() {
	month := func(y int, m time.Month) time.Time {
		return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	}

	// These are the real monthly archives Binance publishes for BTCUSDT at 1mo
	// around early 2024, with the real hole in them: 2024-02 and 2024-04 exist,
	// 2024-03 does not. Loader.Available returns this by listing the bucket;
	// it is written out by hand here so the example needs no network.
	a := binancedata.Availability{
		Symbol:   "BTCUSDT",
		Interval: binancedata.Interval1mo,
		Market:   binancedata.MarketSpot,
		Monthly: []time.Time{
			month(2024, time.January),
			month(2024, time.February),
			month(2024, time.April),
			month(2024, time.May),
		},
		ArchivesThrough: month(2024, time.June),
	}

	for _, gap := range a.MonthlyGaps() {
		fmt.Println(gap.Format("2006-01"))
	}

	// Only interior holes count. May is the last archive and June is where the
	// REST API takes over, so nothing after May is a gap — it is simply
	// unpublished, which is what ArchivesThrough says.
	fmt.Println("archives run out at", a.ArchivesThrough.Format("2006-01"))

	// Output:
	// 2024-03
	// archives run out at 2024-06
}

// ExampleCloses shows extracting a price column for an indicator library, and
// the precision that is given up by doing so.
func ExampleCloses() {
	klines := []binancedata.Kline{
		{Close: udecimal.MustParse("42283.58000000")},
		{Close: udecimal.MustParse("42580.00000000")},
	}

	fmt.Println(binancedata.Closes(klines))

	// The column helpers return []float64 because that is what every Go
	// technical-indicator package takes, and the conversion is lossy by
	// definition. The exact value is still on the Kline, so arithmetic that
	// has to balance — a portfolio total, a fee calculation — should read the
	// udecimal.Decimal field rather than the column.
	fmt.Println(klines[0].Close)

	// Note which column has no helper: QuoteVolume. It is the field that
	// reaches twenty significant digits, which is what ruled out float64 for
	// the whole struct in the first place, so there is no float64 slice of it
	// to hand out.

	// Output:
	// [42283.58 42580]
	// 42283.58
}

// ---------------------------------------------------------------------------
// Compiled examples: these hold a Loader, so running them would fetch from
// Binance. They carry no "// Output:" block and are checked by the compiler.
// ---------------------------------------------------------------------------

// Example is the whole library in one call: build a loader, ask for a range,
// get candles.
func Example() {
	ctx := context.Background()

	// One Loader per process, built once and shared. The concurrency limit,
	// the connection pool and the REST rate limiter all live on it, so two
	// loaders each pacing themselves correctly would still exceed Binance's
	// per-IP budget together.
	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	klines, err := loader.Fetch(ctx, binancedata.Request{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1h,
		Market:   binancedata.MarketSpot,
		Start:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		// The range is closed, so End is the open time of the last candle
		// wanted. Writing 2024-02-01 here would be legal and would ask for one
		// more candle — the one opening at midnight on the 1st — which costs
		// February's archive to fetch.
		End: time.Date(2024, 1, 31, 23, 0, 0, 0, time.UTC),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(len(klines), "candles")
}

// ExampleNewLoader shows the options worth knowing about.
func ExampleNewLoader() {
	// Every option is optional and the defaults are the recommendation: the
	// cache goes in the OS cache directory, eight chunks are fetched at once,
	// and nothing is logged. Options are applied left to right, so a later one
	// overrides an earlier one.
	loader, err := binancedata.NewLoader(
		binancedata.WithCacheDir("/var/cache/backtest"),

		// Turn this *down* for 1s data. Each worker holds one decoded archive,
		// and a month of 1s candles is around 810 MB.
		binancedata.WithConcurrency(4),
	)
	if err != nil {
		// Options validate their arguments, so this reports a bad setting
		// rather than deferring it to the first fetch. The error wraps
		// ErrInvalidRequest.
		log.Fatal(err)
	}

	_ = loader
}

// ExampleLoader_Stream shows how to consume a range too large to hold in
// memory.
func ExampleLoader_Stream() {
	ctx := context.Background()

	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	req := binancedata.Request{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1m,
		Market:   binancedata.MarketSpot,
		Start:    time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		// A zero End means "now, resolved when this call runs". Prefer it to
		// writing time.Now(): a stored end date is a snapshot that ages, and
		// having nothing stored is the whole point of the zero value.
	}

	// Stream returns an iter.Seq2, which `range` consumes directly — this is
	// Go's range-over-function, and the pair yielded is (value, error) rather
	// than (index, value). Five years of 1m candles is about 820 MB as a
	// slice; streamed, the memory in flight is bounded by the concurrency
	// limit instead, at roughly 110 MB.
	var high udecimal.Decimal

	for k, err := range loader.Stream(ctx, req) {
		if err != nil {
			// The error is yielded, not returned, so it has to be checked
			// inside the loop. Returning or breaking here cancels the
			// pipeline behind the iterator and stops every worker.
			log.Fatal(err)
		}

		if k.High.GreaterThan(high) {
			high = k.High
		}
	}

	fmt.Println("highest price:", high)
}

// ExampleLoader_FetchAll shows several requests sharing one concurrency budget.
func ExampleLoader_FetchAll() {
	ctx := context.Background()

	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 3, 31, 23, 0, 0, 0, time.UTC)

	reqs := []binancedata.Request{
		{Symbol: "BTC/USDT", Interval: binancedata.Interval1h, Market: binancedata.MarketSpot, Start: start, End: end},
		{Symbol: "ETH/USDT", Interval: binancedata.Interval1h, Market: binancedata.MarketSpot, Start: start, End: end},
	}

	// One call rather than a goroutine per request, and the difference is not
	// only tidiness: the loader's concurrency limit spans all of them, so
	// twenty requests do not become twenty times the load. Archives two
	// requests have in common are downloaded once.
	//
	// It fails fast — the first error cancels the rest and the map comes back
	// nil, so there is no half-filled result to mistake for a whole one.
	byRequest, err := loader.FetchAll(ctx, reqs)
	if err != nil {
		log.Fatal(err)
	}

	// The map is keyed by the request itself. A Request is comparable — every
	// field is a string, a small integer or a time.Time — which is what lets
	// it be a map key at all, and it saves inventing an ID to correlate
	// results with questions.
	for _, req := range reqs {
		fmt.Println(req.Symbol, len(byRequest[req]), "candles")
	}
}

// ExampleWithProgress shows reporting progress while a long range downloads.
func ExampleWithProgress() {
	ctx := context.Background()

	// The callback is serialised — the loader holds a mutex across it — so it
	// does not need to be safe for concurrent use. The cost of that promise is
	// that it sits on the critical path, so keep it to a counter, a progress
	// bar or a channel send.
	onProgress := func(p binancedata.Progress) {
		if p.Err != nil {
			// Reported here *and* returned from the call. This says which unit
			// of work failed while the run was still going; the returned error
			// is what to act on.
			log.Printf("chunk failed: %v", p.Err)

			return
		}

		log.Printf("%d/%d  %s  %s  %d candles",
			p.Done, p.Total, p.Source, p.Start.Format(time.DateOnly), p.Klines)
	}

	loader, err := binancedata.NewLoader(binancedata.WithProgress(onProgress))
	if err != nil {
		log.Fatal(err)
	}

	_, err = loader.Fetch(ctx, binancedata.Request{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1m,
		Market:   binancedata.MarketSpot,
		Start:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2024, 12, 31, 23, 59, 0, 0, time.UTC),
	})
	if err != nil {
		log.Fatal(err)
	}
}

// ExampleLoader_Available shows asking what Binance actually publishes, rather
// than inferring it from a calendar.
func ExampleLoader_Available() {
	ctx := context.Background()

	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	a, err := loader.Available(ctx, binancedata.AvailabilityQuery{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1mo,
		Market:   binancedata.MarketSpot,
		// Since bounds the answer and its cost: the bucket listing is seeked
		// with a marker built from it, so asking about 2024 onwards is one
		// round trip where asking about everything is seven for a pair that
		// has traded since 2017.
		Since: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(len(a.Monthly), "monthly archives")
	fmt.Println(len(a.MonthlyGaps()), "holes in the middle")
	fmt.Println("REST takes over at", a.ArchivesThrough)
}

// ExampleLoader_VerifyCache shows re-hashing every cached archive against the
// checksum Binance published with it.
func ExampleLoader_VerifyCache() {
	ctx := context.Background()

	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	// An iterator rather than a slice, because a large cache takes a while to
	// hash and a caller wants to report each file as it lands. The outer error
	// is for a failure to walk the cache at all; a single bad archive arrives
	// as CacheEntry.Err with a nil outer error, so one corrupt file does not
	// abandon the scan.
	bad := 0

	for entry, err := range loader.VerifyCache(ctx) {
		if err != nil {
			log.Fatal(err)
		}

		if entry.Err != nil {
			bad++

			fmt.Printf("%s: %v\n", entry.Path, entry.Err)
		}
	}

	fmt.Println(bad, "archives failed verification")
}

// ExampleWriteParquet shows exporting a range for a query engine to read.
func ExampleWriteParquet() {
	ctx := context.Background()

	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("btcusdt-1h-2024.parquet")
	if err != nil {
		log.Fatal(err)
	}

	// The iterator from Stream feeds straight in, which is what lets a range
	// larger than memory reach a file. Prices land as DECIMAL(38,8) and times
	// as TIMESTAMP(MICROS), so DuckDB, Polars or pandas read the exact values
	// rather than float64 approximations of them.
	n, err := binancedata.WriteParquet(ctx, f, loader.Stream(ctx, binancedata.Request{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1h,
		Market:   binancedata.MarketSpot,
		Start:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2024, 12, 31, 23, 0, 0, 0, time.UTC),
	}))
	if err != nil {
		// Parquet writes its footer last, so an interrupted export leaves
		// bytes no reader will accept rather than a short file that looks
		// complete. Write to a temporary file and rename on success if that
		// distinction matters.
		log.Fatal(err)
	}

	// Close explicitly and check the error, rather than only deferring it.
	// WriteParquet has already written the footer by the time it returns — it
	// closes the parquet writer itself — but the bytes may still be sitting in
	// the operating system's buffers, and Close is where a delayed write
	// failure surfaces: a full disk, a network filesystem that gave up. A
	// deferred close whose error is discarded reports success for a file that
	// was never fully written.
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Println(n, "candles written")
}

// Example_errors shows the six sentinels and what each one means you should do.
func Example_errors() {
	ctx := context.Background()

	loader, err := binancedata.NewLoader()
	if err != nil {
		log.Fatal(err)
	}

	_, err = loader.Fetch(ctx, binancedata.Request{
		Symbol:   "BTC/USDT",
		Interval: binancedata.Interval1h,
		Market:   binancedata.MarketSpot,
		Start:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2024, 1, 31, 23, 0, 0, 0, time.UTC),
	})

	// Errors are compared with errors.Is, never with ==. Every error this
	// package returns is wrapped at least once on its way out — with the URL,
	// the archive name, the row number — so the sentinel is somewhere in the
	// chain rather than at the end of it, and == would miss all of them.
	switch {
	case err == nil:
		fmt.Println("got the candles")

	case errors.Is(err, binancedata.ErrIPBanned):
		// Checked before ErrRateLimited, because a 418 carries both. There is
		// no backoff short enough to ride out a ban — it lasts from two
		// minutes to three days — and retrying is what earns the next, longer
		// one. Stop.
		log.Fatal("banned; stop the pipeline: ", err)

	case errors.Is(err, binancedata.ErrRateLimited):
		// Wait and try again. This is the one failure where waiting is the
		// right answer.
		log.Print("slow down: ", err)

	case errors.Is(err, binancedata.ErrInvalidRequest):
		// The request itself is wrong, and no network round trip was spent
		// finding out. Retrying is pointless; fix the request.
		log.Fatal("bad request: ", err)

	case errors.Is(err, binancedata.ErrNotAvailable):
		// Binance does not have this data: a symbol not yet listed on the
		// date asked for, or a day too recent to be published. A fact about
		// the world, and routinely the correct answer.
		log.Print("no data: ", err)

	case errors.Is(err, binancedata.ErrChecksum):
		// The bytes on disk or on the wire are not what Binance published.
		// Transient — the cache discards the file and the next fetch
		// re-downloads it.
		log.Print("corrupted transfer, worth retrying: ", err)

	case errors.Is(err, binancedata.ErrCorruptArchive):
		// Bytes that passed the checksum and still could not be parsed, which
		// means Binance published something this decoder does not understand.
		// Retrying produces the same bytes; this one is a bug report.
		log.Fatal("unparseable archive: ", err)

	default:
		log.Fatal(err)
	}
}
