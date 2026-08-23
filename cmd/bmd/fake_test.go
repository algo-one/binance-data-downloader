package main

// The stand-in for a real Loader, and the candles the tests feed through it.
//
// Why a fake rather than the real pipeline against an httptest.Server: the
// options that point a binancedata.Loader at a test host are unexported, so
// only the library's own tests can build one. That is deliberate — they are a
// test seam, not API — and it decides the shape of the tests here. The pipeline
// is covered end to end in loader_test.go against three fake hosts and real
// archives; what is left for this package is everything between a command line
// and that pipeline, which is what these tests exercise.

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/quagmt/udecimal"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// fakeLoader implements the loader interface in flags.go.
type fakeLoader struct {
	// klines is what Stream yields, in order.
	klines []binancedata.Kline

	// streamErr, if set, is yielded after the candles have been, which is how
	// Loader.Stream reports a failure mid-range.
	streamErr error

	// streamErrs fails one symbol and leaves the others alone, which is what a
	// multi-symbol download needs to exercise: a delisted pair among four good
	// ones is the case the command's error handling exists for, and streamErr
	// above can only fail all of them at once.
	streamErrs map[string]error

	// intervalErrs is the same idea along the other axis, and it exists because
	// -interval takes a list too. One symbol at three intervals where the
	// middle one fails is a case streamErrs cannot express — it would fail all
	// three — and it is the case that tells a failure line naming the symbol
	// alone from one naming the pair.
	intervalErrs map[binancedata.Interval]error

	// available and availableErr are what Available returns.
	available    binancedata.Availability
	availableErr error

	// entries and verifyErr are what VerifyCache yields.
	entries   []binancedata.CacheEntry
	verifyErr error

	// usage and usageErr are what CacheUsage returns.
	usage    binancedata.CacheUsage
	usageErr error

	// results and pruneErr are what PruneArchives yields.
	results  []binancedata.PruneResult
	pruneErr error

	// gotPrune records the options PruneArchives was called with, which is the
	// only way a test can tell a -n that reached the library from one that was
	// parsed and dropped.
	gotPrune binancedata.PruneOptions

	// evicted and evictErr are what EvictCache yields, and gotEvict records
	// what it was asked for. The options matter more here than anywhere else in
	// this fake: `bmd evict` exists to translate flags into a selection, so a
	// -before that was parsed and quietly dropped is the whole failure mode,
	// and nothing about the files that came back would reveal it.
	evicted  []binancedata.EvictResult
	evictErr error
	gotEvict binancedata.EvictOptions

	// gotRequest records what Stream was asked for, which is what most of the
	// download tests are actually about: the flags became this request.
	//
	// gotRequests keeps all of them, in call order, for the multi-symbol tests.
	// gotRequest alone cannot serve those: it holds the last call, so a command
	// that downloaded one symbol and a command that downloaded four ending with
	// that symbol look identical through it.
	gotRequest  binancedata.Request
	gotRequests []binancedata.Request
	gotQuery    binancedata.AvailabilityQuery

	// loaders counts how many times newLoader was called. It is the assertion
	// behind the whole reason `bmd download` takes several symbols: the
	// rate limiter is process-wide, so several symbols must share one loader in
	// one process. A command that built one loader per symbol would pass every
	// test about files and candles.
	loaders int

	// gotOptions records what newLoader was handed. An Option is an opaque
	// interface, so there is nothing to read out of one — but counting them is
	// enough to tell a flag that produced an option from a flag that was
	// quietly dropped, which is the failure commonFlags.options exists to
	// prevent.
	gotOptions []binancedata.Option
}

func (f *fakeLoader) Stream(_ context.Context, req binancedata.Request) iter.Seq2[binancedata.Kline, error] {
	f.gotRequest = req
	f.gotRequests = append(f.gotRequests, req)

	return func(yield func(binancedata.Kline, error) bool) {
		// A per-symbol failure is yielded before the candles rather than after
		// them, so the test sees a symbol that produced nothing. A download that
		// fails mid-range is the streamErr case below, and both reach the
		// command the same way.
		if err, ok := f.streamErrs[req.Symbol]; ok {
			yield(binancedata.Kline{}, err)

			return
		}

		if err, ok := f.intervalErrs[req.Interval]; ok {
			yield(binancedata.Kline{}, err)

			return
		}

		for _, k := range f.klines {
			if !yield(k, nil) {
				return
			}
		}

		if f.streamErr != nil {
			yield(binancedata.Kline{}, f.streamErr)
		}
	}
}

func (f *fakeLoader) Available(
	_ context.Context, q binancedata.AvailabilityQuery,
) (binancedata.Availability, error) {
	f.gotQuery = q

	return f.available, f.availableErr
}

func (f *fakeLoader) VerifyCache(_ context.Context) iter.Seq2[binancedata.CacheEntry, error] {
	return func(yield func(binancedata.CacheEntry, error) bool) {
		for _, e := range f.entries {
			if !yield(e, nil) {
				return
			}
		}

		if f.verifyErr != nil {
			yield(binancedata.CacheEntry{}, f.verifyErr)
		}
	}
}

func (f *fakeLoader) CacheUsage(_ context.Context) (binancedata.CacheUsage, error) {
	return f.usage, f.usageErr
}

func (f *fakeLoader) PruneArchives(
	_ context.Context, opts binancedata.PruneOptions,
) iter.Seq2[binancedata.PruneResult, error] {
	f.gotPrune = opts

	return func(yield func(binancedata.PruneResult, error) bool) {
		for _, r := range f.results {
			if !yield(r, nil) {
				return
			}
		}

		if f.pruneErr != nil {
			yield(binancedata.PruneResult{}, f.pruneErr)
		}
	}
}

func (f *fakeLoader) EvictCache(
	_ context.Context, opts binancedata.EvictOptions,
) iter.Seq2[binancedata.EvictResult, error] {
	f.gotEvict = opts

	return func(yield func(binancedata.EvictResult, error) bool) {
		for _, e := range f.evicted {
			if !yield(e, nil) {
				return
			}
		}

		if f.evictErr != nil {
			yield(binancedata.EvictResult{}, f.evictErr)
		}
	}
}

// install makes newLoader hand out this fake for the duration of one test.
//
// t.Cleanup restores the original, so tests that run in parallel with tests
// that do not touch newLoader stay independent — but two tests that both
// install a fake must not run in parallel with each other, which is why none of
// the command tests call t.Parallel.
func (f *fakeLoader) install(t *testing.T) {
	t.Helper()

	original := newLoader

	newLoader = func(opts ...binancedata.Option) (loader, error) {
		f.gotOptions = opts
		f.loaders++

		return f, nil
	}

	t.Cleanup(func() { newLoader = original })
}

// testKlines builds n hourly candles starting at 2024-01-15T00:00Z.
//
// The values are deliberately awkward. The quote volume is the widest number
// ever measured in real Binance data — twenty significant digits, from
// docs/numbers.md — because the whole reason this project carries a decimal
// type is that such a value does not survive a float64, and an encoder that
// quietly went through one would produce output that looks right.
func testKlines(t *testing.T, n int) []binancedata.Kline {
	t.Helper()

	dec := func(s string) udecimal.Decimal {
		t.Helper()

		d, err := udecimal.Parse(s)
		if err != nil {
			t.Fatalf("udecimal.Parse(%q): %v", s, err)
		}

		return d
	}

	out := make([]binancedata.Kline, 0, n)

	for i := range n {
		open := time.Date(2024, time.January, 15, i, 0, 0, 0, time.UTC)

		out = append(out, binancedata.Kline{
			OpenTime:            open,
			CloseTime:           open.Add(time.Hour - time.Millisecond),
			Open:                dec("42150.01"),
			High:                dec("42380.55"),
			Low:                 dec("42090.10"),
			Close:               dec("42311.99"),
			Volume:              dec("1234.56789012"),
			QuoteVolume:         dec("118661604939.99255335"),
			TakerBuyBaseVolume:  dec("600.12345678"),
			TakerBuyQuoteVolume: dec("25389411.87654321"),
			Trades:              int64(98765 + i),
		})
	}

	return out
}

// mustDate parses a YYYY-MM-DD date for a test, or fails it.
func mustDate(t *testing.T, s string) time.Time {
	t.Helper()

	parsed, err := time.Parse(dateLayout, s)
	if err != nil {
		t.Fatalf("time.Parse(%q): %v", s, err)
	}

	return parsed.UTC()
}
