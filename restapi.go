package binancedata

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// This file is the REST half of the seam download.go opens: it turns a range
// the archives cannot serve into candles, using the transport in
// internal/vision and the row decoder in codec.go.
//
// # Why the tail needs a second source at all
//
// Binance publishes the archives roughly a day behind real time — verified on
// 2026-08-17, when the 2026-08-16 daily archive existed and the 2026-08-17 one
// did not. A request ending today therefore has a tail that has never been
// written to a file. internal/plan already accounts for it: [plan.Expand] emits
// a KindRESTRange chunk for whatever falls past the archive frontier, and
// plan.Substitute ends its fallback ladder here, because 3d, 1w and 1mo have no
// daily archives and a hole in their monthly ones — the real, missing BTCUSDT
// 1mo 2024-03 — has nowhere else to go.
//
// # Why the rows go through codec.go's decoder
//
// The REST response carries the same twelve columns in the same order as the
// CSV inside an archive, verified against the live endpoint on 2026-08-20. So
// [decodeRow] parses both, and [checkTimes] and [checkValues] police both. That
// is not code golf: those checks are the ones that catch a column shifted by
// one, and a second implementation of them would be a second implementation to
// keep in step. The sidecar parser's own doc comment makes the same argument —
// two parsers for one format is one parser that gets fixed and one that does
// not.
//
// Errors from the decoder wrap [ErrCorruptArchive], which reads oddly for a
// JSON body. It is deliberate: the condition a caller branches on is "Binance
// sent bytes this library cannot understand", and that is one condition however
// the bytes were packaged. A sentinel of its own would split it in two and make
// every caller handle both. The same reasoning reaches one layer further down —
// a body that is not JSON at all fails inside internal/vision, which has its
// own vision.ErrMalformedResponse for it, and translateVisionError folds that
// onto this sentinel rather than letting where the failure was noticed decide
// what the caller is told.

// restPageSize is how many candles to ask for per call.
//
// The endpoint's documented maximum, and the right choice for the same reason
// a bulk archive beats a day of API calls: a page costs one round trip and two
// units of quota regardless of how full it is, so a half-empty page is quota
// spent on nothing. A two-day tail of 1m candles is three pages at this size
// and six at the endpoint's default of 500.
const restPageSize = vision.MaxKlinesLimit

// maxRESTPages bounds the pagination loop.
//
// The loop already cannot spin: the cursor is derived from the last candle of
// each page and therefore advances strictly, so it reaches the end of the range
// or runs out of candles. This is the second lock on that door, sized to the
// widest range that legitimately reaches this code — a whole missing daily
// archive of 1s candles is 86,400 candles, or 87 pages, and the tail past the
// frontier is a day or two of the same. A thousand pages is an order of
// magnitude beyond either, and still a number rather than a hope.
const maxRESTPages = 1000

// restWeightWarnThreshold is the used-weight reading past which a fetch says so
// out loud, in weight units spent by this IP in the current minute.
//
// Four fifths of [vision.WeightLimitPerMinute]. The number is chosen against
// what this library alone can produce rather than as a round fraction: the
// default pace is vision.DefaultWeightPerSecond, so a minute of this loader
// running flat out accounts for 2400 of the 6000, and even a loader configured
// to the full quota settles at 6000 only while saturated. A reading of 4800
// therefore means either a burst worth knowing about or — far more likely, and
// the case no amount of local accounting can detect — something else on this
// address spending the same budget.
//
// Warning rather than acting on it is deliberate. The pipeline's response to
// actual pressure is the 429 handling in loader.go, which has the server's own
// word for it; this is the diagnostic that explains why that fired, or that it
// is about to.
const restWeightWarnThreshold = vision.WeightLimitPerMinute * 4 / 5

// restRef names one range to fetch from the REST API.
//
// A struct rather than five parameters, for the reason [archiveRef] is one:
// these values travel together, and two strings and two enums in a positional
// call is a swap waiting to happen.
type restRef struct {
	Market   Market
	Symbol   string // already normalised: "BTCUSDT", never "BTC/USDT"
	Interval Interval
	Start    time.Time // inclusive, UTC
	End      time.Time // exclusive, UTC
}

// String implements fmt.Stringer so a ref reads as itself in a test failure.
func (r restRef) String() string {
	return fmt.Sprintf("%s %s [%s,%s)", r.Symbol, r.Interval,
		r.Start.UTC().Format(time.RFC3339), r.End.UTC().Format(time.RFC3339))
}

// validate checks every field this package interpolates into a query, before a
// request is spent discovering the problem.
//
// It mirrors [archivePrefix], and for the same reason: each of these values
// formats into something that looks like a legal query parameter, so a bad one
// produces a plausible request that fails somewhere unhelpful. An invalid
// Interval formats as "Interval(0)", which the endpoint rejects with its own
// error code — a caller's bug reported as though Binance had refused.
func (r restRef) validate() error {
	// The endpoint is spot-only. Futures live behind a different host and a
	// different path, so accepting a futures Market here would silently return
	// spot candles for it — the one failure mode worse than an error, since
	// the numbers look entirely reasonable. This is the third of the three
	// extension points docs/architecture.md requires be kept open.
	if r.Market != MarketSpot {
		return fmt.Errorf("market %s: the REST endpoint serves spot only: %w", r.Market, ErrInvalidRequest)
	}

	if !r.Interval.IsValid() {
		return fmt.Errorf("interval %s: %w", r.Interval, ErrInvalidRequest)
	}

	if err := checkNormalizedSymbol(r.Symbol); err != nil {
		return err
	}

	return nil
}

// restFetcher reads ranges of candles from the REST API.
type restFetcher struct {
	// api is the transport. It carries the shared http.Client and the retry
	// policy, and its policy carries the rate limiter — which nothing else in
	// this library has, for the reason internal/vision/limiter.go opens with.
	api *vision.API

	// includePartial selects whether the candle currently being formed is
	// returned.
	//
	// # Why the default is to drop it
	//
	// The endpoint always returns the interval in progress. Verified on
	// 2026-08-20: a bare limit=2 call came back with a bar whose interval had
	// not closed, and whose volume, close price and trade count were still
	// moving. The archives never contain one.
	//
	// Everything downstream assumes a candle is settled once seen. The Parquet
	// tier stamps a file with the hash of the bytes it was built from and
	// serves it until that changes; Stage 7 merges overlapping ranges by open
	// time. Both are correct only while a given open time means one fixed set
	// of numbers. Admitting a candle that is still moving breaks that quietly:
	// two identical requests seconds apart disagree, and neither is wrong.
	//
	// So the default is to stop at the last closed candle, which makes a
	// result immutable once returned. It is a field rather than a constant
	// because near-live use is a legitimate thing to want, and Stage 7 can
	// surface it as an option on NewLoader without this file changing.
	includePartial bool

	// logger is where the used-weight diagnostic goes. It is never nil —
	// loader.go passes the configured one, which defaults to a discarding
	// handler — so there is no nil check at the call site, for the reason
	// defaultLoaderConfig gives for defaulting it that way.
	logger *slog.Logger
}

// klines fetches every candle in ref, paginating until the range is covered.
//
// now is the clock, passed in rather than read, as everywhere else in this
// package. Here it decides exactly one thing — whether the newest candle has
// closed — and passing it makes that decision testable without a test having to
// run at a particular second.
//
// # How the pagination terminates
//
// Three independent conditions, because one of them alone would be a bug
// waiting for the right response to arrive:
//
//	an empty page          Binance has nothing more in this range
//	a short page           fewer rows than asked for means the range ran out
//	the cursor reaches End the range is covered
//
// The cursor is taken from the last candle of the page and advanced to the
// instant its successor opens — [intervalEnd], not "one millisecond later".
//
// # Why not one millisecond later
//
// That is what this did first, and it is one millisecond *into* the candle it
// is trying to move past, which is only safe if Binance's inclusive startTime
// filters strictly on open time. If it instead selects the kline whose interval
// *contains* the timestamp — the reading a mid-candle startTime makes plausible
// — page 2 opens with the same candle page 1 ended with, and appendPage's
// strict-increase check fails the whole fetch. Loudly, at least, and on the
// first multi-page range anyone asks for.
//
// One handler cannot settle it, because a handler written in this repository
// necessarily encodes whichever reading its author assumed. So restapi_test.go
// runs the same range against one of each.
//
// Landing exactly on the next open is correct under both readings, needs no
// measurement to justify, and skips nothing — a candle opening at that instant
// is the next one wanted, and if Binance has no candle there (an illiquid pair,
// a gap) the page simply starts at the first one after it. The Python loader
// this replaces advanced by close_time+1, which is the same instant reached
// from the other side.
//
// Since every candle in a page opens at or after the cursor that requested it,
// the next cursor is strictly greater, so the loop cannot stand still.
// [maxRESTPages] guards the case where that reasoning is wrong.
func (f restFetcher) klines(ctx context.Context, ref restRef, now time.Time) ([]Kline, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}

	// The same spec the archive decoder uses, and it does the same job: every
	// candle is checked against the range it was supposed to come from, so a
	// page returned for the wrong symbol or a timestamp in the wrong unit is
	// caught here rather than becoming plausible-looking data.
	spec := decodeSpec{Interval: ref.Interval, Start: ref.Start, End: ref.End}
	if err := spec.validate(); err != nil {
		return nil, err
	}

	// The clock is checked for the same reason every other field is, and it is
	// the one whose absence is invisible. A zero now — a field forgotten in a
	// struct literal, a clock not yet wired — makes intervalEnd(...).After(now)
	// true for every candle ever published, so appendPage stops on row 1 of
	// page 1 and this returns ([], nil): a failed read wearing the shape of an
	// empty range, which is the conflation finding 1 of the Stage 3 review
	// exists to prevent and the whole reason ErrNotAvailable is a typed error.
	if now.IsZero() {
		return nil, fmt.Errorf("%s: the clock is required: %w", ref, ErrInvalidRequest)
	}

	out := make([]Kline, 0, spec.estimateRows())

	// The previous candle's open time. Candles must strictly increase, which
	// covers ordering and duplication at once — and here it does double duty,
	// since it is also what advances the cursor.
	var prev time.Time

	// warnedOnWeight keeps the quota warning to one per fetch. See where it is
	// set for why a per-page warning would be worse than none.
	var warnedOnWeight bool

	cursor := ref.Start

	for page := 1; cursor.Before(ref.End); page++ {
		if page > maxRESTPages {
			// Untyped, deliberately. This is a resource bound rather than a
			// diagnosis: nothing about the data is wrong, there is merely more
			// of it than one REST fetch is willing to page through.
			//
			// It was ErrCorruptArchive, which claims Binance published bytes
			// this library cannot understand and tells the caller that retrying
			// is pointless. Half of that is even true — a retry does hit the
			// same cap — but it would have a caller give up on data that is
			// perfectly fine and merely large, and Stage 7 is the layer that
			// could instead split the range and ask again. ErrInvalidRequest is
			// no better: errors.go promises it costs no network round trip, and
			// this one costs a thousand.
			return nil, fmt.Errorf("%s: still incomplete after %d pages of %d candles; "+
				"split the range and fetch it in parts", ref, maxRESTPages, restPageSize)
		}

		got, err := f.api.Klines(ctx, vision.KlineQuery{
			Symbol:   ref.Symbol,
			Interval: ref.Interval.RESTParam(),
			Start:    cursor,
			End:      ref.End,
			Limit:    restPageSize,
		})
		if err != nil {
			return nil, translateRESTError(err)
		}

		// What Binance says this IP has spent in the current minute, counting
		// this request. It is reported before the empty-page check on purpose:
		// a page with no candles in it still cost quota and still carries the
		// header, and a range that turns out to be empty is exactly when a
		// caller wants to know the budget went somewhere.
		//
		// Zero means the header was absent or unreadable rather than that
		// nothing has been spent — this request alone costs vision.KlinesWeight
		// — so it is not reported as a measurement.
		if got.UsedWeight > 0 {
			f.logger.DebugContext(ctx, "rest quota used",
				"ref", ref,
				"page", page,
				"used_weight", got.UsedWeight,
				"limit", vision.WeightLimitPerMinute)

			// Once per fetch, not once per page. A hundred-page range that
			// crosses the threshold on its first page crosses it on all
			// hundred, and a warning repeated ninety-nine times is a warning
			// nobody reads — the condition is a property of the minute, not of
			// the page that happened to observe it.
			if !warnedOnWeight && got.UsedWeight >= restWeightWarnThreshold {
				warnedOnWeight = true

				f.logger.WarnContext(ctx,
					"rest quota is running low; another process on this IP may be spending it",
					"ref", ref,
					"used_weight", got.UsedWeight,
					"limit", vision.WeightLimitPerMinute,
					"threshold", restWeightWarnThreshold)
			}
		}

		if len(got.Klines) == 0 {
			// Not an error. A range before the pair was listed, or a gap
			// Binance simply has no data for, is a fact rather than a failure
			// — the same reading decodeCSV gives an archive with no rows.
			break
		}

		settled, err := f.appendPage(&out, got.Klines, spec, &prev, now, page)
		if err != nil {
			return nil, err
		}

		if !settled || len(got.Klines) < restPageSize {
			break
		}

		cursor = intervalEnd(prev, ref.Interval)
	}

	return out, nil
}

// appendPage decodes one page onto out and reports whether every row in it was
// a closed candle.
//
// A false result stops the caller's loop: candles ascend, so once one has not
// closed, neither has anything after it, and there is nothing further to ask
// for.
func (f restFetcher) appendPage(
	out *[]Kline,
	rows []vision.RawKline,
	spec decodeSpec,
	prev *time.Time,
	now time.Time,
	page int,
) (bool, error) {
	for i := range rows {
		// rows[i] is [vision.KlineFields]string and decodeRow indexes a
		// []string by the column constants in codec.go. The two counts are
		// checked against each other once, at compile time, so this slice
		// expression cannot be short — see the assertion at the bottom of this
		// file.
		//
		// Indexed rather than `for i, row := range rows`, which binds a copy of
		// the element: a RawKline is twelve string headers, 192 bytes on a
		// 64-bit build, and slicing the copy is also what forces it to be
		// addressable. Slicing rows[i] borrows the row where it already is. A
		// day of 1s candles is 86,400 of them, and the shorter expression is
		// the cheaper one.
		k, err := decodeRow(rows[i][:], spec)
		if err != nil {
			return false, fmt.Errorf("page %d row %d: %w", page, i+1, err)
		}

		if !prev.IsZero() && !k.OpenTime.After(*prev) {
			return false, fmt.Errorf("page %d row %d: open time %s does not follow the previous candle's %s: %w",
				page, i+1, k.OpenTime.Format(time.RFC3339), prev.Format(time.RFC3339), ErrCorruptArchive)
		}

		*prev = k.OpenTime

		// intervalEnd is the instant the candle's successor opens, so a candle
		// has closed once that instant has passed. Comparing against it rather
		// than against the candle's own CloseTime is what keeps this correct
		// across the 2025 millisecond-to-microsecond change, where CloseTime
		// moved by a unit and the interval boundary did not.
		if !f.includePartial && intervalEnd(k.OpenTime, spec.Interval).After(now) {
			return false, nil
		}

		*out = append(*out, k)
	}

	return true, nil
}

// The REST rows and the CSV rows are decoded by the same function, which means
// their column counts have to agree. They are declared in two packages that
// cannot import each other's constants for the layering reasons in
// docs/architecture.md, so the agreement is asserted instead of assumed.
//
// The check costs nothing and runs at compile time. Indexing a one-element
// array is legal only for the constant index 0, so this line builds precisely
// when the two counts are equal and fails in either direction when they are
// not — a broken build the day someone edits one constant, rather than a panic
// inside decodeRow the day someone runs it.
var _ = [1]struct{}{}[vision.KlineFields-csvFields]
