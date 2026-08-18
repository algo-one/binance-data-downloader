package binancedata

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/quagmt/udecimal"
)

// This file is the codec: the one place where the format Binance publishes
// meets the types this package defines. Everything upstream of it deals in URLs
// and bytes and knows nothing about candles; everything downstream deals in
// [Kline] values and knows nothing about CSV.
//
// # What is actually in an archive
//
// A bulk archive is a ZIP holding exactly one CSV member, named after the
// archive itself — BTCUSDT-1h-2024-01-15.zip contains BTCUSDT-1h-2024-01-15.csv.
// Spot files carry no header row, and every data row has twelve fields:
//
//	1705276800000,41732.35000000,42353.94000000,41718.05000000,42279.75000000,
//	2433.52283000,1705280399999,102456168.22559890,66725,1348.93805000,
//	56779695.64663060,0
//
// Reading that into a struct is the easy part. Three things are not, and each
// is a bug this library exists to not repeat:
//
//   - Whether a header row is present is *sniffed*, never assumed. Spot has
//     none and futures has one, and a wrong guess either eats a real candle or
//     parses the word "open_time" as a price.
//   - The timestamp unit is decided *per row*. Binance switched these files
//     from milliseconds to microseconds at 2025-01-01T00:00Z — verified here
//     against the real archives for 2024-12-31 and 2025-01-01, which are the
//     last and first days of each. The ported implementation sniffed the unit
//     from the final row of a file and applied it to all of them.
//   - Every number is parsed as an exact decimal. This is the hot path that
//     motivates the whole two-tier cache: eight [udecimal.Decimal] fields times
//     44,640 rows in a month of one-minute candles.
//
// # Rows are yielded, not returned
//
// The entry points below hand back an [iter.Seq2] — a range-over-function
// iterator, Go's equivalent of a Python generator — rather than a []Kline. That
// is a memory decision. A month of 1s candles is about 2.6 million rows, and a
// [Kline] is 312 bytes, so materialising one archive would cost 810 MB. Stage 5
// writes the cache row by row from this iterator and never holds an archive
// whole; [collectKlines] is there for the callers that genuinely want a slice.
const (
	// CodecVersion identifies the decoding rules implemented in this file. It
	// is stamped into every derived cache file and compared on read.
	//
	// It exists because "same ZIP implies same output" is only true while the
	// *conversion* is unchanged. Fix a parsing bug — the millisecond handling
	// above is the obvious candidate — and every cache entry built by the old
	// code is now wrong while its source archive is byte-for-byte identical.
	// No checksum can detect that, because nothing about the source changed.
	//
	// So: bump this constant in the same commit as any change to what this
	// file produces from given bytes, and every stale cache entry rebuilds
	// itself on next read, offline. Leaving it alone after such a change means
	// the corrected parser never reaches data that is already cached.
	//
	// Being a compile-time constant is load-bearing. Two runs of one binary
	// cannot disagree about it, so a cache entry either matches the running
	// code or does not, with no third state to reason about.
	CodecVersion = 1

	// maxRowsHint caps the capacity [collectKlines] preallocates. A 1s month
	// would otherwise ask for 2.6 million slots — 810 MB reserved before a
	// single row is read, on the strength of arithmetic rather than data.
	maxRowsHint = 1 << 16

	// ctxCheckStride is how often decoding stops to ask whether the caller
	// still wants the answer. Rows are numbered from 1 and the test is
	// row%stride == 1, so the first row is checked too: a context already
	// cancelled costs nothing and is caught immediately rather than after a
	// thousand rows of pointless work.
	//
	// Checking every row would be correct and slightly wasteful — ctx.Err() is
	// an atomic load, which at 1.4 microseconds a row is not free but is not
	// nothing either. Once per thousand rows bounds the wasted work after a
	// cancellation at about a millisecond, which is far below the granularity
	// anybody can observe.
	ctxCheckStride = 1024
)

// The twelve CSV columns, in the order Binance writes them.
//
// iota numbers these 0, 1, 2 ... automatically: it resets at each const block
// and increments per line, so the list only has to be in the right order. The
// payoff is the last entry — csvFields lands on 12 without anybody counting,
// and stays correct if a column is ever inserted above it.
//
// Named constants rather than bare numbers because rec[7] is a claim the reader
// has to verify against a table somewhere, while rec[colQuoteVolume] is one
// they can check on the spot. An off-by-one here would swap two decimal columns
// that both parse cleanly — a silent, plausible, wrong answer.
const (
	colOpenTime = iota
	colOpen
	colHigh
	colLow
	colClose
	colVolume
	colCloseTime
	colQuoteVolume
	colTrades
	colTakerBuyBaseVolume
	colTakerBuyQuoteVolume
	colIgnore // Binance's own name for it; the value is always 0.

	csvFields // must stay last: the count of the columns above.
)

// microsecondCutoff separates the two timestamp units, and it works because
// there is no contest.
//
// A timestamp read as milliseconds crosses 1e14 in the year 5138. Read as
// microseconds it crosses the same value in March 1973. Binance began trading
// in 2017, so every real value is at least three orders of magnitude away from
// the boundary on whichever side it belongs — 2024 is ~1.7e12 in milliseconds
// and ~1.7e15 in microseconds. This is a threshold that cannot be approached by
// data, only by a corrupt field, which is why it is safe to decide per row.
const microsecondCutoff = 1e14

// grid3d anchors the three-day candle grid.
//
// Intervals up to 1d divide a day evenly and therefore line up with the Unix
// epoch, which is midnight UTC. The three longer ones do not, and each is
// anchored differently:
//
//   - 1mo opens on the 1st of the month.
//   - 1w opens on a Monday. The epoch was a Thursday, so a "multiple of seven
//     days since the epoch" test would reject every real weekly candle.
//   - 3d sits on a three-day grid anchored one day *after* the epoch, i.e. on
//     1970-01-02. That is not derivable from anything; it was measured against
//     the live archives for 2018-01, 2021-06, 2024-03, 2024-04, 2024-05 and
//     2025-02, every one of which lands on this grid. The grid does not restart
//     at month boundaries: March 2024's last candle opens on the 31st and
//     April's first opens on the 3rd.
var grid3d = time.Date(1970, time.January, 2, 0, 0, 0, 0, time.UTC)

// decodeSpec is what the caller believes the bytes contain: which interval, and
// which half-open span of time the file covers.
//
// Passing this in is what lets the decoder check its own work. A microsecond
// timestamp misread as milliseconds lands in 1970 and a millisecond one misread
// as microseconds lands in 55000 AD, and both are caught immediately by asking
// whether the candle falls inside the day or month the file claims to be. The
// alternative — decode whatever is there and hope — produces candles at the
// wrong instants that look entirely reasonable in isolation.
//
// The caller always knows these facts: a chunk from internal/plan carries its
// own period, and the archive name was built from it.
type decodeSpec struct {
	// Interval is the candle aggregation the file holds.
	Interval Interval

	// Start and End are the period the file covers, half-open as everywhere
	// else in this package: Start included, End excluded. For a daily archive
	// that is one day; for a monthly one, one calendar month.
	Start time.Time
	End   time.Time
}

// validate rejects a spec the decoder could not check anything against. It runs
// once per file rather than per row, so it costs nothing worth measuring.
func (s decodeSpec) validate() error {
	if !s.Interval.IsValid() {
		return fmt.Errorf("decode: interval: %w", ErrInvalidRequest)
	}

	if s.Start.IsZero() || s.End.IsZero() {
		return fmt.Errorf("decode: period: both start and end are required: %w", ErrInvalidRequest)
	}

	if !s.Start.Before(s.End) {
		return fmt.Errorf("decode: period: start %s is not before end %s: %w",
			s.Start.Format(time.RFC3339), s.End.Format(time.RFC3339), ErrInvalidRequest)
	}

	return nil
}

// estimateRows guesses how many candles the period holds, for use as a slice
// capacity. It is a hint and nothing more: archives are routinely shorter than
// their period — SHIBUSDT's 2021-05 daily archive holds 22 rows for a 31-day
// month because the pair was listed on the 10th — so nothing may depend on it.
func (s decodeSpec) estimateRows() int {
	d, ok := s.Interval.Duration()
	if !ok {
		// 1mo, the one interval with no fixed duration. A monthly archive of
		// monthly candles holds exactly one row.
		return 1
	}

	n := int(s.End.Sub(s.Start) / d)

	return min(max(n, 1), maxRowsHint)
}

// decodeArchive yields every candle in one bulk archive.
//
// r and size are the ZIP: [archive/zip] needs random access, so it takes an
// io.ReaderAt plus a length rather than a stream. Both an *os.File and a
// bytes.Reader satisfy that, which is exactly the two callers this has — the
// cache reads from disk, the downloader from memory.
//
// ctx comes first, as it does everywhere in this package that touches a file or
// the network. A consumer ranging over the iterator can already stop it by
// breaking out of the loop, but that is no help to [decodeArchiveAll], whose
// loop the caller does not control: a 1s month is 2.6 million rows and several
// seconds of work, and a cancelled backtest should not have to wait for it.
//
// Errors arrive through the iterator: a failure yields the zero [Kline] with a
// non-nil error and then stops, so a caller that checks err on every step
// cannot miss one. Every error here wraps [ErrCorruptArchive], because by the
// time bytes reach this function they have already passed transport and
// checksum checks — whatever is wrong with them is in the content.
func decodeArchive(ctx context.Context, r io.ReaderAt, size int64, spec decodeSpec) iter.Seq2[Kline, error] {
	// The returned closure is the iterator. Nothing inside it runs until a
	// caller ranges over the result, which is why opening the ZIP happens here
	// rather than in decodeArchive itself: a for/range statement is where the
	// work belongs, and it is also where the deferred Close below is reached.
	return func(yield func(Kline, error) bool) {
		zr, err := zip.NewReader(r, size)
		if err != nil {
			// Two %w verbs in one Errorf, which Go allows since 1.20. The
			// chain then answers errors.Is for both the sentinel and whatever
			// archive/zip reported, so a caller can branch on ErrCorruptArchive
			// while a human still reads the underlying cause.
			yield(Kline{}, fmt.Errorf("archive: %w: %w", err, ErrCorruptArchive))

			return
		}

		member, err := csvMember(zr)
		if err != nil {
			yield(Kline{}, err)

			return
		}

		rc, err := member.Open()
		if err != nil {
			yield(Kline{}, fmt.Errorf("archive member %s: %w: %w", member.Name, err, ErrCorruptArchive))

			return
		}

		// Closed on every exit path, including the one where the consumer
		// abandons the loop with break — Go's range-over-func machinery
		// returns from this closure in that case, so the defer fires.
		defer func() { _ = rc.Close() }()

		for k, err := range decodeCSV(ctx, rc, spec) {
			if !yield(k, err) {
				return
			}

			// The inner iterator stops after an error, and so must this one:
			// forwarding the error but continuing would offer the caller a
			// stream that resumes after a failure it cannot see the end of.
			if err != nil {
				return
			}
		}
	}
}

// csvMember finds the single CSV file inside an archive.
//
// Insisting on exactly one is deliberate. Binance publishes one member per
// archive today; a second one appearing would mean the format changed, and
// picking the first match would then quietly decode half the data. An archive
// this function cannot make sense of is a loud error instead.
func csvMember(zr *zip.Reader) (*zip.File, error) {
	var found *zip.File

	for _, f := range zr.File {
		if f.FileInfo().IsDir() || !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			continue
		}

		if found != nil {
			return nil, fmt.Errorf("archive holds more than one CSV member (%s, %s): %w",
				found.Name, f.Name, ErrCorruptArchive)
		}

		found = f
	}

	if found == nil {
		return nil, fmt.Errorf("archive holds no CSV member: %w", ErrCorruptArchive)
	}

	return found, nil
}

// decodeCSV yields every candle in the CSV body of an archive.
//
// It is separate from [decodeArchive] because the two failures are different in
// kind — a ZIP that will not open versus a row that will not parse — and
// because tests need to feed it CSV text that no real archive contains, such as
// a file that changes timestamp unit halfway through.
func decodeCSV(ctx context.Context, r io.Reader, spec decodeSpec) iter.Seq2[Kline, error] {
	return func(yield func(Kline, error) bool) {
		if err := spec.validate(); err != nil {
			yield(Kline{}, err)

			return
		}

		cr := csv.NewReader(r)

		// Declaring the field count makes encoding/csv enforce it, so a
		// truncated or extended row is rejected by the reader with the line
		// number attached, rather than by an index expression panicking three
		// frames deeper.
		cr.FieldsPerRecord = csvFields

		// The reader may hand back the same backing array on every call. That
		// is safe here — and worth about one allocation per row, which over a
		// 1s month is 2.6 million of them — precisely because nothing below
		// retains a string from the record: strconv and udecimal both copy
		// what they need into values.
		cr.ReuseRecord = true

		// The open time of the previous row, or the zero time before there is
		// one. Candles must strictly increase, which subsumes both ordering
		// and duplication: an archive that repeats a candle or goes backwards
		// is not something to quietly sort out, it is something wrong.
		var prev time.Time

		for row := 1; ; row++ {
			if row%ctxCheckStride == 1 {
				if err := ctx.Err(); err != nil {
					// Not wrapped in ErrCorruptArchive: nothing is wrong with
					// the data. The caller stopped asking, and context.Canceled
					// or context.DeadlineExceeded is what they test for.
					yield(Kline{}, fmt.Errorf("decoding row %d: %w", row, err))

					return
				}
			}

			rec, err := cr.Read()
			if errors.Is(err, io.EOF) {
				// A file with no rows is not an error. Nothing forbids Binance
				// from publishing an empty period, and the coverage checks
				// upstream are what notice missing data.
				return
			}

			if err != nil {
				yield(Kline{}, fmt.Errorf("row %d: %w: %w", row, err, ErrCorruptArchive))

				return
			}

			if row == 1 {
				// A byte-order mark would otherwise ride along on the first
				// field and make a perfectly good data row look like a header,
				// costing exactly one candle — the kind of loss nobody notices.
				rec[colOpenTime] = strings.TrimPrefix(rec[colOpenTime], "\ufeff")

				if isHeader(rec) {
					continue
				}
			}

			k, err := decodeRow(rec, spec)
			if err != nil {
				yield(Kline{}, fmt.Errorf("row %d: %w", row, err))

				return
			}

			if !prev.IsZero() && !k.OpenTime.After(prev) {
				yield(Kline{}, fmt.Errorf("row %d: open time %s does not follow the previous row's %s: %w",
					row, k.OpenTime.Format(time.RFC3339), prev.Format(time.RFC3339), ErrCorruptArchive))

				return
			}

			prev = k.OpenTime

			// yield reports false when the consumer is done — a break in their
			// range loop, or an early return. Honouring it is what makes
			// "read the first ten candles of a 2.6-million-row archive" cost
			// ten rows of work.
			if !yield(k, nil) {
				return
			}
		}
	}
}

// isHeader reports whether a record is a column-name row rather than data.
//
// Sniffing beats hardcoding because the answer differs by market and could
// change again: spot archives carry no header, futures archives do, and the
// implementation this replaces simply asserted the former. A wrong guess costs
// either a real candle or a row of column names parsed as prices.
//
// Two fields are examined rather than one, and the second is what keeps the
// test honest. A data row opens with an integer timestamp, so a first field
// that is not one suggests a header — but it equally well describes a data row
// whose timestamp is corrupt, and treating that as a header would drop it in
// silence, which is the exact class of failure this package is built to avoid.
// A genuine header has names in every column, so the price field settles it.
//
// What remains ambiguous is a first row where nothing parses at all. That is
// read as a header, because a file of column names with no data is a thing
// Binance could plausibly publish and a row with twelve simultaneously corrupt
// fields is not.
func isHeader(rec []string) bool {
	if _, err := strconv.ParseInt(rec[colOpenTime], 10, 64); err == nil {
		return false
	}

	_, err := udecimal.Parse(rec[colOpen])

	return err != nil
}

// decodeRow turns one CSV record into a candle, and refuses to return one that
// contradicts the file it came from.
//
// The eight decimal parses are written out rather than looped over a table of
// field pointers. This is the hot path — eight parses times 2.6 million rows
// for a 1s month — and the flat form gives the compiler struct fields it can
// fill directly, where a loop over pointers would force the value to escape.
func decodeRow(rec []string, spec decodeSpec) (Kline, error) {
	openTime, err := parseTimestamp("open time", rec[colOpenTime])
	if err != nil {
		return Kline{}, err
	}

	closeTime, err := parseTimestamp("close time", rec[colCloseTime])
	if err != nil {
		return Kline{}, err
	}

	open, err := parseDecimal("open", rec[colOpen])
	if err != nil {
		return Kline{}, err
	}

	high, err := parseDecimal("high", rec[colHigh])
	if err != nil {
		return Kline{}, err
	}

	low, err := parseDecimal("low", rec[colLow])
	if err != nil {
		return Kline{}, err
	}

	closePrice, err := parseDecimal("close", rec[colClose])
	if err != nil {
		return Kline{}, err
	}

	volume, err := parseDecimal("volume", rec[colVolume])
	if err != nil {
		return Kline{}, err
	}

	quoteVolume, err := parseDecimal("quote volume", rec[colQuoteVolume])
	if err != nil {
		return Kline{}, err
	}

	takerBase, err := parseDecimal("taker buy base volume", rec[colTakerBuyBaseVolume])
	if err != nil {
		return Kline{}, err
	}

	takerQuote, err := parseDecimal("taker buy quote volume", rec[colTakerBuyQuoteVolume])
	if err != nil {
		return Kline{}, err
	}

	trades, err := strconv.ParseInt(rec[colTrades], 10, 64)
	if err != nil {
		return Kline{}, fmt.Errorf("trades %q: %w: %w", rec[colTrades], err, ErrCorruptArchive)
	}

	if trades < 0 {
		return Kline{}, fmt.Errorf("trades %d: negative: %w", trades, ErrCorruptArchive)
	}

	k := Kline{
		OpenTime:            openTime,
		CloseTime:           closeTime,
		Open:                open,
		High:                high,
		Low:                 low,
		Close:               closePrice,
		Volume:              volume,
		QuoteVolume:         quoteVolume,
		TakerBuyBaseVolume:  takerBase,
		TakerBuyQuoteVolume: takerQuote,
		Trades:              trades,
	}

	if err := checkTimes(k, spec); err != nil {
		return Kline{}, err
	}

	if err := checkValues(k); err != nil {
		return Kline{}, err
	}

	return k, nil
}

// checkTimes verifies a candle against the file it was found in: inside the
// period, on the interval's grid, and closing before the next candle would open.
//
// The period test is what makes a unit misdetection loud. It is deliberately
// one-directional: every candle must be inside the period, but the period need
// not be full of candles. Archives are legitimately short — a pair listed
// mid-month has no candles before its listing day — and demanding completeness
// here would reject real data. Whether a *range* is fully covered is a question
// for the planner, which asks it about chunks rather than rows.
func checkTimes(k Kline, spec decodeSpec) error {
	if k.OpenTime.Before(spec.Start) || !k.OpenTime.Before(spec.End) {
		return fmt.Errorf("open time %s is outside the period [%s, %s): %w",
			k.OpenTime.Format(time.RFC3339), spec.Start.Format(time.RFC3339),
			spec.End.Format(time.RFC3339), ErrCorruptArchive)
	}

	if !aligned(k.OpenTime, spec.Interval) {
		return fmt.Errorf("open time %s is not aligned to the %s grid: %w",
			k.OpenTime.Format(time.RFC3339), spec.Interval, ErrCorruptArchive)
	}

	// Binance reports an inclusive close time, one unit before the next
	// candle's open — one millisecond before 2025, one microsecond after. This
	// bound is written to hold for both spellings, and to keep holding for a
	// third: what matters is that the candle closes strictly inside its own
	// interval.
	end := intervalEnd(k.OpenTime, spec.Interval)
	if !k.CloseTime.After(k.OpenTime) || !k.CloseTime.Before(end) {
		return fmt.Errorf("close time %s is outside its own interval (%s, %s): %w",
			k.CloseTime.Format(time.RFC3339), k.OpenTime.Format(time.RFC3339),
			end.Format(time.RFC3339), ErrCorruptArchive)
	}

	return nil
}

// checkValues verifies the eight decimal fields against the relationships that
// hold between them by definition.
//
// These cost a handful of comparisons per row and buy the one failure no amount
// of per-field validation would catch: a column index that is off by one.
// Swapping two decimal columns produces values that parse perfectly and are
// simply wrong. The relationships checked are the ones fixed by what the fields
// *mean* — a low is the lowest price, a taker buy is part of the total volume —
// so a shuffle cannot satisfy them.
//
// Every rule here was checked against 132,506 real rows spanning five symbols,
// six intervals and both timestamp eras before being relied on. That matters,
// because a false positive rejects a whole archive: these are only worth
// enforcing while they are true of the data as published, not merely true in
// principle.
func checkValues(k Kline) error {
	if k.Low.GreaterThan(k.High) ||
		k.Low.GreaterThan(k.Open) || k.Low.GreaterThan(k.Close) ||
		k.High.LessThan(k.Open) || k.High.LessThan(k.Close) {
		return fmt.Errorf("prices are inconsistent (open %s, high %s, low %s, close %s): %w",
			k.Open, k.High, k.Low, k.Close, ErrCorruptArchive)
	}

	// One test covers all four prices: Low is the smallest of them by the rule
	// just checked, so nothing else can be negative while it is not.
	if k.Low.Sign() < 0 {
		return fmt.Errorf("negative price (low %s): %w", k.Low, ErrCorruptArchive)
	}

	if k.Volume.Sign() < 0 || k.QuoteVolume.Sign() < 0 ||
		k.TakerBuyBaseVolume.Sign() < 0 || k.TakerBuyQuoteVolume.Sign() < 0 {
		return fmt.Errorf("negative volume (volume %s, quote %s, taker base %s, taker quote %s): %w",
			k.Volume, k.QuoteVolume, k.TakerBuyBaseVolume, k.TakerBuyQuoteVolume, ErrCorruptArchive)
	}

	// The taker-buy figures are the portion of each total where the buyer
	// crossed the spread, so they cannot exceed the total they are a portion of.
	if k.TakerBuyBaseVolume.GreaterThan(k.Volume) {
		return fmt.Errorf("taker buy base volume %s exceeds volume %s: %w",
			k.TakerBuyBaseVolume, k.Volume, ErrCorruptArchive)
	}

	if k.TakerBuyQuoteVolume.GreaterThan(k.QuoteVolume) {
		return fmt.Errorf("taker buy quote volume %s exceeds quote volume %s: %w",
			k.TakerBuyQuoteVolume, k.QuoteVolume, ErrCorruptArchive)
	}

	return nil
}

// parseTimestamp reads one timestamp field, deciding its unit from its own
// magnitude. See [microsecondCutoff] for why that is unambiguous.
func parseTimestamp(field, s string) (time.Time, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q: %w: %w", field, s, err, ErrCorruptArchive)
	}

	if v <= 0 {
		return time.Time{}, fmt.Errorf("%s %d: not a positive timestamp: %w", field, v, ErrCorruptArchive)
	}

	// .UTC() is not decoration. time.UnixMilli returns a time in the local
	// zone, and this package requires UTC everywhere: it is what makes Request
	// safe as a map key and what keeps the calendar arithmetic in the planner
	// free of daylight-saving discontinuities.
	if v >= microsecondCutoff {
		return time.UnixMicro(v).UTC(), nil
	}

	return time.UnixMilli(v).UTC(), nil
}

// parseDecimal reads one price or volume field exactly.
//
// udecimal.Parse is exact or it errors; it never silently rounds, which is the
// property that disqualified two otherwise faster libraries. See docs/numbers.md.
func parseDecimal(field, s string) (udecimal.Decimal, error) {
	d, err := udecimal.Parse(s)
	if err != nil {
		return udecimal.Decimal{}, fmt.Errorf("%s %q: %w: %w", field, s, err, ErrCorruptArchive)
	}

	return d, nil
}

// aligned reports whether an open time sits on the grid its interval uses.
//
// Three intervals are special-cased before the general rule, each because its
// grid is anchored to something other than the Unix epoch. They are written as
// early returns rather than as a switch: a switch on iv invites the reader (and
// the linter) to expect all sixteen intervals to appear, when the point here is
// that thirteen of them share one rule.
func aligned(t time.Time, iv Interval) bool {
	if iv == Interval1mo {
		return t.Day() == 1 && isMidnightUTC(t)
	}

	if iv == Interval1w {
		return t.Weekday() == time.Monday && isMidnightUTC(t)
	}

	if iv == Interval3d {
		return isMidnightUTC(t) && t.Sub(grid3d)%(72*time.Hour) == 0
	}

	d, ok := iv.Duration()
	if !ok {
		// The zero Interval, and any future one without a fixed duration: no
		// grid is known, so nothing can be said to sit on it.
		return false
	}

	// Every remaining interval divides 24 hours evenly, and the Unix epoch is
	// itself midnight UTC, so a plain modulo against the epoch is the grid.
	//
	// Microseconds, not milliseconds. Since 2025 these files are written in
	// microseconds, so a millisecond-granularity test is coarser than the data
	// it is checking and waves through a timestamp that is off the grid by less
	// than 1 ms — which is precisely the corruption this check exists to catch.
	// Nanoseconds would be finer still and buy nothing, since no Binance file
	// has ever carried sub-microsecond precision. The largest interval reaching
	// this line is 1d, or 8.64e10 microseconds, so nothing here overflows.
	return t.UnixMicro()%d.Microseconds() == 0
}

// intervalEnd returns the instant the next candle opens.
//
// Adding [Interval.Duration] is wrong for exactly one interval: months are 28
// to 31 days long, so 1mo has no fixed duration and AddDate does the calendar
// arithmetic instead.
func intervalEnd(t time.Time, iv Interval) time.Time {
	if d, ok := iv.Duration(); ok {
		return t.Add(d)
	}

	return t.AddDate(0, 1, 0)
}

// isMidnightUTC reports whether an instant is exactly a UTC day boundary.
//
// Written as four field comparisons rather than t.Truncate(24 * time.Hour),
// which would also work today and for the wrong reason: Truncate rounds down to
// a multiple of d measured from the *zero time* — January 1 of year 1, not the
// Unix epoch — and it happens to agree with the calendar only because those two
// instants are a whole number of days apart. internal/plan/plan.go declines to
// use Truncate for exactly this reason and says so in writing; this file should
// not quietly contradict it.
func isMidnightUTC(t time.Time) bool {
	return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0
}

// collectKlines drains an iterator into a slice, stopping at the first error.
//
// The sizeHint is a capacity, not a length: append still decides the length. A
// 1h month is 744 rows, and without a hint that slice is grown and copied ten
// times over. [decodeSpec.estimateRows] is the intended source for it.
//
// Prefer ranging over the iterator where the whole range is not needed at once.
// This function is the point where an archive becomes resident in memory, and
// for 1s data that is measured in hundreds of megabytes.
func collectKlines(seq iter.Seq2[Kline, error], sizeHint int) ([]Kline, error) {
	out := make([]Kline, 0, sizeHint)

	// Ranging over a function value is Go 1.23's range-over-func: the loop body
	// is handed to the iterator as its yield, and a break or return here is
	// what makes yield report false to it.
	for k, err := range seq {
		if err != nil {
			return nil, err
		}

		out = append(out, k)
	}

	return out, nil
}

// decodeArchiveAll decodes a whole archive into a slice, sized from the spec.
// It is the convenience form of [decodeArchive] plus [collectKlines], for the
// callers that want every candle at once.
func decodeArchiveAll(ctx context.Context, r io.ReaderAt, size int64, spec decodeSpec) ([]Kline, error) {
	return collectKlines(decodeArchive(ctx, r, size, spec), spec.estimateRows())
}
