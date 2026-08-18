package binancedata

import (
	"fmt"
	"time"
)

// Request describes one range of candles to fetch: which symbol, at which
// interval, in which market, over which span of time.
//
// # Half-open ranges
//
// Start is included in the range and End is excluded — the range mathematicians
// write [Start, End). This is the single most important thing to know about
// this type, because the alternative is so tempting and so quietly wrong.
//
// The reason is composition. Ranges get split into months, months into days,
// days into API pages, and the pieces get merged back together. With half-open
// ranges, adjacent pieces join with no arithmetic at all:
//
//	[Jan 1, Feb 1) + [Feb 1, Mar 1)  =  [Jan 1, Mar 1)
//
// The end of one piece *is* the start of the next, so a boundary can only ever
// be written once and there is nothing to add or subtract. With inclusive ends
// every seam needs a "+1 of something", and the something changes with the
// interval — one millisecond before 2025, one microsecond after. Every one of
// those is a chance to drop a candle or emit it twice, and both failures are
// silent.
//
// So a full year of 2024 is Start 2024-01-01, End 2025-01-01. Asking for
// End 2024-12-31 gets you every candle up to but not including that final day.
// The CLI in a later stage will accept a bare --end date and add a day for you,
// because that is the right place for a human-facing convenience.
//
// # Zero values mean something here
//
// Go gives every struct field a zero value and provides no constructors, so a
// caller can always write Request{} and there is nothing this package can do to
// stop them. The defence is to make the zero value of each field either
// meaningful or detectably invalid:
//
//   - Interval and Market number their constants from 1, so a zero field is
//     an invalid value rather than a plausible one that silently defaults.
//   - Start must be set; a zero Start is rejected.
//   - End is the exception: a zero End means "up to now", resolved at the
//     moment of the call.
//
// That last one is deliberate, and it is a bug fixed rather than ported. The
// Python implementation defaulted its end date with datetime.now(UTC) evaluated
// as a *default argument*, which Python binds once when the module is imported.
// A process that runs for a week therefore keeps asking for data up to the day
// it started, and quietly returns less and less of what was requested. Storing
// the zero value and resolving it per call cannot drift, because there is
// nothing stored to go stale.
//
// # On using Request as a map key
//
// FetchAll returns map[Request][]Kline, which requires Request to be
// comparable — it is, since every field is. But two of those fields are
// time.Time, and time.Time equality under == is not what you want: it compares
// the wall clock, the monotonic reading and the *time.Location pointer. Two
// times naming the same instant in different locations are not == to each
// other, and a time from time.Now() carries a monotonic reading that one read
// back from a database does not.
//
// Requiring Start and End to be UTC is what makes the map key safe. UTC is a
// single shared Location value, and the .UTC() conversion that produces it also
// strips the monotonic reading. So the rule below is not pedantry about time
// zones; it is what stops FetchAll from returning two entries for one request.
type Request struct {
	// Symbol is the trading pair, in any of the spellings [NormalizeSymbol]
	// accepts: "BTC/USDT", "BTC-USDT" or "BTCUSDT".
	Symbol string

	// Interval is the candle aggregation period. Required; the zero value is
	// invalid.
	Interval Interval

	// Market selects the Binance market. Required; the zero value is invalid.
	// [MarketSpot] is the only implemented value.
	Market Market

	// Start is the first instant included in the range. Required, and must be
	// UTC.
	Start time.Time

	// End is the first instant *excluded* from the range, and must be UTC.
	//
	// The zero value means "now, as of the moment the request is executed".
	// Prefer leaving it zero over writing time.Now(): a stored End is a
	// snapshot that ages, and this field exists to not have one.
	End time.Time
}

// Validate reports whether the request is well-formed, without consulting a
// clock or the network. It returns an error wrapping [ErrInvalidRequest], or
// nil.
//
// Every check here is one that can be made before a single byte is sent, which
// is the point: a request that cannot possibly succeed should fail immediately
// and cheaply, naming the field at fault, rather than 404-ing several layers
// away where the cause is no longer obvious.
//
// A request with a zero End passes validation — that spelling is legal and
// means "up to now". What Validate cannot check is whether Start precedes an
// End that does not exist yet; that comparison happens when the range is
// resolved against a clock.
//
// Calling this is optional. [Loader.Fetch] runs the same checks itself, because
// validation a caller has to remember to invoke is validation that eventually
// does not run. It is exported so that a program which builds requests from
// user input — a config file, CLI flags, a web form — can reject bad ones at
// the edge, where it still has the context to say which line was wrong.
func (r Request) Validate() error {
	if _, err := NormalizeSymbol(r.Symbol); err != nil {
		return err // already wraps ErrInvalidRequest and names the symbol
	}

	if !r.Interval.IsValid() {
		return fmt.Errorf("interval: %w", ErrInvalidRequest)
	}

	if !r.Market.IsValid() {
		return fmt.Errorf("market: %w", ErrInvalidRequest)
	}

	// An interval Binance publishes at neither granularity cannot be served
	// from archives at all. No such interval exists today — every entry in the
	// table has monthly archives — but the check is here so that the day one
	// is added, the failure is this message rather than an empty result.
	if !r.Interval.HasDailyArchives() && !r.Interval.HasMonthlyArchives() {
		return fmt.Errorf("interval %s: published at no archive granularity: %w", r.Interval, ErrInvalidRequest)
	}

	if r.Start.IsZero() {
		return fmt.Errorf("start: required: %w", ErrInvalidRequest)
	}

	// Location is compared by pointer, and time.UTC is a single package-level
	// value that every UTC time shares, so this is an identity test rather
	// than a string comparison. A time built with time.Date(..., time.UTC),
	// parsed from an RFC 3339 string ending in Z, or passed through .UTC()
	// all satisfy it.
	//
	// Converting silently instead of rejecting was the alternative, and it is
	// worse: it would accept a Start the caller believed was midnight and
	// fetch from 05:00, returning data that is wrong in a way no error
	// mentions. Refusing makes the caller state which instant they meant.
	if err := requireUTC("start", r.Start); err != nil {
		return err
	}

	// The zero End is the "resolve at call time" spelling, so it is exempt.
	// IsZero is the correct test rather than == time.Time{}, for the same
	// reason Kline has an Equal method: == on a time.Time compares more than
	// the instant.
	if !r.End.IsZero() {
		if err := requireUTC("end", r.End); err != nil {
			return err
		}

		// Equal starts and ends are rejected rather than treated as an empty
		// result. A half-open range [t, t) is empty by definition, and a
		// caller who wrote one almost certainly meant something else — most
		// often they meant an inclusive end. Returning zero candles and no
		// error would leave them debugging the wrong layer.
		if !r.Start.Before(r.End) {
			return fmt.Errorf(
				"start %s is not before end %s (the range is half-open, so end is excluded): %w",
				r.Start.Format(time.RFC3339), r.End.Format(time.RFC3339), ErrInvalidRequest,
			)
		}
	}

	return nil
}

// requireUTC is the shared half of the two location checks above. Pulling it
// out keeps the error text identical for both fields, which matters more than
// it sounds: a caller grepping their logs for one phrasing should find both.
func requireUTC(field string, t time.Time) error {
	if t.Location() != time.UTC {
		return fmt.Errorf(
			"%s %s: must be UTC, not %s (call .UTC() on it): %w",
			field, t.Format(time.RFC3339), t.Location(), ErrInvalidRequest,
		)
	}

	return nil
}

// resolve validates the request and returns it in canonical form: the symbol
// normalised, and a zero End replaced by now.
//
// The clock arrives as a parameter rather than being read from time.Now()
// inside. That is a rule across this package, and it buys two things. Calendar
// behaviour becomes testable — a test can ask what happens on the 1st of a
// month, or at the instant of a leap second, without waiting for one — and the
// resolution of End happens demonstrably per call, since there is nowhere to
// cache a clock reading even by accident.
//
// It is unexported because now is not the caller's business. [Request.Validate]
// is the exported door to the checks that do not need a clock.
func (r Request) resolve(now time.Time) (Request, error) {
	if err := r.Validate(); err != nil {
		return Request{}, err
	}

	symbol, err := NormalizeSymbol(r.Symbol)
	if err != nil {
		return Request{}, err
	}

	// Methods with value receivers — func (r Request), not func (r *Request) —
	// get a copy of the struct, so assigning to r here cannot be seen by the
	// caller. Mutating the copy and returning it is the idiomatic way to write
	// a "with these changes" transformation on a value type, and it is why
	// this function returns a Request rather than modifying one in place.
	r.Symbol = symbol

	if r.End.IsZero() {
		if now.Location() != time.UTC {
			// A caller cannot reach this; the loader supplies the clock. It is
			// a guard on our own wiring, and it is cheap.
			now = now.UTC()
		}

		r.End = now

		// Re-check the ordering now that End is a real instant. A Start in the
		// future is the realistic way to get here — a typo'd year, or a
		// backtest configured to begin tomorrow — and saying so beats
		// returning zero candles.
		if !r.Start.Before(r.End) {
			return Request{}, fmt.Errorf(
				"start %s is not before now (%s), so the range is empty: %w",
				r.Start.Format(time.RFC3339), r.End.Format(time.RFC3339), ErrInvalidRequest,
			)
		}
	}

	return r, nil
}
