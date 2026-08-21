package binancedata

import (
	"fmt"
	"time"
)

// Request describes one range of candles to fetch: which symbol, at which
// interval, in which market, over which span of time.
//
// # Closed ranges
//
// Both ends are included — the range mathematicians write [Start, End]. A
// candle is in the range when
//
//	Start <= OpenTime <= End
//
// so End is most usefully read as *the open time of the last candle you want*.
// For daily candles, End 2024-03-31 returns the candle for the 31st. For hourly
// candles, End 2024-03-31 returns the one candle that opened at 00:00 on the
// 31st — because that is the last candle whose open time is at or before the
// instant named. Ask for the whole day and you are asking for the whole day's
// last instant: 2024-03-31T23:59:59.999999999Z. The bmd CLI does that expansion
// for you when --end is written as a bare date, which is the right place for a
// human-facing convenience.
//
// # Where the half-open ranges went
//
// They are still here; they are just no longer the caller's problem. Inside the
// pipeline every boundary is half-open, because ranges are split into months,
// months into days, days into API pages, and the pieces must join back together
// with no arithmetic at all:
//
//	[Jan 1, Feb 1) + [Feb 1, Mar 1)  =  [Jan 1, Mar 1)
//
// The end of one piece *is* the start of the next, so a seam is written once
// and there is nothing to add or subtract. Inclusive seams would need a "+1 of
// something" at every join, and the something changes with the interval — one
// millisecond before 2025, one microsecond after. Every one of those is a
// chance to drop a candle or emit it twice, silently.
//
// So the conversion from what a caller wrote to what the pipeline uses happens
// exactly once, in [Request.endExclusive], and the something it adds is one
// *nanosecond*. That is a unit no Binance timestamp has ever used: the archives
// publish milliseconds, and microseconds since 2025. Nothing can fall between
// End and End+1ns, so the conversion cannot gain or lose a candle, and it is
// the same single line whichever side of the 2025 switch the data sits on.
//
// # What this costs, stated plainly
//
// A whole year of 2024 is now
//
//	Start: 2024-01-01T00:00:00Z, End: 2024-12-31T23:59:59.999999999Z
//
// Writing End 2025-01-01 instead is not an error and will not be reported as
// one. It asks for the candle that opens exactly at midnight on New Year's Day,
// which is a real candle living in a different month — so the planner fetches
// January's archive to get it, and one extra candle arrives at the end of your
// slice. That is the tax inclusive ends charge, and it is charged to whoever
// writes the boundary. It is here because the alternative was worse: a CLI
// whose --end meant something different from the library's End is the kind of
// difference nobody notices until a backtest is a day short.
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

	// End is the last instant *included* in the range, and must be UTC. A
	// candle is returned when its open time is at or before it.
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

		// Note which comparison this is. Under the old half-open rule a range
		// [t, t) was empty by definition and had to be rejected, so the test
		// was !Start.Before(End) and it caught equality too. A closed range
		// [t, t] is not empty — it holds one instant, and asks for the one
		// candle that opens there — so equality is now a legal request and
		// rejecting it would refuse something meaningful.
		//
		// What remains impossible is an End that precedes its Start, which no
		// reading makes sense of.
		if r.End.Before(r.Start) {
			return fmt.Errorf(
				"end %s is before start %s: %w",
				r.End.Format(time.RFC3339), r.Start.Format(time.RFC3339), ErrInvalidRequest,
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
		if r.End.Before(r.Start) {
			return Request{}, fmt.Errorf(
				"start %s is after now (%s), so the range is empty: %w",
				r.Start.Format(time.RFC3339), r.End.Format(time.RFC3339), ErrInvalidRequest,
			)
		}
	}

	return r, nil
}

// endExclusive converts this request's inclusive End into the exclusive
// boundary every layer below the public API works in.
//
// This is the *only* place the two conventions meet. A caller writes a closed
// range [Start, End]; the planner, the chunk seams, the decoder's range checks
// and the REST endTime parameter all speak half-open [Start, End). One function
// with one caller-facing rule — "a candle counts when OpenTime <= End" —
// becomes one rule for everything underneath: "a candle counts when
// OpenTime < endExclusive". Two conventions are survivable; two conventions
// with the conversion scattered across five call sites are not.
//
// # Why a nanosecond
//
// Because it is a unit Binance has never published in. A time.Time carries
// nanoseconds, archive timestamps are milliseconds (microseconds since 2025),
// and no candle can therefore have an open time strictly between End and
// End+1ns. The conversion is exact rather than approximately right, and it is
// the same line on both sides of the 2025 unit switch — which the "+1 of
// something" alternative is not.
//
// The one input this cannot handle is the maximum representable time.Time,
// which would overflow silently. Nothing guards against it: that instant is in
// the year 219250468, a request reaching it has asked for two hundred million
// years of candles, and Validate does not reject it either. Worth knowing the
// guard is absent rather than assuming it is there.
//
// # It must be called on a resolved request
//
// A zero End means "now" and is filled in by [Request.resolve]. Calling this on
// an unresolved request would hand back one nanosecond past the zero time,
// which is a boundary in the year 1, so callers resolve first. There is no
// guard here because adding one to a value the caller was supposed to have
// filled in cannot be distinguished from a legitimately tiny range without
// reintroducing the clock this method deliberately does not have.
func (r Request) endExclusive() time.Time {
	return r.End.Add(1)
}
