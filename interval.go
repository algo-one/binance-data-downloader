package binancedata

import (
	"fmt"
	"strconv"
	"time"
)

// Interval is a kline (candlestick) aggregation period: 1m, 1h, 1d and so on.
//
// # Why this is a type and not a string
//
// The obvious representation is a plain string, and it is the wrong one. A
// string parameter accepts "1hour", "60m", "" and "DROP TABLE" equally happily,
// and the mistake surfaces as a 404 several layers away from the typo. Defining
// a named type moves that check to a single place — [ParseInterval] — and lets
// the compiler carry the guarantee everywhere afterwards. A function taking an
// Interval cannot be handed an arbitrary string by accident.
//
// This is the single most common Go idiom worth importing into your habits from
// Python: where you would reach for str or an Enum, define a named type over a
// small integer, hang methods on it, and let the type do the arguing.
//
// # The two spellings
//
// The same interval is spelled differently by the two Binance endpoints this
// library uses, and exactly one interval disagrees:
//
//	bulk archives (data.binance.vision)   monthly candles are "1mo"
//	REST API (data-api.binance.vision)    monthly candles are "1M"
//
// Worse, the REST spelling is case-sensitive in a hostile way: "1m" is one
// minute and "1M" is one month, so a stray strings.ToUpper turns a minute into
// a month and returns plausible-looking wrong data. Rather than let each
// endpoint's code carry its own string, an Interval knows both spellings —
// [Interval.String] gives the archive one and [Interval.RESTParam] the REST
// one — so the two paths cannot drift apart.
//
// # Not every interval exists everywhere
//
// Binance does not publish every interval at both archive granularities: 3d, 1w
// and 1mo exist as monthly archives only, since their candles are longer than a
// day. Ask for the wrong combination and you get a 404 that looks exactly like
// "this symbol did not trade yet". [Interval.HasDailyArchives] and
// [Interval.HasMonthlyArchives] answer this before any request is made.
//
// The Python implementation this library replaces declares these same tables
// and then never consults them — and its monthly table is wrong: it omits 1s,
// which Binance does publish monthly. Verified against the live archives on
// 2026-08-18; see the note on the table below.
type Interval uint8

// The intervals Binance publishes klines for.
//
// # Reading the iota
//
// iota is Go's constant generator. Inside a const block it counts from 0,
// incrementing once per ConstSpec line, and a line that omits its expression
// repeats the previous one — so writing the expression once on the first line
// defines the whole ladder.
//
// The `+ 1` is load-bearing. It leaves 0 unassigned, which makes the zero value
// of Interval — what you get from `var iv Interval` or an unset struct field —
// an invalid interval rather than a silently plausible one. Go has no
// constructors and cannot stop a caller from writing binancedata.Request{},
// so "the zero value is detectably wrong" is the only defence available for a
// field a caller must actually choose. [Market] and [DataType] are built the
// same way, for the same reason.
//
// The names are terse on purpose: Interval1h reads at a call site the way the
// documentation reads, and the type name already supplies the noun.
const (
	Interval1s  Interval = iota + 1 // 1 second
	Interval1m                      // 1 minute
	Interval3m                      // 3 minutes
	Interval5m                      // 5 minutes
	Interval15m                     // 15 minutes
	Interval30m                     // 30 minutes
	Interval1h                      // 1 hour
	Interval2h                      // 2 hours
	Interval4h                      // 4 hours
	Interval6h                      // 6 hours
	Interval8h                      // 8 hours
	Interval12h                     // 12 hours
	Interval1d                      // 1 day
	Interval3d                      // 3 days — monthly archives only
	Interval1w                      // 1 week — monthly archives only
	Interval1mo                     // 1 calendar month — monthly archives only
)

// intervalInfo is everything this package knows about one interval, gathered
// into a single row so that adding an interval is one line rather than an edit
// to five scattered switch statements.
//
// The fields are unexported, and so is the type: it is an implementation
// detail of this file. Go's visibility rule is purely the case of the first
// letter — lowercase is package-private, uppercase is exported — with no
// keywords and no per-field annotations.
type intervalInfo struct {
	// archive is the spelling used in data.binance.vision paths and file
	// names, e.g. the "1h" in BTCUSDT-1h-2024-01.zip.
	archive string

	// rest is the spelling used by the REST API's interval query parameter.
	// Identical to archive for every interval except the monthly one.
	rest string

	// duration is the wall-clock length of one candle, or 0 for an interval
	// whose length depends on the calendar. See [Interval.Duration].
	duration time.Duration

	// daily and monthly record which bulk archive granularities Binance
	// publishes this interval at.
	daily   bool
	monthly bool
}

// intervalTable maps each Interval to its row. It is indexed *by the constant
// itself*, which is why the constants are small consecutive integers.
//
// Two pieces of Go syntax are doing real work here:
//
//   - [...]T{} declares an array — fixed length, a value rather than a
//     reference — and asks the compiler to count the elements. An array is used
//     rather than a map because lookup is a bounds check and an offset, with no
//     hashing and no allocation.
//   - The `Interval1s:` labels are *indexed elements* in a composite literal.
//     They pin each row to its constant by name, so the table cannot silently
//     shift if a constant is ever inserted in the middle, and the array is
//     sized from the largest index. Index 0 is left as the zero value, which is
//     the invalid interval and is never read.
//
// Nothing mutates this after startup. Go has no `const` for composite values,
// so a package-level var is as close as the language gets; the convention is
// that unexported package state like this is written once and only read.
var intervalTable = [...]intervalInfo{
	// 1s is published at both granularities, contrary to the table in the
	// Python implementation. Verified 2026-08-18 by HEAD against the live
	// archives for BTCUSDT, ETHUSDT and SOLUSDT: the monthly 2024-03 file is
	// 93 MB of real data, not a stub. Trusting the ported table would have
	// meant expanding every 1s request into ~30 daily downloads where one
	// monthly download would do.
	Interval1s:  {archive: "1s", rest: "1s", duration: time.Second, daily: true, monthly: true},
	Interval1m:  {archive: "1m", rest: "1m", duration: time.Minute, daily: true, monthly: true},
	Interval3m:  {archive: "3m", rest: "3m", duration: 3 * time.Minute, daily: true, monthly: true},
	Interval5m:  {archive: "5m", rest: "5m", duration: 5 * time.Minute, daily: true, monthly: true},
	Interval15m: {archive: "15m", rest: "15m", duration: 15 * time.Minute, daily: true, monthly: true},
	Interval30m: {archive: "30m", rest: "30m", duration: 30 * time.Minute, daily: true, monthly: true},
	Interval1h:  {archive: "1h", rest: "1h", duration: time.Hour, daily: true, monthly: true},
	Interval2h:  {archive: "2h", rest: "2h", duration: 2 * time.Hour, daily: true, monthly: true},
	Interval4h:  {archive: "4h", rest: "4h", duration: 4 * time.Hour, daily: true, monthly: true},
	Interval6h:  {archive: "6h", rest: "6h", duration: 6 * time.Hour, daily: true, monthly: true},
	Interval8h:  {archive: "8h", rest: "8h", duration: 8 * time.Hour, daily: true, monthly: true},
	Interval12h: {archive: "12h", rest: "12h", duration: 12 * time.Hour, daily: true, monthly: true},

	// A Binance "day" is exactly 24 hours of UTC. There is no daylight saving
	// in UTC, so unlike a local-time day this really is a fixed duration.
	Interval1d: {archive: "1d", rest: "1d", duration: 24 * time.Hour, daily: true, monthly: true},
	Interval3d: {archive: "3d", rest: "3d", duration: 72 * time.Hour, monthly: true},
	Interval1w: {archive: "1w", rest: "1w", duration: 7 * 24 * time.Hour, monthly: true},

	// The one interval with no fixed duration, and the one whose two spellings
	// differ. Both facts are consequences of it being a calendar unit.
	Interval1mo: {archive: "1mo", rest: "1M", monthly: true},
}

// intervalBySpelling indexes every accepted spelling — both the archive and the
// REST form of each interval — so that [ParseInterval] is one map lookup.
//
// It is initialised by calling a function at package level rather than in an
// init() function. Both run before main, but this form keeps the variable and
// the code that fills it adjacent, and makes the dependency obvious: Go
// initialises package-level variables in dependency order, so intervalTable is
// guaranteed to be built before this line runs.
var intervalBySpelling = buildIntervalIndex()

// buildIntervalIndex derives the spelling index from [intervalTable] instead of
// repeating all sixteen intervals a second time. A hand-written second list is
// a list that eventually disagrees with the first.
func buildIntervalIndex() map[string]Interval {
	// make's second argument is a capacity hint. It does not change behaviour,
	// only how many times the map grows while being filled — worth supplying
	// when the final size is known, and not worth agonising over otherwise.
	index := make(map[string]Interval, 2*len(intervalTable))

	// Start at 1: index 0 of the table is the unused zero value.
	for i := 1; i < len(intervalTable); i++ {
		interval := Interval(i)
		index[intervalTable[i].archive] = interval
		index[intervalTable[i].rest] = interval
	}

	return index
}

// Intervals returns every valid interval, ordered from shortest to longest.
//
// The slice is freshly built on each call, which is deliberate. Returning a
// package-level slice would hand callers a window onto this package's own
// memory — slices are views over a shared backing array, so a caller writing to
// element 0 would corrupt the library for everyone in the process. Copying a
// sixteen-element slice is cheaper than that class of bug.
func Intervals() []Interval {
	// The third argument to make is capacity: length 0, room for all of them.
	// Appending then never reallocates.
	all := make([]Interval, 0, len(intervalTable)-1)
	for i := 1; i < len(intervalTable); i++ {
		all = append(all, Interval(i))
	}

	return all
}

// ParseInterval converts a spelling of an interval into an [Interval]. It
// accepts both the archive form and the REST form, so "1mo" and "1M" both
// return [Interval1mo].
//
// Matching is exact and case-sensitive, and that is a correctness requirement
// rather than strictness for its own sake: Binance uses "1m" for one minute and
// "1M" for one month. Case-folding the input would quietly turn a request for
// minute candles into a request for monthly ones — the same number of rows
// never comes back, but the data looks superficially fine.
//
// The returned error wraps [ErrInvalidRequest], so callers test it with
// errors.Is rather than by inspecting the message.
func ParseInterval(s string) (Interval, error) {
	// The comma-ok form of a map read. A missing key yields the value type's
	// zero value with no error and no panic, so `ok` is the only way to tell
	// "absent" from "present and zero" — and here the zero value of Interval
	// is itself the invalid interval, which is why the two agree.
	interval, ok := intervalBySpelling[s]
	if !ok {
		// %q quotes the input, so trailing whitespace or an invisible
		// character in a config file shows up in the message instead of
		// producing a baffling "1h is not a valid interval".
		return 0, fmt.Errorf("interval %q: %w", s, ErrInvalidRequest)
	}

	return interval, nil
}

// IsValid reports whether i is one of the intervals Binance publishes.
//
// The zero value is not, which is what makes an unset struct field detectable.
func (i Interval) IsValid() bool {
	// Two bounds because Interval is just a uint8 underneath: nothing stops a
	// caller writing Interval(200). Converting to int before comparing with
	// len() avoids an overflow trap — len returns int, and comparing a uint8
	// against it would otherwise force the constant into a uint8.
	return i != 0 && int(i) < len(intervalTable)
}

// String returns the archive spelling of the interval — the form that appears
// in data.binance.vision paths, where a month is "1mo".
//
// Implementing String() makes Interval satisfy fmt.Stringer, which the fmt
// package looks for: %v and %s on an Interval print "1h" rather than the
// underlying 7. Anything you would want printed nicely in a log line is worth
// giving a String method.
//
// An invalid interval prints as Interval(200) rather than an empty string, so a
// bug shows up in the log instead of leaving a hole in it.
func (i Interval) String() string {
	if !i.IsValid() {
		// strconv.Itoa rather than fmt.Sprintf: no format string to parse and
		// no reflection, on a path that only runs when something is wrong.
		return "Interval(" + strconv.Itoa(int(i)) + ")"
	}

	return intervalTable[i].archive
}

// RESTParam returns the spelling the REST API expects in its `interval` query
// parameter, which differs from [Interval.String] only for [Interval1mo]:
// "1M" rather than "1mo".
//
// It returns the empty string for an invalid interval. Callers building a
// request should reject the interval before they get here — an empty parameter
// would be answered with a 400, which is a far less clear diagnosis than
// [ErrInvalidRequest] raised at the boundary.
func (i Interval) RESTParam() string {
	if !i.IsValid() {
		return ""
	}

	return intervalTable[i].rest
}

// Duration returns the wall-clock length of one candle at this interval, and
// whether that length is fixed at all.
//
// The second result is false for [Interval1mo], whose candles are 28, 29, 30 or
// 31 days long depending on where in the calendar they fall, and for an invalid
// interval. Returning (0, false) rather than a plausible-looking 30*24h is the
// point: a caller computing "how many candles should this range contain?" needs
// to be told that arithmetic does not apply here, not handed an approximation
// that is wrong eleven months a year.
//
// Multiple return values are how Go says "and also"; there is no tuple type and
// no out-parameter. The (value, ok) shape specifically mirrors what map reads
// and type assertions already do, so it reads as familiar rather than novel.
func (i Interval) Duration() (time.Duration, bool) {
	if !i.IsValid() || intervalTable[i].duration == 0 {
		return 0, false
	}

	return intervalTable[i].duration, true
}

// HasDailyArchives reports whether Binance publishes daily ZIP archives for
// this interval. It is false for [Interval3d], [Interval1w] and [Interval1mo],
// whose candles are longer than a day.
func (i Interval) HasDailyArchives() bool {
	return i.IsValid() && intervalTable[i].daily
}

// HasMonthlyArchives reports whether Binance publishes monthly ZIP archives for
// this interval. It is true for every interval — including [Interval1s], whose
// monthly archives are real but large, around 93 MB compressed for BTCUSDT.
func (i Interval) HasMonthlyArchives() bool {
	return i.IsValid() && intervalTable[i].monthly
}

// MarshalText implements encoding.TextMarshaler, writing the archive spelling.
//
// The standard library looks for this interface in several places at once:
// encoding/json uses it for map keys and struct fields, and flag.TextVar builds
// a command-line flag out of any type that implements it and its unmarshalling
// counterpart. Implementing two small methods therefore buys JSON round-tripping
// and CLI parsing without either package knowing this type exists.
func (i Interval) MarshalText() ([]byte, error) {
	if !i.IsValid() {
		return nil, fmt.Errorf("interval %s: %w", i, ErrInvalidRequest)
	}

	return []byte(i.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler, accepting either spelling.
//
// Note the pointer receiver: *Interval, where every other method on this type
// takes a plain Interval. Go passes receivers by value like any other argument,
// so a value receiver would be handed a copy and assigning to it would change
// nothing observable. A method that mutates its receiver must take a pointer —
// and unmarshalling is mutation by definition.
func (i *Interval) UnmarshalText(text []byte) error {
	parsed, err := ParseInterval(string(text))
	if err != nil {
		return err
	}

	// *i is the dereference: assign through the pointer to the caller's
	// variable, not to the local copy of the pointer.
	*i = parsed

	return nil
}
