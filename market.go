package binancedata

import (
	"fmt"
	"strconv"
)

// Market selects which Binance market a request refers to.
//
// Only [MarketSpot] is implemented. The type exists anyway, ahead of any second
// value, because it is one of three deliberate extension points that make
// adding futures an increment rather than a rewrite: every URL this library
// builds passes through a switch on a Market, so a new market is a new case in
// a handful of switches the compiler will point at. Threading a bare "spot"
// string through instead would leave nothing to point at.
//
// The `exhaustive` linter (see .golangci.yml) enforces that: adding a constant
// to this type turns every switch that has not been updated into a lint
// failure, which is as close as Go gets to a compile-time reminder.
type Market uint8

// The supported markets.
//
// As with [Interval], the `+ 1` leaves 0 unassigned so that the zero value —
// what an unset struct field holds — is an invalid market rather than a
// plausible one. A caller must name the market they want.
//
// Making spot the zero value would have been friendlier today and wrong
// tomorrow. Spot is the only market now, so an unset field could only ever
// have meant spot; but the moment futures exists, every request written before
// it existed silently keeps meaning spot, and the one place the compiler could
// have asked "which market?" has been given away permanently. A default is a
// decision you can only make once, and it is much easier to add later than to
// take back.
//
// The rule underneath: Go gives every variable its zero value and no
// constructor can intercept that, so deciding what your zero value means is
// not optional. You either choose it or inherit it by accident. This codebase
// chooses the same thing every time — the zero value is invalid, and being
// explicit is the price of entry.
const (
	MarketSpot Market = iota + 1 // spot market — the only implemented value
)

// IsValid reports whether m is a market this library supports.
func (m Market) IsValid() bool {
	return m == MarketSpot
}

// String returns the market's name as it appears in flags and log lines. It
// also happens to be the path segment data.binance.vision uses for spot, though
// that correspondence will not survive futures — /data/futures/um/ is two
// segments — so URL building gets its own mapping when it arrives in Stage 4.
//
// This implements fmt.Stringer; see [Interval.String] for what that buys.
func (m Market) String() string {
	// A switch rather than an if, even with one case, because this is the
	// pattern every market-dependent switch in the codebase will follow, and
	// because it is what the exhaustive linter watches.
	//
	// Go's switch needs no break: cases do not fall through unless you write
	// an explicit `fallthrough`, which is the opposite default from C and the
	// right one.
	switch m {
	case MarketSpot:
		return "spot"
	default:
		return "Market(" + strconv.Itoa(int(m)) + ")"
	}
}

// ParseMarket converts a market name into a [Market]. It accepts exactly the
// names [Market.String] produces, so the CLI's --market flag and the log lines
// describing what it did use one vocabulary.
//
// The returned error wraps [ErrInvalidRequest].
func ParseMarket(s string) (Market, error) {
	switch s {
	case "spot":
		return MarketSpot, nil
	default:
		return 0, fmt.Errorf("market %q: %w", s, ErrInvalidRequest)
	}
}

// MarshalText implements encoding.TextMarshaler. See [Interval.MarshalText].
func (m Market) MarshalText() ([]byte, error) {
	if !m.IsValid() {
		return nil, fmt.Errorf("market %s: %w", m, ErrInvalidRequest)
	}

	return []byte(m.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler. The pointer receiver is
// required because the method assigns to its receiver; see
// [Interval.UnmarshalText].
func (m *Market) UnmarshalText(text []byte) error {
	parsed, err := ParseMarket(string(text))
	if err != nil {
		return err
	}

	*m = parsed

	return nil
}

// dataType selects which family of market data a request refers to.
//
// It is unexported, and that is a decision rather than an oversight — one worth
// spelling out, because [Market] directly above is exported on reasoning this
// rejects.
//
// Market is exported because callers *pass* one: it is a field of [Request] and
// of [AvailabilityQuery], so a program has to be able to name MarketSpot. A data
// type is not a field of anything. Klines are the only family this library
// parses, both call sites that build a path hardcode the constant below, and no
// exported function accepts one. Exporting it would publish a type a caller can
// name and has nowhere to hand to — the type-level form of the rule
// docs/architecture.md states for options: an accepted-and-ignored setting is a
// defect, not a stub.
//
// Nothing is lost by waiting, because the two directions are not symmetrical.
// Adding an exported type later is a backwards-compatible change, so the day a
// second family arrives — aggTrades, trades, bookTicker, each with its own CSV
// schema and therefore its own parser — this is re-exported and given a home on
// Request. Adding a *required* field to Request later is the change that is not
// backwards compatible, since every existing caller leaves it zero and this
// package's whole enum convention is that zero is invalid. Deciding that now,
// with one family to choose between, would be deciding it with no information.
//
// It stays a named type rather than becoming a bare string constant because it
// is the segment of every archive URL that would change first —
// /data/spot/monthly/klines/ becomes /data/spot/monthly/aggTrades/ — and a type
// keeps the two places that build those paths from spelling it by hand.
type dataType uint8

// The supported data types. As with [Market] and [Interval], 0 is left
// unassigned so the zero value is invalid — the same reasoning applies, and one
// convention across every enum in this package is worth more than a convenience
// saved on the one type that currently has a single member.
const (
	dataTypeKlines dataType = iota + 1 // candlesticks
)

// String returns the data type's name, which is also the path segment
// data.binance.vision uses for it.
//
// There is no IsValid beside it, unlike [Market] and [Interval]. Those two are
// validated because they arrive from a caller and may be anything; a dataType
// is only ever the constant above, so an IsValid here would have no call site
// but its own test. What the zero value must not do is still pinned, and by the
// property that actually matters: it renders as dataType(0), which builds a
// path that 404s loudly, rather than as "klines", which would build a path that
// silently works.
func (d dataType) String() string {
	switch d {
	case dataTypeKlines:
		return "klines"
	default:
		return "dataType(" + strconv.Itoa(int(d)) + ")"
	}
}
