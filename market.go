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

// DataType selects which family of market data a request refers to.
//
// Only [DataTypeKlines] is implemented, and klines are the only type this
// library is scoped to. The type is here because it is the segment of every
// archive URL that would change first — /data/spot/monthly/klines/ becomes
// /data/spot/monthly/aggTrades/ — and because each other data type has a
// different CSV schema, so a second value would arrive with its own parser
// rather than by loosening this one.
type DataType uint8

// The supported data types. As with [Market] and [Interval], 0 is left
// unassigned so the zero value is invalid — the same reasoning applies, and one
// convention across every enum in this package is worth more than a convenience
// saved on the one type that currently has a single member.
const (
	DataTypeKlines DataType = iota + 1 // candlesticks
)

// IsValid reports whether d is a data type this library supports.
func (d DataType) IsValid() bool {
	return d == DataTypeKlines
}

// String returns the data type's name, which is also the path segment
// data.binance.vision uses for it.
func (d DataType) String() string {
	switch d {
	case DataTypeKlines:
		return "klines"
	default:
		return "DataType(" + strconv.Itoa(int(d)) + ")"
	}
}
