package binancedata

import (
	"time"

	"github.com/quagmt/udecimal"
)

// Kline is one candlestick: the open, high, low and close of a single interval,
// with the volume traded during it.
//
// # Why the numbers are not float64
//
// Every price and volume field is a [udecimal.Decimal] rather than a float64,
// and that is the single most consequential decision in this package. Binance
// publishes these values as exact decimal text, and a real BTCUSDT monthly
// candle carries a quote volume of 118661604939.99255335 — twenty significant
// digits. A float64 holds about 15.95, so it cannot represent that number, and
// the failure is not a crash but a quiet drift in the last digits of every
// aggregate computed from it.
//
// Fixed-point integers were measured and rejected too: real meme-coin volumes
// overflow an int64 scaled by 1e8, and Go's integer overflow is silent — the
// number simply comes back negative. There is no single scale that covers both
// BTC prices and SHIB volumes.
//
// udecimal keeps a 128-bit coefficient inline in the struct and falls back to
// big.Int only past 2^128, which real Binance values never reach — so parsing
// the worst case measured takes 25ns and allocates nothing. The cost is that
// arithmetic is methods rather than operators — a.Add(b), not a + b — and that
// a Kline is 312 bytes rather than the 120 a float64 version measures. Convert
// with [udecimal.Decimal.InexactFloat64] at the boundary where you genuinely
// need a float, which for most callers is feeding an indicator library; the
// column helpers below do exactly that.
//
// docs/numbers.md has the full comparison against float64, int64 fixed point,
// govalues/decimal, shopspring/decimal and apd, measured over 1.75 million real
// archive values.
//
// # What is not in here
//
// The Python implementation this replaces repeats the symbol and the interval
// on every row. They are absent here because a []Kline is already the answer to
// one request, for one symbol at one interval — the fields would be the same
// value 2.6 million times over. Go's static typing means a slice cannot quietly
// acquire rows from a different symbol the way a concatenated DataFrame can.
//
// # Field order
//
// Struct fields are laid out in declaration order and padded to keep each one
// aligned, so declaration order is a memory-layout decision. Grouping the two
// 24-byte times, then the eight 32-byte decimals, then the lone int64, leaves
// no padding at all. Five years of one-minute candles is about 2.6 million of
// these, so the layout is worth one minute of thought — and is a large part of
// why the planned Stream API exists as an alternative to holding a whole range
// in memory at once.
type Kline struct {
	// OpenTime is the instant the interval began, and is the candle's
	// identity: it is what deduplication, sorting and range filtering key on.
	// Always UTC.
	OpenTime time.Time

	// CloseTime is the last instant included in the interval, as Binance
	// reports it. Note that it is inclusive and lands one millisecond (or,
	// since 2025, one microsecond) before the next candle's OpenTime, rather
	// than being equal to it.
	CloseTime time.Time

	// Open, High, Low and Close are the first, highest, lowest and last trade
	// prices within the interval.
	Open  udecimal.Decimal
	High  udecimal.Decimal
	Low   udecimal.Decimal
	Close udecimal.Decimal

	// Volume is the quantity traded, denominated in the base asset — the BTC
	// of BTCUSDT.
	Volume udecimal.Decimal

	// QuoteVolume is the same trading denominated in the quote asset, the
	// USDT of BTCUSDT. This is the field that reaches twenty significant
	// digits and rules out float64 for the whole struct.
	QuoteVolume udecimal.Decimal

	// TakerBuyBaseVolume and TakerBuyQuoteVolume are the portion of Volume and
	// QuoteVolume where the buyer was the taker — the aggressor crossing the
	// spread. The sell-side portion is the remainder, so Binance does not
	// publish it separately.
	TakerBuyBaseVolume  udecimal.Decimal
	TakerBuyQuoteVolume udecimal.Decimal

	// Trades is the number of individual trades aggregated into this candle.
	Trades int64
}

// Equal reports whether two candles are the same in every field.
//
// This method exists because == is a trap here, and a silent one. Go's ==
// compares structs field by field, and it compiles for Kline — but a
// [udecimal.Decimal] holds a *big.Int pointer for values beyond 2^128, so ==
// would compare two pointers rather than two numbers and report unequal for
// values that are equal. time.Time is worse: == also compares the monotonic
// clock reading and the *Location pointer, so the same instant read two
// different ways is not ==. reflect.DeepEqual has both problems too.
//
// The correct comparisons are [udecimal.Decimal.Equal] and [time.Time.Equal],
// and gathering them here means no caller has to remember that. Tests in later
// stages compare candles constantly; this is the function they use.
func (k Kline) Equal(other Kline) bool {
	return k.OpenTime.Equal(other.OpenTime) &&
		k.CloseTime.Equal(other.CloseTime) &&
		k.Open.Equal(other.Open) &&
		k.High.Equal(other.High) &&
		k.Low.Equal(other.Low) &&
		k.Close.Equal(other.Close) &&
		k.Volume.Equal(other.Volume) &&
		k.QuoteVolume.Equal(other.QuoteVolume) &&
		k.TakerBuyBaseVolume.Equal(other.TakerBuyBaseVolume) &&
		k.TakerBuyQuoteVolume.Equal(other.TakerBuyQuoteVolume) &&
		k.Trades == other.Trades
}

// column extracts one decimal field from every candle as a float64.
//
// The field to extract arrives as a function, because in Go functions are
// ordinary values: they can be passed, returned and stored like any int. That
// is what lets [Opens], [Highs] and the rest below be one line each instead of
// six near-identical loops — the loop is written once and the differing part is
// a parameter.
//
// This is Python's `operator.attrgetter` or a lambda, with the difference that
// the compiler checks the signature and, since the closures below capture
// nothing, allocates nothing to represent them.
//
// The conversion to float64 is lossy by definition, which is why the exported
// wrappers say so and why the [Kline] fields themselves stay exact.
func column(klines []Kline, field func(Kline) udecimal.Decimal) []float64 {
	// One allocation of exactly the right size. Passing len as the capacity
	// and 0 as the length means append never has to grow and copy.
	out := make([]float64, 0, len(klines))
	for _, k := range klines {
		out = append(out, field(k).InexactFloat64())
	}

	return out
}

// Opens returns the open price of every candle as a float64 slice.
//
// The conversion is inexact: float64 cannot represent every decimal Binance
// publishes. It is the right trade for feeding a technical-indicator library,
// since every Go indicator package takes []float64 — but do the arithmetic that
// has to balance on the [udecimal.Decimal] fields instead.
func Opens(klines []Kline) []float64 {
	return column(klines, func(k Kline) udecimal.Decimal { return k.Open })
}

// Highs returns the high price of every candle as a float64 slice. See [Opens]
// for the precision caveat.
func Highs(klines []Kline) []float64 {
	return column(klines, func(k Kline) udecimal.Decimal { return k.High })
}

// Lows returns the low price of every candle as a float64 slice. See [Opens]
// for the precision caveat.
func Lows(klines []Kline) []float64 {
	return column(klines, func(k Kline) udecimal.Decimal { return k.Low })
}

// Closes returns the close price of every candle as a float64 slice. It is the
// column most indicators want. See [Opens] for the precision caveat.
func Closes(klines []Kline) []float64 {
	return column(klines, func(k Kline) udecimal.Decimal { return k.Close })
}

// Volumes returns the base-asset volume of every candle as a float64 slice. See
// [Opens] for the precision caveat, which bites hardest on volume columns.
func Volumes(klines []Kline) []float64 {
	return column(klines, func(k Kline) udecimal.Decimal { return k.Volume })
}

// OpenTimes returns the open time of every candle, which is what a plotting or
// resampling routine needs alongside a price column.
func OpenTimes(klines []Kline) []time.Time {
	out := make([]time.Time, 0, len(klines))
	for _, k := range klines {
		out = append(out, k.OpenTime)
	}

	return out
}
