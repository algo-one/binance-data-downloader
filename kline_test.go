package binancedata

import (
	"testing"
	"time"

	"github.com/quagmt/udecimal"
)

// dec parses a decimal or fails the test. Test helpers take *testing.T and call
// t.Helper() so that a failure is reported at the line that called the helper
// rather than inside it — otherwise every failure in this file would point at
// the same unhelpful line.
func dec(t *testing.T, s string) udecimal.Decimal {
	t.Helper()

	d, err := udecimal.Parse(s)
	if err != nil {
		t.Fatalf("udecimal.Parse(%q): %v", s, err)
	}

	return d
}

// testKline builds a candle with plausible values, so each test below can vary
// the one field it cares about instead of restating eleven.
func testKline(t *testing.T) Kline {
	t.Helper()

	return Kline{
		OpenTime:            time.Date(2024, time.January, 15, 12, 0, 0, 0, time.UTC),
		CloseTime:           time.Date(2024, time.January, 15, 12, 59, 59, 999000000, time.UTC),
		Open:                dec(t, "42150.01"),
		High:                dec(t, "42380.00"),
		Low:                 dec(t, "42090.55"),
		Close:               dec(t, "42311.99"),
		Volume:              dec(t, "1234.56789012"),
		QuoteVolume:         dec(t, "52216384.12345678"),
		TakerBuyBaseVolume:  dec(t, "600.12345678"),
		TakerBuyQuoteVolume: dec(t, "25389411.87654321"),
		Trades:              98765,
	}
}

// TestKlineEqual documents why [Kline.Equal] exists at all: == compiles for
// this struct and gives wrong answers, so every case below is a value pair that
// == would misjudge.
func TestKlineEqual(t *testing.T) {
	t.Parallel()

	t.Run("identical candles are equal", func(t *testing.T) {
		t.Parallel()

		a, b := testKline(t), testKline(t)
		if !a.Equal(b) {
			t.Error("two identically built candles compared unequal")
		}
	})

	t.Run("trailing zeros do not change the value", func(t *testing.T) {
		t.Parallel()

		// "1.50" and "1.5" are the same number, but udecimal records the
		// decimal places it was given, so the two structs differ bit for bit.
		// == reports them unequal; Equal compares the numbers.
		a, b := testKline(t), testKline(t)
		a.Open = dec(t, "1.50")
		b.Open = dec(t, "1.5")

		if a.Open == b.Open {
			t.Error("precondition failed: == unexpectedly agreed, so this test proves nothing")
		}
		if !a.Equal(b) {
			t.Error("1.50 and 1.5 compared unequal")
		}
	})

	t.Run("values beyond 128 bits are compared by value", func(t *testing.T) {
		t.Parallel()

		// Past 2^128 udecimal switches to a big.Int, which lives behind a
		// pointer. == would compare the two pointers, which are never the
		// same, and report unequal for the same number.
		const huge = "340282366920938463463374607431768211456"

		a, b := testKline(t), testKline(t)
		a.QuoteVolume = dec(t, huge)
		b.QuoteVolume = dec(t, huge)

		if a.QuoteVolume == b.QuoteVolume {
			t.Error("precondition failed: == unexpectedly agreed, so this test proves nothing")
		}
		if !a.Equal(b) {
			t.Error("two decimals holding the same huge value compared unequal")
		}
	})

	t.Run("the same instant in a different location is the same time", func(t *testing.T) {
		t.Parallel()

		// time.Time's == compares the wall clock, the monotonic reading and
		// the *Location pointer. The same instant expressed in another zone is
		// a different struct and the same moment.
		a, b := testKline(t), testKline(t)
		b.OpenTime = a.OpenTime.In(time.FixedZone("UTC+1", 3600))

		// Comparing two time.Time values with == is the mistake being
		// demonstrated, so staticcheck's correct advice is suppressed at this
		// one line rather than switched off in .golangci.yml.
		//nolint:staticcheck // QF1009: == on time.Time is the point of this check
		if a.OpenTime == b.OpenTime {
			t.Error("precondition failed: == unexpectedly agreed, so this test proves nothing")
		}
		if !a.Equal(b) {
			t.Error("the same instant in two locations compared unequal")
		}
	})

	// Every field must actually participate. A copy-pasted comparison that
	// checks High twice and Low never is exactly the kind of bug this catches.
	t.Run("each field is compared", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			mutate func(*Kline)
		}{
			{name: "OpenTime", mutate: func(k *Kline) { k.OpenTime = k.OpenTime.Add(time.Minute) }},
			{name: "CloseTime", mutate: func(k *Kline) { k.CloseTime = k.CloseTime.Add(time.Minute) }},
			{name: "Open", mutate: func(k *Kline) { k.Open = udecimal.MustParse("1") }},
			{name: "High", mutate: func(k *Kline) { k.High = udecimal.MustParse("2") }},
			{name: "Low", mutate: func(k *Kline) { k.Low = udecimal.MustParse("3") }},
			{name: "Close", mutate: func(k *Kline) { k.Close = udecimal.MustParse("4") }},
			{name: "Volume", mutate: func(k *Kline) { k.Volume = udecimal.MustParse("5") }},
			{name: "QuoteVolume", mutate: func(k *Kline) { k.QuoteVolume = udecimal.MustParse("6") }},
			{name: "TakerBuyBaseVolume", mutate: func(k *Kline) { k.TakerBuyBaseVolume = udecimal.MustParse("7") }},
			{name: "TakerBuyQuoteVolume", mutate: func(k *Kline) { k.TakerBuyQuoteVolume = udecimal.MustParse("8") }},
			{name: "Trades", mutate: func(k *Kline) { k.Trades++ }},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				a := testKline(t)

				// b starts as a copy of a. Kline holds no slices or maps, so
				// assignment copies every field and the two are independent —
				// which is why the mutation below cannot reach back into a.
				b := a
				tt.mutate(&b)

				if a.Equal(b) {
					t.Errorf("candles differing only in %s compared equal", tt.name)
				}
				// Equality must not depend on which side you ask.
				if b.Equal(a) {
					t.Errorf("Equal is not symmetric for %s", tt.name)
				}
			})
		}
	})
}

// TestColumnHelpers checks that each helper reads the field it is named after.
// Getting Highs and Lows the wrong way round is a silent, plausible-looking
// bug: the numbers still look like prices.
func TestColumnHelpers(t *testing.T) {
	t.Parallel()

	first := testKline(t)
	second := testKline(t)
	second.OpenTime = first.OpenTime.Add(time.Hour)
	second.Open = dec(t, "1.5")
	second.High = dec(t, "2.5")
	second.Low = dec(t, "0.5")
	second.Close = dec(t, "2.0")
	second.Volume = dec(t, "10.25")

	klines := []Kline{first, second}

	tests := []struct {
		name string
		got  []float64
		want []float64
	}{
		{name: "Opens", got: Opens(klines), want: []float64{42150.01, 1.5}},
		{name: "Highs", got: Highs(klines), want: []float64{42380.00, 2.5}},
		{name: "Lows", got: Lows(klines), want: []float64{42090.55, 0.5}},
		{name: "Closes", got: Closes(klines), want: []float64{42311.99, 2.0}},
		{name: "Volumes", got: Volumes(klines), want: []float64{1234.56789012, 10.25}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if len(tt.got) != len(tt.want) {
				t.Fatalf("%s returned %d values, want %d", tt.name, len(tt.got), len(tt.want))
			}

			for i := range tt.want {
				// Exact float comparison is fine here: both sides come from
				// the same decimal text through the same conversion, so any
				// difference means the wrong field was read, not that rounding
				// drifted.
				if tt.got[i] != tt.want[i] {
					t.Errorf("%s[%d] = %v, want %v", tt.name, i, tt.got[i], tt.want[i])
				}
			}
		})
	}

	t.Run("OpenTimes", func(t *testing.T) {
		t.Parallel()

		times := OpenTimes(klines)
		if len(times) != len(klines) {
			t.Fatalf("OpenTimes returned %d values, want %d", len(times), len(klines))
		}
		for i, got := range times {
			if !got.Equal(klines[i].OpenTime) {
				t.Errorf("OpenTimes[%d] = %v, want %v", i, got, klines[i].OpenTime)
			}
		}
	})
}

// TestColumnHelpersOnEmptyInput checks the boundary every column helper shares.
// A nil slice is a perfectly ordinary value in Go — len(nil) is 0 and ranging
// over it iterates zero times — so this must return an empty slice rather than
// panic.
func TestColumnHelpersOnEmptyInput(t *testing.T) {
	t.Parallel()

	if got := Closes(nil); len(got) != 0 {
		t.Errorf("Closes(nil) returned %d values, want 0", len(got))
	}
	if got := OpenTimes([]Kline{}); len(got) != 0 {
		t.Errorf("OpenTimes(empty) returned %d values, want 0", len(got))
	}
}
