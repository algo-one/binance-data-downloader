package binancedata

import (
	"errors"
	"testing"
)

// TestMarketZeroValue pins the convention this package applies to every enum:
// an unset field is invalid, so a caller must name the market rather than
// inherit one. Spot being the only market today is exactly why this is worth
// asserting — the day futures is added, requests written before it existed must
// not silently keep meaning spot.
func TestMarketZeroValue(t *testing.T) {
	t.Parallel()

	var unset Market
	if unset == MarketSpot {
		t.Error("the zero value of Market must not be MarketSpot; callers must choose")
	}
	if unset.IsValid() {
		t.Error("the zero value of Market must not be valid")
	}
}

func TestMarketString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		market Market
		want   string
	}{
		{name: "spot", market: MarketSpot, want: "spot"},
		{name: "the invalid zero value prints its number", market: Market(0), want: "Market(0)"},
		{name: "unsupported value prints its number", market: Market(9), want: "Market(9)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.market.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseMarket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Market
		wantErr bool
	}{
		{name: "spot", input: "spot", want: MarketSpot},
		{name: "empty string", input: "", wantErr: true},
		{name: "case matters", input: "Spot", wantErr: true},
		{name: "surrounding whitespace is not trimmed", input: " spot", wantErr: true},

		// Not supported yet. When futures arrives these become passing cases
		// rather than new ones, which is the point of listing them now.
		{name: "usd-m futures is not supported yet", input: "futures/um", wantErr: true},
		{name: "coin-m futures is not supported yet", input: "futures/cm", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseMarket(tt.input)

			if tt.wantErr {
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("ParseMarket(%q) error = %v, want one wrapping ErrInvalidRequest", tt.input, err)
				}

				// A failed parse must not return a usable market alongside
				// the error. This only holds because the zero value is
				// invalid — it would have been untestable when the zero value
				// was MarketSpot.
				if got.IsValid() {
					t.Errorf("ParseMarket(%q) = %v with an error; want the invalid zero value", tt.input, got)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseMarket(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseMarket(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestMarketRoundTrip closes the loop between the two directions: whatever
// String produces, ParseMarket must accept. Keeping those two in agreement is
// what lets a log line be pasted back into a --market flag.
func TestMarketRoundTrip(t *testing.T) {
	t.Parallel()

	got, err := ParseMarket(MarketSpot.String())
	if err != nil {
		t.Fatalf("ParseMarket(MarketSpot.String()): %v", err)
	}
	if got != MarketSpot {
		t.Errorf("round trip produced %v, want %v", got, MarketSpot)
	}
}

func TestMarketTextMarshalling(t *testing.T) {
	t.Parallel()

	t.Run("round trip through text", func(t *testing.T) {
		t.Parallel()

		text, err := MarketSpot.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(): %v", err)
		}

		var got Market
		if err := got.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if got != MarketSpot {
			t.Errorf("round trip produced %v, want %v", got, MarketSpot)
		}
	})

	t.Run("marshalling an unsupported market fails", func(t *testing.T) {
		t.Parallel()

		if _, err := Market(9).MarshalText(); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("MarshalText() on Market(9): error = %v, want one wrapping ErrInvalidRequest", err)
		}
	})

	t.Run("unmarshalling rejects unknown text and leaves the target alone", func(t *testing.T) {
		t.Parallel()

		got := MarketSpot
		if err := got.UnmarshalText([]byte("options")); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("UnmarshalText(\"options\"): error = %v, want one wrapping ErrInvalidRequest", err)
		}
		if got != MarketSpot {
			t.Errorf("failed UnmarshalText overwrote the target: got %v", got)
		}
	})
}

func TestDataType(t *testing.T) {
	t.Parallel()

	// Same convention as Market and Interval: the zero value is invalid.
	var unset DataType
	if unset == DataTypeKlines {
		t.Error("the zero value of DataType must not be DataTypeKlines")
	}
	if unset.IsValid() {
		t.Error("the zero value of DataType must not be valid")
	}

	// The string is also the path segment data.binance.vision uses, so a typo
	// here would produce a 404 rather than a compile error.
	if got, want := DataTypeKlines.String(), "klines"; got != want {
		t.Errorf("DataTypeKlines.String() = %q, want %q", got, want)
	}

	if DataType(9).IsValid() {
		t.Error("DataType(9) must not be valid")
	}
	if got, want := DataType(9).String(), "DataType(9)"; got != want {
		t.Errorf("DataType(9).String() = %q, want %q", got, want)
	}
}
