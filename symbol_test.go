package binancedata

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeSymbol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		// The three spellings docs/cli.md promises to accept.
		{name: "already normalised", input: "BTCUSDT", want: "BTCUSDT"},
		{name: "slash separator", input: "BTC/USDT", want: "BTCUSDT"},
		{name: "hyphen separator", input: "BTC-USDT", want: "BTCUSDT"},

		{name: "lowercase", input: "btcusdt", want: "BTCUSDT"},
		{name: "lowercase with separator", input: "btc/usdt", want: "BTCUSDT"},
		{name: "mixed case", input: "BtC-uSdT", want: "BTCUSDT"},

		{name: "surrounding whitespace is trimmed", input: "  BTC/USDT\n", want: "BTCUSDT"},
		{name: "tabs are trimmed", input: "\tETHBTC\t", want: "ETHBTC"},

		// Real symbols that start with a digit, which a letters-only rule
		// would have rejected.
		{name: "leading digits", input: "1INCH/USDT", want: "1INCHUSDT"},
		{name: "thousand-multiplier symbol", input: "1000SATS/USDT", want: "1000SATSUSDT"},

		{name: "non-usdt quote asset", input: "eth/btc", want: "ETHBTC"},
		{name: "at the maximum length", input: strings.Repeat("A", maxSymbolLength), want: strings.Repeat("A", maxSymbolLength)},

		{name: "empty string", input: "", wantErr: true},
		{name: "whitespace only", input: "   ", wantErr: true},
		{name: "separator only", input: "/", wantErr: true},
		{name: "too short after normalising", input: "A/B", wantErr: true},
		{name: "over the maximum length", input: strings.Repeat("A", maxSymbolLength+1), wantErr: true},

		{name: "inner whitespace", input: "BTC USDT", wantErr: true},
		{name: "dot separator is not accepted", input: "BTC.USDT", wantErr: true},

		// Not stripped, and not accepted. Binance delivery futures are named
		// BTCUSDT_240329, so an underscore is a real part of a real symbol
		// rather than a separator to discard — see the note in symbol.go.
		{name: "underscore is rejected rather than stripped", input: "BTC_USDT", wantErr: true},

		// Non-ASCII input. The second case is the trap the byte-wise loop
		// exists for: U+FF22 FULLWIDTH LATIN CAPITAL LETTER B looks like a B
		// and must not be treated as one.
		{name: "non-ascii", input: "BTC€USDT", wantErr: true},
		{name: "en-dash is not a hyphen", input: "BTC–USDT", wantErr: true},
		{name: "fullwidth letters are not ascii letters", input: "ＢＴＣUSDT", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NormalizeSymbol(tt.input)

			if tt.wantErr {
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("NormalizeSymbol(%q) error = %v, want one wrapping ErrInvalidRequest", tt.input, err)
				}
				if got != "" {
					t.Errorf("NormalizeSymbol(%q) = %q with an error; want the empty string", tt.input, got)
				}

				return
			}

			if err != nil {
				t.Fatalf("NormalizeSymbol(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("NormalizeSymbol(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestNormalizeSymbolIsIdempotent checks that normalising an already-normalised
// symbol changes nothing. It matters because the normalised form becomes part
// of a cache path: if a second pass could alter it, the same request could land
// in two different cache directories depending on how many layers had touched
// it.
func TestNormalizeSymbolIsIdempotent(t *testing.T) {
	t.Parallel()

	inputs := []string{"BTC/USDT", "btcusdt", "1000SATS-USDT", " eth/btc "}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			once, err := NormalizeSymbol(input)
			if err != nil {
				t.Fatalf("NormalizeSymbol(%q): %v", input, err)
			}

			twice, err := NormalizeSymbol(once)
			if err != nil {
				t.Fatalf("NormalizeSymbol(%q) on the second pass: %v", once, err)
			}

			if once != twice {
				t.Errorf("not idempotent: %q then %q", once, twice)
			}
		})
	}
}

// TestNormalizeSymbolErrorMentionsOriginal checks that a rejection quotes what
// the caller actually wrote. Reporting the half-rewritten working copy instead
// would make a message about a typo unrecognisable to the person who made it.
func TestNormalizeSymbolErrorMentionsOriginal(t *testing.T) {
	t.Parallel()

	const input = "btc usdt"

	_, err := NormalizeSymbol(input)
	if err == nil {
		t.Fatalf("NormalizeSymbol(%q) unexpectedly succeeded", input)
	}

	if !strings.Contains(err.Error(), input) {
		t.Errorf("error %q does not mention the original input %q", err, input)
	}
}
