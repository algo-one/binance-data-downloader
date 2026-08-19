package binancedata

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Symbol length bounds, used as a sanity check rather than a guest list.
//
// The upper bound is Binance's own documented cap: their API describes a symbol
// as matching ^[A-Z0-9-_.]{1,20}$. The lower bound is this library's, and it is
// deliberately loose — the shortest spot pair in practice is six characters
// (ETHBTC), but a floor of three catches the cases actually worth catching, an
// empty string and a lone separator, without pretending to know which pairs
// Binance will list next year.
//
// Whether a symbol *exists* is not knowable here and is not guessed at:
// [ErrNotAvailable], produced by a 404 from Binance, is the answer to that.
const (
	minSymbolLength = 3
	maxSymbolLength = 20
)

// NormalizeSymbol converts the ways a trading pair is commonly written into the
// single form Binance uses in URLs and file names. All three of "BTC/USDT",
// "BTC-USDT" and "btcusdt" become "BTCUSDT".
//
// Surrounding whitespace is trimmed, the separators "/" and "-" are removed,
// and ASCII letters are upper-cased. Anything that survives that and is still
// not an ASCII letter or digit is an error, as is a result outside
// [minSymbolLength]..[maxSymbolLength]. The returned error wraps
// [ErrInvalidRequest].
//
// Normalising early matters for more than tidiness. The symbol becomes part of
// a URL, part of a cache path and part of a cache key, so two spellings of one
// pair that reach those layers unnormalised produce two cache entries, two
// downloads, and a "why is my cache twice the size" question much later.
//
// An underscore is *not* stripped, though it is a plausible separator. Binance
// delivery-futures contracts are named BTCUSDT_240329, so stripping it would
// corrupt a real symbol the moment futures support arrives. It is rejected
// today because spot symbols never contain one; that rejection is the single
// line to relax at the futures extension point.
func NormalizeSymbol(s string) (string, error) {
	// Keep the caller's original spelling for error messages. By the time a
	// problem is detected the working copy has been rewritten, and reporting
	// "symbol \"BTCUSDT\" is invalid" when the user typed "btc usdt" is worse
	// than useless.
	original := s

	// A Go string is an immutable, read-only slice of bytes that happens to
	// hold UTF-8. TrimSpace returns a new string sharing the same underlying
	// bytes — no copy, because nothing can write through either view.
	s = strings.TrimSpace(s)

	// strings.Builder accumulates a string without the quadratic cost of
	// repeated concatenation: `out += string(c)` in a loop allocates a fresh
	// string every iteration, because strings cannot be modified in place.
	// Builder appends into a growable byte slice and hands over the result at
	// the end without copying it again.
	var b strings.Builder
	b.Grow(len(s)) // one allocation; the result is never longer than the input

	// Indexing a string yields a *byte*, not a character — s[0] of "€" is
	// 0xE2. That would be a bug if this loop had to reason about characters,
	// but it does not, and byte-wise is exactly right here: every byte of a
	// multi-byte UTF-8 sequence is >= 0x80, so no part of any non-ASCII
	// character can masquerade as an ASCII letter or digit. Non-ASCII input
	// therefore falls through to the default case and is rejected, one byte at
	// a time, with no decoding needed.
	//
	// `for i := 0; i < len(s); i++` rather than `for _, r := range s` because
	// range over a string decodes UTF-8 into runes, which is work this loop
	// does not want done.
	for i := 0; i < len(s); i++ {
		c := s[i]

		switch {
		case c == '/' || c == '-':
			// A separator the caller wrote for readability. Drop it.

		case c >= 'a' && c <= 'z':
			// Upper-case by arithmetic rather than strings.ToUpper. ToUpper
			// applies Unicode case rules, and Unicode case rules have edge
			// cases that do not belong anywhere near a symbol: the Turkish
			// dotless 'ı' upper-cases to 'I' under some rules and not others,
			// and the Kelvin sign 'K' lower-cases to an ASCII 'k'. Restricting
			// the operation to ASCII removes the entire question.
			//
			// 'a' - 'A' is 32; subtracting it turns any ASCII lower-case
			// letter into its upper-case form. Character literals in Go are
			// untyped integer constants, so this is ordinary arithmetic.
			b.WriteByte(c - ('a' - 'A'))

		case (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'):
			b.WriteByte(c)

		default:
			// Spaces inside the symbol, punctuation, underscores, and every
			// byte of every non-ASCII character land here.
			//
			// The message decodes the full character even though the loop
			// does not, because the byte on its own makes a terrible error.
			// The realistic way to reach this case is pasting a symbol whose
			// hyphen is really an en-dash, and reporting the first byte of
			// U+2013 as the character 'â' would send the reader looking for a
			// problem that is not there. Decoding costs nothing here: this is
			// the path where the function is about to return an error anyway.
			r, _ := utf8.DecodeRuneInString(s[i:])

			return "", fmt.Errorf("symbol %q: unexpected character %q: %w", original, r, ErrInvalidRequest)
		}
	}

	normalized := b.String()

	// Length is checked after normalisation, not before, so that "BTC/USDT"
	// (8 characters) and "BTCUSDT" (7) are judged as the same symbol.
	if len(normalized) < minSymbolLength || len(normalized) > maxSymbolLength {
		return "", fmt.Errorf(
			"symbol %q: normalised to %q, which is not between %d and %d characters: %w",
			original, normalized, minSymbolLength, maxSymbolLength, ErrInvalidRequest,
		)
	}

	return normalized, nil
}

// checkNormalizedSymbol reports whether s is already exactly what
// [NormalizeSymbol] would produce, and is what the path builders assert rather
// than normalising for themselves.
//
// The distinction between checking and normalising is the whole point. A
// function that quietly normalised would accept "BTC/USDT" here and "BTCUSDT"
// there, and the two spellings would go on to produce two cache entries for one
// pair — the exact outcome [NormalizeSymbol]'s own doc comment says normalising
// early exists to prevent. Normalising happens once, at the edge, in
// [Request.resolve]; everything downstream states that it has already happened
// and fails loudly if it has not.
//
// The check earns its place because the failure it catches is silent. path.Join
// drops empty segments and cleans away "..", so an unchecked symbol does not
// produce a malformed key — it produces a well-formed key naming a *different*
// object, which 404s and is then reported as "Binance has not published this",
// a claim about the calendar standing in for a bug in the caller.
func checkNormalizedSymbol(s string) error {
	normalized, err := NormalizeSymbol(s)
	if err != nil {
		return err // already wraps ErrInvalidRequest and names the symbol
	}

	if normalized != s {
		return fmt.Errorf(
			"symbol %q is not normalised (want %q): %w", s, normalized, ErrInvalidRequest)
	}

	return nil
}
