package binancedata

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// sentinels is every error this package exports, paired with its name so a
// failure message says which one broke.
//
// Declared at package scope rather than inside a test because both tests below
// walk it. Note that it is a slice of an anonymous struct type: the type exists
// only to give the two fields names, so it is never worth naming.
var sentinels = []struct {
	name string
	err  error
}{
	{name: "ErrInvalidRequest", err: ErrInvalidRequest},
	{name: "ErrNotAvailable", err: ErrNotAvailable},
	{name: "ErrChecksum", err: ErrChecksum},
	{name: "ErrCorruptArchive", err: ErrCorruptArchive},
	{name: "ErrRateLimited", err: ErrRateLimited},
}

// TestSentinelsAreDistinct checks that no two sentinels are the same value.
//
// This is not as paranoid as it looks. A sentinel declared as
// `ErrChecksum = ErrCorruptArchive` — a plausible copy-paste — compiles, passes
// vet, and makes errors.Is(err, ErrChecksum) return true for a corrupt archive.
// Every branch that distinguishes the two then silently takes the wrong one.
func TestSentinelsAreDistinct(t *testing.T) {
	t.Parallel()

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}

			// errors.Is in both directions, because that is how callers will
			// actually ask, and it catches an alias regardless of which of the
			// two was written first.
			if errors.Is(a.err, b.err) {
				t.Errorf("errors.Is(%s, %s) is true; the two sentinels are the same value", a.name, b.name)
			}
		}
	}
}

// TestSentinelsSurviveWrapping is the property the whole error model rests on:
// context can be added at every layer and the original condition is still
// recognisable at the top.
func TestSentinelsSurviveWrapping(t *testing.T) {
	t.Parallel()

	for _, sentinel := range sentinels {
		t.Run(sentinel.name, func(t *testing.T) {
			t.Parallel()

			if sentinel.err.Error() == "" {
				t.Fatal("sentinel has an empty message")
			}

			// Wrap twice, as the real call stack does: once where the problem
			// is detected, once where the caller adds its own context.
			inner := fmt.Errorf("BTCUSDT-1h-2024-01.zip: %w", sentinel.err)
			outer := fmt.Errorf("fetch BTC/USDT 1h: %w", inner)

			if !errors.Is(outer, sentinel.err) {
				t.Errorf("errors.Is failed to find %s through two layers of wrapping", sentinel.name)
			}

			// The context must actually reach the message, or wrapping is
			// costing an allocation and buying nothing.
			const wantFragment = "BTCUSDT-1h-2024-01.zip"
			if got := outer.Error(); !strings.Contains(got, wantFragment) {
				t.Errorf("wrapped message %q does not contain %q", got, wantFragment)
			}

			// The trap this project's linter config exists to prevent: == sees
			// only the outermost error and reports false, so a caller using it
			// would treat a recognised condition as an unknown failure.
			//
			//nolint:errorlint // demonstrating the failure mode is the point
			if outer == sentinel.err {
				t.Error("precondition failed: == unexpectedly matched, so this test proves nothing")
			}
		})
	}
}
