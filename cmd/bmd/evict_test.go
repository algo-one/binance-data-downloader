package main

// Tests for `bmd evict`.
//
// What this command does is translate flags into a selection, so most of these
// assert on the EvictOptions that reached the library rather than on files.
// That is the failure mode worth guarding: a -before that parsed and was
// quietly dropped deletes more than the person asked for, and no assertion
// about the output would notice — the run would look like a success and the
// receipt would list exactly what it deleted.

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// evictResults builds n results a fake can yield, one entry each.
func evictResults(t *testing.T, names ...string) []binancedata.EvictResult {
	t.Helper()

	out := make([]binancedata.EvictResult, 0, len(names))

	for _, name := range names {
		out = append(out, binancedata.EvictResult{
			Name:     name,
			Symbol:   "BTCUSDT",
			Interval: binancedata.Interval1h,
			Period:   mustDate(t, "2024-01-15"),
			Files:    []string{name + ".zip", name + ".zip.CHECKSUM", name + ".parquet"},
			Size:     1024,
			Removed:  true,
		})
	}

	return out
}

// TestEvictRefusesToRunWithNoSelector is the guard that matters most, because
// the mistake it catches is unrecoverable.
//
// A bare `bmd evict` has one tempting reading — "evict everything" — and that
// reading turns a mistyped command into a deleted cache. So it is a usage
// error, and removing everything has a spelling nobody arrives at by accident.
func TestEvictRefusesToRunWithNoSelector(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"evict"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("bare `bmd evict` returned nil, want a usage error")
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}

	if !strings.Contains(err.Error(), "-all") {
		t.Errorf("error = %v, want it to name the flag that does mean everything", err)
	}

	if f.loaders != 0 {
		t.Error("built a loader before rejecting the command line")
	}
}

// TestEvictRefusesAllWithAFilter: a command saying both "everything" and "only
// these" has two readings, and neither is safe to guess at when the answer is
// what gets deleted.
func TestEvictRefusesAllWithAFilter(t *testing.T) {
	tests := [][]string{
		{"evict", "-all", "-symbol", "BTC/USDT"},
		{"evict", "-all", "-interval", "1h"},
		{"evict", "-all", "-before", "2024-01-01"},
	}

	for _, args := range tests {
		t.Run(strings.Join(args[1:], " "), func(t *testing.T) {
			f := &fakeLoader{}
			f.install(t)

			var stdout, stderr bytes.Buffer

			err := run(t.Context(), args, &stdout, &stderr)
			if err == nil {
				t.Fatal("run returned nil, want a usage error")
			}

			if got := report(err, &bytes.Buffer{}); got != exitUsage {
				t.Errorf("exit status = %d, want %d", got, exitUsage)
			}

			if f.loaders != 0 {
				t.Error("built a loader before rejecting the command line")
			}
		})
	}
}

// TestEvictPassesTheSelectionThrough is the test the command exists to pass.
//
// Every flag is checked against the option it becomes, because each one that
// failed to arrive would widen the deletion rather than narrow it: options the
// library never received are filters that were never applied.
func TestEvictPassesTheSelectionThrough(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want binancedata.EvictOptions
	}{
		{
			name: "one symbol",
			args: []string{"-symbol", "BTC/USDT"},
			want: binancedata.EvictOptions{Symbols: []string{"BTCUSDT"}},
		},
		{
			name: "symbols and intervals as lists",
			args: []string{"-symbol", "BTC/USDT,ETH/USDT", "-interval", "1h,1d"},
			want: binancedata.EvictOptions{
				Symbols:   []string{"BTCUSDT", "ETHUSDT"},
				Intervals: []binancedata.Interval{binancedata.Interval1h, binancedata.Interval1d},
			},
		},
		{
			name: "repeated flags",
			args: []string{"-symbol", "BTC/USDT", "-symbol", "ETH/USDT"},
			want: binancedata.EvictOptions{Symbols: []string{"BTCUSDT", "ETHUSDT"}},
		},
		{
			name: "duplicates collapse",
			args: []string{"-symbol", "BTC/USDT,BTCUSDT", "-interval", "1mo,1M"},
			want: binancedata.EvictOptions{
				Symbols:   []string{"BTCUSDT"},
				Intervals: []binancedata.Interval{binancedata.Interval1mo},
			},
		},
		{
			name: "everything",
			args: []string{"-all"},
			want: binancedata.EvictOptions{All: true},
		},
		{
			name: "a dry run",
			args: []string{"-all", "-n"},
			want: binancedata.EvictOptions{All: true, DryRun: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{}
			f.install(t)

			var stdout, stderr bytes.Buffer

			if err := run(t.Context(), append([]string{"evict"}, tt.args...), &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}

			got := f.gotEvict

			if !equalStrings(got.Symbols, tt.want.Symbols) {
				t.Errorf("Symbols = %v, want %v", got.Symbols, tt.want.Symbols)
			}

			if len(got.Intervals) != len(tt.want.Intervals) {
				t.Errorf("Intervals = %v, want %v", got.Intervals, tt.want.Intervals)
			} else {
				for i := range got.Intervals {
					if got.Intervals[i] != tt.want.Intervals[i] {
						t.Errorf("Intervals = %v, want %v", got.Intervals, tt.want.Intervals)

						break
					}
				}
			}

			if got.All != tt.want.All {
				t.Errorf("All = %v, want %v", got.All, tt.want.All)
			}

			if got.DryRun != tt.want.DryRun {
				t.Errorf("DryRun = %v, want %v", got.DryRun, tt.want.DryRun)
			}
		})
	}
}

// TestEvictBeforeIsMidnightNotEndOfDay is the one place `bmd evict` must not
// copy `bmd download`, and getting it wrong would delete a day nobody asked to
// lose.
//
// -end names the last instant you want, so a bare date there is stretched to
// 23:59:59.999999999. -before names the first instant you do *not* want, so a
// bare date is midnight: `-before 2024-01-01` removes 2023 and keeps every
// candle of 2024. Reusing the -end expansion here would push the bound to the
// end of the 1st and take the 1st of January with it.
func TestEvictBeforeIsMidnightNotEndOfDay(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{"evict", "-before", "2024-01-01"}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	want := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	if got := f.gotEvict.Before; !got.Equal(want) {
		t.Errorf("Before = %s, want %s — midnight, not the end of the day",
			got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

// TestEvictBeforeAcceptsATimestamp: somebody who wrote the instant out has said
// which one they mean.
func TestEvictBeforeAcceptsATimestamp(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{"evict", "-before", "2024-01-15T06:00:00Z"}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	want := time.Date(2024, time.January, 15, 6, 0, 0, 0, time.UTC)
	if got := f.gotEvict.Before; !got.Equal(want) {
		t.Errorf("Before = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

// TestEvictRejectsBadFlagValues, each as a usage error naming the flag that was
// typed — and with nothing deleted, since the loader is never built.
func TestEvictRejectsBadFlagValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"a date that will not parse", []string{"-before", "last tuesday"}, "-before"},
		{"a malformed symbol", []string{"-symbol", "BTC USDT"}, "-symbol"},
		{"an interval Binance does not publish", []string{"-interval", "7h"}, "-interval"},
		{"an empty symbol flag", []string{"-symbol", ""}, "names no symbol"},
		{"an empty interval flag", []string{"-interval", ""}, "names no interval"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{}
			f.install(t)

			var stdout, stderr bytes.Buffer

			err := run(t.Context(), append([]string{"evict"}, tt.args...), &stdout, &stderr)
			if err == nil {
				t.Fatalf("run returned nil for %v, want a usage error", tt.args)
			}

			if got := report(err, &bytes.Buffer{}); got != exitUsage {
				t.Errorf("exit status = %d, want %d", got, exitUsage)
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}

			if f.loaders != 0 {
				t.Error("built a loader before rejecting the command line")
			}
		})
	}
}

// TestEvictPrintsAReceipt. Each evicted entry is named on stdout, which is
// where `bmd evict` deliberately differs from `bmd prune`: a prune prints only
// its exceptions because its removals cost nothing, and here every line is data
// that is gone.
func TestEvictPrintsAReceipt(t *testing.T) {
	f := &fakeLoader{evicted: evictResults(t, "BTCUSDT-1h-2024-01-15", "BTCUSDT-1h-2024-01-16")}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"evict", "-all"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, want := range []string{"BTCUSDT-1h-2024-01-15", "BTCUSDT-1h-2024-01-16"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout = %q, want it to name %q", stdout.String(), want)
		}
	}

	if want := "evicted 2 entries, freed 2.0 KB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestEvictDryRunSaysWould. A dry run and a real one otherwise print the same
// sentence, and somebody who has just deleted 3 GB should never have to re-read
// their own command line to find out whether they really did.
func TestEvictDryRunSaysWould(t *testing.T) {
	results := evictResults(t, "BTCUSDT-1h-2024-01-15")
	results[0].Removed = false // what a dry run reports

	f := &fakeLoader{evicted: results}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"evict", "-all", "-n"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if want := "would evict 1 entry, freeing 1.0 KB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}

	if strings.Contains(stderr.String(), "evicted 1") {
		t.Errorf("stderr = %q, want the conditional tense for a dry run", stderr.String())
	}
}

// TestEvictCountsFromTheVerdictNotTheAction is the trap in the result shape,
// and it is the same one `bmd prune` documents: Removed is false throughout a
// dry run, so a count taken from it would report that a dry run would free
// nothing — which is the one number the flag exists to produce.
func TestEvictCountsFromTheVerdictNotTheAction(t *testing.T) {
	results := evictResults(t, "a", "b", "c")
	for i := range results {
		results[i].Removed = false
	}

	f := &fakeLoader{evicted: results}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"evict", "-all", "-n"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if want := "would evict 3 entries"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestEvictReportsNothingMatched. An eviction that selected nothing is a real
// answer and deserves a sentence: silence reads as a command that did not run.
func TestEvictReportsNothingMatched(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"evict", "-symbol", "SOL/USDT"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if want := "nothing matched"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestEvictExitsNonZeroWhenAnEntryWillNotGo, having still reported the ones
// that did. A failure here means files left on disk that the summary would
// otherwise have counted as reclaimed.
func TestEvictExitsNonZeroWhenAnEntryWillNotGo(t *testing.T) {
	results := evictResults(t, "BTCUSDT-1h-2024-01-15", "BTCUSDT-1h-2024-01-16")
	results[0].Removed = false
	results[0].Err = errors.New("permission denied")
	results[0].Size = 0

	f := &fakeLoader{evicted: results}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"evict", "-all"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want an error so the process exits non-zero")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}

	if !strings.Contains(stdout.String(), "permission denied") {
		t.Errorf("stdout = %q, want the failure named beside its entry", stdout.String())
	}

	// The entry that did go is still counted, and the one that did not is not.
	if want := "evicted 1 entry, freed 1.0 KB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestEvictReturnsTheWalksOwnError. An unreadable cache directory ends the run,
// and must not be reported as a clean eviction that happened to find nothing.
func TestEvictReturnsTheWalksOwnError(t *testing.T) {
	want := errors.New("opening the cache: permission denied")

	f := &fakeLoader{evictErr: want}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"evict", "-all"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the walk's error")
	}

	if !errors.Is(err, want) {
		t.Errorf("error = %v, want it to wrap the walk's own error", err)
	}

	if strings.Contains(stderr.String(), "nothing matched") {
		t.Errorf("stderr = %q, want no summary for a walk that failed", stderr.String())
	}
}

// TestEvictQuietPrintsTheReceiptAndNotTheSummary. -quiet says "nothing but
// errors" on stderr, and the receipt is on stdout, so it survives: a caller
// piping the list of what was deleted into a file gets it either way.
func TestEvictQuietPrintsTheReceiptAndNotTheSummary(t *testing.T) {
	f := &fakeLoader{evicted: evictResults(t, "BTCUSDT-1h-2024-01-15")}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"evict", "-all", "-quiet"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "BTCUSDT-1h-2024-01-15") {
		t.Errorf("stdout = %q, want the receipt", stdout.String())
	}

	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want nothing under -quiet", stderr.String())
	}
}

// TestEvictRejectsAPositionalArgument, like every other command: every value is
// given with a flag, and a bare word is a flag somebody forgot to name.
func TestEvictRejectsAPositionalArgument(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"evict", "-all", "BTCUSDT"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil for a positional argument, want a usage error")
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}
}

// TestPluralHandlesEntries is a unit test on the helper, because "entrys" is
// the kind of thing that reads as a bug in the program rather than a slip in
// its grammar — and because the rule it now carries is more than the one word
// that prompted it.
func TestPluralHandlesEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    int
		word string
		want string
	}{
		{1, "entry", "entry"},
		{2, "entry", "entries"},
		{0, "entry", "entries"},
		{1, "symbol", "symbol"},
		{2, "symbol", "symbols"},
		{2, "archive", "archives"},
		{2, "day", "days"},
		{2, "y", "ys"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			if got := plural(tt.n, tt.word); got != tt.want {
				t.Errorf("plural(%d, %q) = %q, want %q", tt.n, tt.word, got, tt.want)
			}
		})
	}
}
