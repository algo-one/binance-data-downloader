package main

// Tests for `bmd download` with more than one interval, and for the cross
// product of the two lists.
//
// These are multisymbol_test.go's counterpart along the other axis, and they
// exist for the same reason rather than for symmetry. Binance enforces
// REQUEST_WEIGHT per IP address and the limiter honouring it is process-wide,
// so `for iv in 1m 1h 1d; do bmd download -interval $iv ...; done` is three
// limiters against a ceiling built for one — the exact shape a list of symbols
// was added to remove, reached by varying the other flag.
//
// What is asserted here is therefore the same three things: that the requests
// go through one loader in one process, that every pair is downloaded, and that
// the output a person reads names whichever of the two actually varies.

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// pairsOf lists the (symbol, interval) pairs a fake was asked for, in call
// order, rendered the way the command's own output lines render them.
func pairsOf(f *fakeLoader) []string {
	out := make([]string, 0, len(f.gotRequests))

	for _, req := range f.gotRequests {
		out = append(out, req.Symbol+" "+req.Interval.String())
	}

	return out
}

// TestDownloadSeveralIntervalsUsesOneLoader is the test this feature exists
// for, and the one that would still fail if every other test here passed.
//
// It is deliberately the same assertion TestDownloadSeveralSymbolsUsesOneLoader
// makes, because it is the same requirement: the rate limiter is built once per
// process by sync.OnceValue and shared by everything going through it, since
// the quota it respects is enforced per IP. Three intervals fetched by three
// processes would pass every test about files and candles and would still be
// 120 weight per second against a ceiling of 100.
func TestDownloadSeveralIntervalsUsesOneLoader(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT", "-interval", "1m,1h,1d",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", dir,
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if f.loaders != 1 {
		t.Errorf("built %d loaders for 3 intervals, want 1 — each one carries its own rate limiter", f.loaders)
	}

	if got, want := pairsOf(f), []string{"BTCUSDT 1m", "BTCUSDT 1h", "BTCUSDT 1d"}; !equalStrings(got, want) {
		t.Errorf("streamed %v, want %v", got, want)
	}
}

// TestDownloadCrossesSymbolsWithIntervals pins both halves of the cross
// product: that every pair is downloaded, and the order they come in.
//
// Every pair, because the alternative readings both lose data silently. Pairing
// the lists off positionally would need them to be the same length, and would
// turn `-symbol BTC/USDT,ETH/USDT -interval 1h` into one download rather than
// two.
//
// Symbol-major, because that is what makes a directory of the results readable:
// a symbol's intervals sit together, and the progress display finishes one
// symbol before starting the next. The order is asserted rather than sorted for
// the assertion, since it is the property, not an implementation detail.
func TestDownloadCrossesSymbolsWithIntervals(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT,ETH/USDT", "-interval", "1h,1d",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", dir,
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	want := []string{"BTCUSDT 1h", "BTCUSDT 1d", "ETHUSDT 1h", "ETHUSDT 1d"}
	if got := pairsOf(f); !equalStrings(got, want) {
		t.Errorf("streamed %v, want %v", got, want)
	}
}

// TestDownloadWritesOneFilePerPair: the generated name already carried the
// interval, so this is not a change so much as a property that only now has a
// way to be violated. Two intervals of one symbol landing on one name would
// mean two downloads writing the same path, each through its own temporary
// file, with the second rename silently replacing the first's work.
func TestDownloadWritesOneFilePerPair(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 2)}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT,ETH/USDT", "-interval", "1h,1d",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", dir,
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, name := range []string{
		"BTCUSDT-1h-2024-01-15_2024-01-15.csv",
		"BTCUSDT-1d-2024-01-15_2024-01-15.csv",
		"ETHUSDT-1h-2024-01-15_2024-01-15.csv",
		"ETHUSDT-1d-2024-01-15_2024-01-15.csv",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("expected %s: %v", name, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	// Four pairs, four files: a fifth would mean a name was generated that
	// nothing above expects, and three would mean two downloads collided.
	if len(entries) != 4 {
		t.Errorf("wrote %d files, want 4", len(entries))
	}
}

// TestDownloadAcceptsBothIntervalSpellings: a list is what somebody types, and
// repetition is what a shell loop building an argument slice produces. stdlib
// flag keeps only the last occurrence unless the flag is a flag.Value, so the
// repeated spelling is the one that would silently download 1d alone.
func TestDownloadAcceptsBothIntervalSpellings(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
		want  []string
	}{
		{"comma-separated", []string{"-interval", "1h,1d"}, []string{"BTCUSDT 1h", "BTCUSDT 1d"}},
		{"repeated", []string{"-interval", "1h", "-interval", "1d"}, []string{"BTCUSDT 1h", "BTCUSDT 1d"}},
		{
			"both at once",
			[]string{"-interval", "1h", "-interval", "1d,1w"},
			[]string{"BTCUSDT 1h", "BTCUSDT 1d", "BTCUSDT 1w"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{klines: testKlines(t, 1)}
			f.install(t)

			var stdout, stderr bytes.Buffer

			args := []string{"download", "-symbol", "BTC/USDT"}
			args = append(args, tt.flags...)
			args = append(args, "-start", "2024-01-15", "-end", "2024-01-15", "-out", t.TempDir())

			if err := run(t.Context(), args, &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}

			if got := pairsOf(f); !equalStrings(got, tt.want) {
				t.Errorf("streamed %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDownloadDeduplicatesIntervals is a correctness test rather than a
// tidiness one, and the duplicate here is not contrived: Binance spells the
// monthly interval "1mo" in an archive path and "1M" in a REST parameter,
// ParseInterval accepts both on purpose, and both are therefore things a person
// has in front of them. They are one interval, they generate one file name, and
// two downloads writing that name would have the second silently replace the
// first.
func TestDownloadDeduplicatesIntervals(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 2)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT", "-interval", "1mo,1M,1mo",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", t.TempDir(),
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := pairsOf(f); !equalStrings(got, []string{"BTCUSDT 1mo"}) {
		t.Errorf("streamed %v, want one BTCUSDT 1mo — the three spellings are one interval", got)
	}
}

// TestDownloadRefusesOutputThatCannotHoldSeveralIntervals: one symbol at two
// intervals is two files, so the -out spellings that name a single stream are
// refused exactly as they are for two symbols. The count that decides this is
// of requests and not of symbols, which is the only thing that had to change.
func TestDownloadRefusesOutputThatCannotHoldSeveralIntervals(t *testing.T) {
	tests := []struct {
		name string
		out  string
	}{
		{"stdout", "-"},
		{"a single file", "candles.csv"},
		{"a directory that does not exist", "no/such/dir"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{klines: testKlines(t, 1)}
			f.install(t)

			var stdout, stderr bytes.Buffer

			args := []string{
				"download", "-symbol", "BTC/USDT", "-interval", "1h,1d",
				"-start", "2024-01-15", "-end", "2024-01-15", "-out", tt.out,
			}

			err := run(t.Context(), args, &stdout, &stderr)
			if err == nil {
				t.Fatalf("run returned nil for -out %q with two intervals, want a usage error", tt.out)
			}

			if got := report(err, &bytes.Buffer{}); got != exitUsage {
				t.Errorf("exit status = %d, want %d", got, exitUsage)
			}

			if len(f.gotRequests) != 0 {
				t.Errorf("streamed %v before rejecting the flags; nothing should be fetched", pairsOf(f))
			}
		})
	}
}

// TestDownloadAllowsASingleIntervalToStdout: the restriction above is about
// several downloads, and must not have taken away what one download could
// always do. One symbol at one interval is still one stream.
func TestDownloadAllowsASingleIntervalToStdout(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 2)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", "-",
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "open_time") {
		t.Errorf("stdout = %q, want the csv", stdout.String())
	}
}

// TestDownloadValidatesEveryIntervalBeforeFetching: a typo in the third
// interval must not be discovered after two downloads have already run.
func TestDownloadValidatesEveryIntervalBeforeFetching(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT", "-interval", "1h,1d,7h",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", t.TempDir(),
	}

	err := run(t.Context(), args, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil for an interval Binance does not publish, want a usage error")
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}

	if len(f.gotRequests) != 0 {
		t.Errorf("streamed %v before validating them all", pairsOf(f))
	}
}

// TestDownloadRejectsAnEmptyIntervalFlag is the trap checkListFlag exists for,
// checked on the flag it was generalised to cover: `-interval "$INTERVALS"`
// with the variable unset is a command that meant to name something, and
// answering it with "-interval is required" points at a flag that was given.
func TestDownloadRejectsAnEmptyIntervalFlag(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT", "-interval", "",
		"-start", "2024-01-15", "-end", "2024-01-15",
	}

	err := run(t.Context(), args, &stdout, &stderr)
	if err == nil {
		t.Fatal(`run returned nil for -interval "", want a usage error`)
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}

	if got, want := err.Error(), "names no interval"; !strings.Contains(got, want) {
		t.Errorf("error = %q, want it to contain %q rather than claiming the flag is missing", got, want)
	}
}

// TestDownloadPairsShareOneEndInstant is TestDownloadSymbolsShareOneEndInstant
// extended across the cross product. With -end left off the range ends "now",
// and reading the clock per request would give the four downloads of one
// command four different ends — so their generated file names would disagree
// about the range and a directory of them would no longer describe one
// download.
func TestDownloadPairsShareOneEndInstant(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT,ETH/USDT", "-interval", "1h,1d",
		"-start", "2024-01-15", "-out", t.TempDir(),
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(f.gotRequests) != 4 {
		t.Fatalf("streamed %d requests, want 4", len(f.gotRequests))
	}

	for _, req := range f.gotRequests[1:] {
		if !req.End.Equal(f.gotRequests[0].End) {
			t.Errorf("%s %s ended at %v, want the whole command's %v",
				req.Symbol, req.Interval, req.End, f.gotRequests[0].End)
		}
	}
}

// TestDownloadFailureNamesSymbolAndInterval covers the line a person actually
// reads when one download of several fails.
//
// Naming the symbol alone was right while the symbol was the only thing that
// varied. With three intervals of one symbol it labels all three failures
// identically, and the file that is missing is the one the *pair* identifies —
// so the line has to carry both, matching the success line, which always did.
func TestDownloadFailureNamesSymbolAndInterval(t *testing.T) {
	f := &fakeLoader{
		klines:       testKlines(t, 2),
		intervalErrs: map[binancedata.Interval]error{binancedata.Interval1d: errors.New("data not available")},
	}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT", "-interval", "1h,1d,1w",
		"-start", "2024-01-15", "-end", "2024-01-15", "-out", dir,
	}

	err := run(t.Context(), args, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want an error so the process exits non-zero")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}

	if want := "BTCUSDT 1d: data not available"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}

	// The interval after the failure was still attempted, and the two that
	// worked still produced their files.
	if got, want := pairsOf(f), []string{"BTCUSDT 1h", "BTCUSDT 1d", "BTCUSDT 1w"}; !equalStrings(got, want) {
		t.Errorf("streamed %v, want %v — a failure must not abandon the rest", got, want)
	}

	for _, name := range []string{
		"BTCUSDT-1h-2024-01-15_2024-01-15.csv",
		"BTCUSDT-1w-2024-01-15_2024-01-15.csv",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("expected %s to have been written anyway: %v", name, err)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "BTCUSDT-1d-2024-01-15_2024-01-15.csv")); err == nil {
		t.Error("the failed download left a file behind; a partial download must leave nothing")
	}
}

// TestSummaryUnit is a unit test on the choice rather than on a rendered line,
// because the choice is what is easy to get wrong and the rendering is one
// Fprintf away from it.
//
// The middle two rows are the whole reason this function exists. Both produce
// two requests, and a count of requests cannot tell them apart — only the two
// list lengths can say whether the run varied its symbols or its intervals.
func TestSummaryUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		symbols   int
		intervals int
		want      string
	}{
		{"one of each", 1, 1, "symbol"},
		{"several symbols", 3, 1, "symbol"},
		{"several intervals", 1, 3, "interval"},
		{"both", 2, 3, "download"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := summaryUnit(tt.symbols, tt.intervals); got != tt.want {
				t.Errorf("summaryUnit(%d, %d) = %q, want %q", tt.symbols, tt.intervals, got, tt.want)
			}
		})
	}
}

// TestDownloadSummaryNamesWhatVaried is the same property seen from the outside,
// through the line a person reads. Both cases below are the same number of
// downloads, which is what makes the assertion worth making twice.
func TestDownloadSummaryNamesWhatVaried(t *testing.T) {
	tests := []struct {
		name     string
		symbol   string
		interval string
		want     string
	}{
		{"several symbols", "BTC/USDT,ETH/USDT", "1h", "2 of 2 symbols, 4 candles in total"},
		{"several intervals", "BTC/USDT", "1h,1d", "2 of 2 intervals, 4 candles in total"},
		{"both", "BTC/USDT,ETH/USDT", "1h,1d", "4 of 4 downloads, 8 candles in total"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{klines: testKlines(t, 2)}
			f.install(t)

			var stdout, stderr bytes.Buffer

			args := []string{
				"download", "-symbol", tt.symbol, "-interval", tt.interval,
				"-start", "2024-01-15", "-end", "2024-01-15", "-out", t.TempDir(),
			}

			if err := run(t.Context(), args, &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}

			if !strings.Contains(stderr.String(), tt.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.want)
			}
		})
	}
}

// TestProgressLabelsWhatVaries asserts the two booleans and what they do to a
// line, for all four shapes.
//
// Both halves are checked for the same reason TestProgressLabelsLinesOnlyFor
// SeveralSymbols checks both: either alone would pass while the other was wrong
// — a renderer that ignored the flags, or a command that never set them.
//
// The third row is the case the two separate booleans exist for. One symbol at
// two intervals must label the interval and not the symbol, and a single "there
// is more than one request" flag would print both or neither.
func TestProgressLabelsWhatVaries(t *testing.T) {
	tests := []struct {
		name         string
		symbol       string
		interval     string
		wantSymbol   bool
		wantInterval bool
	}{
		{"one of each", "BTC/USDT", "1h", false, false},
		{"several symbols", "BTC/USDT,ETH/USDT", "1h", true, false},
		{"several intervals", "BTC/USDT", "1h,1d", false, true},
		{"both", "BTC/USDT,ETH/USDT", "1h,1d", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			captured := captureProgress(t)

			f := &fakeLoader{klines: testKlines(t, 1)}
			f.install(t)

			var stdout, stderr bytes.Buffer

			args := []string{
				"download", "-symbol", tt.symbol, "-interval", tt.interval,
				"-start", "2024-01-15", "-end", "2024-01-15", "-out", t.TempDir(),
			}

			if err := run(t.Context(), args, &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}

			if *captured == nil {
				t.Fatal("no progress reporter was built")
			}

			if got := (*captured).showSymbol; got != tt.wantSymbol {
				t.Errorf("showSymbol = %v, want %v", got, tt.wantSymbol)
			}

			if got := (*captured).showInterval; got != tt.wantInterval {
				t.Errorf("showInterval = %v, want %v", got, tt.wantInterval)
			}

			// And what those flags do to a line. The fake loader reports no
			// events, so the reporter is driven directly: what is under test
			// here is the rendering, not whether the library called back.
			var drawn bytes.Buffer

			p := &progress{w: &drawn, showSymbol: tt.wantSymbol, showInterval: tt.wantInterval}
			p.report(binancedata.Progress{
				Request: binancedata.Request{Symbol: "ETHUSDT", Interval: binancedata.Interval1d},
				Total:   2, Done: 1, Klines: 24,
			})

			if got := strings.Contains(drawn.String(), "ETHUSDT"); got != tt.wantSymbol {
				t.Errorf("progress line %q contains the symbol = %v, want %v",
					drawn.String(), got, tt.wantSymbol)
			}

			// "1d" rather than the Interval value, because that is what a
			// reader sees. The search is for a space-delimited "1d" so that it
			// cannot be satisfied by the "1d" inside a date or another field.
			if got := strings.Contains(drawn.String(), " 1d "); got != tt.wantInterval {
				t.Errorf("progress line %q contains the interval = %v, want %v",
					drawn.String(), got, tt.wantInterval)
			}
		})
	}
}

// captureProgress swaps in a newProgress that hands back the reporter it built,
// so a test can read the fields the command set on it.
//
// It returns a pointer to the slot rather than the reporter, because the
// reporter does not exist until run() calls newProgress.
func captureProgress(t *testing.T) **progress {
	t.Helper()

	var captured *progress

	original := newProgress

	newProgress = func(w io.Writer, quiet bool) *progress {
		captured = original(w, quiet)

		return captured
	}

	t.Cleanup(func() { newProgress = original })

	return &captured
}
