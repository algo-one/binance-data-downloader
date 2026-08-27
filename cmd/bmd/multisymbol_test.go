package main

// Tests for `bmd download` with more than one symbol.
//
// The command existed for one symbol from Stage 8. What these cover is the
// reason it now takes several: Binance enforces REQUEST_WEIGHT per IP address
// and the limiter honouring it is process-wide, so N symbols must be N requests
// inside one process rather than N processes. Every test that only checked files
// and candles would pass on a command that shelled out to itself once per
// symbol, so the shape of the run is asserted directly.

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

// symbolsOf lists the symbols a fake was asked for, in call order.
func symbolsOf(f *fakeLoader) []string {
	out := make([]string, 0, len(f.gotRequests))

	for _, req := range f.gotRequests {
		out = append(out, req.Symbol)
	}

	return out
}

// downloadArgs is the flag set the tests below share, with the symbol spelling
// and any extra flags supplied per test.
func downloadArgs(symbolFlags []string, extra ...string) []string {
	args := []string{"download"}
	args = append(args, symbolFlags...)
	args = append(args, "-interval", "1h", "-start", "2024-01-15", "-end", "2024-01-15")

	return append(args, extra...)
}

// TestDownloadSeveralSymbolsUsesOneLoader is the test this whole feature exists
// for, and the one that would still fail if every other test here passed.
//
// The rate limiter is built once per process by sync.OnceValue and is shared by
// every request through it, because the quota it respects is enforced per IP
// address. Two limiters each honouring 40 weight per second permit 80 against a
// ceiling of 100. So the requirement is not "several symbols work" but "several
// symbols go through one loader in one process", and only a count can say that.
func TestDownloadSeveralSymbolsUsesOneLoader(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT,ETH/USDT,SOL/USDT"}, "-out", dir)

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if f.loaders != 1 {
		t.Errorf("built %d loaders for 3 symbols, want 1 — each one carries its own rate limiter", f.loaders)
	}

	if got, want := symbolsOf(f), []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}; !equalStrings(got, want) {
		t.Errorf("streamed %v, want %v", got, want)
	}
}

// TestDownloadSeveralSymbolsWritesOneFilePerSymbol: each symbol gets its own
// generated name, so a directory of them says what each file holds.
func TestDownloadSeveralSymbolsWritesOneFilePerSymbol(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT,ETH/USDT"}, "-out", dir)

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, name := range []string{
		"BTCUSDT-1h-2024-01-15_2024-01-15.csv",
		"ETHUSDT-1h-2024-01-15_2024-01-15.csv",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("expected %s: %v", name, err)
		}
	}

	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want the candles in files and nothing on stdout", stdout.String())
	}
}

// TestDownloadSymbolsShareOneEndInstant covers the reason buildRequests parses
// everything but the symbol once.
//
// With -end left off, the range ends "now". Reading the clock per symbol would
// give each a slightly different end, so their generated file names would
// disagree about the range and a directory of them would no longer describe one
// download.
func TestDownloadSymbolsShareOneEndInstant(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := []string{
		"download", "-symbol", "BTC/USDT,ETH/USDT",
		"-interval", "1h", "-start", "2024-01-15", "-out", dir,
	}

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(f.gotRequests) != 2 {
		t.Fatalf("streamed %d requests, want 2", len(f.gotRequests))
	}

	if !f.gotRequests[0].End.Equal(f.gotRequests[1].End) {
		t.Errorf("the two symbols ended at %v and %v, want one instant for the whole command",
			f.gotRequests[0].End, f.gotRequests[1].End)
	}
}

// TestDownloadAcceptsBothSymbolSpellings: a list is what somebody types, and
// repetition is what a shell loop building an argument slice produces. stdlib
// flag keeps only the last occurrence unless the flag is a flag.Value, so the
// repeated spelling is the one that would silently download ETHUSDT alone.
func TestDownloadAcceptsBothSymbolSpellings(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
	}{
		{"comma-separated", []string{"-symbol", "BTC/USDT,ETH/USDT"}},
		{"repeated", []string{"-symbol", "BTC/USDT", "-symbol", "ETH/USDT"}},
		{"both at once", []string{"-symbol", "BTC/USDT", "-symbol", "ETH/USDT,SOL/USDT"}},
	}

	want := map[string][]string{
		"comma-separated": {"BTCUSDT", "ETHUSDT"},
		"repeated":        {"BTCUSDT", "ETHUSDT"},
		"both at once":    {"BTCUSDT", "ETHUSDT", "SOLUSDT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{klines: testKlines(t, 1)}
			f.install(t)

			var stdout, stderr bytes.Buffer

			if err := run(t.Context(), downloadArgs(tt.flags, "-out", t.TempDir()), &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}

			if got := symbolsOf(f); !equalStrings(got, want[tt.name]) {
				t.Errorf("streamed %v, want %v", got, want[tt.name])
			}
		})
	}
}

// TestDownloadDeduplicatesSymbols is a correctness test rather than a tidiness
// one. The three spellings below are one symbol, every symbol's file name is
// generated from the normalised form, and two downloads writing the same path
// would each write a temporary file and rename it — the second silently
// replacing the first's work.
func TestDownloadDeduplicatesSymbols(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 2)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT,BTC-USDT,btcusdt"}, "-out", t.TempDir())

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := symbolsOf(f); !equalStrings(got, []string{"BTCUSDT"}) {
		t.Errorf("streamed %v, want one BTCUSDT — the three spellings are one symbol", got)
	}
}

// TestDownloadRefusesOutputThatCannotHoldSeveral covers the -out spellings that
// name a single stream. Neither has a reading for several symbols, and both
// failure modes are silent: one file would interleave headers into nonsense, and
// honouring one symbol would drop the rest without a word.
func TestDownloadRefusesOutputThatCannotHoldSeveral(t *testing.T) {
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

			args := downloadArgs([]string{"-symbol", "BTC/USDT,ETH/USDT"}, "-out", tt.out)

			err := run(t.Context(), args, &stdout, &stderr)
			if err == nil {
				t.Fatalf("run returned nil for -out %q with two symbols, want a usage error", tt.out)
			}

			if got := report(err, &bytes.Buffer{}); got != exitUsage {
				t.Errorf("exit status = %d, want %d", got, exitUsage)
			}

			if len(f.gotRequests) != 0 {
				t.Errorf("streamed %v before rejecting the flags; nothing should be fetched", symbolsOf(f))
			}
		})
	}
}

// TestDownloadAllowsASingleSymbolToStdout: the restriction above is about
// several symbols, and must not have taken away what one symbol could always do.
func TestDownloadAllowsASingleSymbolToStdout(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 2)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), downloadArgs([]string{"-symbol", "BTC/USDT"}, "-out", "-"), &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "open_time") {
		t.Errorf("stdout = %q, want the csv", stdout.String())
	}
}

// TestDownloadContinuesAfterOneSymbolFails is the batch behaviour, and the
// argument for it: the user named these symbols in one command, and the ones
// that work are still worth having. Nothing is lost quietly — the failure is
// printed, counted, and reaches the exit status.
func TestDownloadContinuesAfterOneSymbolFails(t *testing.T) {
	f := &fakeLoader{
		klines:     testKlines(t, 2),
		streamErrs: map[string]error{"ETHUSDT": errors.New("data not available")},
	}
	f.install(t)

	dir := t.TempDir()

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT,ETH/USDT,SOL/USDT"}, "-out", dir)

	err := run(t.Context(), args, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want an error so the process exits non-zero")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}

	// The symbol after the failure was still attempted.
	if got, want := symbolsOf(f), []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}; !equalStrings(got, want) {
		t.Errorf("streamed %v, want %v — a failure must not abandon the rest", got, want)
	}

	for _, name := range []string{
		"BTCUSDT-1h-2024-01-15_2024-01-15.csv",
		"SOLUSDT-1h-2024-01-15_2024-01-15.csv",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("expected %s to have been written anyway: %v", name, err)
		}
	}

	// The failed symbol leaves nothing behind: output goes through a temporary
	// file and is renamed only once the encoder finishes.
	failedName := filepath.Join(dir, "ETHUSDT-1h-2024-01-15_2024-01-15.csv")
	if _, err := os.Stat(failedName); err == nil {
		t.Error("the failed symbol left a file behind; a partial download must leave nothing")
	}

	if !strings.Contains(stderr.String(), "ETHUSDT") {
		t.Errorf("stderr = %q, want it to name the symbol that failed", stderr.String())
	}

	if !strings.Contains(err.Error(), "1 of 3") {
		t.Errorf("error = %v, want it to count the failures against the total", err)
	}
}

// TestDownloadSingleSymbolReportsItsErrorUnchanged pins what the batch handling
// above must not have altered. With one symbol the error goes back for main to
// print, rather than being printed here and replaced by a count.
func TestDownloadSingleSymbolReportsItsErrorUnchanged(t *testing.T) {
	want := errors.New("data not available")

	f := &fakeLoader{streamErrs: map[string]error{"BTCUSDT": want}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), downloadArgs([]string{"-symbol", "BTC/USDT"}, "-out", t.TempDir()), &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the stream's error")
	}

	if !errors.Is(err, want) {
		t.Errorf("error = %v, want it to wrap the stream's own error", err)
	}

	if strings.Contains(err.Error(), "of 1 symbols") {
		t.Errorf("error = %v, want the error itself rather than a count", err)
	}
}

// TestDownloadSummarisesSeveralSymbols: with more than one symbol the per-symbol
// lines no longer say how the command as a whole went.
func TestDownloadSummarisesSeveralSymbols(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 4)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT,ETH/USDT"}, "-out", t.TempDir())

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if want := "2 of 2 symbols, 8 candles in total"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestDownloadDoesNotSummariseOneSymbol: the per-symbol line already says
// everything, and a second line repeating it is how a reader learns to skip
// both.
func TestDownloadDoesNotSummariseOneSymbol(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 4)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT"}, "-out", t.TempDir())

	if err := run(t.Context(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if strings.Contains(stderr.String(), "in total") {
		t.Errorf("stderr = %q, want no run summary for a single symbol", stderr.String())
	}
}

// TestDownloadRejectsAnEmptySymbolFlag is the trap commonFlags.options guards
// for -cache-dir, checked on -symbol: `-symbol "$SYMBOLS"` with the variable
// unset is a command that meant to name something, and accepting it would
// surface later as "-symbol is required" pointing at a flag that was given.
func TestDownloadRejectsAnEmptySymbolFlag(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), downloadArgs([]string{"-symbol", ""}), &stdout, &stderr)
	if err == nil {
		t.Fatal(`run returned nil for -symbol "", want a usage error`)
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}
}

// TestDownloadValidatesEverySymbolBeforeFetching: a typo in the fourth symbol
// must not be discovered after three downloads have already run.
//
// The malformed symbol carries a "$" rather than a doubled separator, because
// NormalizeSymbol drops "/" and "-" wherever they appear — "BTC//USDT" is a
// perfectly valid spelling of BTCUSDT, which the first draft of this test got
// wrong.
func TestDownloadValidatesEverySymbolBeforeFetching(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	args := downloadArgs([]string{"-symbol", "BTC/USDT,ETH/USDT,SOL/USDT,BTC$USDT"}, "-out", t.TempDir())

	err := run(t.Context(), args, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil for a malformed symbol, want a usage error")
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}

	if len(f.gotRequests) != 0 {
		t.Errorf("streamed %v before validating them all", symbolsOf(f))
	}
}

// TestProgressLabelsLinesOnlyForSeveralSymbols: with one symbol the name is
// already in the summary line and the file name, so a column repeating it on
// every chunk is noise. With several it is the only thing telling one [3/12]
// from another.
//
// The command's choice and the renderer's behaviour are both asserted, because
// either alone would pass while the other was wrong: a renderer that ignored
// showSymbol, or a command that never set it.
func TestProgressLabelsLinesOnlyForSeveralSymbols(t *testing.T) {
	tests := []struct {
		name       string
		symbols    string
		wantSymbol bool
	}{
		{"one symbol", "BTC/USDT", false},
		{"two symbols", "BTC/USDT,ETH/USDT", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured *progress

			original := newProgress

			newProgress = func(w io.Writer, quiet bool) *progress {
				captured = original(w, quiet)

				return captured
			}

			t.Cleanup(func() { newProgress = original })

			f := &fakeLoader{klines: testKlines(t, 1)}
			f.install(t)

			var stdout, stderr bytes.Buffer

			args := downloadArgs([]string{"-symbol", tt.symbols}, "-out", t.TempDir())

			if err := run(t.Context(), args, &stdout, &stderr); err != nil {
				t.Fatalf("run: %v", err)
			}

			if captured == nil {
				t.Fatal("no progress reporter was built")
			}

			if captured.showSymbol != tt.wantSymbol {
				t.Errorf("showSymbol = %v, want %v", captured.showSymbol, tt.wantSymbol)
			}

			// And what that flag actually does to a line. The fake loader
			// reports no events, so the reporter is driven directly: what is
			// under test here is the rendering, not whether the library called
			// back.
			var drawn bytes.Buffer

			p := &progress{w: &drawn, showSymbol: tt.wantSymbol}
			p.report(binancedata.Progress{
				Request: binancedata.Request{Symbol: "ETHUSDT"},
				Total:   2, Done: 1, Klines: 24,
			})

			if got := strings.Contains(drawn.String(), "ETHUSDT"); got != tt.wantSymbol {
				t.Errorf("progress line %q contains the symbol = %v, want %v",
					drawn.String(), got, tt.wantSymbol)
			}
		})
	}
}

// equalStrings compares two string slices. slices.Equal would do, but this file
// asserts on symbol order often enough that a named helper reads better at the
// call sites.
func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}

	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}

// TestProgressDoesNotPadTheNextSymbolsFirstLine is a unit test on the display
// itself rather than on a command, because what it asserts is a field's value
// after a call and there is no way to see that through run().
//
// The padding exists to overwrite a line that is still on screen: a redraw ends
// with "\r" and no newline, so a shorter line after a longer one would leave the
// tail of the longer one visible. done() ends that line with a newline, and
// after it there is nothing on screen to overwrite — so a width carried across
// pads the next symbol's first line out to the previous symbol's last, on a line
// that is already empty.
func TestProgressDoesNotPadTheNextSymbolsFirstLine(t *testing.T) {
	var buf bytes.Buffer

	p := &progress{w: &buf, tty: true, showSymbol: true}

	// A long line, from a symbol with many chunks and a wide candle count.
	p.report(binancedata.Progress{
		Request: binancedata.Request{Symbol: "1000SATSUSDT"},
		Done:    50, Total: 100, Source: binancedata.SourceMonthlyArchive,
		Start:  mustDate(t, "2024-01-01"),
		Klines: 44640,
	})

	wide := p.width

	p.done()

	if p.width != 0 {
		t.Errorf("width after done() = %d, want 0: the line it measured is gone", p.width)
	}

	buf.Reset()

	// The next symbol, with a shorter line.
	p.report(binancedata.Progress{
		Request: binancedata.Request{Symbol: "BNBBTC"},
		Done:    1, Total: 1, Source: binancedata.SourceDailyArchive,
		Start:  mustDate(t, "2024-01-01"),
		Klines: 24,
	})

	line := buf.String()

	if len(line) >= wide {
		t.Errorf("the next symbol's first line is %q (%d bytes), want it not padded out to the "+
			"previous symbol's %d", line, len(line), wide)
	}

	if strings.HasSuffix(line, " ") {
		t.Errorf("the next symbol's first line is %q, want no trailing padding on an empty line", line)
	}
}

// TestOneSymbolsErrorCarriesNoSymbolPrefix pins the promise docs/cli.md makes
// about the single-symbol case: "with one symbol nothing above changes ... the
// error is reported as it always was".
//
// Wrapping every per-symbol failure in "SYMBOL: " on the way into the failed
// slice breaks that, and breaks it invisibly — the multi-symbol path prints its
// own prefixed line and reads only the slice's length, so the wrap shows up
// nowhere except here, in the one error that is returned to main rather than
// printed.
func TestOneSymbolsErrorCarriesNoSymbolPrefix(t *testing.T) {
	f := &fakeLoader{streamErr: binancedata.ErrNotAvailable}
	f.install(t)

	t.Chdir(t.TempDir())

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT",
		"-interval", "1h",
		"-start", "2024-01-15",
		"-end", "2024-01-15",
		"-out", ".",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the stream failure")
	}

	if strings.HasPrefix(err.Error(), "BTCUSDT") {
		t.Errorf("error = %q, want no symbol prefix on the only symbol the user named", err)
	}

	// Still the library's error, not a count: main prints this one directly.
	if !errors.Is(err, binancedata.ErrNotAvailable) {
		t.Errorf("error = %v, want it to still wrap ErrNotAvailable", err)
	}
}
