package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// TestDownloadEndIsInclusive is the flag-parsing half of the decision the
// library's closed range was made for, and the case that would be silently
// wrong if it were dropped.
//
// -end 2024-03-31 must cover the whole of the 31st. Handing the bare date
// through would set End to midnight, and since End is inclusive of an *instant*
// that returns the single candle opening at 00:00 — twenty-three candles short,
// with nothing to show for it. So the CLI expands a bare date to that day's
// last instant, and this is what says so.
func TestDownloadEndIsInclusive(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	dir := t.TempDir()
	t.Chdir(dir)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTC/USDT",
		"-interval", "1h",
		"-start", "2024-01-01",
		"-end", "2024-03-31",
		"-quiet",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	wantStart := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	if got := f.gotRequest.Start; !got.Equal(wantStart) {
		t.Errorf("Start = %s, want %s", got.Format(time.RFC3339Nano), wantStart.Format(time.RFC3339Nano))
	}

	// The last instant of the 31st, at nanosecond resolution. One nanosecond
	// later is April, and one nanosecond is also exactly what the library adds
	// to get its exclusive bound — so this value puts that bound on the month
	// seam and no April archive is fetched for the boundary.
	wantEnd := time.Date(2024, time.March, 31, 23, 59, 59, 999999999, time.UTC)
	if got := f.gotRequest.End; !got.Equal(wantEnd) {
		t.Errorf("End = %s, want %s", got.Format(time.RFC3339Nano), wantEnd.Format(time.RFC3339Nano))
	}

	if got := f.gotRequest.Symbol; got != "BTCUSDT" {
		t.Errorf("Symbol = %q, want the normalised BTCUSDT", got)
	}
}

// TestDownloadEndAsATimestampIsTakenLiterally is the other half of the rule:
// somebody who wrote the time out has said which instant they mean, and the
// day-expansion must not apply to it.
func TestDownloadEndAsATimestampIsTakenLiterally(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	t.Chdir(t.TempDir())

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15T06:00:00Z",
		"-end", "2024-01-15T18:00:00Z",
		"-quiet",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	want := time.Date(2024, time.January, 15, 18, 0, 0, 0, time.UTC)
	if got := f.gotRequest.End; !got.Equal(want) {
		t.Errorf("End = %s, want %s exactly", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

// TestDownloadWritesAFileInTheWorkingDirectory covers the default destination:
// no -out means a generated name where the command was run.
func TestDownloadWritesAFileInTheWorkingDirectory(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	dir := t.TempDir()
	t.Chdir(dir)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	path := filepath.Join(dir, "BTCUSDT-1h-2024-01-15_2024-01-15.csv")

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the generated file: %v", err)
	}

	checkGolden(t, "download.csv", got)

	// Nothing on stdout: the data went to a file, so a caller redirecting
	// stdout gets an empty file rather than a duplicate of the download.
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want nothing when the data went to a file", stdout.String())
	}

	// The summary names the file, and is on stderr.
	if !strings.Contains(stderr.String(), path) {
		t.Errorf("stderr = %q, want it to name %q", stderr.String(), path)
	}

	if !strings.Contains(stderr.String(), "wrote 3 candles") {
		t.Errorf("stderr = %q, want it to report the candle count", stderr.String())
	}
}

// TestDownloadToStdout covers the spelling a pipe needs, and the promise that
// goes with it: stdout carries the candles and nothing else.
func TestDownloadToStdout(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	dir := t.TempDir()
	t.Chdir(dir)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15",
		"-out", "-", "-format", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	checkGolden(t, "download.json", stdout.Bytes())

	// And no file was created on the way past.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the working directory: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("the working directory holds %d entries, want none", len(entries))
	}
}

// TestDownloadToANamedFile covers -out with a path, including the case where
// the parent directory is not the working one.
func TestDownloadToANamedFile(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	path := filepath.Join(t.TempDir(), "candles.csv")

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15",
		"-out", path, "-quiet",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	checkGolden(t, "download.csv", got)
}

// TestDownloadLeavesNoFileWhenTheStreamFails is the end-to-end version of the
// atomic-write property, and the one that matters: a range whose last chunk
// fails must not leave a CSV that looks complete.
func TestDownloadLeavesNoFileWhenTheStreamFails(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 2), streamErr: errors.New("data not available")}
	f.install(t)

	dir := t.TempDir()
	t.Chdir(dir)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15",
		"-quiet",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the stream's error")
	}

	if !strings.Contains(err.Error(), "data not available") {
		t.Errorf("error = %v, want the stream's own message", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the working directory: %v", err)
	}

	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}

		t.Errorf("a failed download left %v behind", names)
	}
}

// TestDownloadRejectsBadFlags keeps every mistake that can be caught without a
// network on the cheap side of the network, and as a usage error rather than a
// failure — so a script can tell "I typed it wrong" from "Binance is down".
func TestDownloadRejectsBadFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no symbol",
			args: []string{"-interval", "1h", "-start", "2024-01-01"},
			want: "-symbol is required",
		},
		{
			name: "no interval",
			args: []string{"-symbol", "BTCUSDT", "-start", "2024-01-01"},
			want: "-interval is required",
		},
		{
			name: "an interval Binance does not publish",
			args: []string{"-symbol", "BTCUSDT", "-interval", "7h", "-start", "2024-01-01"},
			want: `-interval "7h"`,
		},
		{
			name: "a symbol with a space in it",
			args: []string{"-symbol", "BTC USDT", "-interval", "1h", "-start", "2024-01-01"},
			want: `-symbol "BTC USDT"`,
		},
		{
			name: "no start",
			args: []string{"-symbol", "BTCUSDT", "-interval", "1h"},
			want: "-start is required",
		},
		{
			name: "a start that is not a date",
			args: []string{"-symbol", "BTCUSDT", "-interval", "1h", "-start", "last tuesday"},
			want: "want YYYY-MM-DD",
		},
		{
			name: "an end before the start",
			args: []string{"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-03-01", "-end", "2024-01-01"},
			want: "is before start",
		},
		{
			name: "an unknown format",
			args: []string{
				"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-01-01",
				"-format", "xlsx",
			},
			want: `-format "xlsx"`,
		},
		{
			name: "an unknown market",
			args: []string{
				"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-01-01",
				"-market", "futures",
			},
			want: `-market "futures"`,
		},
		{
			// The flag package takes the next argument as the value whether or
			// not it starts with a dash, so this parses cleanly and the number
			// reaches the code that has to judge it. Left unjudged it ran at
			// the default 8 and said nothing.
			name: "a negative concurrency",
			args: []string{
				"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-01-01",
				"-concurrency", "-4",
			},
			want: "-concurrency -4: must be at least 1",
		},
		{
			// Zero is the flag's declared default, which is how "not given" is
			// spelled internally — but typing it is not the same as omitting
			// it, and the help text says the default is 8.
			name: "an explicit zero concurrency",
			args: []string{
				"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-01-01",
				"-concurrency", "0",
			},
			want: "-concurrency 0: must be at least 1",
		},
		{
			// What `-cache-dir "$CACHE_DIR"` does when the variable is unset.
			// Silently falling back to the default cache is the dangerous
			// reading of it, because the caller believes they named a
			// directory — and on `bmd verify -rm` the default cache is the
			// user's real one.
			name: "an empty cache dir",
			args: []string{
				"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-01-01",
				"-cache-dir", "",
			},
			want: "-cache-dir is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeLoader{}
			f.install(t)

			var stdout, stderr bytes.Buffer

			err := run(t.Context(), append([]string{"download"}, tt.args...), &stdout, &stderr)

			if !errors.Is(err, errUsage) {
				t.Fatalf("error = %v, want it to wrap errUsage", err)
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}

			// Nothing was fetched: every one of these is decidable from the
			// command line alone.
			if !f.gotRequest.Start.IsZero() {
				t.Error("the loader was asked for candles despite the bad flags")
			}
		})
	}
}

// TestDownloadHonoursTheFlagsItAccepts is the other half of the rule the table
// above enforces. Rejecting a bad -concurrency is only worth anything if a good
// one still reaches the loader — an over-strict check and a silently dropped
// value are the same defect seen from opposite sides.
//
// An Option is opaque, so what is asserted is that both flags produced one.
// docs/architecture.md states the rule they are counted against: every option a
// constructor accepts must be honoured, and an accepted-and-ignored setting is
// a defect rather than a stub.
func TestDownloadHonoursTheFlagsItAccepts(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	t.Chdir(t.TempDir())

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT",
		"-interval", "1h",
		"-start", "2024-01-15",
		"-end", "2024-01-15",
		"-concurrency", "4",
		"-cache-dir", t.TempDir(),
		"-quiet",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// -cache-dir and -concurrency, and nothing else: -quiet suppresses the
	// progress callback and there is no -verbose, so a third option here would
	// mean something was added without this test being told.
	if got := len(f.gotOptions); got != 2 {
		t.Errorf("newLoader got %d options, want 2 (-cache-dir and -concurrency)", got)
	}
}

// TestDownloadSummaryStartsOnItsOwnLine covers the one thing about the progress
// display that only shows up on a real terminal.
//
// On a tty every redraw is "\r", the line, and no newline, so when the last one
// returns the cursor is sitting at the end of it. Whatever is printed next
// begins at that column. The summary therefore has to wait for done() to
// release the line — and a deferred done() does not, because a defer runs after
// the function body, which is to say after the summary. The visible result was
// one run-together line:
//
//	[60/60] monthly archive 2024-03-31  720 candlesBTCUSDT 1h: wrote 720 ...
//
// newProgress is replaced rather than isTerminal because the reporter has to
// have drawn something for done() to owe a newline at all: it checks active,
// which only a report sets. Reporting at construction stands in for the
// library's callback, which in a real run has fired many times by this point.
func TestDownloadSummaryStartsOnItsOwnLine(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	t.Chdir(t.TempDir())

	original := newProgress
	t.Cleanup(func() { newProgress = original })

	newProgress = func(w io.Writer, _ bool) *progress {
		p := &progress{w: w, tty: true}

		p.report(binancedata.Progress{
			Done: 1, Total: 1, Source: binancedata.SourceDailyArchive,
			Start:  mustDate(t, "2024-01-15"),
			Klines: 1,
		})

		return p
	}

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT",
		"-interval", "1h",
		"-start", "2024-01-15",
		"-end", "2024-01-15",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Split on the newline the display owes, not on every line: what is being
	// asserted is that one was written between the redraw and the summary.
	before, after, found := strings.Cut(stderr.String(), "\n")
	if !found {
		t.Fatalf("stderr = %q, want a newline releasing the progress line", stderr.String())
	}

	if !strings.HasPrefix(before, "\r") || !strings.Contains(before, "1 candles") {
		t.Errorf("the first line is %q, want the progress redraw", before)
	}

	if !strings.HasPrefix(after, "BTCUSDT 1h: wrote") {
		t.Errorf("the summary is %q, want it at the start of a line of its own", after)
	}
}

// TestDownloadWithNoEndUsesNow checks the flag that is allowed to be missing.
//
// The clock is read in the CLI rather than left to the library, which is the
// one place in this project that reads one inside logic. It is correct here and
// this is what it buys: the generated file name, the summary and the candles
// all describe the same instant, where a zero End would have the library
// resolve its own "now" a moment later.
func TestDownloadWithNoEndUsesNow(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 1)}
	f.install(t)

	t.Chdir(t.TempDir())

	before := time.Now().UTC()

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h", "-start", "2024-01-15",
		"-quiet",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	after := time.Now().UTC()

	got := f.gotRequest.End
	if got.Before(before) || got.After(after) {
		t.Errorf("End = %s, want an instant between %s and %s",
			got.Format(time.RFC3339Nano), before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano))
	}

	// It must not be the zero value either, since that would mean the library
	// resolves it and the file name would be named after the year 1.
	if got.IsZero() {
		t.Error("End is the zero time; the CLI should have resolved it")
	}
}

// TestDownloadQuietPrintsNothingButErrors pins what -quiet means. The flag says
// "nothing to stderr but errors", so it takes the summary as well as the
// progress — a summary is not an error, and a flag that silenced only half of
// the noise would need its own explanation.
func TestDownloadQuietPrintsNothingButErrors(t *testing.T) {
	f := &fakeLoader{klines: testKlines(t, 3)}
	f.install(t)

	t.Chdir(t.TempDir())

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15",
		"-quiet",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want nothing under -quiet", stderr.String())
	}
}

// TestCommandsOfferOnlyTheFlagsTheyHonour is the mechanical form of the rule in
// docs/architecture.md: an accepted-and-ignored setting is a defect, not a stub.
//
// -concurrency on a sequential walk and -cache-dir on a command that never
// opens the cache are both flags a user can set, that the help advertises, and
// that change nothing. This asserts they are not accepted at all, so the
// failure is "flag provided but not defined" rather than silence.
func TestCommandsOfferOnlyTheFlagsTheyHonour(t *testing.T) {
	tests := []struct {
		command string
		flag    string
		want    bool
	}{
		{command: "download", flag: "-cache-dir", want: true},
		{command: "download", flag: "-concurrency", want: true},
		{command: "download", flag: "-quiet", want: true},
		{command: "download", flag: "-verbose", want: true},

		{command: "verify", flag: "-cache-dir", want: true},
		{command: "verify", flag: "-quiet", want: true},
		{command: "verify", flag: "-verbose", want: true},
		{command: "verify", flag: "-concurrency", want: false},

		{command: "list", flag: "-verbose", want: true},
		{command: "list", flag: "-cache-dir", want: false},
		{command: "list", flag: "-concurrency", want: false},
		{command: "list", flag: "-quiet", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.command+" "+tt.flag, func(t *testing.T) {
			f := &fakeLoader{}
			f.install(t)

			var stdout, stderr bytes.Buffer

			// A value that parses for both the string and int flags, with no
			// other arguments — so the command fails on a missing -symbol if
			// the flag was accepted, and on the flag itself if it was not.
			err := run(t.Context(), []string{tt.command, tt.flag, "1"}, &stdout, &stderr)

			undefined := err != nil && strings.Contains(err.Error(), "not defined")

			if tt.want && undefined {
				t.Errorf("%s does not accept %s, and honours it", tt.command, tt.flag)
			}

			if !tt.want && !undefined {
				t.Errorf("%s accepts %s but does nothing with it; error = %v", tt.command, tt.flag, err)
			}
		})
	}
}
