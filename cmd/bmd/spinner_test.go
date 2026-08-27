package main

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode/utf8"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// TestNewSpinnerReturnsNilWhenThereShouldBeNone covers the gate every other
// test in this package leans on: a bytes.Buffer is not a terminal, so the whole
// suite runs down the nil path and the drawing code is only reached where a
// test forces it (below).
func TestNewSpinnerReturnsNilWhenThereShouldBeNone(t *testing.T) {
	tests := []struct {
		name    string
		quiet   bool
		verbose bool
	}{
		{"not a terminal", false, false},
		{"not a terminal and -quiet", true, false},
		{"not a terminal and -verbose", false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if sp := newSpinner(&bytes.Buffer{}, tc.quiet, tc.verbose, "working"); sp != nil {
				t.Errorf("newSpinner = %v, want nil", sp)
			}
		})
	}

	// Neither gate on its own — -quiet or -verbose with a terminal — is
	// isolated here, for the same reason progress.go's tty branch is not: a
	// test writer is never a terminal, so the conditions cannot be separated
	// without a real pty. newSpinner is a var so the callers can still be
	// tested with a forced spinner.
}

// TestNilSpinnerMethodsDoNothing: the whole point of returning nil rather than
// a no-op object is that callers never branch, so every method has to accept a
// nil receiver.
func TestNilSpinnerMethodsDoNothing(t *testing.T) {
	var sp *spinner

	sp.setLabel("still nothing")
	sp.stop()
	sp.stop() // idempotent on nil too
}

// TestSpinnerStopErasesItsLineAndEndsItsGoroutine drives the real drawing path
// through startSpinner, which skips the terminal gate.
func TestSpinnerStopErasesItsLineAndEndsItsGoroutine(t *testing.T) {
	var buf bytes.Buffer

	sp := startSpinner(&buf, "working")
	sp.setLabel("working — 3 checked")

	sp.stop()

	// Read the buffer only after stop: the handshake inside it guarantees the
	// animate goroutine has returned, so this is the one goroutine touching buf.
	if got := buf.String(); !strings.Contains(got, "\r\x1b[K") {
		t.Errorf("buffer = %q, want it to contain a clear-line sequence", got)
	}

	select {
	case <-sp.finished:
	default:
		t.Error("animate goroutine still running after stop")
	}

	sp.stop() // must not panic or block on the second call
}

// TestSpinnerLabelNeverNarrows covers the fix for the library's redraw: it
// writes "\r" + prefix + glyph with no erase to end of line, so a label that
// gets shorter — verify's byte total going from "1023.9 MB" to "1.0 GB" as it
// crosses a unit — would leave the tail of the wider one on screen. setPrefix
// pads every label back to the widest handed to the spinner so far.
func TestSpinnerLabelNeverNarrows(t *testing.T) {
	var buf bytes.Buffer

	sp := startSpinner(&buf, "verifying cached archives — 5 checked (1023.9 MB)")
	wide := sp.width

	// The next archive tips the total over 1 GB, and the rendered size is three
	// characters shorter.
	sp.setLabel("verifying cached archives — 6 checked (1.0 GB)")

	sp.stop() // joins animate, so the fields below are this goroutine's to read

	if sp.width != wide {
		t.Errorf("spinner width went from %d to %d; a shorter label must not narrow the line", wide, sp.width)
	}

	// The prefix handed to the bar is the padded label plus the one space
	// before the glyph, so its width is the widest label seen, plus one. Runes,
	// not bytes: the label carries a three-byte em dash.
	if got, want := utf8.RuneCountInString(sp.bar.Prefix), wide+1; got != want {
		t.Errorf("padded prefix = %q (width %d), want width %d", sp.bar.Prefix, got, want)
	}
}

// TestDownloadPlanSpinnerStopsBeforeSummary covers the empty-range path. The
// library reports a Progress event per chunk, and progress.report stops the
// planning spinner on the first one — but a range that yields no chunk produces
// no event, so report never runs. Without the explicit stopPlan() in
// downloadOne the spinner would still be animating when the summary printed to
// the same stream, and the deferred sp.stop() would then erase part of it.
func TestDownloadPlanSpinnerStopsBeforeSummary(t *testing.T) {
	restoreProgress := newProgress
	restoreSpinner := newSpinner
	t.Cleanup(func() { newProgress = restoreProgress; newSpinner = restoreSpinner })

	// A reporter that thinks it is on a terminal, and a real spinner on the
	// same buffer — the pairing a genuine tty run has.
	newProgress = func(w io.Writer, _ bool) *progress { return &progress{w: w, tty: true} }
	newSpinner = func(w io.Writer, _, _ bool, label string) *spinner { return startSpinner(w, label) }

	// No klines: the range is empty, so Stream yields nothing and no progress
	// event is ever reported.
	f := &fakeLoader{}
	f.install(t)

	t.Chdir(t.TempDir())

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"download",
		"-symbol", "BTCUSDT", "-interval", "1h",
		"-start", "2024-01-15", "-end", "2024-01-15",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	s := stderr.String()

	const summary = "BTCUSDT 1h: wrote 0 candles"

	i := strings.Index(s, summary)
	if i < 0 {
		t.Fatalf("stderr = %q, want it to carry the summary line", s)
	}

	// The spinner's clear sequence has to be the last thing written before the
	// summary: stopPlan() erased the line, then the summary began.
	if !strings.HasSuffix(s[:i], "\r\x1b[K") {
		t.Errorf("summary is not preceded by the spinner's clear sequence: stderr = %q", s)
	}

	// And nothing the spinner writes may follow the summary — no too-late erase
	// from the deferred stop, which sync.Once has already spent.
	if strings.ContainsRune(s[i:], '\x1b') {
		t.Errorf("an escape sequence follows the summary: %q", s[i:])
	}
}

// TestSpinnerNeverReachesStdout is the invariant that matters: the display is
// stderr decoration, and the data a caller pipes must be untouched by it. It
// runs `bmd verify` with a real spinner forced on and checks stdout carries the
// failure lines and nothing the spinner writes.
func TestSpinnerNeverReachesStdout(t *testing.T) {
	original := newSpinner
	t.Cleanup(func() { newSpinner = original })

	newSpinner = func(w io.Writer, _, _ bool, label string) *spinner {
		return startSpinner(w, label)
	}

	f := &fakeLoader{entries: []binancedata.CacheEntry{
		{Path: "/cache/good.zip", Size: 100},
		{
			Path: "/cache/rotten.zip",
			Size: 200,
			Err:  fmt.Errorf("cache: rotten.zip hashes to abc, sidecar says def: %w", binancedata.ErrChecksum),
		},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"verify"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want an error for the rotten archive")
	}

	out := stdout.String()

	if !strings.Contains(out, "/cache/rotten.zip") {
		t.Errorf("stdout = %q, want the failing path on it", out)
	}

	// stdout is data. Not one escape byte, not one carriage return from the
	// redraw, may reach it.
	if strings.ContainsAny(out, "\x1b\r") {
		t.Errorf("stdout = %q, want no terminal control characters", out)
	}
}
