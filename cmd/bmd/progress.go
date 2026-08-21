package main

// The progress display, which is one line on a terminal and one line per chunk
// anywhere else.

import (
	"fmt"
	"io"
	"strings"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// progress renders binancedata.Progress events onto a stream.
//
// What it can show is decided by what the library reports, and that is chunks
// rather than bytes — Progress carries Done, Total and the candles each chunk
// produced, deliberately with no byte counter and no cache-hit flag, because
// exposing those would mean widening the cache's interface for the sake of a
// progress bar. So this counts units of work, which is the honest thing it has.
type progress struct {
	w io.Writer

	// tty decides between redrawing one line and printing many. A terminal can
	// take a carriage return and be rewritten; a log file or a CI transcript
	// turns the same output into one enormous line with no newlines in it.
	tty bool

	// width is the length of the last line written, so the next redraw can
	// pad over it. Without this, a short line leaves the tail of a longer
	// predecessor on screen and the result reads as corrupted output.
	width int

	// active records whether anything has been drawn, so done knows whether it
	// owes the terminal a newline.
	active bool
}

// newProgress builds the reporter, or returns nil when there should not be one.
//
// A nil return rather than a silent no-op reporter: the caller uses it to
// decide whether to register a progress callback at all, and a library that is
// not calling back is cheaper than one calling into a function that discards
// its argument.
func newProgress(w io.Writer, quiet bool) *progress {
	if quiet {
		return nil
	}

	return &progress{w: w, tty: isTerminal(w)}
}

// report is what binancedata.WithProgress receives.
//
// The library serialises calls into it, so this needs no lock of its own — see
// the note on Loader.progressMu. That is worth knowing before adding a mutex
// here out of habit: it would be harmless and it would also be a lie about
// where the synchronisation lives.
func (p *progress) report(ev binancedata.Progress) {
	line := fmt.Sprintf("[%*d/%d] %s %s  %s",
		digits(ev.Total), ev.Done, ev.Total,
		ev.Source,
		ev.Start.Format(dateLayout),
		outcome(ev))

	if !p.tty {
		// One line per chunk, with a newline, so a redirected stderr stays
		// readable and greppable.
		_, _ = fmt.Fprintln(p.w, line)

		return
	}

	// Pad to cover whatever was there before, then return to the start of the
	// line without ending it, so the next event overwrites this one.
	if pad := p.width - len(line); pad > 0 {
		line += strings.Repeat(" ", pad)
	}

	p.width = len(line)
	p.active = true

	_, _ = fmt.Fprintf(p.w, "\r%s", line)
}

// done releases the line the display was drawing on.
//
// Only a terminal needs it, and only if something was drawn: the last redraw
// ended without a newline, so anything printed afterwards would start in the
// middle of it.
func (p *progress) done() {
	if p == nil || !p.tty || !p.active {
		return
	}

	_, _ = fmt.Fprintln(p.w)

	p.active = false
}

// outcome renders the part of an event that says how the chunk went.
func outcome(ev binancedata.Progress) string {
	if ev.Err != nil {
		return "failed: " + ev.Err.Error()
	}

	// Zero is normal at the leading edge of a symbol's history and for a range
	// that is still forming, so it is reported rather than flagged. The loader
	// is what decides whether an empty span is a gap worth failing over, and
	// it has already made that decision by the time this runs.
	return fmt.Sprintf("%d candles", ev.Klines)
}

// digits is how wide a count needs to be printed, so that [ 1/60] and [60/60]
// line up and the display does not jitter as the number grows.
func digits(n int) int {
	width := 1
	for n >= 10 {
		n /= 10
		width++
	}

	return width
}
