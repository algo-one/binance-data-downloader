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

	// showSymbol adds the symbol to each line, and is set when one command is
	// downloading more than one of them.
	//
	// Conditional rather than always on. With one symbol the name is already in
	// the summary line and in the file name, so a column repeating it on every
	// chunk is noise; with several it is the only thing distinguishing one
	// [3/12] from another.
	showSymbol bool

	// showInterval does the same for the interval, and is set independently:
	// `-symbol BTC/USDT -interval 1h,1d` varies only the interval, so that is
	// the column worth printing and the symbol is the noise. The two are
	// separate booleans rather than one "label the lines" flag for exactly that
	// case.
	showInterval bool
}

// newProgress builds the reporter, or returns nil when there should not be one.
//
// A nil return rather than a silent no-op reporter: the caller uses it to
// decide whether to register a progress callback at all, and a library that is
// not calling back is cheaper than one calling into a function that discards
// its argument.
//
// # Why it is a variable
//
// For the same reason newLoader is — so a test can replace it — and here that
// is the only way to reach the terminal branch at all. Every test in this
// package writes to a bytes.Buffer, a bytes.Buffer is not an *os.File, so
// isTerminal answers false and tty is false in the whole suite. The half that
// goes untested that way is not a cosmetic one: it is the half that leaves the
// cursor sitting mid-line with no newline after it, which is what makes *when*
// done() is called matter. A summary printed before it lands on the end of the
// progress line, and no assertion here could see that until this became a var.
var newProgress = func(w io.Writer, quiet bool) *progress {
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
	// Both labels come from the event rather than from a field set per download,
	// which is what makes this correct if the reporting ever stops being
	// sequential: Progress carries the request the chunk belongs to, so a line
	// cannot end up labelled with whichever request happened to start last.
	label := ""
	if p.showSymbol {
		label = ev.Request.Symbol + " "
	}

	if p.showInterval {
		label += ev.Request.Interval.String() + " "
	}

	line := fmt.Sprintf("[%*d/%d] %s%s %s  %s",
		digits(ev.Total), ev.Done, ev.Total,
		label,
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

	// The width goes with the line it measured. It exists to overwrite what is
	// still on screen, and after the newline above there is nothing there —
	// so leaving it set would pad the first line of the next symbol out to the
	// width of the last line of the previous one, on a line that is already
	// empty.
	p.width = 0
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
