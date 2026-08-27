package main

// The download progress display: a redrawn bar on a terminal, one line per
// chunk anywhere else.
//
// It is hand-drawn rather than handed to fortio.org/progressbar (which
// spinner.go uses) for one reason found the hard way: that library's redraw
// writes a carriage return and the new frame, but does not erase to end of
// line, so a frame narrower than the one before it — 44,640 candles this month,
// 24 the next — leaves the tail of the wider frame on screen ("24 candleses").
// The fix is to pad every frame to the width of the widest so far, which is a
// few lines here and was already how this file worked before the bar existed.

import (
	"fmt"
	"io"
	"strings"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// barWidth is how many cells the bar itself occupies, between its brackets. Kept
// short so the whole line — label, bar, percentage and the per-chunk detail —
// fits an 80-column terminal without wrapping.
const barWidth = 16

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

	// width is the length of the last line written, so the next redraw can pad
	// over it. Without this, a shorter line leaves the tail of a longer
	// predecessor on screen and the result reads as corrupted output — see the
	// note at the top of this file.
	width int

	// active records whether anything has been drawn, so done knows whether it
	// owes the terminal a newline.
	active bool

	// showSymbol adds the symbol to each line, and is set when one command is
	// downloading more than one of them.
	//
	// Conditional rather than always on. With one symbol the name is already in
	// the summary line and in the file name, so a column repeating it on every
	// chunk is noise; with several it is the only thing distinguishing one bar
	// from another.
	showSymbol bool

	// showInterval does the same for the interval, and is set independently:
	// `-symbol BTC/USDT -interval 1h,1d` varies only the interval, so that is
	// the column worth printing and the symbol is the noise. The two are
	// separate booleans rather than one "label the lines" flag for exactly that
	// case.
	showInterval bool

	// plan is the spinner that covers the gap before the first chunk: the
	// bucket listing and routing the library does inside Stream, which is
	// otherwise dead air on the terminal. downloadOne sets it per request; the
	// first report() below stops it, because the bar is now ready to take over.
	// nil off a terminal and under -quiet, and every spinner method is nil-safe.
	plan *spinner
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
	// The first event means the plan is done and a chunk has finished, so the
	// preparing spinner has served its purpose. Stop it before the line below
	// is drawn, or the two fight for the same row. Idempotent and nil-safe, so
	// the later events and the non-terminal case cost nothing.
	p.stopPlan()

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

	// The per-chunk detail, the same text in both forms: which source answered,
	// which period, and how the chunk went.
	detail := fmt.Sprintf("%s %s  %s", ev.Source, ev.Start.Format(dateLayout), outcome(ev))

	if !p.tty {
		// One line per chunk, with a newline, so a redirected stderr stays
		// readable and greppable. The count is padded so [ 1/60] and [60/60]
		// line up and the column does not jitter as the number grows.
		_, _ = fmt.Fprintf(p.w, "[%*d/%d] %s%s\n",
			digits(ev.Total), ev.Done, ev.Total, label, detail)

		return
	}

	// On a terminal, a bar. Its fill is Done/Total — units of work, which is
	// what the library reports; see the type comment on why there is no
	// byte-level number.
	line := fmt.Sprintf("%s%s %3d%%  %s",
		label, drawBar(ev.Done, ev.Total), percent(ev.Done, ev.Total), detail)

	// Pad to cover whatever was there before, then return to the start of the
	// line without ending it, so the next event overwrites this one.
	if pad := p.width - len(line); pad > 0 {
		line += strings.Repeat(" ", pad)
	}

	p.width = len(line)
	p.active = true

	_, _ = fmt.Fprintf(p.w, "\r%s", line)
}

// stopPlan ends the preparing spinner if it is still running.
//
// report() stops it on the first progress event, but a request that produces no
// event at all — an empty range, where Stream yields nothing — never reaches
// report(), and then the spinner is still animating on stderr when downloadOne
// prints its summary to the same stream. So downloadOne also calls this
// explicitly on the way out, the way every other command stops its spinner by
// hand before it prints.
//
// Nil-safe on the receiver, because -quiet leaves the whole *progress nil and
// downloadOne still calls this unconditionally, exactly as it does done().
// Idempotent, because spinner.stop is. The three call sites — report(), here,
// and the defer in downloadOne — overlap on purpose: none of them is guaranteed
// to be the one that fires first.
func (p *progress) stopPlan() {
	if p == nil {
		return
	}

	p.plan.stop()
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
	// still on screen, and after the newline above there is nothing there — so
	// leaving it set would pad the first line of the next request out to the
	// width of the last line of this one, on a line that is already empty.
	p.width = 0
}

// drawBar renders the [####    ] bar for a done/total pair.
//
// ASCII on purpose: the line is measured with len() for the padding above, so a
// multi-byte block glyph would make the measurement disagree with the display
// and the padding drift. A zero total cannot happen — a request is at least one
// chunk — but it renders as an empty bar rather than dividing by zero.
func drawBar(done, total int) string {
	filled := 0
	if total > 0 {
		filled = done * barWidth / total
	}

	if filled > barWidth {
		filled = barWidth
	}

	return "[" + strings.Repeat("#", filled) + strings.Repeat(" ", barWidth-filled) + "]"
}

// percent is the whole-number percentage for the bar's label.
//
// Clamped to 100, the same guard drawBar puts on its cell count. The library
// reports Done in 1..Total, so this cannot fire today — but the two helpers
// take the same pair and are read side by side, so a "150%" next to a full bar
// is a state one of them should not be able to show while the other cannot.
func percent(done, total int) int {
	if total <= 0 {
		return 0
	}

	p := done * 100 / total
	if p > 100 {
		p = 100
	}

	return p
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
