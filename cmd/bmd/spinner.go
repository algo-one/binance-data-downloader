package main

// spinner.go — the "something is happening" display for the commands that do
// one slow thing before they have anything to print.
//
// progress.go already covers `bmd download`, where the library reports work
// chunk by chunk and there is a running [n/total] to draw. The other slow
// commands are a different shape. `bmd list` makes up to seven network round
// trips inside one Available call; `bmd verify` re-hashes every archive in the
// cache; `bmd cache` and `bmd prune` walk the whole cache tree; `bmd evict`
// walks it before its first line of output. Each is one call, or one loop with
// no total known ahead of time, and each is silent today until its result
// lands. This is the spinner they share.
//
// # Why a spinner and not a percent bar
//
// A percent bar needs a denominator. Available, CacheUsage and the VerifyCache
// / PruneArchives / EvictCache iterators none of them say how many archives
// there are, or how many requests a listing will take, before they begin — so
// a percentage would be invented, and an invented "73%" that then stops moving
// is worse than an honest spinner. verify and prune do put a running count on
// the spinner's label ("341 checked"), which is a real number that simply has
// no known ceiling.
//
// # Why it lives here and not in the library
//
// The same reason progress.go does. The library's job is to report facts —
// Progress values, iterator items — and painting them on a terminal is a
// presentation choice the caller owns. A library that writes carriage returns
// to stderr on its own is one a larger Go program cannot embed without its
// own output being scribbled over.
//
// # The one dependency
//
// fortio.org/progressbar, chosen for having no dependencies of its own — see
// the note in go.mod, where a short require list is a stated goal. It supplies
// the carriage-return redraw and the frame timing that progress.go hand-rolls
// for the download line. Writing that a second time here, by hand, to save one
// leaf dependency was the trade we chose not to make once a dependency was on
// the table at all.
//
// # The terminal seam, and testing
//
// newSpinner returns nil when stderr is not a terminal, or when -quiet or
// -verbose was given, and every method is a no-op on a nil receiver, so a
// command writes
//
//	sp := newSpinner(stderr, quiet, verbose, "verifying cached archives")
//	defer sp.stop()
//
// with no branch of its own. A redirected or piped run gets a nil spinner and
// its output is byte-for-byte what it was before this file existed — which is
// also why the whole test suite, writing to a bytes.Buffer, never reaches the
// drawing path. newSpinner is a package var so one test can force a spinner
// backed by a buffer; the genuinely terminal-only part (the escape codes
// reaching a real tty) is left uncovered on purpose, exactly as it is for
// progress.go.

import (
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"fortio.org/progressbar"
)

// spinnerFrame is how often the spinner advances. The library's own throttle is
// switched off below (UpdateInterval = 0), so this ticker is the sole clock:
// every tick draws exactly one new frame.
const spinnerFrame = 100 * time.Millisecond

// spinner animates a one-line "working" indicator on a stream until stop.
type spinner struct {
	// w is where the erase sequences go. The library's Bar writes here too — it
	// was handed the same writer as its ScreenWriter — so the two never target
	// different streams.
	w   io.Writer
	bar *progressbar.Bar

	// width is the on-screen width — the rune count — of the widest label
	// handed to the bar so far. The library redraws with "\r" + prefix + glyph
	// and no erase-to-end-of-line — the download bar in progress.go hit the
	// same wall — so a label that gets shorter would leave the tail of the
	// wider one on screen for the rest of the run. That is not hypothetical:
	// verify's label carries a byte total, which narrows from "1023.9 MB" to
	// "1.0 GB" the moment it crosses a unit. setPrefix pads every label back
	// out to this width, the same fix progress.go uses for the bar: never draw
	// a line narrower than one already drawn. Touched only from the command
	// goroutine (startSpinner before animate starts, then setLabel), never from
	// animate, so it needs no lock.
	width int

	quit     chan struct{} // closed by stop, to tell animate to return
	finished chan struct{} // closed by animate, once it has
	once     sync.Once     // stop is called more than once on the download path
}

// newSpinner builds a running spinner, or returns nil when there should be none.
//
// nil for -quiet, the flag whose whole meaning is "errors only". nil for
// -verbose too: that flag points stderr at the pipeline's slog stream (see
// commonFlags.options), and a spinner redrawing the same stream would scribble
// over the log lines the flag was asked for. nil when w is not a terminal,
// because a spinner is carriage returns and escape codes and a log file or a
// pipe wants neither. A nil *spinner is a valid receiver for every method
// below, so no caller has to test for it.
//
// It is a var, not a func, for the reason newLoader and newProgress are: it is
// a seam a test in this package can swap. The construction itself is
// startSpinner, so a test can also reach the drawing code directly — a
// bytes.Buffer is never a terminal, so the gate here would otherwise send every
// test down the nil path.
var newSpinner = func(w io.Writer, quiet, verbose bool, label string) *spinner {
	if quiet || verbose || !isTerminal(w) {
		return nil
	}

	return startSpinner(w, label)
}

// startSpinner builds the spinner and starts its goroutine, with no gate. Split
// from newSpinner so the terminal-only drawing path is reachable from a test.
func startSpinner(w io.Writer, label string) *spinner {
	cfg := progressbar.DefaultConfig()
	cfg.ScreenWriter = w
	cfg.Spinner = true
	cfg.NoPercent = true   // indeterminate mode: there is no percentage to show
	cfg.UseColors = false  // the rest of this tool's output is uncoloured
	cfg.UpdateInterval = 0 // let animate's ticker be the only rate limit

	s := &spinner{
		w:        w,
		bar:      cfg.NewBar(),
		quit:     make(chan struct{}),
		finished: make(chan struct{}),
	}

	// Seeds s.width and the bar's prefix in one place. Straight through
	// setPrefix rather than cfg.Prefix so the very first label is measured too
	// — see setPrefix for why every label is padded rather than set as-is.
	s.setPrefix(label)

	go s.animate()

	return s
}

// setPrefix pads label to the widest handed to this spinner so far and installs
// it as the bar's prefix. Every label change goes through here — the first one
// from startSpinner and each setLabel — so the library's no-erase redraw can
// never uncover the tail of a longer previous label. See the note on
// spinner.width.
//
// Width is a rune count, not a byte length: these labels carry an em dash ("—",
// three bytes, one column), and the pad is spaces, so a rune count is what
// lines up on screen. No double-width runes reach here, so one rune is one
// column. A fresh spinner after a verify or prune failure starts its own width
// at zero, which is fine — the count in the label only grows from there, and a
// unit crossing in the same step is a one-frame smear at worst.
func (s *spinner) setPrefix(label string) {
	n := utf8.RuneCountInString(label)
	if n > s.width {
		s.width = n
	}

	// One trailing space before the spinner glyph, past the pad, so the glyph
	// keeps its column as the label shrinks.
	s.bar.UpdatePrefix(label + strings.Repeat(" ", s.width-n) + " ")
}

// animate advances one frame every spinnerFrame until quit is closed.
//
// It is the only goroutine that calls bar.Progress, but Bar guards its own
// state with a mutex, so setLabel can call bar.UpdatePrefix from the command's
// goroutine without a race.
func (s *spinner) animate() {
	defer close(s.finished)

	t := time.NewTicker(spinnerFrame)
	defer t.Stop()

	for {
		select {
		case <-s.quit:
			return

		case <-t.C:
			// A negative percentage is progressbar's indeterminate mode: prefix,
			// one spinner frame, suffix — no bar and no number — each write led
			// by a carriage return so it overwrites the last.
			s.bar.Progress(-1)
		}
	}
}

// setLabel changes the text in front of the spinner.
//
// verify and prune call it once per item with a running count — a true number
// that just has no known total, which is the reason this is a spinner and not a
// bar. Safe to call while animate runs; see the note there.
func (s *spinner) setLabel(label string) {
	if s == nil {
		return
	}

	s.setPrefix(label)
}

// stop ends the animation and erases the spinner, leaving the cursor at the
// start of a clean line for whatever prints next.
//
// Idempotent. The download path calls it from the first progress event and
// again from a defer; every other command calls it explicitly before its
// summary and again from a defer on the error paths. sync.Once makes the
// repeat calls free.
//
// It waits for animate to return before it writes the erase, which is what
// keeps the whole type free of a data race: animate is the only goroutine that
// writes to s.w, and after this handshake it is gone. A command that needs to
// print a real line mid-walk — a verify failure, a prune "kept" — calls stop,
// prints, then starts a fresh spinner, rather than writing around a live one.
//
// Deliberately not the library's Bar.End, which writes a closing newline and a
// "done" glyph — right for a bar that reached 100%, wrong for a status line
// that should vanish without trace.
func (s *spinner) stop() {
	if s == nil {
		return
	}

	s.once.Do(func() {
		close(s.quit)
		<-s.finished

		_, _ = io.WriteString(s.w, "\r\x1b[K")
	})
}
