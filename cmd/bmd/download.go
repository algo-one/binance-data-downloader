package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// download fetches a range of candles and writes them out.
//
// The function is flag parsing and wiring; everything it does with the values
// is one call to the library and one call to an encoder. That split is what
// keeps the claim in docs/cli.md true — nothing happens here that a Go program
// could not do with the same three lines.
func download(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bmd download", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		symbols   listFlag
		intervals listFlag
		start     = fs.String("start", "", "first day or instant to include (YYYY-MM-DD or RFC 3339, UTC)")
		end       = fs.String("end", "", "last day or instant to include, inclusive (default: now)")
		market    = fs.String("market", "spot", "market to read: spot")
		out       = fs.String("out", "", `where to write: a file, a directory, or "-" for stdout`)
		format    = fs.String("format", formatCSV, "output format: csv, json or parquet")

		common commonFlags
	)

	fs.Var(&symbols, "symbol",
		"trading pair: BTC/USDT, BTC-USDT or BTCUSDT; repeat or comma-separate for several")
	fs.Var(&intervals, "interval",
		"candle interval: 1s, 1m, 1h, 1d, 1w, 1mo ...; repeat or comma-separate for several")

	common.registerCacheDir(fs)
	common.registerConcurrency(fs)
	common.registerQuiet(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd download - fetch candles for symbols, intervals and a time range

Usage:
  bmd download -symbol BTC/USDT -interval 1h -start 2024-01-01 [-end 2024-03-31]
  bmd download -symbol BTC/USDT,ETH/USDT -interval 1h -start 2024-01-01 -out ./data
  bmd download -symbol BTC/USDT -interval 1h,1d -start 2024-01-01 -out ./data

Both -start and -end are inclusive, and both are UTC. A bare -end date covers
that whole day, so -end 2024-03-31 includes every candle of the 31st.

With no -out, the candles are written to a generated file name in the current
directory. Use -out - to write them to stdout instead.

-symbol and -interval both take lists, and every pair of them is downloaded:
two symbols at three intervals is six downloads. One process does the lot,
which is what keeps them inside Binance's per-IP rate limit — one bmd per
symbol or per interval does not. Each download gets its own file, so -out must
then name a directory, or be left off.

Flags:
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() > 0 {
		return usagef("unexpected argument %q; every value is given with a flag", fs.Arg(0))
	}

	if err := checkListFlag(fs, "symbol", symbols, "BTC/USDT"); err != nil {
		return err
	}

	if err := checkListFlag(fs, "interval", intervals, "1h"); err != nil {
		return err
	}

	reqs, unit, err := buildRequests(symbols, intervals, *market, *start, *end)
	if err != nil {
		return err
	}

	if err := checkOutForSeveral(*out, len(reqs)); err != nil {
		return err
	}

	encode, err := encoderFor(ctx, *format)
	if err != nil {
		return err
	}

	// Binary to a terminal is nobody's intention. A pipe or a file is not a
	// terminal, so `bmd download -format parquet -out - | duckdb` still works;
	// this only catches the case where the bytes would be scribbled into a
	// session, which is both useless and unpleasant to recover from.
	//
	// Only one download can reach stdout — checkOutForSeveral has already
	// refused -out - for more — so this needs no loop.
	if *format == formatParquet && *out == stdoutPath && isTerminal(stdout) {
		return usagef("-format parquet writes binary; give -out a file, or redirect stdout")
	}

	opts, err := common.options(fs, stderr)
	if err != nil {
		return err
	}

	progress := newProgress(stderr, common.quiet)
	if progress != nil {
		// Which symbol and which interval an event belongs to are each worth a
		// column only when more than one of them was asked for. Adding either
		// unconditionally would widen every line of the ordinary single
		// download to say something the summary line already says once.
		//
		// The two are decided separately rather than from len(reqs), because
		// the pair that varies is what a reader needs: one symbol at three
		// intervals wants the interval on every line and the symbol on none of
		// them, and a single count cannot tell that case from its mirror.
		progress.showSymbol = len(symbols) > 1
		progress.showInterval = len(intervals) > 1

		opts = append(opts, binancedata.WithProgress(progress.report))

		// The deferred call is for the paths that leave without reaching the
		// summary — a fetch that failed, a cancelled context — where the error
		// main is about to print would otherwise land on the progress line.
		// The success path releases the line explicitly below instead, because
		// a defer runs *after* the summary rather than before it.
		defer progress.done()
	}

	l, err := newLoader(opts...)
	if err != nil {
		return err
	}

	return runDownload(ctx, l, reqs, downloadOpts{
		out:      *out,
		format:   *format,
		quiet:    common.quiet,
		verbose:  common.verbose,
		unit:     unit,
		encode:   encode,
		progress: progress,
	}, stdout, stderr)
}

// downloadOpts is what runDownload needs that is not a request.
//
// A struct rather than eight parameters, because six of them are strings and
// bools and a call site listing them positionally is one transposition away from
// writing the format into the path.
type downloadOpts struct {
	out    string
	format string
	quiet  bool

	// verbose is here for one reason: it decides, along with quiet, whether the
	// planning spinner is created. -verbose points stderr at the loader's log
	// stream, and a spinner redrawing that stream would scribble over it — the
	// same collision newSpinner guards every other command against.
	verbose bool

	// unit is the noun the summary counts in: "symbol", "interval" or
	// "download". See [buildRequests], which picks it from whichever of the two
	// lists actually has more than one entry.
	unit string

	encode   encoder
	progress *progress
}

// runDownload fetches each request in turn and writes it out.
//
// It is separate from flag parsing so a test can drive it with a loader it
// supplied itself.
//
// # Why one process, and why in turn
//
// One process is the entire reason this command takes lists at all. Binance's
// REQUEST_WEIGHT quota is enforced per IP address, and the limiter that
// respects it is process-wide — see internal/vision/limiter.go, where sharing
// is called "not an optimisation, it is the requirement". Two limiters each
// honouring 40 weight per second permit 80 against a ceiling of 100, so three
// `bmd download` processes started together are over it, and the penalty is
// HTTP 418 against the whole IP for anything from two minutes to three days.
//
// That argument is about processes and says nothing about symbols, which is why
// -interval takes a list too. A shell loop over intervals breaks the limit in
// exactly the way a shell loop over symbols does, and the fix is the same one:
// the pairs become requests inside one process rather than processes.
//
// In turn rather than concurrently, and that is a smaller decision than it
// looks. The Loader's own semaphore already spans calls, so the chunks of one
// request saturate the fetch pool for any range worth downloading; running them
// in parallel would fill the pool only for the narrow case of many requests
// each wanting one or two chunks. Against that it would interleave the progress
// display, which redraws a single line, and it would need a second concurrency
// bound outside the library's — the nested limit docs/architecture.md warns
// about. Sequential keeps memory at one candle, the display readable, and the
// failure of one request easy to describe.
//
// # Streaming, not FetchAll
//
// FetchAll exists and is deliberately not used. It returns
// map[Request][]Kline — every candle of every request resident at once — and
// this command streams precisely so that a range's size does not become the
// process's memory. Five years of minute candles is about 820 MB for one
// symbol. FetchAll's advantage is deduplicating overlapping requests, and the
// requests built here are distinct by construction: both lists are
// deduplicated before they are crossed.
func runDownload(
	ctx context.Context,
	l loader,
	reqs []binancedata.Request,
	opts downloadOpts,
	stdout, stderr io.Writer,
) error {
	var (
		failed  []error
		written int
		total   int
	)

	for _, req := range reqs {
		// Checked before each request rather than only inside the stream: a
		// Ctrl-C between two downloads should stop the run, not start the next
		// one and let the library discover the cancellation.
		if err := ctx.Err(); err != nil {
			return err
		}

		rows, err := downloadOne(ctx, l, req, opts, stdout, stderr)
		if err != nil {
			// A cancellation is not this request's fault and is not a partial
			// failure to report at the end — it ends the run. Without this the
			// loop would carry on and report every remaining one as failed.
			if ctx.Err() != nil {
				return err
			}

			// Stored as it came back, with no prefix wrapped around it. Two
			// things read this slice and neither wants one: the batch branch
			// reads only its length, and prints its own "SYMBOL 1h: ..." line
			// just below, so a wrapped copy would be allocated and thrown away
			// — and the single-request branch returns failed[0] to main, where a
			// prefix naming the only thing the user asked for is noise that was
			// not there in Stage 8.
			failed = append(failed, err)

			// One failure does not abandon the rest. The user named these
			// symbols and intervals in one command and the ones that work are
			// still worth having; the error is printed, counted, and reaches the
			// exit status below, so nothing is lost quietly. The single-request
			// case keeps its old behaviour — see the return at the end.
			//
			// Symbol and interval both, matching the success line below. With
			// several intervals of one symbol the symbol alone would name three
			// failures identically, and the file that is missing is the one the
			// pair identifies.
			if len(reqs) > 1 {
				opts.progress.done()

				_, _ = fmt.Fprintf(stderr, "%s %s: %v\n", req.Symbol, req.Interval, err)
			}

			continue
		}

		written++
		total += rows
	}

	if len(reqs) > 1 && !opts.quiet {
		_, _ = fmt.Fprintf(stderr, "%d of %d %s, %d %s in total\n",
			written, len(reqs), plural(len(reqs), opts.unit), total, plural(total, "candle"))
	}

	switch {
	case len(failed) == 0:
		return nil

	case len(reqs) == 1:
		// Unchanged from when this command took one symbol at one interval: the
		// error goes back for main to print, rather than being printed here and
		// summarised.
		return failed[0]

	default:
		return fmt.Errorf("%d of %d %s failed", len(failed), len(reqs), plural(len(reqs), opts.unit))
	}
}

// downloadOne streams one request into one destination.
func downloadOne(
	ctx context.Context,
	l loader,
	req binancedata.Request,
	opts downloadOpts,
	stdout, stderr io.Writer,
) (int, error) {
	// Resolved per request, because the generated name carries both the symbol
	// and the interval in it.
	dest, err := resolveDestination(opts.out, outputName(req, opts.format))
	if err != nil {
		return 0, err
	}

	// A spinner for the gap before the first chunk. Stream does the bucket
	// listing and routing on the first pull, and for a wide range that is
	// several serial requests with nothing on screen.
	//
	// Three things stop it, because none of them always fires first: report()
	// on the first progress event, the explicit stopPlan() below just before
	// the summary is printed, and this defer for the error return between here
	// and there. The empty-range path is why the explicit stop is needed —
	// writeTo returns (0, nil) with no event, so report() never runs, and
	// without stopPlan() the summary would land on a still-animating spinner
	// and the too-late defer would then erase part of the summary.
	//
	// Wired onto the reporter only when there is one: -quiet leaves
	// opts.progress nil, and newSpinner would return nil for -quiet anyway, so
	// this is belt and braces. Off a terminal, or under -verbose, newSpinner
	// returns nil and the per-chunk lines are unchanged.
	if opts.progress != nil {
		sp := newSpinner(stderr, opts.quiet, opts.verbose,
			"preparing "+req.Symbol+" "+req.Interval.String())
		opts.progress.plan = sp

		defer sp.stop()
	}

	// Stream, not Fetch. The candles go straight into an encoder, so there is
	// no reason for the whole range to exist at once — five years of minute
	// candles is about 820 MB of Kline, and this way the CLI's memory use is
	// set by the output buffer rather than by the range asked for.
	rows, err := dest.writeTo(stdout, opts.encode, l.Stream(ctx, req))
	if err != nil {
		return 0, err
	}

	// Stop the planning spinner before anything is printed below. On the normal
	// path report() already did, from the first chunk event; this is for the
	// request that produced no event — an empty range — where the spinner would
	// otherwise still be animating when the summary lands on the same stream,
	// and the deferred sp.stop() only fires after it. Nil-safe and idempotent.
	opts.progress.stopPlan()

	// The progress display owns the last line on the terminal until this point.
	// On a terminal its last redraw ended with \r and the line, and no newline,
	// so the cursor is sitting at the end of it — anything printed now would be
	// appended to "[60/60] monthly archive 2024-03-31  720 candles" rather than
	// starting a line of its own. done() releases it, and it has to be called
	// here rather than left to the caller's defer, which runs after the summary.
	//
	// Calling it twice is harmless: it clears active, and it is nil-safe, so
	// this needs no guard for -quiet having suppressed the display entirely.
	opts.progress.done()

	// The summary goes to stderr even when the data went to a file, so that
	// `bmd download -out - > candles.csv` produces a file with nothing in it
	// but candles. -quiet takes it away along with the progress: the flag says
	// nothing but errors, and a summary is not an error.
	if !opts.quiet {
		_, _ = fmt.Fprintf(stderr, "%s %s: wrote %d %s to %s\n",
			req.Symbol, req.Interval, rows, plural(rows, "candle"), dest.describe())
	}

	return rows, nil
}

// checkOutForSeveral refuses the -out spellings that cannot hold more than one
// download.
//
// Each download is written to its own file, so the two spellings that name a
// single stream — "-" and a path that is not an existing directory — have no
// reading when several were asked for. Both are refused rather than resolved
// somehow: writing everything to one file would interleave headers into
// nonsense, and picking one download to honour would silently drop the others.
//
// A directory that does not exist is refused too, rather than created. Nothing
// else in this tool creates a directory, and a mistyped -out that silently
// produces one is how a download ends up somewhere nobody looks.
//
// The count is of requests rather than of symbols, which is the whole of what
// changed when -interval became a list: one symbol at two intervals is two
// files and needs a directory exactly as two symbols at one interval do.
func checkOutForSeveral(out string, downloads int) error {
	if downloads < 2 {
		return nil
	}

	if out == stdoutPath {
		return usagef("-out - writes one stream; %d downloads need one file each, so give -out a directory",
			downloads)
	}

	if out == "" {
		return nil
	}

	info, err := os.Stat(out)
	if err != nil || !info.IsDir() {
		return usagef("-out %q must be an existing directory when several symbols or intervals are given", out)
	}

	return nil
}

// buildRequests turns the flag strings into requests the library will accept:
// one per (symbol, interval) pair.
//
// Everything except those two is parsed once and shared, which is not merely
// tidier. The clock below is the important one: reading it per request would
// give the requests in a single command different end instants, so their
// generated file names would disagree about the range and a directory of them
// would no longer describe one download.
//
// It validates before returning, so a mistyped symbol or interval fails here
// rather than after a loader has been built and a listing has been fetched —
// and it fails before *any* download runs, rather than after the first three
// succeeded.
//
// # The cross product, and the order of it
//
// Every symbol is downloaded at every interval, because that is the only
// reading of two lists that does not silently drop something: pairing them off
// positionally would need them to be the same length, and would make
// `-symbol BTC/USDT,ETH/USDT -interval 1h` mean one download rather than two.
//
// Symbol-major, so a directory of output files groups a symbol's intervals
// together and the progress display works through one symbol before moving on.
// The alternative is not wrong, only less useful to read.
//
// # The unit it returns
//
// The second return value is the noun the run summary counts in, and it comes
// from here because here is where both lists are still in scope. "2 of 2
// symbols" is the right sentence for two symbols at one interval and the wrong
// one for one symbol at two intervals, and a count of requests cannot tell
// those apart — they are both 2. When both lists are plural neither noun is
// right, and the pairs are what is being counted, so it is "downloads".
func buildRequests(
	symbols, intervals []string,
	market, start, end string,
) (reqs []binancedata.Request, unit string, err error) {
	normalized, err := parseSymbols(symbols)
	if err != nil {
		return nil, "", err
	}

	ivs, err := parseIntervals(intervals)
	if err != nil {
		return nil, "", err
	}

	m, err := parseMarket(market)
	if err != nil {
		return nil, "", err
	}

	from, err := parseStart(start)
	if err != nil {
		return nil, "", err
	}

	// An omitted -end means "up to now", and the clock is read here rather
	// than left to the library.
	//
	// binancedata.Request documents the opposite preference, and is right for
	// the case it is written about: a long-running process that stores a
	// Request must leave End zero, or it freezes the end of its range at
	// startup. A CLI invocation is the other case — one process, one call,
	// which then exits — and resolving it here buys something the zero value
	// cannot. The generated file name, the summary line and the candles all
	// come from the same instant, rather than the name saying one thing and
	// the data covering another.
	//
	// This is also the only place in the project that reads a clock inside
	// logic, and it is the layer where that is correct: everything below takes
	// its time as a parameter precisely so that the reading happens at the
	// edge, once, where a test never has to reach it.
	to := time.Now().UTC()

	if end != "" {
		if to, err = parseEnd(end); err != nil {
			return nil, "", err
		}
	}

	reqs = make([]binancedata.Request, 0, len(normalized)*len(ivs))

	for _, symbol := range normalized {
		for _, iv := range ivs {
			req := binancedata.Request{
				Symbol:   symbol,
				Interval: iv,
				Market:   m,
				Start:    from,
				End:      to,
			}

			if err := req.Validate(); err != nil {
				return nil, "", usagef("%v", err)
			}

			reqs = append(reqs, req)
		}
	}

	return reqs, summaryUnit(len(normalized), len(ivs)), nil
}

// summaryUnit names what a run of several downloads is counting.
//
// Split out of buildRequests so that the choice can be tested directly against
// the four shapes rather than inferred from a rendered summary line.
func summaryUnit(symbols, intervals int) string {
	switch {
	case symbols > 1 && intervals > 1:
		return "download"

	case intervals > 1:
		return "interval"

	default:
		// Including the single-request case, where nothing is summarised at
		// all: runDownload prints the summary only for more than one request,
		// so the value is unused rather than wrong.
		return "symbol"
	}
}
