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
		symbols  symbolList
		interval = fs.String("interval", "", "candle interval: 1s, 1m, 1h, 1d, 1w, 1mo ...")
		start    = fs.String("start", "", "first day or instant to include (YYYY-MM-DD or RFC 3339, UTC)")
		end      = fs.String("end", "", "last day or instant to include, inclusive (default: now)")
		market   = fs.String("market", "spot", "market to read: spot")
		out      = fs.String("out", "", `where to write: a file, a directory, or "-" for stdout`)
		format   = fs.String("format", formatCSV, "output format: csv, json or parquet")

		common commonFlags
	)

	fs.Var(&symbols, "symbol",
		"trading pair: BTC/USDT, BTC-USDT or BTCUSDT; repeat or comma-separate for several")

	common.registerCacheDir(fs)
	common.registerConcurrency(fs)
	common.registerQuiet(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd download - fetch candles for one or more symbols and a time range

Usage:
  bmd download -symbol BTC/USDT -interval 1h -start 2024-01-01 [-end 2024-03-31]
  bmd download -symbol BTC/USDT,ETH/USDT -interval 1h -start 2024-01-01 -out ./data

Both -start and -end are inclusive, and both are UTC. A bare -end date covers
that whole day, so -end 2024-03-31 includes every candle of the 31st.

With no -out, the candles are written to a generated file name in the current
directory. Use -out - to write them to stdout instead.

Several symbols are downloaded by one process, which is what keeps them inside
Binance's per-IP rate limit — one bmd per symbol does not. Each symbol gets its
own file, so -out must then name a directory, or be left off.

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

	if err := checkSymbolFlag(fs, symbols); err != nil {
		return err
	}

	reqs, err := buildRequests(symbols, *interval, *market, *start, *end)
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
	// Only one symbol can reach stdout — checkOutForSeveral has already refused
	// -out - for more — so this needs no loop.
	if *format == formatParquet && *out == stdoutPath && isTerminal(stdout) {
		return usagef("-format parquet writes binary; give -out a file, or redirect stdout")
	}

	opts, err := common.options(fs, stderr)
	if err != nil {
		return err
	}

	progress := newProgress(stderr, common.quiet)
	if progress != nil {
		// Which symbol an event belongs to is worth a column only when there is
		// more than one of them. Adding it unconditionally would widen every
		// line of the ordinary single-symbol run to say something the summary
		// already says once.
		progress.showSymbol = len(reqs) > 1

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
	out      string
	format   string
	quiet    bool
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
// One process is the entire reason this command takes more than one symbol.
// Binance's REQUEST_WEIGHT quota is enforced per IP address, and the limiter
// that respects it is process-wide — see internal/vision/limiter.go, where
// sharing is called "not an optimisation, it is the requirement". Two limiters
// each honouring 40 weight per second permit 80 against a ceiling of 100, so
// three `bmd download` processes started together are over it. Before this,
// running several symbols at once was only possible the way that breaks the
// limit.
//
// In turn rather than concurrently, and that is a smaller decision than it
// looks. The Loader's own semaphore already spans calls, so the chunks of one
// symbol saturate the fetch pool for any range worth downloading; running
// symbols in parallel would fill the pool only for the narrow case of many
// symbols each wanting one or two chunks. Against that it would interleave the
// progress display, which redraws a single line, and it would need a second
// concurrency bound outside the library's — the nested limit
// docs/architecture.md warns about. Sequential keeps memory at one candle, the
// display readable, and the failure of one symbol easy to describe.
//
// # Streaming, not FetchAll
//
// FetchAll exists and is deliberately not used. It returns
// map[Request][]Kline — every candle of every symbol resident at once — and
// this command streams precisely so that a range's size does not become the
// process's memory. Five years of minute candles is about 820 MB for one
// symbol. FetchAll's advantage is deduplicating overlapping requests, and a
// list of distinct symbols has nothing to deduplicate.
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
		// Checked before each symbol rather than only inside the stream: a
		// Ctrl-C between two symbols should stop the run, not start the next
		// download and let the library discover the cancellation.
		if err := ctx.Err(); err != nil {
			return err
		}

		rows, err := downloadOne(ctx, l, req, opts, stdout, stderr)
		if err != nil {
			// A cancellation is not this symbol's fault and is not a partial
			// failure to report at the end — it ends the run. Without this the
			// loop would carry on and report every remaining symbol as failed.
			if ctx.Err() != nil {
				return err
			}

			failed = append(failed, fmt.Errorf("%s: %w", req.Symbol, err))

			// One symbol failing does not abandon the rest. The user named
			// these symbols in one command and the ones that work are still
			// worth having; the error is printed, counted, and reaches the exit
			// status below, so nothing is lost quietly. The single-symbol case
			// keeps its old behaviour — see the return at the end.
			if len(reqs) > 1 {
				opts.progress.done()

				_, _ = fmt.Fprintf(stderr, "%s: %v\n", req.Symbol, err)
			}

			continue
		}

		written++
		total += rows
	}

	if len(reqs) > 1 && !opts.quiet {
		_, _ = fmt.Fprintf(stderr, "%d of %d %s, %d %s in total\n",
			written, len(reqs), plural(len(reqs), "symbol"), total, plural(total, "candle"))
	}

	switch {
	case len(failed) == 0:
		return nil

	case len(reqs) == 1:
		// Unchanged from when this command took one symbol: the error goes back
		// for main to print, rather than being printed here and summarised.
		return failed[0]

	default:
		return fmt.Errorf("%d of %d symbols failed", len(failed), len(reqs))
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
	// Resolved per symbol, because the generated name carries the symbol in it.
	dest, err := resolveDestination(opts.out, outputName(req, opts.format))
	if err != nil {
		return 0, err
	}

	// Stream, not Fetch. The candles go straight into an encoder, so there is
	// no reason for the whole range to exist at once — five years of minute
	// candles is about 820 MB of Kline, and this way the CLI's memory use is
	// set by the output buffer rather than by the range asked for.
	rows, err := dest.writeTo(stdout, opts.encode, l.Stream(ctx, req))
	if err != nil {
		return 0, err
	}

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
// symbol.
//
// Each symbol is written to its own file, so the two spellings that name a
// single stream — "-" and a path that is not an existing directory — have no
// reading when several were asked for. Both are refused rather than resolved
// somehow: writing every symbol to one file would interleave headers into
// nonsense, and picking one symbol to honour would silently drop the others.
//
// A directory that does not exist is refused too, rather than created. Nothing
// else in this tool creates a directory, and a mistyped -out that silently
// produces one is how a download ends up somewhere nobody looks.
func checkOutForSeveral(out string, symbols int) error {
	if symbols < 2 {
		return nil
	}

	if out == stdoutPath {
		return usagef("-out - writes one stream; %d symbols need one file each, so give -out a directory", symbols)
	}

	if out == "" {
		return nil
	}

	info, err := os.Stat(out)
	if err != nil || !info.IsDir() {
		return usagef("-out %q must be an existing directory when several symbols are given", out)
	}

	return nil
}

// buildRequests turns the flag strings into requests the library will accept,
// one per symbol.
//
// Everything except the symbol is parsed once and shared, which is not merely
// tidier. The clock below is the important one: reading it per symbol would give
// the symbols in a single command different end instants, so their generated
// file names would disagree about the range and a directory of them would no
// longer describe one download.
//
// It validates before returning, so a mistyped symbol fails here rather than
// after a loader has been built and a listing has been fetched — and it fails
// before *any* symbol is downloaded, rather than after the first three
// succeeded.
func buildRequests(symbols []string, interval, market, start, end string) ([]binancedata.Request, error) {
	normalized, err := parseSymbols(symbols)
	if err != nil {
		return nil, err
	}

	iv, err := parseInterval(interval)
	if err != nil {
		return nil, err
	}

	m, err := parseMarket(market)
	if err != nil {
		return nil, err
	}

	from, err := parseStart(start)
	if err != nil {
		return nil, err
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
			return nil, err
		}
	}

	reqs := make([]binancedata.Request, 0, len(normalized))

	for _, symbol := range normalized {
		req := binancedata.Request{
			Symbol:   symbol,
			Interval: iv,
			Market:   m,
			Start:    from,
			End:      to,
		}

		if err := req.Validate(); err != nil {
			return nil, usagef("%v", err)
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}
