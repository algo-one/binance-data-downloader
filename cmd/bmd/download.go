package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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
		symbol   = fs.String("symbol", "", "trading pair: BTC/USDT, BTC-USDT or BTCUSDT")
		interval = fs.String("interval", "", "candle interval: 1s, 1m, 1h, 1d, 1w, 1mo ...")
		start    = fs.String("start", "", "first day or instant to include (YYYY-MM-DD or RFC 3339, UTC)")
		end      = fs.String("end", "", "last day or instant to include, inclusive (default: now)")
		market   = fs.String("market", "spot", "market to read: spot")
		out      = fs.String("out", "", `where to write: a file, a directory, or "-" for stdout`)
		format   = fs.String("format", formatCSV, "output format: csv, json or parquet")

		common commonFlags
	)

	common.registerCacheDir(fs)
	common.registerConcurrency(fs)
	common.registerQuiet(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd download - fetch candles for a symbol and time range

Usage:
  bmd download -symbol BTC/USDT -interval 1h -start 2024-01-01 [-end 2024-03-31]

Both -start and -end are inclusive, and both are UTC. A bare -end date covers
that whole day, so -end 2024-03-31 includes every candle of the 31st.

With no -out, the candles are written to a generated file name in the current
directory. Use -out - to write them to stdout instead.

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

	req, err := buildRequest(*symbol, *interval, *market, *start, *end)
	if err != nil {
		return err
	}

	encode, err := encoderFor(ctx, *format)
	if err != nil {
		return err
	}

	dest, err := resolveDestination(*out, outputName(req, *format))
	if err != nil {
		return err
	}

	// Binary to a terminal is nobody's intention. A pipe or a file is not a
	// terminal, so `bmd download -format parquet -out - | duckdb` still works;
	// this only catches the case where the bytes would be scribbled into a
	// session, which is both useless and unpleasant to recover from.
	if *format == formatParquet && dest.isStdout() && isTerminal(stdout) {
		return usagef("-format parquet writes binary; give -out a file, or redirect stdout")
	}

	opts := common.options(stderr)

	if progress := newProgress(stderr, common.quiet); progress != nil {
		opts = append(opts, binancedata.WithProgress(progress.report))

		// The progress display owns the last line on the terminal until the
		// command is done with it. Finishing it before anything else is
		// printed is what stops a summary landing halfway through a redraw.
		defer progress.done()
	}

	l, err := newLoader(opts...)
	if err != nil {
		return err
	}

	// Stream, not Fetch. The candles go straight into an encoder, so there is
	// no reason for the whole range to exist at once — five years of minute
	// candles is about 820 MB of Kline, and this way the CLI's memory use is
	// set by the output buffer rather than by the range asked for.
	rows, err := dest.writeTo(stdout, encode, l.Stream(ctx, req))
	if err != nil {
		return err
	}

	// The summary goes to stderr even when the data went to a file, so that
	// `bmd download -out - > candles.csv` produces a file with nothing in it
	// but candles. -quiet takes it away along with the progress: the flag says
	// nothing but errors, and a summary is not an error.
	if !common.quiet {
		_, _ = fmt.Fprintf(stderr, "%s %s: wrote %d %s to %s\n",
			req.Symbol, req.Interval, rows, plural(rows, "candle"), dest.describe())
	}

	return nil
}

// buildRequest turns five flag strings into a request the library will accept.
//
// It validates before returning, so a mistyped symbol fails here rather than
// after a loader has been built and a listing has been fetched.
func buildRequest(symbol, interval, market, start, end string) (binancedata.Request, error) {
	normalized, iv, err := parseSymbolInterval(symbol, interval)
	if err != nil {
		return binancedata.Request{}, err
	}

	m, err := parseMarket(market)
	if err != nil {
		return binancedata.Request{}, err
	}

	from, err := parseStart(start)
	if err != nil {
		return binancedata.Request{}, err
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
			return binancedata.Request{}, err
		}
	}

	req := binancedata.Request{
		Symbol:   normalized,
		Interval: iv,
		Market:   m,
		Start:    from,
		End:      to,
	}

	if err := req.Validate(); err != nil {
		return binancedata.Request{}, usagef("%v", err)
	}

	return req, nil
}
