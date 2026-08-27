package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// evict deletes whole cache entries — the data itself, not just the archives.
//
// # Why this is a second command and not a flag on prune
//
// Because they differ in the only way that matters when a command deletes
// things. `bmd prune` cannot cost you data: it removes archives whose parquet
// already answers reads, so everything that worked before it works after it,
// and the worst case is a download the day the codec version moves. This
// removes the entry, and every read of it goes back to Binance.
//
// Folding the two together would put a safe operation and a destructive one
// behind one name, and would end the property that makes `bmd prune` something
// you can run without thinking about it. A guarantee that holds for half of a
// command's behaviour is not one anybody can lean on.
//
// # Why there is no automatic expiry
//
// The two policies a cache usually gets are both unreliable here, and it is
// worth knowing which rather than assuming the feature was forgotten.
// Expiring by file age keys on when an entry was *downloaded*, not when it was
// used, so the symbol-month a backtest reads on every run expires on schedule.
// A least-recently-used size cap needs a recency signal the filesystem does not
// reliably give — access times are off or coarse on most Linux setups — so the
// library would have to write on every cache hit to record one.
//
// What a person does have is knowledge of their own window: "the backtest
// starts at 2023 now", "I am done with those pairs". That is what this takes.
func evict(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bmd evict", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		symbols   listFlag
		intervals listFlag
		before    = fs.String("before", "", "evict entries ending at or before this instant (YYYY-MM-DD or RFC 3339, UTC)")
		all       = fs.Bool("all", false, "evict the entire cache")

		// -n rather than -dry-run, matching `bmd prune`: deleting is what the
		// command is for, so the flag that earns a short name is the one that
		// stops it.
		dryRun = fs.Bool("n", false, "say what would be evicted and delete nothing")

		common commonFlags
	)

	fs.Var(&symbols, "symbol",
		"trading pair: BTC/USDT, BTC-USDT or BTCUSDT; repeat or comma-separate for several")
	fs.Var(&intervals, "interval",
		"candle interval: 1s, 1m, 1h, 1d, 1w, 1mo ...; repeat or comma-separate for several")

	common.registerCacheDir(fs)
	common.registerQuiet(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd evict - delete cached data you no longer want

Usage:
  bmd evict [-symbol PAIR] [-interval IV] [-before DATE] [-n]
  bmd evict -all [-n]

Deletes whole cache entries: the archive, its .CHECKSUM sidecar and the parquet
built from it. Unlike 'bmd prune', this removes the data — every read of an
evicted entry goes back to Binance.

-symbol and -interval both take lists. -before bounds the data rather than the
files: an entry is evicted only when it ends at or before the instant given, so
-before 2024-01-15 keeps January's monthly archive, half of which you still
want. Use -before 2024-02-01 to remove it.

At least one of -symbol, -interval or -before is required. Evicting everything
is -all, which cannot be combined with the others.

The cache never evicts on its own. There is no size cap and no expiry: file
times record when an entry was downloaded rather than when it was last used,
and a least-recently-used cap would need this tool to write to disk on every
cache hit.

Run 'bmd cache' to see what is there, or -n to see what this would remove.

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

	evictOpts, err := buildEvictOptions(fs, symbols, intervals, *before, *all, *dryRun)
	if err != nil {
		return err
	}

	opts, err := common.options(fs, stderr)
	if err != nil {
		return err
	}

	l, err := newLoader(opts...)
	if err != nil {
		return err
	}

	return runEvict(ctx, l, evictOpts, stdout, stderr, common.quiet, common.verbose)
}

// buildEvictOptions turns the flags into the library's options, and refuses the
// combinations that have no reading.
//
// The library validates all of this again and would reject the same calls. It
// is repeated here for one reason: an error out of the library is about a
// programming mistake and exits 1, while a person who typed the wrong flags
// should be told which flag and get the exit status that means "your command
// line was wrong". The two checks agreeing is not duplication to remove — it is
// the library refusing to trust a caller, which is the rule everywhere else in
// this package too.
func buildEvictOptions(
	fs *flag.FlagSet, symbols, intervals listFlag, before string, all, dryRun bool,
) (binancedata.EvictOptions, error) {
	// Empty-but-given is caught before "no selector at all", so that
	// `bmd evict -symbol "$SYMBOLS"` with the variable unset is told its flag
	// names nothing rather than that it named nothing — the distinction
	// checkListFlag exists to draw.
	for _, given := range []struct {
		name    string
		values  listFlag
		example string
	}{
		{"symbol", symbols, "BTC/USDT"},
		{"interval", intervals, "1h"},
	} {
		if wasGiven(fs, given.name) && len(given.values) == 0 {
			return binancedata.EvictOptions{}, usagef("-%s was given but names no %s", given.name, given.name)
		}
	}

	filtered := len(symbols) > 0 || len(intervals) > 0 || before != ""

	switch {
	case all && filtered:
		return binancedata.EvictOptions{}, usagef(
			"-all evicts everything and cannot be combined with -symbol, -interval or -before")

	case !all && !filtered:
		return binancedata.EvictOptions{}, usagef(
			"nothing to evict: give -symbol, -interval or -before, or -all to remove the whole cache")
	}

	opts := binancedata.EvictOptions{All: all, DryRun: dryRun}

	if len(symbols) > 0 {
		// Normalised and deduplicated here rather than handed over raw, so a
		// typo is a usage error naming the symbol instead of a library error
		// arriving from two layers down.
		normalized, err := parseSymbols(symbols)
		if err != nil {
			return binancedata.EvictOptions{}, err
		}

		opts.Symbols = normalized
	}

	if len(intervals) > 0 {
		ivs, err := parseIntervals(intervals)
		if err != nil {
			return binancedata.EvictOptions{}, err
		}

		opts.Intervals = ivs
	}

	if before != "" {
		// parseStart rather than parseEnd, and the difference is the whole
		// meaning of the flag. -end names the last instant you want and a bare
		// date is stretched to cover that whole day; -before names the first
		// instant you do *not* want, so a bare date is midnight and
		// `-before 2024-01-01` keeps every candle of 2024 and removes 2023.
		t, err := parseStart(before)
		if err != nil {
			// parseStart phrases its failures as "-start ...", which is not the
			// flag that was typed.
			return binancedata.EvictOptions{}, usagef("-before %q: want YYYY-MM-DD or an RFC 3339 timestamp", before)
		}

		opts.Before = t
	}

	return opts, nil
}

// wasGiven reports whether a flag appeared on the command line, as opposed to
// holding its zero value. It is the same fs.Visit trick commonFlags.options and
// checkListFlag use, and it is here because those two answer the question for
// their own flags only.
func wasGiven(fs *flag.FlagSet, name string) bool {
	given := false

	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})

	return given
}

// runEvict performs the eviction and reports. It is separate from flag parsing
// so a test can drive it with a loader it built itself.
//
// # What goes where
//
// Every evicted entry is named on stdout, one per line, and this is the one
// place `bmd evict` deliberately differs from `bmd prune`. A prune prints only
// its exceptions, because removals there are the expected outcome and cost
// nothing. Here each line is data that is gone, so the list is the receipt —
// and it is on stdout because it is the thing worth keeping.
func runEvict(
	ctx context.Context, l loader, opts binancedata.EvictOptions, stdout, stderr io.Writer, quiet, verbose bool,
) error {
	var (
		evicted int
		freed   int64
		failed  int
	)

	// The walk is over every directory in the cache and can be a visible pause
	// before the first line — most of all when a filter matches nothing, which
	// prints only "nothing matched" at the end. Unlike verify and prune, this
	// command prints a line for every entry it does touch, so a running spinner
	// would fight its own receipt: the spinner covers the gap before output
	// starts and is stopped by the first result, whichever kind it is.
	sp := newSpinner(stderr, quiet, verbose, "scanning the cache")
	defer sp.stop()

	for result, err := range l.EvictCache(ctx, opts) {
		// The iterator's own error ends the walk: bad options, an unreadable
		// cache directory, a cancelled context. It is a different thing from
		// one entry that would not delete, and merging them would let an
		// unreadable cache be reported as a clean eviction.
		if err != nil {
			return err
		}

		// The first result of any kind hands the screen to the receipt lines.
		// Idempotent, so calling it every iteration costs nothing.
		sp.stop()

		if result.Err != nil {
			failed++

			_, _ = fmt.Fprintf(stdout, "%s: %v\n", result.Name, result.Err)

			continue
		}

		// Counted from the verdict rather than from Removed, which is false
		// throughout a dry run — the same trap `bmd prune` documents.
		evicted++
		freed += result.Size

		_, _ = fmt.Fprintf(stdout, "%s %s %s\n", result.Symbol, result.Interval, result.Name)
	}

	// For the match-nothing case, where the loop above never ran.
	sp.stop()

	if !quiet {
		writeEvictSummary(stderr, opts.DryRun, evicted, freed, failed)
	}

	if failed > 0 {
		return fmt.Errorf("%d %s could not be evicted", failed, plural(failed, "entry"))
	}

	return nil
}

// writeEvictSummary writes the one line that says what happened.
//
// The two tenses are not decoration, for the reason writePruneSummary gives:
// somebody who has just deleted 3 GB of cache should never have to re-read
// their own command line to find out whether they really did.
func writeEvictSummary(w io.Writer, dryRun bool, evicted int, freed int64, failed int) {
	if evicted == 0 && failed == 0 {
		_, _ = fmt.Fprintln(w, "nothing matched")

		return
	}

	if dryRun {
		_, _ = fmt.Fprintf(w, "would evict %d %s, freeing %s\n",
			evicted, plural(evicted, "entry"), humanBytes(freed))

		return
	}

	_, _ = fmt.Fprintf(w, "evicted %d %s, freed %s\n",
		evicted, plural(evicted, "entry"), humanBytes(freed))
}
