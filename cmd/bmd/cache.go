package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// cacheReport prints what the cache directory holds and how much of it a prune
// would reclaim.
//
// It is the report half of cache management; `bmd prune` is the half that acts.
// Splitting them is what lets this one be safe to run at any time — it opens
// files read-only, writes nothing and downloads nothing — while the command
// that deletes is one a person has to type on purpose.
func cacheReport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bmd cache", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var common commonFlags

	common.registerCacheDir(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd cache - show what the cache holds

Usage:
  bmd cache [-cache-dir DIR]

Reports the cache's size, broken down by tier, and how many bytes 'bmd prune'
would reclaim. Nothing is downloaded, written or deleted.

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

	opts, err := common.options(fs, stderr)
	if err != nil {
		return err
	}

	l, err := newLoader(opts...)
	if err != nil {
		return err
	}

	return runCacheReport(ctx, l, stdout, stderr, common.verbose)
}

// runCacheReport asks for the measurement and renders it. It is separate from
// flag parsing so a test can drive it with a cache it built itself.
//
// It takes stderr only for the spinner: CacheUsage opens every parquet footer
// under the root to reach the prunable figure, which on a large cache is a
// visible pause, and the report is silent until it returns.
func runCacheReport(ctx context.Context, l loader, stdout, stderr io.Writer, verbose bool) error {
	// stderr, so the table on stdout stays clean; stopped before the table is
	// written, so the two never share a line. `bmd cache` has no -quiet — its
	// report is its output (see commonFlags) — but it has -verbose, and a
	// spinner must not redraw over the log stream that flag turns on. Off a
	// terminal newSpinner returns nil and the run is byte-for-byte unchanged.
	sp := newSpinner(stderr, false, verbose, "measuring the cache")
	defer sp.stop()

	usage, err := l.CacheUsage(ctx)
	sp.stop()

	if err != nil {
		return err
	}

	return writeCacheUsage(stdout, usage)
}

// writeCacheUsage renders the measurement.
//
// text/tabwriter for the same reason `bmd list` uses it: the numbers have known
// widths but the labels do not, and a table that realigns itself cannot drift
// out of alignment when a label is reworded.
func writeCacheUsage(w io.Writer, u binancedata.CacheUsage) error {
	if _, err := fmt.Fprintf(w, "%s\n\n", u.Root); err != nil {
		return err
	}

	// The four categories are disjoint and cover every file under the root, so
	// this is the file count of the whole cache. It is computed once because
	// two lines below need it and they must agree: the one that decides the
	// cache is empty, and the total row.
	files := u.ArchiveCount + u.SidecarCount + u.ParquetCount + u.OtherCount

	// An empty cache is a real answer and deserves a sentence rather than a
	// table of zeroes. It is also the ordinary state of a machine that has not
	// run a download yet, so it should not read like something went wrong.
	//
	// Files rather than bytes, and the difference is not pedantic. os.CreateTemp
	// creates a zero-byte file before anything is written into it, so a process
	// killed at that instant leaves a cache holding one file and no bytes —
	// and the "other" row below exists precisely to make that file visible.
	// Deciding from u.Total() would answer "empty" for the one cache state the
	// report was extended to describe.
	if files == 0 {
		_, err := fmt.Fprintln(w, "empty")

		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)

	// The tiers in the order the read path uses them, not in order of size:
	// a reader who knows what these files are should find them where the
	// design puts them, and docs/caching.md counts from tier 1 down.
	writeCacheRow(tw, "archives", u.ArchiveCount, u.Archives)
	writeCacheRow(tw, "sidecars", u.SidecarCount, u.Sidecars)
	writeCacheRow(tw, "parquet", u.ParquetCount, u.Parquet)

	// Printed only when there is something to print. Zero is the healthy value
	// and a permanent row of zeroes trains the eye to skip the line that
	// matters on the one run where it is not.
	if u.OtherCount > 0 {
		writeCacheRow(tw, "other", u.OtherCount, u.Other)
	}

	writeCacheRow(tw, "total", files, u.Total())

	if err := tw.Flush(); err != nil {
		return err
	}

	return writePrunable(w, u)
}

// writeCacheRow prints one label, count and size.
func writeCacheRow(w io.Writer, label string, count int, size int64) {
	_, _ = fmt.Fprintf(w, "%s\t%d\t%s\t\n", label, count, humanBytes(size))
}

// writePrunable prints the reclaimable figure, and names the command that would
// reclaim it.
//
// Naming the command is the point of the line. A number with no next step is
// something the reader has to go and look up, and the whole reason this report
// measures prunable bytes at all — at the cost of opening every parquet footer —
// is to answer "is it worth pruning?" in one run.
func writePrunable(w io.Writer, u binancedata.CacheUsage) error {
	if u.ArchiveCount == 0 {
		return nil
	}

	if u.PrunableCount == 0 {
		_, err := fmt.Fprintf(w, "\nnothing to prune: every archive is still needed to rebuild its parquet\n")

		return err
	}

	_, err := fmt.Fprintf(w, "\n%s in %d %s can be freed with 'bmd prune'\n",
		humanBytes(u.Prunable), u.PrunableCount, plural(u.PrunableCount, "archive"))

	return err
}
