package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// prune deletes cached archives that the parquet tier no longer needs.
//
// Tier 1 exists to build tier 2 and to rebuild it; a read touches neither. So an
// archive whose parquet is valid is occupying disk for a rebuild that may never
// come, and this is the command that says so out loud and reclaims it.
//
// It is about 40% of a cache — measured on BTCUSDT 1m for 2024-01, 2,169,570
// bytes of archive against 3,226,820 of parquet. Less than the obvious guess,
// because tier 2 is deliberately the larger of the two: it trades size for the
// read speed the whole cache exists for.
//
// What it is not is a garbage collector. Nothing here runs on its own or on a
// schedule, because the cost of pruning is a download later — see
// [binancedata.Loader.PruneArchives] — and spending somebody's bandwidth is not
// a decision a cache should take by itself.
func prune(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bmd prune", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		// -n rather than -dry-run, and rather than verify's -rm inverted.
		// Deleting is what a command called prune is for, so the flag that
		// earns a short name is the one that stops it — the same spelling make
		// and rsync use, for the same reason.
		dryRun = fs.Bool("n", false, "say what would be freed and delete nothing")

		common commonFlags
	)

	common.registerCacheDir(fs)
	common.registerQuiet(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd prune - reclaim disk by deleting archives the cache no longer reads

Usage:
  bmd prune [-cache-dir DIR] [-n]

Cached reads are served from the parquet tier, which carries the hash of the
archive it was built from. An archive whose parquet is valid is therefore only
needed to rebuild it, and this deletes those archives. The .CHECKSUM sidecars
and the parquet files are kept: every read that worked before a prune works
after it.

The cost is paid later, and only if a rebuild is needed — when the codec version
changes, or when a parquet file is damaged. What would have been a local decode
becomes a download.

Run 'bmd cache' first to see how much there is to reclaim, or -n to see what
this would do.

This never deletes parquet files, so it does not reduce what a read can reach.
To delete parquet files too, use 'bmd evict' instead: it removes the whole
entry — archive, sidecar and parquet together — and the data has to be
downloaded again afterwards.

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

	return runPrune(ctx, l, stdout, stderr, *dryRun, common.quiet)
}

// runPrune walks the cache and reports. It is separate from flag parsing so a
// test can drive it with a cache it built itself.
//
// # What goes where
//
// stdout carries the exceptions — an archive that was kept, or one that could
// not be deleted — one per line, which is the same split `bmd verify` uses and
// for the same reason: those lines are the answer somebody may want to pipe
// into a file, and in a healthy cache there are none of them. The removals
// themselves are not printed. They are the expected outcome, there can be
// thousands, and the summary counts them.
func runPrune(ctx context.Context, l loader, stdout, stderr io.Writer, dryRun, quiet bool) error {
	var (
		pruned int
		freed  int64
		kept   int
		failed int
	)

	for result, err := range l.PruneArchives(ctx, binancedata.PruneOptions{DryRun: dryRun}) {
		// The iterator's own error is the one that ends the walk: the cache
		// directory could not be read, or the context was cancelled. It is a
		// different thing from an archive that was kept, and merging the two
		// would let an unreadable directory be reported as a clean prune.
		if err != nil {
			return err
		}

		switch {
		case result.Err != nil:
			failed++

			_, _ = fmt.Fprintf(stdout, "%s: %v\n", result.Path, result.Err)

		case result.Kept != nil:
			kept++

			// Said out loud, on stdout, beside the file it belongs to. A prune
			// that quietly declines to remove something looks from the outside
			// exactly like one that failed to, and the reason — no parquet yet,
			// or one built by an older codec — is what tells the reader whether
			// to care.
			_, _ = fmt.Fprintf(stdout, "kept %s: %v\n", filepath.Base(result.Path), result.Kept)

		default:
			// Counted in a dry run as well: Removed is false there even for an
			// archive that would have gone, so the count that describes the
			// work has to come from the verdict rather than from the action.
			pruned++
			freed += result.Size
		}
	}

	if !quiet {
		writePruneSummary(stderr, dryRun, pruned, freed, kept, failed)
	}

	if failed > 0 {
		// An error rather than a bare message, so the process exits non-zero.
		// The failures themselves have already been printed one per line.
		return fmt.Errorf("%d %s could not be removed", failed, plural(failed, "archive"))
	}

	return nil
}

// writePruneSummary writes the one line that says what happened.
//
// The two tenses are not decoration. A dry run and a real one otherwise print
// the same sentence, and a reader who has just freed 3 GB should never have to
// check which flags they typed to find out whether they actually did.
//
// failed is taken for the sake of one branch — the early return below — and is
// otherwise unused, because the failures have already been printed one per line
// and the error runPrune returns counts them. Without it, a prune of three
// archives on a read-only mount reaches this with pruned and kept both zero and
// announces "no cached archives" directly underneath three "permission denied"
// lines about the archives it just found.
func writePruneSummary(w io.Writer, dryRun bool, pruned int, freed int64, kept, failed int) {
	// Every archive this walk saw ended up in exactly one of the three counts,
	// so all three being zero is the only shape that means the walk found
	// nothing at all.
	if pruned == 0 && kept == 0 && failed == 0 {
		_, _ = fmt.Fprintln(w, "no cached archives")

		return
	}

	if dryRun {
		_, _ = fmt.Fprintf(w, "would prune %d %s, freeing %s\n",
			pruned, plural(pruned, "archive"), humanBytes(freed))
	} else {
		_, _ = fmt.Fprintf(w, "pruned %d %s, freed %s\n",
			pruned, plural(pruned, "archive"), humanBytes(freed))
	}

	// Only when there are any. A kept archive is unusual — it means a parquet
	// that has not been built or cannot be used — and a trailing "0 kept" on
	// every run is how an unusual number becomes invisible.
	if kept > 0 {
		_, _ = fmt.Fprintf(w, "kept %d %s still needed to rebuild\n", kept, plural(kept, "archive"))
	}
}
