package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// verify re-hashes every cached archive against its .CHECKSUM sidecar.
//
// The read path does not do this, on purpose: verification is paid once, at
// download, because re-hashing a 93 MB archive on every read would cost more
// than the parse the second cache tier exists to avoid. That leaves one gap —
// a file that was correct when written and was damaged afterwards — and this
// command is how it is closed.
func verify(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bmd verify", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		remove = fs.Bool("rm", false, "delete archives that fail, so the next download replaces them")

		common commonFlags
	)

	common.registerCacheDir(fs)
	common.registerQuiet(fs)
	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd verify - re-hash cached archives against their checksums

Usage:
  bmd verify [-cache-dir DIR] [-rm]

Every archive in the cache is read and hashed, and the result is compared with
the SHA-256 Binance published in the .CHECKSUM file beside it. Nothing is
downloaded, and nothing is deleted unless -rm is given.

The exit status is 1 if any archive failed, so this is usable in a cron job.

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

	l, err := newLoader(common.options(stderr)...)
	if err != nil {
		return err
	}

	return runVerify(ctx, l, stdout, stderr, *remove, common.quiet)
}

// runVerify walks the cache and reports. It is separate from flag parsing so a
// test can drive it with a cache it built itself.
func runVerify(ctx context.Context, l loader, stdout, stderr io.Writer, remove, quiet bool) error {
	var (
		checked int
		bytes   int64
		bad     int
	)

	for entry, err := range l.VerifyCache(ctx) {
		// The iterator's own error is the one that ends the walk: the cache
		// directory could not be read, or the context was cancelled. It is a
		// different thing from an archive that failed, and merging the two
		// would let one unreadable directory be reported as clean.
		if err != nil {
			return err
		}

		checked++
		bytes += entry.Size

		if entry.Err == nil {
			continue
		}

		bad++

		// Failures go to stdout, not stderr. They are this command's output —
		// the answer it was run to produce — and a caller piping it into a
		// file wants them in the file.
		_, _ = fmt.Fprintf(stdout, "%s: %v\n", entry.Path, entry.Err)

		if remove {
			removeEntry(stdout, stderr, entry)
		}
	}

	// The failures themselves are stdout and are printed either way; this is
	// the reassurance line, which is exactly what -quiet is for.
	if !quiet {
		_, _ = fmt.Fprintf(stderr, "checked %d %s (%s), %d failed\n",
			checked, plural(checked, "archive"), humanBytes(bytes), bad)
	}

	if bad > 0 {
		// An error rather than a bare message, so the process exits non-zero
		// and a cron job notices. The count is the message; the failures
		// themselves have already been printed one per line.
		return fmt.Errorf("%d of %d cached archives failed verification", bad, checked)
	}

	return nil
}

// removeEntry deletes a failed archive and the sidecar naming it.
//
// Both, because they are one cache entry: an archive with no sidecar and a
// sidecar with no archive are each treated as a miss by the read path, so
// leaving half behind achieves nothing and leaves a file that outlives its
// purpose. The derived parquet is deliberately left alone — it carries the
// archive's published hash in its footer, so it is still valid data built from
// the bytes Binance served, and the cache is documented to keep serving from
// tier 2 when tier 1 has been pruned.
func removeEntry(stdout, stderr io.Writer, entry binancedata.CacheEntry) {
	for _, path := range []string{entry.Path, entry.Path + ".CHECKSUM"} {
		err := os.Remove(path)

		switch {
		case err == nil:
			_, _ = fmt.Fprintf(stdout, "removed %s\n", filepath.Base(path))

		case errors.Is(err, os.ErrNotExist):
			// The sidecar's absence is one of the things that gets an archive
			// reported in the first place, so this is expected rather than
			// worth mentioning.

		default:
			_, _ = fmt.Fprintf(stderr, "could not remove %s: %v\n", path, err)
		}
	}
}

// humanBytes renders a byte count at a scale a person can read.
//
// Powers of 1024 with the SI-ish short names, which is what every disk tool
// prints and what a cache measured in gigabytes wants. Exact byte counts are
// not useful here: the number exists to tell you whether the walk did a little
// work or a lot.
func humanBytes(n int64) string {
	const unit = 1024

	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	div, exp := int64(unit), 0

	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGT"[exp])
}

// plural picks the singular or plural form of a word for a count.
//
// Only the regular -s case, because the only words this tool counts are
// "archive" and "candle". A message that says "1 archives" reads as a bug in
// everything around it.
func plural(n int, word string) string {
	if n == 1 {
		return word
	}

	return word + "s"
}
