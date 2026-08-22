package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// fullCache is the measurement a healthy cache of 412 symbol-months produces.
//
// The numbers are scaled from real ones rather than invented, and the ratio is
// the part that matters: 2,169,570 bytes of archive against 3,226,820 of parquet
// and an 88-byte sidecar, measured on BTCUSDT 1m for 2024-01. Tier 2 is the
// larger tier, which is the opposite of the obvious guess — so a fixture built
// on the guess would have quietly taught every reader of this test the wrong
// thing about what pruning is worth.
func fullCache() binancedata.CacheUsage {
	const months = 412

	return binancedata.CacheUsage{
		Root:          "/home/ivan/.cache/bmd",
		Archives:      months * 2_169_570,
		ArchiveCount:  months,
		Sidecars:      months * 88,
		SidecarCount:  months,
		Parquet:       months * 3_226_820,
		ParquetCount:  months,
		Prunable:      409 * 2_169_570,
		PrunableCount: 409,
	}
}

// TestCacheReportIsAnExactDocument pins the table.
//
// A golden file rather than a set of Contains checks, for the reason
// output_test.go gives about the encoders: the thing under test is an aligned
// document, and an assertion that the output "contains archives" would pass on a
// table with its columns misaligned, its rows in a different order, or a size
// rendered against the wrong count.
func TestCacheReportIsAnExactDocument(t *testing.T) {
	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	checkGolden(t, "cache-report.txt", stdout.Bytes())

	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want the report on stdout alone", stderr.String())
	}
}

// TestCacheReportHidesTheStrayRowWhenThereIsNone: "other" is the row that exists
// to make a problem visible, and a permanent row of zeroes is how a reader
// learns to skip the line that matters on the one run where it is not zero.
func TestCacheReportHidesTheStrayRowWhenThereIsNone(t *testing.T) {
	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if strings.Contains(stdout.String(), "other") {
		t.Errorf("stdout = %q, want no 'other' row when nothing is unaccounted for", stdout.String())
	}
}

// TestCacheReportShowsStrayFiles is the other half: when there is something
// there, it is named and counted into the total.
func TestCacheReportShowsStrayFiles(t *testing.T) {
	usage := fullCache()
	usage.Other = 4096
	usage.OtherCount = 1

	f := &fakeLoader{usage: usage}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "other") {
		t.Errorf("stdout = %q, want an 'other' row naming the unrecognised file", stdout.String())
	}
}

// TestCacheReportOnAnEmptyCache: the first run on any machine. A table of
// zeroes reads like something went wrong; a sentence does not.
func TestCacheReportOnAnEmptyCache(t *testing.T) {
	f := &fakeLoader{usage: binancedata.CacheUsage{Root: "/home/ivan/.cache/bmd"}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "empty") {
		t.Errorf("stdout = %q, want it to say the cache is empty", stdout.String())
	}

	if !strings.Contains(stdout.String(), "/home/ivan/.cache/bmd") {
		t.Errorf("stdout = %q, want it to name the directory it measured", stdout.String())
	}
}

// TestCacheReportNamesTheCommandThatReclaims is the reason the report opens
// every parquet footer. A number with no next step is one the reader has to go
// and look up.
func TestCacheReportNamesTheCommandThatReclaims(t *testing.T) {
	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "bmd prune") {
		t.Errorf("stdout = %q, want it to name the command that frees the space", stdout.String())
	}
}

// TestCacheReportSaysWhenThereIsNothingToPrune covers the case a bare number
// cannot: zero prunable bytes in a cache that is not empty means every archive
// is still needed, which is a fact about the cache rather than an absence.
func TestCacheReportSaysWhenThereIsNothingToPrune(t *testing.T) {
	usage := fullCache()
	usage.Prunable = 0
	usage.PrunableCount = 0

	f := &fakeLoader{usage: usage}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "nothing to prune") {
		t.Errorf("stdout = %q, want it to say why there is nothing to reclaim", stdout.String())
	}
}

// TestCacheReportsAWalkFailure: the report cannot answer, so it must not print a
// number. A cache directory that cannot be read is not an empty one.
func TestCacheReportsAWalkFailure(t *testing.T) {
	f := &fakeLoader{usageErr: errors.New("cache: measuring /cache: permission denied")}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"cache"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the walk failure")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}
}

// TestCacheRejectsAnEmptyCacheDir is the trap commonFlags.options exists for,
// checked on the new command too: -cache-dir "$CACHE_DIR" with the variable
// unset must not quietly measure the user's real cache.
func TestCacheRejectsAnEmptyCacheDir(t *testing.T) {
	f := &fakeLoader{usage: fullCache()}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"cache", "-cache-dir", ""}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want a usage error for an empty -cache-dir")
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}
}

// TestCacheReportOnAZeroByteStrayFile is the case the byte total cannot answer.
//
// os.CreateTemp creates a file before anything is written into it, so a process
// killed at that instant leaves a cache holding one file and no bytes. That is
// precisely the stray the "other" row was added to surface — and a report that
// decides emptiness from CacheUsage.Total would call it an empty cache and print
// no row at all. The counts are what say whether there are files; the bytes only
// say how big they are.
func TestCacheReportOnAZeroByteStrayFile(t *testing.T) {
	f := &fakeLoader{usage: binancedata.CacheUsage{
		Root:       "/home/ivan/.cache/bmd",
		Other:      0,
		OtherCount: 1,
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"cache"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if strings.Contains(stdout.String(), "empty") {
		t.Errorf("stdout = %q, want the stray file reported rather than an empty cache", stdout.String())
	}

	if !strings.Contains(stdout.String(), "other") {
		t.Errorf("stdout = %q, want an 'other' row naming the zero-byte file", stdout.String())
	}
}
