package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// prunable is one archive a prune would remove.
func prunable(name string, size int64) binancedata.PruneResult {
	return binancedata.PruneResult{Path: "/cache/" + name, Size: size, Removed: true}
}

// TestPruneIsSilentAboutAnOrdinaryRun: the removals are the expected outcome and
// there can be thousands of them, so stdout carries only the exceptions and the
// count goes in the summary. Same split as `bmd verify`.
func TestPruneIsSilentAboutAnOrdinaryRun(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{
		prunable("BTCUSDT-1h-2024-01.zip", 1<<20),
		prunable("BTCUSDT-1h-2024-02.zip", 3<<20),
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want nothing when every archive was prunable", stdout.String())
	}

	if want := "pruned 2 archives, freed 4.0 MB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestPruneDryRunSaysSoAndPassesTheFlagDown covers both halves of -n, and the
// second half is the one worth having: a flag the CLI parses and does not pass
// on is the accepted-and-ignored setting docs/architecture.md calls a defect,
// and here it would delete the user's cache while reporting a dry run.
func TestPruneDryRunSaysSoAndPassesTheFlagDown(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{
		// Removed is false throughout a dry run, which is exactly the trap:
		// the summary has to count the verdict rather than the action.
		{Path: "/cache/BTCUSDT-1h-2024-01.zip", Size: 1 << 20},
		{Path: "/cache/BTCUSDT-1h-2024-02.zip", Size: 3 << 20},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune", "-n"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !f.gotPrune.DryRun {
		t.Error("-n was parsed but PruneOptions.DryRun was false: the library would have deleted")
	}

	if want := "would prune 2 archives, freeing 4.0 MB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestPruneWithoutDryRunDoesNotSetTheFlag is the mirror of the test above. Both
// are needed: a CLI that hardcoded DryRun true would pass the test above and
// never delete anything.
func TestPruneWithoutDryRunDoesNotSetTheFlag(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{prunable("BTCUSDT-1h-2024-01.zip", 1<<20)}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if f.gotPrune.DryRun {
		t.Error("PruneOptions.DryRun was true without -n: the command would never delete")
	}
}

// TestPruneNamesWhatItKept is the case a summary alone cannot explain. An
// archive that stays is unusual, it is the reason a prune freed less than
// `bmd cache` offered, and the reason it stayed is what tells the reader whether
// to care.
func TestPruneNamesWhatItKept(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{
		prunable("BTCUSDT-1h-2024-01.zip", 1<<20),
		{
			Path: "/cache/BTCUSDT-1h-2024-02.zip",
			Size: 3 << 20,
			Kept: fmt.Errorf("open /cache/BTCUSDT-1h-2024-02.parquet: %w", fs.ErrNotExist),
		},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "BTCUSDT-1h-2024-02.zip") {
		t.Errorf("stdout = %q, want it to name the archive that was kept", stdout.String())
	}

	if !strings.Contains(stdout.String(), "file does not exist") {
		t.Errorf("stdout = %q, want it to say why the archive was kept", stdout.String())
	}

	if strings.Contains(stdout.String(), "2024-01") {
		t.Errorf("stdout = %q, want the removals left to the summary", stdout.String())
	}

	if want := "pruned 1 archive, freed 1.0 MB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}

	if want := "kept 1 archive still needed"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestPruneHidesTheKeptLineWhenThereAreNone: a trailing "kept 0" on every run is
// how an unusual number becomes invisible.
func TestPruneHidesTheKeptLineWhenThereAreNone(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{prunable("BTCUSDT-1h-2024-01.zip", 1<<20)}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if strings.Contains(stderr.String(), "kept") {
		t.Errorf("stderr = %q, want no kept line when nothing was kept", stderr.String())
	}
}

// TestPruneExitsNonZeroWhenADeleteFails separates the two ways an archive can
// survive a prune. Being kept is a verdict and is fine; failing to delete is a
// failure and has to reach the exit status, because a script that prunes to make
// room needs to know the room is not there.
func TestPruneExitsNonZeroWhenADeleteFails(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{
		prunable("BTCUSDT-1h-2024-01.zip", 1<<20),
		{
			Path: "/cache/BTCUSDT-1h-2024-02.zip",
			Size: 3 << 20,
			Err:  errors.New("remove /cache/BTCUSDT-1h-2024-02.zip: permission denied"),
		},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"prune"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want an error so the process exits non-zero")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}

	if !strings.Contains(stdout.String(), "permission denied") {
		t.Errorf("stdout = %q, want it to name the failure", stdout.String())
	}

	// The one that did work is still counted. A prune that freed 1 MB and hit
	// one error freed 1 MB.
	if want := "pruned 1 archive, freed 1.0 MB"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestPruneReportsAWalkFailure: the iterator's own error ends the walk, and is a
// different thing from an archive that was kept. Merging them would let an
// unreadable cache directory be reported as a clean prune.
func TestPruneReportsAWalkFailure(t *testing.T) {
	f := &fakeLoader{pruneErr: errors.New("cache: pruning /cache: permission denied")}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"prune"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the walk failure")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}
}

// TestPruneOnAnEmptyCache: nothing to do is not a failure, and deserves a
// sentence rather than "pruned 0 archives, freed 0 B".
func TestPruneOnAnEmptyCache(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stderr.String(), "no cached archives") {
		t.Errorf("stderr = %q, want it to say the cache holds nothing", stderr.String())
	}
}

// TestPruneQuietKeepsTheFailures covers what -quiet means here: it suppresses
// the summary on stderr, never the per-archive lines on stdout, which are the
// command's output.
func TestPruneQuietKeepsTheFailures(t *testing.T) {
	f := &fakeLoader{results: []binancedata.PruneResult{
		prunable("BTCUSDT-1h-2024-01.zip", 1<<20),
		{
			Path: "/cache/BTCUSDT-1h-2024-02.zip",
			Size: 3 << 20,
			Kept: fmt.Errorf("open /cache/BTCUSDT-1h-2024-02.parquet: %w", fs.ErrNotExist),
		},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"prune", "-quiet"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want -quiet to suppress the summary", stderr.String())
	}

	if !strings.Contains(stdout.String(), "BTCUSDT-1h-2024-02.zip") {
		t.Errorf("stdout = %q, want -quiet to keep the per-archive lines", stdout.String())
	}
}

// TestPruneRejectsAnEmptyCacheDir is the trap commonFlags.options exists for,
// and it matters most on this command: -cache-dir "$CACHE_DIR" with the variable
// unset would otherwise point a deleting command at the user's real cache.
func TestPruneRejectsAnEmptyCacheDir(t *testing.T) {
	f := &fakeLoader{}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"prune", "-cache-dir", ""}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want a usage error for an empty -cache-dir")
	}

	if got := report(err, &bytes.Buffer{}); got != exitUsage {
		t.Errorf("exit status = %d, want %d", got, exitUsage)
	}
}
