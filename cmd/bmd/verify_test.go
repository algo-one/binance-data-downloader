package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// TestVerifyIsSilentAboutAHealthyCache: the failures are the output, so a clean
// cache produces no lines on stdout and a one-line summary on stderr. That is
// what makes `bmd verify` usable in a cron job without a mail filter.
func TestVerifyIsSilentAboutAHealthyCache(t *testing.T) {
	f := &fakeLoader{entries: []binancedata.CacheEntry{
		{Path: "/cache/BTCUSDT-1h-2024-01.zip", Size: 1 << 20},
		{Path: "/cache/BTCUSDT-1h-2024-02.zip", Size: 2 << 20},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"verify"}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}

	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want nothing when every archive verified", stdout.String())
	}

	if want := "checked 2 archives (3.0 MB), 0 failed"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

// TestVerifyReportsFailuresAndExitsNonZero covers the case the command exists
// for, and the two things a caller needs from it: the bad paths named on
// stdout, and an error so the process exits 1.
func TestVerifyReportsFailuresAndExitsNonZero(t *testing.T) {
	f := &fakeLoader{entries: []binancedata.CacheEntry{
		{Path: "/cache/good.zip", Size: 100},
		{
			Path: "/cache/rotten.zip",
			Size: 200,
			Err:  fmt.Errorf("cache: rotten.zip hashes to abc, sidecar says def: %w", binancedata.ErrChecksum),
		},
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"verify"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want an error so the process exits non-zero")
	}

	if got := report(err, &bytes.Buffer{}); got != exitFailure {
		t.Errorf("exit status = %d, want %d", got, exitFailure)
	}

	if !strings.Contains(stdout.String(), "/cache/rotten.zip") {
		t.Errorf("stdout = %q, want it to name the failing archive", stdout.String())
	}

	if strings.Contains(stdout.String(), "/cache/good.zip") {
		t.Errorf("stdout = %q, want it to mention only the failures", stdout.String())
	}

	if !strings.Contains(err.Error(), "1 of 2") {
		t.Errorf("error = %v, want it to count the failures against the total", err)
	}
}

// TestVerifyKeepsGoingAfterAFailure is the property that separates a report
// from a check. Stopping at the first bad file would mean learning about a
// damaged cache one archive per run.
func TestVerifyKeepsGoingAfterAFailure(t *testing.T) {
	bad := func(name string) binancedata.CacheEntry {
		return binancedata.CacheEntry{
			Path: "/cache/" + name,
			Err:  fmt.Errorf("%s: %w", name, binancedata.ErrChecksum),
		}
	}

	f := &fakeLoader{entries: []binancedata.CacheEntry{bad("a.zip"), bad("b.zip"), bad("c.zip")}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"verify"}, &stdout, &stderr); err == nil {
		t.Fatal("run returned nil, want an error")
	}

	for _, name := range []string{"a.zip", "b.zip", "c.zip"} {
		if !strings.Contains(stdout.String(), name) {
			t.Errorf("stdout = %q, want it to name %s", stdout.String(), name)
		}
	}
}

// TestVerifyStopsOnAWalkFailure covers the other error channel. A cache
// directory that could not be read is not a bad archive: it means part of the
// cache was never looked at, and reporting "0 failed" for it would be a clean
// bill of health nobody earned.
func TestVerifyStopsOnAWalkFailure(t *testing.T) {
	f := &fakeLoader{
		entries:   []binancedata.CacheEntry{{Path: "/cache/one.zip"}},
		verifyErr: errors.New("permission denied"),
	}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{"verify"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the walk's error")
	}

	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("error = %v, want the walk's own message", err)
	}

	// The summary is not printed, because there is no total to report: the
	// walk did not finish, so "checked 1 archive" would be an undercount
	// presented as a fact.
	if strings.Contains(stderr.String(), "checked") {
		t.Errorf("stderr = %q, want no summary after an incomplete walk", stderr.String())
	}
}

// TestVerifyRemovesFailedArchives covers -rm, including the part that is easy
// to get half right: the sidecar goes with the archive, and the derived parquet
// stays. A parquet file carries the archive's published hash in its footer, so
// it is still valid data, and the cache is documented to serve from tier 2 when
// tier 1 has been pruned.
func TestVerifyRemovesFailedArchives(t *testing.T) {
	dir := t.TempDir()

	archive := filepath.Join(dir, "BTCUSDT-1h-2024-01-15.zip")
	sidecar := archive + ".CHECKSUM"
	derived := filepath.Join(dir, "BTCUSDT-1h-2024-01-15.parquet")

	for _, path := range []string{archive, sidecar, derived} {
		if err := os.WriteFile(path, []byte("contents"), 0o644); err != nil {
			t.Fatalf("seeding %s: %v", path, err)
		}
	}

	f := &fakeLoader{entries: []binancedata.CacheEntry{{
		Path: archive,
		Size: 8,
		Err:  fmt.Errorf("mismatch: %w", binancedata.ErrChecksum),
	}}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	if err := run(t.Context(), []string{"verify", "-rm"}, &stdout, &stderr); err == nil {
		t.Fatal("run returned nil; -rm does not make a failed archive a success")
	}

	for _, path := range []string{archive, sidecar} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s still exists after -rm; err = %v", filepath.Base(path), err)
		}
	}

	if _, err := os.Stat(derived); err != nil {
		t.Errorf("the derived parquet was removed as well: %v", err)
	}

	if !strings.Contains(stdout.String(), "removed BTCUSDT-1h-2024-01-15.zip") {
		t.Errorf("stdout = %q, want it to say what was removed", stdout.String())
	}
}

// TestVerifyWithoutRmRemovesNothing is the other half of the same decision, and
// the more important one: a command that reads your cache must not write to it
// unless it was asked to.
func TestVerifyWithoutRmRemovesNothing(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "BTCUSDT-1h-2024-01-15.zip")

	if err := os.WriteFile(archive, []byte("contents"), 0o644); err != nil {
		t.Fatalf("seeding the archive: %v", err)
	}

	f := &fakeLoader{entries: []binancedata.CacheEntry{{
		Path: archive,
		Err:  fmt.Errorf("mismatch: %w", binancedata.ErrChecksum),
	}}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	_ = run(t.Context(), []string{"verify"}, &stdout, &stderr)

	if _, err := os.Stat(archive); err != nil {
		t.Errorf("the archive was removed without -rm: %v", err)
	}
}

// TestHumanBytes covers the scale boundaries, since an off-by-one in the loop
// reports gigabytes as megabytes and nobody notices for a year.
func TestHumanBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    int64
		want string
	}{
		{n: 0, want: "0 B"},
		{n: 512, want: "512 B"},
		{n: 1023, want: "1023 B"},
		{n: 1024, want: "1.0 KB"},
		{n: 1536, want: "1.5 KB"},
		{n: 1 << 20, want: "1.0 MB"},
		{n: 1 << 30, want: "1.0 GB"},
		{n: 1 << 40, want: "1.0 TB"},
		// Past the largest unit the loop knows, it keeps counting in that unit
		// rather than wrapping round to bytes.
		{n: 1 << 42, want: "4.0 TB"},
	}

	for _, tt := range tests {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

// TestPlural covers the two forms, because "1 archives" reads as a bug in
// everything around it.
func TestPlural(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    int
		want string
	}{
		{n: 0, want: "candles"},
		{n: 1, want: "candle"},
		{n: 2, want: "candles"},
	}

	for _, tt := range tests {
		if got := plural(tt.n, "candle"); got != tt.want {
			t.Errorf("plural(%d, candle) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
