package binancedata

// Tests for cache eviction: Loader.EvictCache and the walk beneath it.
//
// Nearly all of these build a cache tree with os.WriteFile rather than by
// downloading fixtures, and that is a statement about the code under test
// rather than a shortcut. Eviction never opens a file: it decides from the
// directory a file sits in and the name it carries, so the bytes inside are
// irrelevant and a seeded tree exercises exactly what a real one would. The one
// test that does warm a real cache is the one asserting the consequence —
// TestEvictedEntryIsFetchedAgain — because that is the claim a seeded tree
// cannot make.

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The three files of a seeded entry, and their sizes. Distinct sizes so that a
// Size assertion says which files were counted rather than only how many.
const (
	seedArchiveBytes  = 100
	seedSidecarBytes  = 20
	seedParquetBytes  = 300
	seedEntryBytes    = seedArchiveBytes + seedSidecarBytes + seedParquetBytes
	seedPrunedBytes   = seedSidecarBytes + seedParquetBytes
	seedDirPermission = 0o755
)

// seedCache builds a cache over a temporary directory holding one complete
// entry per stem given.
//
// A stem is written slash-separated and relative to the root, exactly as the
// layout spells it:
//
//	spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15
//
// The cache is constructed directly rather than through newCache, which
// requires a downloader. Eviction never fetches anything, so a nil one is the
// honest value: a test that had to supply a live downloader to delete files
// would be describing a dependency this code does not have.
func seedCache(t *testing.T, stems ...string) (*cache, string) {
	t.Helper()

	root := t.TempDir()

	for _, stem := range stems {
		seedEntry(t, root, stem, archiveExt, archiveExt+".CHECKSUM", parquetExt)
	}

	return &cache{root: root}, root
}

// seedEntry writes one entry's files, choosing which of the three exist.
func seedEntry(t *testing.T, root, stem string, exts ...string) {
	t.Helper()

	full := filepath.Join(root, filepath.FromSlash(stem))

	if err := os.MkdirAll(filepath.Dir(full), seedDirPermission); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	sizes := map[string]int{
		archiveExt:               seedArchiveBytes,
		archiveExt + ".CHECKSUM": seedSidecarBytes,
		parquetExt:               seedParquetBytes,
	}

	for _, ext := range exts {
		size, ok := sizes[ext]
		if !ok {
			t.Fatalf("seedEntry: unknown extension %q", ext)
		}

		if err := os.WriteFile(full+ext, make([]byte, size), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
}

// collectEvict drains an eviction into a slice, or returns the error that
// ended it.
func collectEvict(t *testing.T, c *cache, opts EvictOptions) ([]EvictResult, error) {
	t.Helper()

	var results []EvictResult

	for result, err := range c.evict(t.Context(), opts) {
		if err != nil {
			return results, err
		}

		results = append(results, result)
	}

	return results, nil
}

// treeFiles lists every file under root, relative and slash-separated, sorted.
// Directories are excluded, so an emptied tree reads as no files even while the
// directories are still there — which is why removeEmptyDirs has a test of its
// own below.
func treeFiles(t *testing.T, root string) []string {
	t.Helper()

	var out []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		out = append(out, filepath.ToSlash(rel))

		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	slices.Sort(out)

	return out
}

// namesOf lists the entry names an eviction reported, in the order it reported
// them.
func namesOf(results []EvictResult) []string {
	out := make([]string, 0, len(results))

	for _, result := range results {
		out = append(out, result.Name)
	}

	return out
}

// TestEvictRequiresASelector is the guard on the zero value, and it is the most
// important test in this file.
//
// A zero EvictOptions is what a caller gets by building the struct and
// forgetting to fill it in, and the cost of reading that as "everything" is the
// whole cache. So it is an error, and deleting everything has its own spelling
// that cannot be arrived at by omission.
func TestEvictRequiresASelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts EvictOptions
	}{
		{"the zero value", EvictOptions{}},
		{"a dry run with no selector", EvictOptions{DryRun: true}},
		{"All with a symbol", EvictOptions{All: true, Symbols: []string{"BTCUSDT"}}},
		{"All with an interval", EvictOptions{All: true, Intervals: []Interval{Interval1h}}},
		{"All with a bound", EvictOptions{All: true, Before: utc(2024, 1, 1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, root := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

			before := treeFiles(t, root)

			_, err := collectEvict(t, c, tt.opts)
			if err == nil {
				t.Fatal("evict returned no error, want one: this call names nothing to evict")
			}

			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want it to wrap ErrInvalidRequest", err)
			}

			if got := treeFiles(t, root); !slices.Equal(got, before) {
				t.Errorf("files after the rejected call = %v, want %v untouched", got, before)
			}
		})
	}
}

// TestEvictRemovesTheWholeEntry: an entry is the unit, so all three files go
// together. Leaving any one behind would leave a cache that cannot serve the
// entry and cannot account for it either.
func TestEvictRemovesTheWholeEntry(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("considered %d entries, want 1", len(results))
	}

	got := results[0]

	switch {
	case got.Err != nil:
		t.Fatalf("removing the entry: %v", got.Err)
	case !got.Removed:
		t.Fatal("Removed is false after an eviction that was not a dry run")
	case got.Name != "BTCUSDT-1h-2024-01-15":
		t.Errorf("Name is %q, want %q", got.Name, "BTCUSDT-1h-2024-01-15")
	case got.Symbol != "BTCUSDT":
		t.Errorf("Symbol is %q, want BTCUSDT", got.Symbol)
	case got.Interval != Interval1h:
		t.Errorf("Interval is %s, want 1h", got.Interval)
	case !got.Period.Equal(utc(2024, 1, 15)):
		t.Errorf("Period is %v, want 2024-01-15", got.Period)
	case got.Size != seedEntryBytes:
		t.Errorf("Size is %d, want %d — all three files", got.Size, seedEntryBytes)
	case len(got.Files) != 3:
		t.Errorf("Files is %v, want the archive, the sidecar and the parquet", got.Files)
	}

	if files := treeFiles(t, root); len(files) != 0 {
		t.Errorf("files left behind: %v", files)
	}
}

// TestEvictRemovesAnEntryThatWasAlreadyPruned is the case an implementation
// written around archives gets wrong, and it is not an edge case: `bmd prune`
// leaves the sidecar and the parquet, so every entry a prune has been over has
// no .zip. Those are precisely the entries a long-lived cache is full of.
//
// A pruner-shaped walk — find every .zip, act on it — would report nothing here
// and delete nothing, while claiming success.
func TestEvictRemovesAnEntryThatWasAlreadyPruned(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	c := &cache{root: root}

	stem := "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15"
	seedEntry(t, root, stem, archiveExt+".CHECKSUM", parquetExt)

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("considered %d entries, want 1 — a pruned entry is still an entry", len(results))
	}

	got := results[0]

	if !got.Removed {
		t.Errorf("Removed is false: %v", got.Err)
	}

	if got.Size != seedPrunedBytes {
		t.Errorf("Size is %d, want %d — the two files that were there", got.Size, seedPrunedBytes)
	}

	if len(got.Files) != 2 {
		t.Errorf("Files is %v, want only the two files on disk", got.Files)
	}

	if files := treeFiles(t, root); len(files) != 0 {
		t.Errorf("files left behind: %v", files)
	}
}

// TestEvictSelectsBySymbolAndInterval covers the two filters that come from the
// directory tree rather than from a file name, including the case that would
// pass on a filter that ignored its argument: naming one of two.
func TestEvictSelectsBySymbolAndInterval(t *testing.T) {
	t.Parallel()

	stems := []string{
		"spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15",
		"spot/klines/BTCUSDT/1d/monthly/BTCUSDT-1d-2024-01",
		"spot/klines/ETHUSDT/1h/daily/ETHUSDT-1h-2024-01-15",
		"spot/klines/ETHUSDT/1d/monthly/ETHUSDT-1d-2024-01",
	}

	tests := []struct {
		name string
		opts EvictOptions
		want []string
	}{
		{
			"one symbol",
			EvictOptions{Symbols: []string{"BTCUSDT"}},
			[]string{"BTCUSDT-1d-2024-01", "BTCUSDT-1h-2024-01-15"},
		},
		{
			"one interval",
			EvictOptions{Intervals: []Interval{Interval1h}},
			[]string{"BTCUSDT-1h-2024-01-15", "ETHUSDT-1h-2024-01-15"},
		},
		{
			"a symbol and an interval together",
			EvictOptions{Symbols: []string{"ETHUSDT"}, Intervals: []Interval{Interval1d}},
			[]string{"ETHUSDT-1d-2024-01"},
		},
		{
			"two symbols",
			EvictOptions{Symbols: []string{"BTCUSDT", "ETHUSDT"}, Intervals: []Interval{Interval1d}},
			[]string{"BTCUSDT-1d-2024-01", "ETHUSDT-1d-2024-01"},
		},
		{
			"a symbol that is not cached",
			EvictOptions{Symbols: []string{"SOLUSDT"}},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, root := seedCache(t, stems...)

			results, err := collectEvict(t, c, tt.opts)
			if err != nil {
				t.Fatalf("evict: %v", err)
			}

			got := namesOf(results)
			slices.Sort(got)

			if !slices.Equal(got, tt.want) {
				t.Errorf("evicted %v, want %v", got, tt.want)
			}

			// And what is actually left on disk, which is the assertion that
			// would catch a filter applied to the report and not to the
			// deletion.
			wantFiles := (len(stems) - len(tt.want)) * 3
			if files := treeFiles(t, root); len(files) != wantFiles {
				t.Errorf("%d files left, want %d: %v", len(files), wantFiles, files)
			}
		})
	}
}

// TestEvictBeforeBoundsTheDataAndNotTheFile is the semantic worth pinning: the
// bound is compared against the instant an entry stops covering, not the one it
// starts at.
//
// The middle rows are the point. A caller keeping everything from 2024-01-15
// onward must keep January's monthly archive, because half of it is data they
// said they still want and a file cannot be split. Comparing the start instead
// would delete those fifteen days and report it as success.
func TestEvictBeforeBoundsTheDataAndNotTheFile(t *testing.T) {
	t.Parallel()

	stems := []string{
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2023-12",
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-01",
		"spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15",
		"spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-16",
	}

	tests := []struct {
		name   string
		before time.Time
		want   []string
	}{
		{
			"the start of the cached range evicts nothing",
			utc(2023, 12, 1),
			nil,
		},
		{
			"mid-January keeps the month it would have to split",
			utc(2024, 1, 15),
			[]string{"BTCUSDT-1h-2023-12"},
		},
		{
			"a day boundary evicts the day that ends on it",
			utc(2024, 1, 16),
			[]string{"BTCUSDT-1h-2023-12", "BTCUSDT-1h-2024-01-15"},
		},
		{
			"the start of February evicts January entirely",
			utc(2024, 2, 1),
			[]string{
				"BTCUSDT-1h-2023-12",
				"BTCUSDT-1h-2024-01",
				"BTCUSDT-1h-2024-01-15",
				"BTCUSDT-1h-2024-01-16",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := seedCache(t, stems...)

			results, err := collectEvict(t, c, EvictOptions{Before: tt.before})
			if err != nil {
				t.Fatalf("evict: %v", err)
			}

			got := namesOf(results)
			slices.Sort(got)

			if !slices.Equal(got, tt.want) {
				t.Errorf("evicted %v, want %v", got, tt.want)
			}
		})
	}
}

// TestEvictDryRunDeletesNothing pins the flag that makes the command safe to
// try, and the trap in its result shape: a dry run reports the files and their
// size — which is what a caller totals to answer "how much would this free" —
// while Removed stays false throughout.
func TestEvictDryRunDeletesNothing(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t,
		"spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15",
		"spot/klines/ETHUSDT/1h/daily/ETHUSDT-1h-2024-01-15",
	)

	before := treeFiles(t, root)

	results, err := collectEvict(t, c, EvictOptions{All: true, DryRun: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("considered %d entries, want 2", len(results))
	}

	var total int64

	for _, result := range results {
		if result.Removed {
			t.Errorf("%s: Removed is true in a dry run", result.Name)
		}

		if len(result.Files) != 3 {
			t.Errorf("%s: Files is %v, want the three files it would delete", result.Name, result.Files)
		}

		total += result.Size
	}

	if want := int64(2 * seedEntryBytes); total != want {
		t.Errorf("a dry run totalled %d bytes, want %d", total, want)
	}

	if got := treeFiles(t, root); !slices.Equal(got, before) {
		t.Errorf("files after a dry run = %v, want %v", got, before)
	}
}

// TestEvictLeavesWhatThisLibraryDidNotWrite is the guard that keeps a command
// asked to remove 2019 from sweeping up whatever else is in the directory.
//
// The three cases are the three ways a file can be in the tree without being
// ours: an extension the cache never writes, a stem naming a different symbol
// than the directory it sits in, and a temporary file from a write that was
// interrupted. The second is the one a looser parse would take: it matches an
// extension, it is in a data directory, and it is still not an entry.
func TestEvictLeavesWhatThisLibraryDidNotWrite(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

	dir := filepath.Join(root, "spot", "klines", "BTCUSDT", "1h", "daily")

	strays := []string{
		"notes.txt",
		"ETHUSDT-1h-2024-01-15.zip",
		"BTCUSDT-1h-2024-01-15.zip.tmp-9134",
	}

	for _, name := range strays {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if got, want := namesOf(results), []string{"BTCUSDT-1h-2024-01-15"}; !slices.Equal(got, want) {
		t.Errorf("evicted %v, want %v", got, want)
	}

	got := treeFiles(t, root)
	slices.Sort(strays)

	want := make([]string, 0, len(strays))
	for _, name := range strays {
		want = append(want, "spot/klines/BTCUSDT/1h/daily/"+name)
	}

	slices.Sort(want)

	if !slices.Equal(got, want) {
		t.Errorf("files left = %v, want the three strays %v", got, want)
	}
}

// TestEvictSkipsDirectoryLevelsThatAreNotOurs: the same strictness one level
// up. A directory that is not part of the layout is not descended into, so a
// file inside it is never even a candidate — however much its name looks like
// an archive.
func TestEvictSkipsDirectoryLevelsThatAreNotOurs(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

	// A data type this library does not write, and a granularity that is
	// neither monthly nor daily — both holding a perfectly well-formed name.
	seedEntry(t, root, "spot/aggTrades/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15", archiveExt)
	seedEntry(t, root, "spot/klines/BTCUSDT/1h/hourly/BTCUSDT-1h-2024-01-15", archiveExt)

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if got, want := namesOf(results), []string{"BTCUSDT-1h-2024-01-15"}; !slices.Equal(got, want) {
		t.Errorf("evicted %v, want only the one in a klines/daily directory", got)
	}

	want := []string{
		"spot/aggTrades/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15.zip",
		"spot/klines/BTCUSDT/1h/hourly/BTCUSDT-1h-2024-01-15.zip",
	}

	if got := treeFiles(t, root); !slices.Equal(got, want) {
		t.Errorf("files left = %v, want %v", got, want)
	}
}

// TestEvictRemovesTheDirectoriesItEmpties, and stops at the cache root.
//
// Nothing else collects these. Without it a cache that has had everything
// evicted reports as empty while its whole shape is still on disk, which reads
// as a failed deletion to anyone looking at the directory rather than at
// `bmd cache`.
func TestEvictRemovesTheDirectoriesItEmpties(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t,
		"spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15",
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-01",
	)

	// Evicting the daily entry empties the daily directory and nothing above
	// it, because the monthly directory beside it still holds an entry.
	if _, err := collectEvict(t, c, EvictOptions{Before: utc(2024, 1, 16)}); err != nil {
		t.Fatalf("evict: %v", err)
	}

	daily := filepath.Join(root, "spot", "klines", "BTCUSDT", "1h", "daily")
	if _, err := os.Stat(daily); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the emptied daily directory is still there: %v", err)
	}

	interval := filepath.Join(root, "spot", "klines", "BTCUSDT", "1h")
	if _, err := os.Stat(interval); err != nil {
		t.Errorf("the interval directory went with it, and it still holds monthly: %v", err)
	}

	// Now the rest, which empties the whole tree.
	if _, err := collectEvict(t, c, EvictOptions{All: true}); err != nil {
		t.Fatalf("evict: %v", err)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("the cache root still holds %d entries, want an empty tree", len(entries))
	}

	// The root itself stays. A caller may have pointed the cache at a directory
	// it does not own — a shared parent, a mount point — and removing it is not
	// something an eviction was asked to do.
	if _, err := os.Stat(root); err != nil {
		t.Errorf("the cache root was removed: %v", err)
	}
}

// TestEvictNormalisesSymbols: the tree is keyed by the normalised symbol, so a
// filter that did not normalise would match no directory and report success
// having deleted nothing — the quietest possible failure for a deletion.
func TestEvictNormalisesSymbols(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{"BTC/USDT", "BTC-USDT", "btcusdt", "BTCUSDT"} {
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()

			c, _ := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

			results, err := collectEvict(t, c, EvictOptions{Symbols: []string{spelling}})
			if err != nil {
				t.Fatalf("evict: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("evicted %d entries for -symbol %q, want 1", len(results), spelling)
			}
		})
	}
}

// TestEvictRejectsOptionsItCannotAct on: a malformed symbol, an interval that
// is not one, and a Before in the wrong location.
//
// The last is the same rule Request applies to Start and End, and it is not
// pedantry: a local-zone bound selects a different set of entries on two
// machines running the same command.
func TestEvictRejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts EvictOptions
	}{
		{"a malformed symbol", EvictOptions{Symbols: []string{"BTC USDT"}}},
		{"an interval that is not one", EvictOptions{Intervals: []Interval{Interval(99)}}},
		{"the zero interval", EvictOptions{Intervals: []Interval{0}}},
		{
			"a Before that is not UTC",
			EvictOptions{Before: time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("CET", 3600))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, root := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

			before := treeFiles(t, root)

			if _, err := collectEvict(t, c, tt.opts); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want it to wrap ErrInvalidRequest", err)
			}

			if got := treeFiles(t, root); !slices.Equal(got, before) {
				t.Errorf("files after a rejected call = %v, want %v", got, before)
			}
		})
	}
}

// TestEvictOnAnAbsentRootYieldsNothing. A cache directory that has never been
// written to is an empty cache, not a fault — the same answer `bmd cache` and
// `bmd prune` give.
func TestEvictOnAnAbsentRootYieldsNothing(t *testing.T) {
	t.Parallel()

	c := &cache{root: filepath.Join(t.TempDir(), "never-created")}

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("yielded %d results for a cache that does not exist", len(results))
	}
}

// TestEvictReportsEntriesOldestFirst. ReadDir sorts by name and the dates in
// these names are ISO, so chronological order comes out of the walk without a
// sort — the same property the S3 listing relies on. A report of what was
// deleted reads correctly because of it, so it is worth pinning rather than
// leaving to be rediscovered.
func TestEvictReportsEntriesOldestFirst(t *testing.T) {
	t.Parallel()

	c, _ := seedCache(t,
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-03",
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2023-11",
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-01",
	)

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	want := []string{"BTCUSDT-1h-2023-11", "BTCUSDT-1h-2024-01", "BTCUSDT-1h-2024-03"}
	if got := namesOf(results); !slices.Equal(got, want) {
		t.Errorf("reported %v, want %v", got, want)
	}
}

// TestEvictStopsWhenTheConsumerBreaks. The iterator is a range-over-function,
// so a caller that breaks out must stop the walk rather than have it run to
// completion deleting everything it was going to.
func TestEvictStopsWhenTheConsumerBreaks(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t,
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-01",
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-02",
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-03",
	)

	seen := 0

	for result, err := range c.evict(t.Context(), EvictOptions{All: true}) {
		if err != nil {
			t.Fatalf("evict: %v", err)
		}

		seen++

		_ = result

		break
	}

	if seen != 1 {
		t.Fatalf("saw %d results after breaking on the first, want 1", seen)
	}

	// Two entries' worth of files are still there: the walk stopped rather than
	// running on with nobody listening.
	if got, want := len(treeFiles(t, root)), 2*3; got != want {
		t.Errorf("%d files left, want %d — the walk carried on past the break", got, want)
	}
}

// TestEvictHonoursCancellation.
func TestEvictHonoursCancellation(t *testing.T) {
	t.Parallel()

	c, root := seedCache(t, "spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-15")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var err error

	for _, e := range c.evict(ctx, EvictOptions{All: true}) {
		if e != nil {
			err = e
		}
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}

	if got := len(treeFiles(t, root)); got != 3 {
		t.Errorf("%d files left, want 3 — a cancelled eviction deletes nothing", got)
	}
}

// TestEvictedEntryIsFetchedAgain is the consequence, against a real cache
// rather than a seeded tree: eviction removes the data, so the next read pays
// for the network.
//
// It is the mirror image of TestPrunedCacheStillServesReadsWithoutRequests,
// and the pair of them is what separates the two operations. Pruning must not
// cost a request; evicting must. Both are asserted by counting requests,
// because both return the right candles either way.
func TestEvictedEntryIsFetchedAgain(t *testing.T) {
	t.Parallel()

	c, requests, want, p := warmCache(t)

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 1 || !results[0].Removed {
		t.Fatalf("evicted %d entries, want 1 removed: %+v", len(results), results)
	}

	for _, path := range []string{p.archive, p.sidecar, p.parquet} {
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s survived the eviction: %v", filepath.Base(path), err)
		}
	}

	got, err := c.klines(t.Context(), testRef(), testSpec())
	if err != nil {
		t.Fatalf("reading after an eviction: %v", err)
	}

	assertSameKlines(t, got, want)

	// Two more: the sidecar and then the archive, exactly as the first fill.
	if n := requests.Load(); n != 4 {
		t.Errorf("made %d requests in total, want 4 — the eviction should have cost a re-download", n)
	}
}

// TestEvictNameCarriesTheGranularity: the two archive date layouts are what
// tell a monthly entry from a daily one in a report, and EvictResult.Name is
// where a caller reads it. Period alone cannot say: a daily entry for the first
// of a month and a monthly entry for that month have the same Period.
func TestEvictNameCarriesTheGranularity(t *testing.T) {
	t.Parallel()

	c, _ := seedCache(t,
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-01",
		"spot/klines/BTCUSDT/1h/daily/BTCUSDT-1h-2024-01-01",
	)

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("evicted %d entries, want 2", len(results))
	}

	for _, result := range results {
		if !result.Period.Equal(utc(2024, 1, 1)) {
			t.Errorf("%s: Period is %v, want 2024-01-01 for both", result.Name, result.Period)
		}
	}

	names := namesOf(results)
	slices.Sort(names)

	want := []string{"BTCUSDT-1h-2024-01", "BTCUSDT-1h-2024-01-01"}
	if !slices.Equal(names, want) {
		t.Errorf("names = %v, want %v — the layout is what distinguishes them", names, want)
	}
}

// TestEntryStemGroupsTheSidecarWithItsArchive is a unit test on the ordering
// inside entryStem, because the failure it prevents is invisible from outside:
// a sidecar is named <archive>.CHECKSUM, so it *contains* ".zip". Testing the
// archive suffix first would leave a stem ending in ".zip", the sidecar would
// group under a key of its own, and an eviction would delete the archive and
// the parquet while leaving the sidecar behind.
func TestEntryStemGroupsTheSidecarWithItsArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		ok   bool
	}{
		{"BTCUSDT-1h-2024-01.zip", "BTCUSDT-1h-2024-01", true},
		{"BTCUSDT-1h-2024-01.zip.CHECKSUM", "BTCUSDT-1h-2024-01", true},
		{"BTCUSDT-1h-2024-01.parquet", "BTCUSDT-1h-2024-01", true},
		{"BTCUSDT-1h-2024-01.zip.tmp-9134", "", false},
		{"notes.txt", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := entryStem(tt.name)
			if ok != tt.ok {
				t.Fatalf("entryStem(%q) ok = %v, want %v", tt.name, ok, tt.ok)
			}

			if got != tt.want {
				t.Errorf("entryStem(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

// TestEvictReportsAFileItCannotDelete, without ending the walk. A file that
// will not go is this entry's problem and not the run's: the entries after it
// are still evictable and reporting on every one of them is the job.
func TestEvictReportsAFileItCannotDelete(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root, which can unlink out of a read-only directory")
	}

	t.Parallel()

	c, root := seedCache(t,
		"spot/klines/BTCUSDT/1h/monthly/BTCUSDT-1h-2024-01",
		"spot/klines/ETHUSDT/1h/monthly/ETHUSDT-1h-2024-01",
	)

	// Unlinking needs write permission on the *directory*, not on the file, so
	// this is what makes a delete fail without making the file unreadable.
	locked := filepath.Join(root, "spot", "klines", "BTCUSDT", "1h", "monthly")
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	t.Cleanup(func() { _ = os.Chmod(locked, seedDirPermission) })

	results, err := collectEvict(t, c, EvictOptions{All: true})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("considered %d entries, want 2 — one failure must not end the walk", len(results))
	}

	byName := make(map[string]EvictResult, len(results))
	for _, result := range results {
		byName[result.Name] = result
	}

	if got := byName["BTCUSDT-1h-2024-01"]; got.Err == nil || got.Removed {
		t.Errorf("the locked entry reported Err=%v Removed=%v, want an error and Removed false",
			got.Err, got.Removed)
	}

	if got := byName["ETHUSDT-1h-2024-01"]; got.Err != nil || !got.Removed {
		t.Errorf("the entry after the failure reported Err=%v Removed=%v, want it removed",
			got.Err, got.Removed)
	}

	if !strings.Contains(strings.Join(treeFiles(t, root), " "), "BTCUSDT") {
		t.Error("the locked entry's files are gone; nothing should have been deleted from it")
	}
}
