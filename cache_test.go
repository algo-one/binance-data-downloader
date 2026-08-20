package binancedata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// Every test here runs against t.TempDir() and an httptest.Server serving the
// committed fixtures — see newFixtureServer in download_test.go. Nothing
// touches Binance, and nothing touches a directory the suite did not create.

// testRef and testSpec name the fixture most of these tests use: one real day
// of hourly BTCUSDT candles, published 2024-01-15.
func testRef() archiveRef {
	return archiveRef{
		Market:   MarketSpot,
		Symbol:   "BTCUSDT",
		Interval: Interval1h,
		Agg:      aggDaily,
		Period:   utc(2024, 1, 15),
	}
}

func testSpec() decodeSpec {
	start, end := dayRange(2024, 1, 15)

	return decodeSpec{Interval: Interval1h, Start: start, End: end}
}

func newTestCache(t *testing.T, mutate func(name string, body []byte) ([]byte, int)) (*cache, *atomic.Int64) {
	t.Helper()

	dl, requests := newFixtureServer(t, mutate)

	c, err := newCache(t.TempDir(), dl)
	if err != nil {
		t.Fatalf("newCache: %v", err)
	}

	return c, requests
}

// warmCache fills the cache with the test fixture and returns it along with the
// candles that were loaded and where the three files landed.
func warmCache(t *testing.T) (*cache, *atomic.Int64, []Kline, cachePaths) {
	t.Helper()

	c, requests := newTestCache(t, nil)

	klines, err := c.klines(t.Context(), testRef(), testSpec())
	if err != nil {
		t.Fatalf("first load: %v", err)
	}

	p, err := c.paths(testRef())
	if err != nil {
		t.Fatalf("paths: %v", err)
	}

	// Two requests, not three or one: the sidecar and then the archive. The
	// sidecar is fetched first so that an unpublished month costs 91 bytes
	// rather than a large transfer that ends in a 404.
	if got := requests.Load(); got != 2 {
		t.Fatalf("filling the cache made %d requests, want 2", got)
	}

	for _, path := range []string{p.archive, p.sidecar, p.parquet} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("after the first load: %v", err)
		}
	}

	return c, requests, klines, p
}

func TestCachePaths(t *testing.T) {
	t.Parallel()

	root := filepath.Join("/tmp", "bmd-test")

	tests := []struct {
		name string
		ref  archiveRef

		wantDir     string
		wantArchive string
		wantErr     bool
	}{
		{
			// The layout docs/caching.md specifies, with the aggregation last:
			// spot/klines/BTCUSDT/1h/daily/.
			name:        "daily",
			ref:         testRef(),
			wantDir:     filepath.Join(root, "spot", "klines", "BTCUSDT", "1h", "daily"),
			wantArchive: "BTCUSDT-1h-2024-01-15.zip",
		},
		{
			name: "monthly",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1mo,
				Agg: aggMonthly, Period: utc(2024, 1, 1),
			},
			wantDir:     filepath.Join(root, "spot", "klines", "BTCUSDT", "1mo", "monthly"),
			wantArchive: "BTCUSDT-1mo-2024-01.zip",
		},
		{
			// Each of the four validation cases below would otherwise create a
			// directory tree that no later run could ever hit again: an unset
			// interval renders through fmt.Stringer as "Interval(0)", which is
			// a perfectly legal directory name.
			name: "unset market",
			ref: archiveRef{
				Symbol: "BTCUSDT", Interval: Interval1h, Agg: aggDaily, Period: utc(2024, 1, 15),
			},
			wantErr: true,
		},
		{
			name: "unset aggregation",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Period: utc(2024, 1, 15),
			},
			wantErr: true,
		},
		{
			name: "unset interval",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Agg: aggDaily, Period: utc(2024, 1, 15),
			},
			wantErr: true,
		},
		{
			name: "unnormalised symbol",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTC/USDT", Interval: Interval1h,
				Agg: aggDaily, Period: utc(2024, 1, 15),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &cache{root: root}

			got, err := c.paths(tt.ref)

			if tt.wantErr {
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("paths = %+v, %v; want an error wrapping ErrInvalidRequest", got, err)
				}

				return
			}

			if err != nil {
				t.Fatalf("paths: %v", err)
			}

			want := cachePaths{
				name:    tt.wantArchive,
				archive: filepath.Join(tt.wantDir, tt.wantArchive),
				sidecar: filepath.Join(tt.wantDir, tt.wantArchive+".CHECKSUM"),
				parquet: filepath.Join(tt.wantDir, strings.TrimSuffix(tt.wantArchive, ".zip")+".parquet"),
			}

			if got != want {
				t.Errorf("paths:\n got %+v\nwant %+v", got, want)
			}
		})
	}
}

// TestCacheSecondReadIsOffline is the property the library is built for: the
// same range on the second run costs nothing on the network.
func TestCacheSecondReadIsOffline(t *testing.T) {
	t.Parallel()

	c, requests, first, _ := warmCache(t)

	second, err := c.klines(t.Context(), testRef(), testSpec())
	if err != nil {
		t.Fatalf("second load: %v", err)
	}

	if got := requests.Load(); got != 2 {
		t.Errorf("the second load made %d requests in total, want the original 2", got)
	}

	assertSameKlines(t, second, first)

	if len(first) != 24 {
		t.Errorf("got %d candles, want 24 hourly candles in a day", len(first))
	}
}

// TestCacheWritesThePublishedSidecar checks that tier 1 on disk is the pair of
// files Binance served rather than the archive plus a hash in a format of this
// project's own invention. The comparison is against the real published
// sidecar, byte for byte.
func TestCacheWritesThePublishedSidecar(t *testing.T) {
	t.Parallel()

	_, _, _, p := warmCache(t)

	got, err := os.ReadFile(p.sidecar)
	if err != nil {
		t.Fatalf("reading the cached sidecar: %v", err)
	}

	want, err := os.ReadFile(filepath.Join("testdata", p.name+".CHECKSUM"))
	if err != nil {
		t.Fatalf("reading the published sidecar: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("cached sidecar:\n got %q\nwant %q", got, want)
	}
}

// TestCacheHitDoesNotReadTheArchive is the claim docs/caching.md makes about
// the cost of a hit, tested the only way it can be: by making the archive
// unusable and requiring the read to succeed anyway.
//
// Corrupting the file rather than removing its permissions is deliberate — it
// tests the same thing on every platform, and it also proves the archive is not
// being re-hashed, which a permission check alone would not.
func TestCacheHitDoesNotReadTheArchive(t *testing.T) {
	t.Parallel()

	c, requests, want, p := warmCache(t)

	if err := os.WriteFile(p.archive, []byte("this is not a zip file"), cacheFilePerm); err != nil {
		t.Fatalf("corrupting the archive: %v", err)
	}

	got, err := c.klines(t.Context(), testRef(), testSpec())
	if err != nil {
		t.Fatalf("reading with a corrupt archive on disk: %v", err)
	}

	assertSameKlines(t, got, want)

	if n := requests.Load(); n != 2 {
		t.Errorf("made %d requests in total, want the original 2", n)
	}
}

// TestCacheServesWithoutTheArchive covers the pruning case: tier 1 is only
// needed to build, so a cache whose archives were deleted to reclaim disk still
// answers every read that does not need a rebuild.
func TestCacheServesWithoutTheArchive(t *testing.T) {
	t.Parallel()

	c, requests, want, p := warmCache(t)

	if err := os.Remove(p.archive); err != nil {
		t.Fatalf("removing the archive: %v", err)
	}

	got, err := c.klines(t.Context(), testRef(), testSpec())
	if err != nil {
		t.Fatalf("reading with the archive pruned: %v", err)
	}

	assertSameKlines(t, got, want)

	if n := requests.Load(); n != 2 {
		t.Errorf("made %d requests in total, want the original 2", n)
	}
}

// TestCacheReportsAnUnreadableArchive covers the difference between "tier 1 is
// not there" and "tier 1 could not be looked at".
//
// Only the first is an empty cache slot. The second is a fault in the
// filesystem — a lost search permission, a network mount returning EIO, a stale
// handle — and treating it as the first would re-download and overwrite an
// archive that this cache's own contract calls immutable, on the strength of a
// question that was never answered.
//
// The unreadable state here is a symlink pointing at itself, which is the
// portable way to make os.Stat fail with something that is not fs.ErrNotExist.
func TestCacheReportsAnUnreadableArchive(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("creating a symlink on Windows needs a privilege the test runner may not have")
	}

	c, requests, _, p := warmCache(t)

	// Force the rebuild path, which is the only one that needs tier 1.
	if err := os.Remove(p.parquet); err != nil {
		t.Fatalf("removing the parquet: %v", err)
	}

	if err := os.Remove(p.archive); err != nil {
		t.Fatalf("removing the archive: %v", err)
	}

	if err := os.Symlink(p.archive, p.archive); err != nil {
		t.Fatalf("creating the symlink loop: %v", err)
	}

	// Confirm the premise: the stat fails, and not with "does not exist".
	if _, err := os.Stat(p.archive); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("os.Stat = %v; want an error that is not fs.ErrNotExist", err)
	}

	if _, err := c.klines(t.Context(), testRef(), testSpec()); err == nil {
		t.Fatal("klines succeeded with an unreadable archive; want the stat error reported")
	}

	// The sharper half of the assertion. Before the stat error was
	// distinguished, this path fell through and re-fetched tier 1.
	if n := requests.Load(); n != 2 {
		t.Errorf("made %d requests in total, want the original 2 — the archive was re-downloaded", n)
	}
}

// TestCacheRebuildsTierTwo covers all three of the rebuild triggers from
// docs/caching.md plus the corruption case, and asserts the property they share:
// a rebuild reads tier 1 from disk and never goes back to the network.
func TestCacheRebuildsTierTwo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		break_ func(t *testing.T, p cachePaths, klines []Kline)
	}{
		{
			name: "the parquet does not exist",
			break_: func(t *testing.T, p cachePaths, _ []Kline) {
				t.Helper()

				if err := os.Remove(p.parquet); err != nil {
					t.Fatalf("removing the parquet: %v", err)
				}
			},
		},
		{
			// Binance republishing a corrected archive is the only thing that
			// makes this fire in practice, which is to say it never has.
			name: "it was built from a different archive",
			break_: func(t *testing.T, p cachePaths, klines []Kline) {
				t.Helper()

				b := rawParquet(t, klines, map[string]string{
					metaSourceFile:   p.name,
					metaSourceSHA256: strings.Repeat("ab", 32),
					metaCodecVersion: strconv.Itoa(CodecVersion),
					metaRows:         strconv.Itoa(len(klines)),
				})

				if err := os.WriteFile(p.parquet, b, cacheFilePerm); err != nil {
					t.Fatalf("writing a stale parquet: %v", err)
				}
			},
		},
		{
			// The upgrade case: the parser changed, so rows built by the old
			// one have to go however intact the archive beside them still is.
			name: "it was built by a different codec version",
			break_: func(t *testing.T, p cachePaths, klines []Kline) {
				t.Helper()

				sum, err := readSidecar(p.sidecar, p.name)
				if err != nil {
					t.Fatalf("reading the sidecar: %v", err)
				}

				b := rawParquet(t, klines, map[string]string{
					metaSourceFile:   p.name,
					metaSourceSHA256: sum,
					metaCodecVersion: strconv.Itoa(CodecVersion + 1),
					metaRows:         strconv.Itoa(len(klines)),
				})

				if err := os.WriteFile(p.parquet, b, cacheFilePerm); err != nil {
					t.Fatalf("writing a parquet from a newer codec: %v", err)
				}
			},
		},
		{
			name: "it is not a parquet file at all",
			break_: func(t *testing.T, p cachePaths, _ []Kline) {
				t.Helper()

				if err := os.WriteFile(p.parquet, []byte("torn write"), cacheFilePerm); err != nil {
					t.Fatalf("corrupting the parquet: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, requests, want, p := warmCache(t)

			tt.break_(t, p, want)

			got, err := c.klines(t.Context(), testRef(), testSpec())
			if err != nil {
				t.Fatalf("rebuilding: %v", err)
			}

			assertSameKlines(t, got, want)

			if n := requests.Load(); n != 2 {
				t.Errorf("the rebuild made %d requests in total, want the original 2", n)
			}

			// The rebuilt file must itself be usable, or the next run rebuilds
			// again and the cache never converges.
			again, err := c.klines(t.Context(), testRef(), testSpec())
			if err != nil {
				t.Fatalf("reading the rebuilt parquet: %v", err)
			}

			assertSameKlines(t, again, want)
		})
	}
}

// TestCacheRebuildIsReproducible is what makes a cache diffable across
// machines: the same archive and the same codec version produce the same bytes.
func TestCacheRebuildIsReproducible(t *testing.T) {
	t.Parallel()

	_, _, _, p := warmCache(t)

	first, err := os.ReadFile(p.parquet)
	if err != nil {
		t.Fatalf("reading the parquet: %v", err)
	}

	// A second cache, with its own root and its own downloader, building the
	// same archive from scratch.
	c2, _ := newTestCache(t, nil)

	if _, err := c2.klines(t.Context(), testRef(), testSpec()); err != nil {
		t.Fatalf("second cache: %v", err)
	}

	p2, err := c2.paths(testRef())
	if err != nil {
		t.Fatalf("paths: %v", err)
	}

	second, err := os.ReadFile(p2.parquet)
	if err != nil {
		t.Fatalf("reading the second parquet: %v", err)
	}

	if string(first) != string(second) {
		t.Errorf("two caches built different files: %d and %d bytes", len(first), len(second))
	}
}

// TestCacheDeduplicatesConcurrentLoads pins correctness requirement 5: several
// goroutines asking for the same archive at once produce one download, not
// several.
//
// The server blocks on the first request so the overlap is arranged rather than
// hoped for — a test that merely starts goroutines and counts requests passes
// just as happily when the deduplication is missing.
func TestCacheDeduplicatesConcurrentLoads(t *testing.T) {
	t.Parallel()

	const callers = 8

	release := make(chan struct{})

	var arrivals atomic.Int64

	c, requests := newTestCache(t, func(name string, body []byte) ([]byte, int) {
		// Only the very first request waits. Everything after it — the archive
		// that follows the sidecar — proceeds normally.
		if arrivals.Add(1) == 1 {
			<-release
		}

		return body, http.StatusOK
	})

	results := make([][]Kline, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup

	for i := range callers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			results[i], errs[i] = c.klines(t.Context(), testRef(), testSpec())
		}()
	}

	// Wait until the leader is inside the server and therefore holding the
	// singleflight entry, so the other seven are queued behind it rather than
	// racing to start their own work.
	waitFor(t, func() bool { return requests.Load() >= 1 })
	close(release)

	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}

		assertSameKlines(t, results[i], results[0])
	}

	if got := requests.Load(); got != 2 {
		t.Errorf("%d concurrent callers made %d requests, want 2", callers, got)
	}

	// Each caller must own its slice. Stage 7 merges and trims these in place,
	// so one caller's trim writing through into another's range would be a
	// silent, data-dependent bug.
	results[0][0].Trades = -1

	if results[1][0].Trades == -1 {
		t.Error("two callers share one backing array")
	}
}

// TestCacheReportsMissingArchives checks that a month Binance never published
// is a fact rather than a failure, and that it leaves nothing behind.
func TestCacheReportsMissingArchives(t *testing.T) {
	t.Parallel()

	c, _ := newTestCache(t, nil)

	// 2024-01-16 has no fixture, so the server answers 404 exactly as the
	// bucket does for an archive that was never published.
	ref := testRef()
	ref.Period = utc(2024, 1, 16)

	spec := testSpec()
	spec.Start, spec.End = dayRange(2024, 1, 16)

	_, err := c.klines(t.Context(), ref, spec)
	if !errors.Is(err, ErrNotAvailable) {
		t.Fatalf("klines = %v, want an error wrapping ErrNotAvailable", err)
	}

	p, err := c.paths(ref)
	if err != nil {
		t.Fatalf("paths: %v", err)
	}

	assertNoFiles(t, filepath.Dir(p.archive))
}

// TestCacheRejectsCorruptedDownloads checks that bytes which fail their
// checksum never become a cache entry.
func TestCacheRejectsCorruptedDownloads(t *testing.T) {
	t.Parallel()

	c, _ := newTestCache(t, func(name string, body []byte) ([]byte, int) {
		if strings.HasSuffix(name, ".zip") {
			// One byte, in the middle of a 20 KB archive. The sidecar is served
			// untouched, so the mismatch is discovered by hashing rather than
			// by anything about the transfer going visibly wrong.
			body = append([]byte(nil), body...)
			body[len(body)/2] ^= 0xff
		}

		return body, http.StatusOK
	})

	_, err := c.klines(t.Context(), testRef(), testSpec())
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("klines = %v, want an error wrapping ErrChecksum", err)
	}

	p, err := c.paths(testRef())
	if err != nil {
		t.Fatalf("paths: %v", err)
	}

	assertNoFiles(t, filepath.Dir(p.archive))
}

func TestCacheHonoursCancellation(t *testing.T) {
	t.Parallel()

	c, requests := newTestCache(t, nil)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := c.klines(ctx, testRef(), testSpec()); !errors.Is(err, context.Canceled) {
		t.Fatalf("klines = %v, want context.Canceled", err)
	}

	if got := requests.Load(); got != 0 {
		t.Errorf("a cancelled call made %d requests, want 0", got)
	}

	// Nothing may be left on disk either, and this half is the part that was
	// missing.
	//
	// klines hands its work to singleflight.DoChan, which runs it in a new
	// goroutine, and the cancellation path deliberately walks away rather than
	// stopping it — correct when other callers are waiting, wrong when the
	// caller was cancelled before it asked, because then there are none. That
	// goroutine reached writeAtomic's MkdirAll and CreateTemp, both of which
	// run *before* the callback where the cancelled download fails, so a call
	// that returned an error still created directories afterwards. MkdirAll's
	// are never cleaned up: only the temp file is.
	//
	// It showed up as flakiness rather than as a failure. t.TempDir removes the
	// cache root the moment the test returns, racing the abandoned goroutine
	// writing into it, and fails with "directory not empty" — reproducible at
	// -count=300, invisible at -count=1.
	//
	// Be clear about what the check below does and does not do. It is *not*
	// what catches that race: measured against the unfixed code over 200 runs,
	// the cleanup failed 169 times and this assertion fired zero, because the
	// goroutine has not reached MkdirAll yet at the instant the test body looks.
	// The detector is t.TempDir's own cleanup under -count. What this adds is
	// the invariant stated out loud — a cancelled call leaves the cache exactly
	// as it found it — which catches the version of this bug that is not a
	// race, such as klines creating its directories eagerly before checking the
	// context.
	entries, err := os.ReadDir(c.root)
	if err != nil {
		t.Fatalf("reading the cache root: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("a cancelled call left %s in the cache root, want it untouched", names(entries))
	}
}

func TestCacheValidatesItsArguments(t *testing.T) {
	t.Parallel()

	c, requests := newTestCache(t, nil)

	tests := []struct {
		name string
		ref  archiveRef
		spec decodeSpec
	}{
		{name: "bad ref", ref: archiveRef{Symbol: "BTCUSDT"}, spec: testSpec()},
		{name: "bad spec", ref: testRef(), spec: decodeSpec{Interval: Interval1h}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := c.klines(t.Context(), tt.ref, tt.spec); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("klines = %v, want an error wrapping ErrInvalidRequest", err)
			}
		})
	}

	// Validation happens before any I/O, which is the point of doing it in the
	// cache rather than leaving it to the decoder.
	if got := requests.Load(); got != 0 {
		t.Errorf("an invalid request made %d requests, want 0", got)
	}
}

func TestNewCache(t *testing.T) {
	t.Parallel()

	dl, _ := newFixtureServer(t, nil)

	t.Run("a downloader is required", func(t *testing.T) {
		t.Parallel()

		if _, err := newCache(t.TempDir(), nil); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("newCache = %v, want an error wrapping ErrInvalidRequest", err)
		}
	})

	t.Run("the root is made absolute", func(t *testing.T) {
		t.Parallel()

		c, err := newCache("relative/cache", dl)
		if err != nil {
			t.Fatalf("newCache: %v", err)
		}

		if !filepath.IsAbs(c.root) {
			t.Errorf("root is %q, want an absolute path", c.root)
		}
	})

	t.Run("an empty root means the platform cache directory", func(t *testing.T) {
		t.Parallel()

		c, err := newCache("", dl)
		if err != nil {
			t.Fatalf("newCache: %v", err)
		}

		want, err := defaultCacheDir()
		if err != nil {
			t.Fatalf("defaultCacheDir: %v", err)
		}

		if c.root != want {
			t.Errorf("root is %q, want %q", c.root, want)
		}
	})

	t.Run("nothing is created on disk", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		root := filepath.Join(dir, "cache")

		if _, err := newCache(root, dl); err != nil {
			t.Fatalf("newCache: %v", err)
		}

		if _, err := os.Stat(root); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("newCache created %s; constructing a cache should not touch the filesystem", root)
		}
	})
}

func TestWriteAtomic(t *testing.T) {
	t.Parallel()

	t.Run("creates the directory and the file", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "a", "b", "file.txt")

		if err := writeAtomic(path, func(w io.Writer) error {
			_, err := io.WriteString(w, "hello")

			return err
		}); err != nil {
			t.Fatalf("writeAtomic: %v", err)
		}

		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}

		if string(got) != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}

		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}

		// CreateTemp makes a file only its owner can read, which is right for a
		// temporary file and wrong for a cache entry other tools should be able
		// to open.
		if perm := info.Mode().Perm(); perm != cacheFilePerm {
			t.Errorf("permissions are %v, want %v", perm, cacheFilePerm)
		}
	})

	t.Run("replaces an existing file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "file.txt")

		if err := os.WriteFile(path, []byte("old"), cacheFilePerm); err != nil {
			t.Fatalf("writing: %v", err)
		}

		if err := writeAtomic(path, func(w io.Writer) error {
			_, err := io.WriteString(w, "new")

			return err
		}); err != nil {
			t.Fatalf("writeAtomic: %v", err)
		}

		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}

		if string(got) != "new" {
			t.Errorf("got %q, want %q", got, "new")
		}

		assertOnlyFiles(t, dir, "file.txt")
	})

	t.Run("a failed write leaves nothing behind", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "file.txt")
		boom := errors.New("no")

		err := writeAtomic(path, func(w io.Writer) error {
			// Some bytes are written before the failure, which is the case
			// that matters: a write that fails immediately would leave an
			// empty file rather than a torn one.
			if _, err := io.WriteString(w, "half a file"); err != nil {
				return err
			}

			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("writeAtomic = %v, want the write's own error", err)
		}

		assertNoFiles(t, dir)
	})

	t.Run("a write that never starts creates nothing at all", func(t *testing.T) {
		t.Parallel()

		// The ordinary way to fetch a month Binance never published:
		// fetchArchive asks for the 91-byte sidecar first, it 404s, and the
		// callback returns without writing a byte. That is a routine answer
		// rather than a fault, and it used to leave the whole directory tree
		// and a temporary file behind — created before the callback ran, on
		// the way to discovering there was nothing to put in them.
		root := t.TempDir()
		path := filepath.Join(root, "spot", "daily", "klines", "FOOUSDT", "1h", "file.zip")
		boom := errors.New("404")

		err := writeAtomic(path, func(io.Writer) error { return boom })
		if !errors.Is(err, boom) {
			t.Fatalf("writeAtomic = %v, want the write's own error", err)
		}

		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("reading the cache root: %v", err)
		}

		if len(entries) != 0 {
			t.Errorf("the cache root holds %d entries after a write that never began, want none", len(entries))
		}
	})

	t.Run("a write of nothing still produces a file", func(t *testing.T) {
		t.Parallel()

		// The other side of the same change. Deferring creation to the first
		// byte must not turn a successful write of zero bytes into a
		// successful call that left no file — the caller would be told the
		// entry is cached and find nothing there.
		dir := t.TempDir()
		path := filepath.Join(dir, "empty.txt")

		if err := writeAtomic(path, func(io.Writer) error { return nil }); err != nil {
			t.Fatalf("writeAtomic: %v", err)
		}

		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}

		if len(got) != 0 {
			t.Errorf("got %q, want an empty file", got)
		}

		assertOnlyFiles(t, dir, "empty.txt")
	})

	t.Run("a failed write leaves an existing file alone", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "file.txt")

		if err := os.WriteFile(path, []byte("old"), cacheFilePerm); err != nil {
			t.Fatalf("writing: %v", err)
		}

		if err := writeAtomic(path, func(w io.Writer) error {
			_, _ = io.WriteString(w, "new")

			return errors.New("no")
		}); err == nil {
			t.Fatal("writeAtomic returned nil for a failed write")
		}

		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}

		if string(got) != "old" {
			t.Errorf("got %q, want the original %q", got, "old")
		}

		assertOnlyFiles(t, dir, "file.txt")
	})
}

func TestReadSidecar(t *testing.T) {
	t.Parallel()

	const (
		name = "BTCUSDT-1h-2024-01-15.zip"
		sum  = "ea666c96c3441451554ba118c469805b34455156c8f1da0234bd7f2546874d99"
	)

	tests := []struct {
		name     string
		contents string
		wantName string
		want     string
		wantErr  bool
	}{
		{
			name:     "the published format",
			contents: sum + "  " + name,
			wantName: name,
			want:     sum,
		},
		{
			// A sidecar describing a different archive means a hand-copied or
			// hand-edited cache directory. Accepting it would compare an
			// archive against another archive's hash, and the mismatch would
			// surface later as corruption that never happened.
			name:     "names a different archive",
			contents: sum + "  BTCUSDT-1h-2024-01-16.zip",
			wantName: name,
			wantErr:  true,
		},
		{
			name:     "not a hash",
			contents: "not-a-hash  " + name,
			wantName: name,
			wantErr:  true,
		},
		{
			name:     "empty",
			contents: "",
			wantName: name,
			wantErr:  true,
		},
		{
			name:     "far too large",
			contents: strings.Repeat("x", vision.MaxChecksumSize+1),
			wantName: name,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), name+".CHECKSUM")

			if err := os.WriteFile(path, []byte(tt.contents), cacheFilePerm); err != nil {
				t.Fatalf("writing: %v", err)
			}

			got, err := readSidecar(path, tt.wantName)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("readSidecar = %q, want an error", got)
				}

				return
			}

			if err != nil {
				t.Fatalf("readSidecar: %v", err)
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("missing", func(t *testing.T) {
		t.Parallel()

		_, err := readSidecar(filepath.Join(t.TempDir(), "absent.CHECKSUM"), name)
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("readSidecar = %v, want an error wrapping fs.ErrNotExist", err)
		}
	})
}

// assertSameKlines compares two ranges candle by candle, using Kline.Equal
// rather than == for the reasons that method documents.
func assertSameKlines(t *testing.T, got, want []Kline) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d candles, want %d", len(got), len(want))
	}

	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("candle %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

// assertNoFiles requires dir to be absent or empty. It is how the tests state
// "the failure left nothing behind", including the temporary files an atomic
// write creates on its way to a rename.
func assertNoFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}

	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}

	if len(entries) != 0 {
		t.Errorf("%s holds %s, want nothing", dir, names(entries))
	}
}

// assertOnlyFiles requires dir to hold exactly the named entries, which is what
// catches a temporary file that was never cleaned up.
func assertOnlyFiles(t *testing.T, dir string, want ...string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}

	got := names(entries)
	if got != fmt.Sprint(want) {
		t.Errorf("%s holds %s, want %v", dir, got, want)
	}
}

func names(entries []fs.DirEntry) string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}

	return fmt.Sprint(out)
}

// waitFor blocks until cond is true, and fails the test if it never becomes so.
//
// Polling rather than a channel because the condition being waited on is a
// counter inside an httptest handler, and threading a signal out of it would
// change the thing under test. The deadline is generous: it exists to turn a
// hang into a failure, not to measure anything.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the condition")
		}

		time.Sleep(time.Millisecond)
	}
}
