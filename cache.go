package binancedata

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sync/singleflight"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// This file is the cache: two tiers on disk, and the rules for when each may be
// believed. docs/caching.md is the design; this is the implementation of it.
//
// # The two tiers
//
// Tier 1 is the archive exactly as Binance served it, beside the .CHECKSUM
// sidecar it was published with. It is the source of truth, it is immutable
// once written, and it is never re-downloaded.
//
// Tier 2 is a parquet file derived from tier 1 by [writeKlines]. It is what
// reads actually hit, and it is disposable by construction: anything wrong with
// it is answered by building it again from tier 1.
//
//	cache/spot/klines/BTCUSDT/1h/monthly/
//	    BTCUSDT-1h-2024-01.zip
//	    BTCUSDT-1h-2024-01.zip.CHECKSUM
//	    BTCUSDT-1h-2024-01.parquet
//
// # What a hit costs
//
// Reading the sidecar — 91 bytes — and the parquet footer, which is one seek to
// the end of the file. Under a millisecond, against the ~62 ms of CSV parsing
// it replaces. On that path the archive is never opened and never re-hashed:
// re-hashing tier 1 on every read would cost more than the parse this whole
// design exists to avoid.
//
// # Every write is atomic
//
// Nothing is written to a final path. Bytes go to a temporary file in the
// destination directory and are renamed into place, so a crash leaves either
// the old file or the new one. Writing straight to the destination is the trap:
// an interrupted run leaves a truncated file that looks perfectly valid to the
// next process to open it, and a parquet file truncated after its footer is a
// short candle range with nothing to say it is short.

const (
	// cacheDirPerm and cacheFilePerm are what the cache creates.
	//
	// They are the ordinary permissions for a cache under a user's home
	// directory rather than the tighter 0700/0600: nothing here is secret —
	// every byte came from a public bucket — and one of the stated benefits of
	// tier 2 being parquet is that other tools can read it. A file this
	// library's own CLI can read but pandas cannot would forfeit that for no
	// gain.
	//
	// The two are not reached the same way, and the difference is easy to state
	// wrongly. cacheDirPerm goes to os.MkdirAll, which creates, so the process
	// umask filters it: under umask 077 a cache directory is 0700. cacheFilePerm
	// goes to os.Chmod, which sets a mode outright and is *not* umask-filtered,
	// so a cache file is 0644 whatever the umask says. That asymmetry is a
	// consequence of writing atomically — os.CreateTemp hardcodes 0600, so the
	// mode has to be set after the fact — and it is left as it is on purpose,
	// because the reason above is about the file rather than about the caller's
	// default. A private umask should not quietly make the parquet tier
	// unreadable to the tools it exists for.
	cacheDirPerm  fs.FileMode = 0o755
	cacheFilePerm fs.FileMode = 0o644

	// parquetExt and archiveExt are the two file extensions in a cache
	// directory. The sidecar's suffix comes from [vision.ChecksumSuffix],
	// because it is Binance's name for it rather than ours.
	archiveExt = ".zip"
	parquetExt = ".parquet"

	// cacheRetriesOnForeignCancel bounds how often [cache.klines] will re-ask
	// for work that was abandoned by a *different* caller's cancellation. See
	// the comment on that method for what this is defending against, and why a
	// bound rather than an unconditional loop.
	cacheRetriesOnForeignCancel = 2
)

// cache stores archives and their decoded form under a root directory.
//
// It is safe for concurrent use, and more than merely safe: concurrent requests
// for the same archive are collapsed into one piece of work by the singleflight
// group below.
type cache struct {
	// root is the directory holding every cached file, as an absolute path.
	// Absolute because a relative one would follow the process's working
	// directory, and a long-running program that calls os.Chdir would then have
	// two caches without ever being told.
	root string

	// dl fetches archives that are not on disk yet.
	dl *vision.Downloader

	// sf collapses duplicate concurrent work.
	//
	// # What singleflight is
	//
	// A map from a key to a call that is currently running. Ten goroutines that
	// ask for the same key while the first one is still working share its
	// result: the function runs once and the other nine wait. Nothing is
	// remembered afterwards — it merges calls that overlap in time, which makes
	// it a deduplicator rather than a cache.
	//
	// # Why this type needs one
	//
	// Two overlapping requests — January to March and February to April for the
	// same symbol — both want February. Without this they both download the
	// same archive and both build the same parquet file, and while the atomic
	// rename means neither corrupts the other, the second one is pure waste:
	// twice the bytes off the network and twice the CPU.
	//
	// This is also where the ported implementation had a real bug. It
	// registered its deduplication entry *after* taking a concurrency permit,
	// so a saturated pool let several tasks past the check before any of them
	// registered, and the same month was downloaded several times over.
	// Registration here happens on the way in, before any I/O, which is what
	// correctness requirement 5 asks for; Stage 7's bounded pool then sits
	// outside this, never between the check and the work.
	//
	// The zero value is ready to use, which is why there is no constructor call
	// for it in [newCache].
	sf singleflight.Group
}

// newCache returns a cache rooted at dir, downloading through dl.
//
// An empty dir means [defaultCacheDir]. dl is required: a cache that cannot
// fetch what it does not have is one that reports an unhelpful error the first
// time it is asked for anything, and a constructor that returns an error is the
// place to say so.
func newCache(dir string, dl *vision.Downloader) (*cache, error) {
	if dl == nil {
		return nil, fmt.Errorf("cache: a downloader is required: %w", ErrInvalidRequest)
	}

	if dir == "" {
		d, err := defaultCacheDir()
		if err != nil {
			return nil, err
		}

		dir = d
	}

	// Resolved once, here, rather than on every path built from it.
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("cache: resolving %q: %w", dir, err)
	}

	// The directory is deliberately not created here. Constructing a loader
	// should not touch the filesystem, and the write path creates what it needs
	// anyway — so a caller that only ever gets cache hits, or one whose
	// requests all fail validation, leaves no empty directory tree behind.
	return &cache{root: abs, dl: dl}, nil
}

// defaultCacheDir is where the cache lives when nobody says otherwise:
// a "bmd" directory inside the operating system's own cache location.
//
// os.UserCacheDir is the portable spelling of a place that differs everywhere —
// ~/Library/Caches on macOS, $XDG_CACHE_HOME or ~/.cache on Linux,
// %LocalAppData% on Windows. Using it means the cache lands where the platform
// expects, which matters because these directories are the ones a system knows
// it may clear: the data here is re-downloadable by definition, and nothing in
// it should ever be backed up.
func defaultCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("cache: locating the user cache directory: %w", err)
	}

	return filepath.Join(base, "bmd"), nil
}

// cachePaths is where one archive's three files live.
//
// They are computed together because they share a directory and a stem, and
// because computing them separately is how two of them end up disagreeing.
type cachePaths struct {
	// name is the archive's file name, e.g. BTCUSDT-1h-2024-01.zip. It is what
	// the sidecar must name and what the parquet footer records.
	name string

	// archive, sidecar and parquet are absolute paths.
	archive string
	sidecar string
	parquet string
}

// paths locates ref's three files inside the cache.
//
// Every component is validated first, for the reason archivePrefix validates
// the same four values: each of them formats into something that looks like a
// legal path segment, so an unset interval becomes a directory named
// "Interval(0)" and an empty symbol lets the interval slide into its place.
// A bucket key built that way 404s, which is at least visible. A *file* path
// built that way is worse — it silently creates a second cache tree that will
// never be hit again and never cleaned up.
func (c *cache) paths(ref archiveRef) (cachePaths, error) {
	market, err := marketPath(ref.Market)
	if err != nil {
		return cachePaths{}, err
	}

	if !ref.Agg.valid() {
		return cachePaths{}, fmt.Errorf("aggregation %s: %w", ref.Agg, ErrInvalidRequest)
	}

	if !ref.Interval.IsValid() {
		return cachePaths{}, fmt.Errorf("interval %s: %w", ref.Interval, ErrInvalidRequest)
	}

	if err := checkNormalizedSymbol(ref.Symbol); err != nil {
		return cachePaths{}, err
	}

	name, err := archiveName(ref.Symbol, ref.Interval, ref.Agg, ref.Period)
	if err != nil {
		return cachePaths{}, err
	}

	// filepath.Join, not path.Join: these are filesystem paths, and on Windows
	// the separator is a backslash. The bucket keys in availability.go use
	// path.Join for the mirror-image reason — URLs are always forward slashes.
	dir := filepath.Join(c.root, market, DataTypeKlines.String(), ref.Symbol, ref.Interval.String(), ref.Agg.String())

	return cachePaths{
		name:    name,
		archive: filepath.Join(dir, name),
		sidecar: filepath.Join(dir, name+vision.ChecksumSuffix),
		parquet: filepath.Join(dir, strings.TrimSuffix(name, archiveExt)+parquetExt),
	}, nil
}

// klines returns every candle in one archive, from the cache where possible.
//
// This is the cache's whole public surface within this package. Stage 7 calls
// it once per chunk; everything below it — which tier answered, whether
// anything was downloaded, whether the parquet had to be rebuilt — is this
// file's business.
//
// # Concurrency
//
// Calls that name the same archive while one is already running are collapsed
// into that one call. The waiters get a copy of its result rather than the
// result itself, because a []Kline handed to two callers is a slice two callers
// can write into: Stage 7 merges and trims these in place, and one caller's
// trim would silently truncate another's range.
//
// # The retry loop
//
// singleflight has one sharp edge, and it is worth understanding rather than
// working around. The function runs under the context of whichever caller
// arrived first, so if *that* caller cancels — a backtest interrupted, a
// timeout elapsed — every other caller waiting on the same archive is handed
// its context error, describing a cancellation that has nothing to do with
// them. The loop below detects exactly that case: an error that is a
// cancellation, on a call whose own context is still perfectly alive. The
// answer is to ask again, since the abandoned call has already left the group
// and a fresh one runs under a live context.
//
// It is bounded rather than unconditional because "ask again" is only certainly
// progress if nobody else is cancelling in a loop, and an unbounded retry that
// depends on the good behaviour of other callers is a hang waiting for the
// right workload.
func (c *cache) klines(ctx context.Context, ref archiveRef, spec decodeSpec) ([]Kline, error) {
	p, err := c.paths(ref)
	if err != nil {
		return nil, err
	}

	if err := spec.validate(); err != nil {
		return nil, err
	}

	// A context that is already dead gets no work started for it.
	//
	// This is not a shortcut — it is the only place the caller and the work can
	// still be separated. Below, singleflight.DoChan runs load in a *new*
	// goroutine, and the select deliberately abandons it on cancellation rather
	// than stopping it: whoever else is waiting still wants the download, and
	// if nobody is, it finishes and populates the cache for the next run. That
	// is the right policy for a cache whose directory outlives any one request.
	//
	// It is the wrong policy for a caller who was cancelled before asking,
	// because there is no "whoever else" — the entry is created by this call.
	// The goroutine would run load with the same dead context, get as far as
	// writeAtomic's MkdirAll and CreateTemp (both of which happen *before* the
	// callback where the cancelled download actually fails), and only then give
	// up. So a call that returned an error still left directories and a
	// temporary file behind, written after the caller had gone.
	//
	// That surfaced as a flaky test rather than as a bug report: t.TempDir's
	// cleanup runs RemoveAll on the cache root the instant the test returns,
	// races the abandoned goroutine creating paths inside it, and fails with
	// "directory not empty" perhaps once in a hundred runs.
	//
	// The same shape as [Policy.wait], which checks ctx even for a zero delay
	// so that a cancelled context cannot perform one more request.
	//
	// It sits inside the loop rather than in front of it, which matters for the
	// one path that comes back around: the foreign-cancellation retry below
	// re-enters DoChan, and between that decision and this point the caller's
	// own context can end. Guarding only the entrance would leave the last
	// attempt starting exactly the abandoned goroutine the guard exists to
	// prevent — rarely, and therefore worse.
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// DoChan rather than Do, because Do offers no way to stop waiting.
		// A caller whose own context is cancelled has to be able to walk away
		// from a download that is still running for somebody else, and only a
		// channel can be selected against ctx.Done().
		ch := c.sf.DoChan(p.parquet, func() (any, error) {
			// The result crosses back as `any`, which is Go's empty interface:
			// a value of any type at all. It is what a library written before
			// generics has to use, and the type assertion below is the price.
			return c.load(ctx, ref, spec, p)
		})

		select {
		case <-ctx.Done():
			// Walking away does not cancel the work. Whoever else is waiting
			// still wants it, and if nobody is, it finishes and populates the
			// cache for the next run.
			return nil, ctx.Err()

		case res := <-ch:
			if res.Err != nil {
				if isForeignCancellation(ctx, res.Err) {
					if attempt < cacheRetriesOnForeignCancel {
						continue
					}

					// The retries are spent and the error still belongs to
					// somebody else. Calling Error() rather than wrapping with
					// %w is the same deliberate flattening load does with a
					// superseded read, and for the same kind of reason: leaving
					// context.Canceled in the chain would let a caller whose own
					// context is perfectly alive ask errors.Is(err,
					// context.Canceled), get true, and conclude it was the one
					// who gave up.
					return nil, fmt.Errorf("cache: %s: abandoned %d times by other callers' cancellations (last: %s)",
						p.name, attempt+1, res.Err.Error())
				}

				return nil, res.Err
			}

			// The comma-ok form of a type assertion cannot panic, which matters
			// here: a panic in a library is a caller's crash, and there is a
			// perfectly good error to return instead. It should be impossible —
			// load returns []Kline and nothing else does.
			klines, ok := res.Val.([]Kline)
			if !ok {
				return nil, fmt.Errorf("cache: %s: internal error: unexpected result type %T", p.name, res.Val)
			}

			if res.Shared {
				// Shared reports that more than one caller received this
				// value, so the copy is only paid for when it is needed.
				klines = slices.Clone(klines)
			}

			return klines, nil
		}
	}
}

// isForeignCancellation reports whether err is a cancellation belonging to
// somebody else: a context error, on a call whose own context is still live.
func isForeignCancellation(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// load is the body of one cache lookup, run once per archive by singleflight.
//
// The order of what it tries is the design:
//
//  1. Read the sidecar for the archive's hash, then read the parquet and check
//     it against that hash and against [CodecVersion]. This is the steady
//     state, and it touches neither the archive nor the network.
//  2. Failing that, make sure tier 1 is present, downloading and verifying it
//     if it is not.
//  3. Build tier 2 from tier 1, and return the candles that were decoded on the
//     way through.
//
// Step 1 deliberately does not require the archive to exist. Tier 1 is only
// needed to build, so a cache whose archives have been pruned to reclaim disk
// still serves every read that does not need a rebuild.
func (c *cache) load(ctx context.Context, ref archiveRef, spec decodeSpec, p cachePaths) ([]Kline, error) {
	var stale error

	// Read once, here, and passed to ensureArchive below. Both steps want the
	// same 91 bytes, and reading them twice would open, read and parse the same
	// file twice on every cold path. An unreadable sidecar is reported as the
	// empty string rather than an error, because at this point it is not one:
	// no sidecar is the ordinary state of an archive that has never been
	// fetched, and the only thing either caller does about it is download.
	sum, err := readSidecar(p.sidecar, p.name)
	if err != nil {
		sum = ""
	}

	if sum != "" {
		klines, err := readParquetFile(ctx, p.parquet, parquetStamp{SourceFile: p.name, SourceSHA256: sum})
		switch {
		case err == nil:
			return klines, nil

		case ctx.Err() != nil:
			// The read was interrupted rather than found wanting. Falling
			// through to the download would turn a cancellation into network
			// traffic.
			return nil, err

		case !errors.Is(err, fs.ErrNotExist):
			// Worth carrying: "there was a file here and it could not be used"
			// is the interesting half of a later failure. A file that simply
			// does not exist yet is not, and is the ordinary first-run case.
			stale = err
		}
	}

	sum, err = c.ensureArchive(ctx, ref, p, sum)
	if err != nil {
		return nil, err
	}

	klines, err := c.build(ctx, spec, p, sum)
	if err != nil {
		if stale != nil {
			// The discarded read's message is flattened to a string on
			// purpose. Wrapping it with a second %w would put it in the error
			// chain, and errors.Is would then answer questions about a file
			// that is already gone — reporting ErrCorruptArchive, say, for a
			// failure whose actual cause was a 404.
			return nil, fmt.Errorf("%w (rebuilt after: %s)", err, stale.Error())
		}

		return nil, err
	}

	return klines, nil
}

// ensureArchive makes sure tier 1 is on disk and returns its published SHA-256.
//
// If both files are already there, nothing is downloaded, opened or hashed —
// the hash comes from the sidecar, which was written only after the bytes it
// describes were verified against it.
//
// The two files are written archive first, sidecar second. The order gives the
// stronger of the two possible invariants: a sidecar on disk implies the
// archive beside it, so a crash between the two writes leaves an archive with
// no sidecar, which the next run treats as absent and downloads again. The
// reverse order would leave a sidecar vouching for a file that is not there.
// sum is the hash [load] already read from the sidecar, or "" if there was
// nothing readable there. A non-empty sum means the sidecar exists and names
// this archive, so only the archive's own presence is still in question.
func (c *cache) ensureArchive(ctx context.Context, ref archiveRef, p cachePaths, sum string) (string, error) {
	if sum != "" {
		switch _, err := os.Stat(p.archive); {
		case err == nil:
			return sum, nil

		case !errors.Is(err, fs.ErrNotExist):
			// Anything other than "it is not there" is a fault in the
			// filesystem, not an empty cache slot: a directory that lost
			// search permission, a network mount returning EIO, a stale NFS
			// handle. Falling through on one of those would overwrite tier 1 —
			// which this function's own contract says is never re-downloaded —
			// on the strength of a question that was never actually answered.
			return "", fmt.Errorf("cache: %s: %w", p.name, err)
		}
	}

	// res carries out of the closure below, which is how a function that hands
	// its writer to somebody else returns anything about what was written.
	var res vision.Result

	err := writeAtomic(p.archive, func(w io.Writer) error {
		var err error
		res, err = fetchArchive(ctx, c.dl, ref, w)

		return err
	})
	if err != nil {
		return "", err
	}

	// fetchArchive already compared this hash against Binance's sidecar and
	// refused to return without a match, so what is written here is a value
	// this process computed over the bytes on disk rather than one it copied
	// from a server on trust.
	err = writeAtomic(p.sidecar, func(w io.Writer) error {
		_, err := io.WriteString(w, vision.FormatChecksum(res.SHA256, p.name))

		return err
	})
	if err != nil {
		return "", err
	}

	return res.SHA256, nil
}

// build decodes tier 1 into tier 2 and returns the candles.
//
// The archive is not re-hashed on the way through. That is the same decision as
// on the read path, for the same reason — verification happens where it is
// affordable, at download time and on demand — and it has one consequence worth
// naming: the hash this stamps into the footer is the one the sidecar carries,
// so bit rot in a stored archive would be inherited by the file built from it
// rather than caught here. `bmd verify` in Stage 8 is what re-hashes tier 1.
//
// A failed build leaves nothing behind: the parquet is written to a temporary
// file and renamed only after [writeKlines] returns, so a corrupt archive
// produces an error and no cache entry, and the next run tries again rather
// than reading a half-built file.
func (c *cache) build(ctx context.Context, spec decodeSpec, p cachePaths, sum string) ([]Kline, error) {
	f, err := os.Open(p.archive)
	if err != nil {
		return nil, fmt.Errorf("cache: opening %s: %w", p.name, err)
	}

	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cache: %s: %w", p.name, err)
	}

	stamp := parquetStamp{SourceFile: p.name, SourceSHA256: sum}

	var klines []Kline

	err = writeAtomic(p.parquet, func(w io.Writer) error {
		var err error
		klines, err = writeKlines(ctx, w, decodeArchive(ctx, f, info.Size(), spec), stamp, spec.estimateRows())

		return err
	})
	if err != nil {
		return nil, err
	}

	return klines, nil
}

// readParquetFile opens a tier-2 file and reads it if it may be used.
//
// A missing file returns an error wrapping [fs.ErrNotExist], which the caller
// distinguishes from every other failure: it is the ordinary state of a cache
// entry that has not been built yet, not a fault.
func readParquetFile(ctx context.Context, path string, want parquetStamp) ([]Kline, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cache: %s: %w", filepath.Base(path), err)
	}

	// An *os.File is an io.ReaderAt, which is what parquet needs: the format is
	// read by seeking to the footer and then to the pages that are wanted,
	// never front to back. That is the same requirement archive/zip has, and
	// the reason both take a size alongside the reader — neither format can be
	// located without knowing where the end is.
	return readKlines(ctx, f, info.Size(), want)
}

// readSidecar reads a .CHECKSUM file from disk and returns the hash in it.
//
// wantName is the archive the sidecar must name. Checking it here as well as on
// the network path is what stops a mismatched pair — one archive's sidecar
// beside another's data, which no ordinary accident produces but a hand-edited
// or hand-copied cache directory does — from surfacing later as a checksum
// failure that sends the reader hunting for corruption that never happened.
//
// # Why there is no context here
//
// CLAUDE.md asks every function that does I/O to take one first, and this is a
// deliberate exception rather than an oversight. The rule exists so that work
// which can block is work a caller can walk away from; a bounded read of 91
// bytes from a local file cannot block long enough for that to mean anything,
// and neither os.Open nor io.ReadAll would consult a context if it were passed.
// A parameter nobody reads advertises a cancellation that does not exist, which
// is worse than not offering one. [writeAtomic] is the same exception for the
// same reason.
func readSidecar(path, wantName string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer func() { _ = f.Close() }()

	// The bound and the parse both live in vision, beside the writer of this
	// same format. The local path and the network path read sidecars from
	// different sources but have the identical two decisions to make about the
	// bytes, and one copy of those is the whole point of the shared function.
	sum, err := vision.ReadChecksum(f, wantName)
	if err != nil {
		return "", fmt.Errorf("cache: %s: %w", filepath.Base(path), err)
	}

	return sum, nil
}

// writeAtomic writes a file by writing another one and renaming it.
//
// # Why not just write the file
//
// Because a write is not one operation. Bytes reach the disk in pieces, and a
// process that is killed — or a machine that loses power — between the first
// piece and the last leaves a file that exists, has a plausible size, and is
// wrong. Every reader afterwards is entitled to believe it. This is the tenth
// of the correctness requirements in docs/architecture.md and the tenth of the
// ported implementation's bugs.
//
// rename(2) is the fix, because within one filesystem it is atomic: any other
// process sees either the old file or the new one, never a mixture and never a
// partial. The temporary file therefore has to be created in the *destination
// directory* rather than in the system temp directory, since a rename across
// filesystems is a copy followed by a delete, which is exactly the
// non-atomic write this avoids.
//
// # The cleanup
//
// The deferred function removes the temporary file unless the rename succeeded,
// so every failure — a network error mid-download, a corrupt archive, a
// cancelled context — leaves the cache exactly as it was.
//
// # Nothing is created until there are bytes
//
// The directory tree and the temporary file used to be made first, before the
// callback ran, which meant every failure that happened before the first byte
// still left something behind. That is not a rare path: [fetchArchive] asks for
// the 91-byte sidecar before the archive, so fetching a month Binance never
// published — an ordinary answer, not an error in the system — created
// data/spot/daily/klines/FOOUSDT/1h/ and a temporary file inside it purely to
// learn that there was nothing to put there. Deferring both to the first Write
// means a write that never starts leaves no trace at all. See [tempFile].
//
// # No context here either
//
// For the reason given on [readSidecar]. The work inside this function that can
// actually block — the download, the parquet build — happens in the write
// callback, and both of those already take a context and already stop when it
// is cancelled. Sync and Rename are not cancellable at any layer, so a context
// parameter here would decorate the signature without changing what a caller
// can do.
func writeAtomic(path string, write func(io.Writer) error) error {
	dir := filepath.Dir(path)

	// The pattern's "*" is where CreateTemp puts its random suffix. The leading
	// dot hides the file from a casual listing, and the name deliberately does
	// not end in .zip or .parquet so that nothing scanning the directory for
	// cache entries can mistake a half-written one for a finished one.
	t := &tempFile{dir: dir, pattern: "." + filepath.Base(path) + ".tmp-*"}

	renamed := false

	defer func() {
		if !renamed {
			t.discard()
		}
	}()

	if err := write(t); err != nil {
		return err
	}

	// A write that succeeded without producing a byte still has to produce a
	// file, or the rename below has nothing to rename and a successful call
	// would leave no cache entry. Nothing this library writes is legitimately
	// empty, so this is the guard for a case that should not arise rather than
	// a path with a use — which is exactly the kind worth handling, since the
	// alternative is a nil dereference.
	if err := t.create(); err != nil {
		return err
	}

	f, tmp := t.f, t.f.Name()

	// Sync asks the operating system to put the bytes on the physical device.
	// Without it the rename can reach the disk before the data does, so a
	// crash leaves a correctly named file full of zeroes — the exact failure
	// this function exists to prevent, arrived at by a different route.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("cache: syncing %s: %w", tmp, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("cache: closing %s: %w", tmp, err)
	}

	// CreateTemp makes the file readable only by its owner, which is right for
	// a temporary file and wrong for a cache entry. Chmod rather than a mode on
	// creation, and so deliberately not umask-filtered — see cacheFilePerm.
	if err := os.Chmod(tmp, cacheFilePerm); err != nil {
		return fmt.Errorf("cache: setting permissions on %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("cache: renaming %s: %w", tmp, err)
	}

	renamed = true

	// The file's own bytes were synced above; this syncs the directory entry
	// that now points at them. They are two separate pieces of state, and only
	// the second one records that the name exists at all — so without this a
	// crash just after the rename can come back to a directory that never heard
	// about it, even though every byte of the file is safely on the platter.
	//
	// It matters most for tier 1, where the sidecar is written straight after
	// the archive: the invariant that a sidecar implies the archive beside it is
	// only worth as much as the archive's rename is durable.
	syncDir(dir)

	return nil
}

// tempFile is the destination [writeAtomic] hands to its callback: an io.Writer
// that brings its own file into existence the first time it is written to, and
// not before.
//
// # Why this is not just an *os.File
//
// Because creating the file is itself a side effect on a shared directory, and
// the decision to write can fail after the writer exists and before any byte
// reaches it — a 404 for an archive that was never published, a cancelled
// context, a checksum that will not match. Creating eagerly turns every one of
// those into a directory tree and a temporary file that outlive the call.
//
// # What it still leaves behind
//
// Directories, once a write has genuinely begun and then failed. Those are not
// removed, and the reason is a race rather than an oversight: several months of
// one symbol share a directory, Stage 7 fetches them concurrently, and a failed
// writer tidying away a directory it created can delete the one another writer
// has just created and is about to write into. An empty directory costs an
// inode; a spurious ENOENT costs a download. The trade is not close.
type tempFile struct {
	dir     string
	pattern string

	// f is nil until the first write. Its presence is the record of whether
	// anything needs cleaning up.
	f *os.File
}

// Write implements io.Writer, creating the file on the way past if it does not
// exist yet.
func (t *tempFile) Write(p []byte) (int, error) {
	if err := t.create(); err != nil {
		return 0, err
	}

	return t.f.Write(p)
}

// create makes the directory and the temporary file, once. Calling it again is
// free, which is what lets Write call it on every byte without checking.
func (t *tempFile) create() error {
	if t.f != nil {
		return nil
	}

	if err := os.MkdirAll(t.dir, cacheDirPerm); err != nil {
		return fmt.Errorf("cache: creating %s: %w", t.dir, err)
	}

	f, err := os.CreateTemp(t.dir, t.pattern)
	if err != nil {
		return fmt.Errorf("cache: creating a temporary file in %s: %w", t.dir, err)
	}

	t.f = f

	return nil
}

// discard closes and removes the file if there is one, and does nothing at all
// if the write never started.
//
// Both errors are ignored deliberately, and the explicit assignment says so at
// the call site rather than leaving a reader to wonder. Close after a failed
// write is very likely to fail again, and Remove after a failed Close cannot be
// acted on — the operation being reported is the one that already went wrong.
func (t *tempFile) discard() {
	if t.f == nil {
		return
	}

	_ = t.f.Close()
	_ = os.Remove(t.f.Name())
}

// syncDir flushes a directory's own metadata, which is what makes a rename into
// it survive a crash. It is best effort, and it reports nothing.
//
// # Why every error here is dropped
//
// Because by the time this runs the write has already succeeded by every
// measure the caller has: the rename returned, and the file is readable now by
// this process and any other. What is still in question is only whether that
// fact survives a power loss in the next few milliseconds — and the answer when
// it does not is a cache miss, not a wrong answer. The next run finds the
// archive absent and downloads it, or finds the parquet absent and rebuilds it.
// Both tiers are re-derivable by construction, and that is the property that
// makes a failure here not worth failing a completed write over.
//
// It is also not portable. Syncing a directory is a POSIX idea: Windows will
// not open a directory handle this way at all, and some filesystems answer with
// EINVAL or ENOTSUP rather than doing anything. Reporting those would break
// cache writes on platforms where the extra durability was never on offer.
//
// The explicit assignments are this repository's convention for an error that
// was considered and dropped, rather than one that was forgotten.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}

	_ = d.Sync()
	_ = d.Close()
}

// ---------------------------------------------------------------------------
// Verification on demand
// ---------------------------------------------------------------------------

// CacheEntry is one cached archive and what verifying it found.
type CacheEntry struct {
	// Path is the archive's absolute path on disk.
	Path string

	// Sidecar is the absolute path of the .CHECKSUM file that names this
	// archive's published hash. It is always set, including when reading it is
	// what failed, because a caller acting on a failed entry needs to name both
	// halves of it.
	//
	// It is a field rather than something a caller derives, and the reason is
	// worth stating: deriving it means knowing that the suffix is ".CHECKSUM"
	// and that it is appended to the whole file name rather than replacing the
	// extension. That is Binance's naming rule, not this library's, and a
	// caller who hardcodes it has taken on a rule it cannot see change. The
	// cost of the rule moving is silent — a delete that removes the archive and
	// leaves an orphan — which is the kind of coupling a field cheaply removes.
	Sidecar string

	// Size is the archive's size in bytes, which is what makes a progress
	// display possible: hashing is proportional to it.
	Size int64

	// Err is nil when the archive's bytes hash to the value in its .CHECKSUM
	// sidecar. Otherwise it says why not, and the three answers call for
	// different responses:
	//
	//   - wrapping [ErrChecksum]: the bytes on disk are not what Binance
	//     published. Delete the file; the next fetch downloads it again.
	//   - wrapping [fs.ErrNotExist]: half an entry. The cache writes the
	//     archive first and the sidecar second, so a crash between the two
	//     leaves exactly this. It is not corruption, and the read path already
	//     treats it as a cache miss — but what is left cannot be verified or
	//     used, so deleting it is safe and reclaims the space.
	//   - anything else: an I/O failure reading one of the two files, or a
	//     sidecar whose contents will not parse. This is a fact about the disk
	//     rather than about the data, and the archive may be perfectly good.
	//     Do not delete on this one; report it and let a person look.
	//
	// Compare with [errors.Is], never with ==. The first two cases are
	// deliberately distinguishable that way, because "delete it" and "leave it
	// alone" is the decision every caller of [Loader.VerifyCache] has to make.
	Err error
}

// verify re-hashes every archive under the cache root against its .CHECKSUM
// sidecar, yielding one [CacheEntry] per archive in directory order.
//
// [Loader.VerifyCache] is the exported door to it, and carries the part of this
// comment a caller needs. What follows is why it is written the way it is.
//
// This is the on-demand half of correctness requirement 9. The read path
// deliberately does not do it: verification is paid once, at download, and
// re-hashing a 93 MB archive on every read would cost more than the CSV parse
// the whole second tier exists to avoid. That leaves one gap — a file that was
// correct when written and rotted afterwards — and this is how it is closed,
// when a caller decides to spend the I/O.
//
// # What the two error channels mean
//
// The yielded error is the one that ends iteration: the cache directory could
// not be walked, or the context was cancelled. A per-archive verdict is not
// that, and does not stop anything — reporting every bad file is the entire
// job, so a mismatch arrives in [CacheEntry.Err] with a nil error beside it.
// This is the same split [Loader.Stream] uses.
//
// # What it does not check
//
// The second tier. A parquet file carries the source archive's hash and the
// codec version in its footer, and the read path checks both on every read and
// rebuilds when either fails — so tier 2 is verified continuously by the code
// that uses it, and a scan here would only repeat work that already happens.
// Tier 1 is the tier nothing re-reads, which is why it is the tier this walks.
//
// An empty cache yields nothing and no error. So does a cache directory that
// was never created, since a cache with no files in it is exactly what that is.
func (c *cache) verify(ctx context.Context) iter.Seq2[CacheEntry, error] {
	return func(yield func(CacheEntry, error) bool) {
		root := c.root

		// WalkDir over Walk: it reads directory entries lazily and hands back
		// a fs.DirEntry, so it does not stat every file it passes. On a cache
		// of a hundred thousand archives that is the difference between one
		// syscall per file and two.
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				// A directory that vanished under us, or one we cannot read.
				// Returning it stops the walk, which is the honest outcome:
				// a report that silently skipped a subtree would say the cache
				// is clean when part of it was never looked at.
				return err
			}

			if d.IsDir() || filepath.Ext(path) != archiveExt {
				return nil
			}

			// Checked per file rather than per directory: hashing is the
			// expensive part, and a cancelled caller should not wait out one
			// more 93 MB archive.
			if err := ctx.Err(); err != nil {
				return err
			}

			entry := verifyArchive(path, d)

			if !yield(entry, nil) {
				// The consumer broke out of the range loop. fs.SkipAll ends
				// the walk without reporting an error, which is right: nothing
				// failed, somebody stopped asking.
				return fs.SkipAll
			}

			return nil
		})

		switch {
		case err == nil, errors.Is(err, fs.ErrNotExist):
			// A cache directory that does not exist yet holds no bad files.
			return
		default:
			yield(CacheEntry{}, fmt.Errorf("cache: verifying %s: %w", root, err))
		}
	}
}

// verifyArchive re-hashes one archive and compares it with its sidecar.
//
// Every failure lands in the returned entry rather than being returned, because
// this function's caller is a walk that must not stop for a bad file.
func verifyArchive(path string, d fs.DirEntry) CacheEntry {
	// Both paths are filled in before anything can fail, so every return below
	// — including the ones that never get as far as opening the sidecar —
	// describes a whole cache entry rather than half of one.
	entry := CacheEntry{Path: path, Sidecar: path + vision.ChecksumSuffix}

	info, err := d.Info()
	if err != nil {
		entry.Err = err

		return entry
	}

	entry.Size = info.Size()

	// The sidecar first, and deliberately so: it is 91 bytes against up to
	// 93 MB, so a cache entry missing its sidecar costs one open instead of a
	// full hash to diagnose. readSidecar also checks that the sidecar names
	// this archive, which catches a file copied out of the wrong directory.
	want, err := readSidecar(entry.Sidecar, filepath.Base(path))
	if err != nil {
		entry.Err = err

		return entry
	}

	sum, err := hashFile(path)
	if err != nil {
		entry.Err = err

		return entry
	}

	if sum != want {
		entry.Err = fmt.Errorf("cache: %s hashes to %s, sidecar says %s: %w",
			filepath.Base(path), sum, want, ErrChecksum)
	}

	return entry
}

// hashFile returns the SHA-256 of a file's contents, in lowercase hex.
//
// io.Copy into the hash rather than os.ReadFile into memory: an archive can be
// 93 MB, and there is no reason for any of it to be resident at once. io.Copy
// reuses a 32 KB buffer, and sha256.New's Write is what consumes it.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer func() { _ = f.Close() }()

	h := sha256.New()

	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	// %x on a byte slice is lowercase hex, which is the case Binance publishes
	// its sidecars in and the case ReadChecksum normalises to.
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
