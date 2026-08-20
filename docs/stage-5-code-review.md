# Stage 5 code review

**Date:** 2026-08-19
**Branch:** `main` (uncommitted working tree, on top of `7c1db91 Remove the Stage 4 review doc`)
**Scope:** the Stage 5 diff — `cache.go`, `parquet.go` and their tests, plus the touched
support files (`internal/vision/download.go`, `.github/workflows/ci.yml`, `CLAUDE.md`,
`README.md`, `docs/architecture.md`, `docs/caching.md`, `go.mod`, `go.sum`, `mise.toml`).

15 findings, ten sub-agent finder angles run in parallel (five correctness, three
cleanup, one altitude/design, one conventions), each spot-checked against the source.

Fix order recommendation: **#1** first — it is the only finding that panics a goroutine
instead of returning an error, which is exactly the failure mode every other corruption
path in this file was written to avoid. **#2** and **#3** next, since both can make the
cache silently do the wrong thing (re-download, or leak a stranger's cancellation)
without ever surfacing as an error a caller can act on.

---

## Resolution (2026-08-20)

Each finding was re-checked against the source before anything was changed. Where a
finding's claim and its stated consequence disagreed, the claim was fixed and the
consequence corrected here.

| # | Outcome |
| --- | --- |
| 1 | **Fixed.** Reproduced first: a file with `high` stored as `INT64` panics in `unsafe.Slice` exactly as described. `checkSchema` now compares physical type and width as well as name, and `readGenericPage` checks the value's kind before asking for its bytes. Covered by `TestCheckSchemaRejectsAMistypedColumn` and `TestReadGenericPageRejectsAMistypedValue`. |
| 2 | **Fixed**, with the scenario corrected. A directory that has lost search permission fails at `os.CreateTemp` before any download, so the re-download only happens on a file-specific fault such as `EIO` or a stale handle. `ensureArchive` now reports any stat error that is not `fs.ErrNotExist`. `TestCacheReportsAnUnreadableArchive` uses a self-referential symlink to produce one; without the fix that test does not merely error differently — the read *succeeds*, having silently re-downloaded and overwritten tier 1. |
| 3 | **Declined as written; a smaller change made instead.** The bound is deliberate and `klines`' own doc comment already argues why unbounded retry is worse. The real defect is narrower: the error returned once the retries are spent still carried `context.Canceled` in its chain. It is now flattened to a string, the same way `load` handles a superseded read, so a live caller cannot mistake it for its own cancellation. |
| 4 | **Fixed**, with the stated consequence withdrawn. Losing the archive's rename does not undermine the archive-first/sidecar-second invariant: `ensureArchive` stats the archive, finds it missing and re-downloads, and a surviving parquet stays valid because validity keys on the sidecar hash rather than the archive's presence — which `load` documents as intentional. `writeAtomic` now syncs the containing directory anyway, best effort and reporting nothing, because the cost of the sync failing is a cache miss and both tiers are re-derivable. |
| 5 | **Declined.** Needs `GOARCH=386`/`arm`; CI builds `ubuntu-latest` and `macos-latest` only, and on a 32-bit build `strconv.Atoi` already bounds `stamped`. No reachable failure on a supported target. |
| 6 | **Declined.** The finding calls itself benign, and it is: a negative `stamped` fires the existing guard immediately. |
| 7 | **Fixed.** `vision.ReadChecksum` now holds the bounded read both call sites shared, and `maxSidecarBytes` is gone — the one limit is `vision.MaxChecksumSize`. |
| 8 | **Fixed.** `load` reads the sidecar once and passes the hash to `ensureArchive`. The "doubles the cost of the cold path" framing oversells it — it was one 91-byte read against a multi-megabyte download — but the redundancy is real and the fix is smaller than the argument for it. |
| 9 | **Declined for the code, comment added.** `writeAtomic`'s cancellable work happens inside the `write` callback, where `fetchArchive` and `writeKlines` both take a context and both stop; `Sync` and `Rename` are not cancellable at any layer. A parameter nobody consults advertises a cancellation that does not exist. Both functions now record the exception and its reasoning. |
| 10 | **Fixed**, in the code rather than the docs. Measured at 497-499 allocations over five runs, so `docs/` was right and `parquet.go`'s "461" was not reproducible. After finding 11 the figure is **479**, now consistent across `parquet.go`, `docs/caching.md` and `docs/architecture.md`. |
| 11 | **Fixed**, and smaller than claimed. The batch buffer is hoisted to `readColumn` via a `pageBuffer`. Measured saving: 19 allocations of 498, about 4%, and roughly 1% of bytes — not "the hot read path the design's performance case rests on". |
| 12 | **Declined.** Two explicit loops read better than one helper taking two function-typed parameters, given this file's stated bar for being readable as a teaching text. |
| 13 | **Fixed by removing the duplication**, though the consequence was overstated: those name strings only fed an error message, and a mis-*pairing* would already have been caught by `TestKlineParquetRoundTrip`, since the real fixtures have distinct values in all eight decimal columns. `toParquetRow` now takes the names from `klineColumns` and there is no second list; `firstDecimalColumn` pins the offset and `TestKlineColumnsMatchTheSchema` checks it. |
| 14 | **Fixed.** Both functions now carry doc comments, including the count-then-error contract that explains why the loop is shaped the way it is. |
| 15 | **Declined.** The finding says itself it is not a defect. |

Nine fixed, six declined. `mise run ci` is green: fmt, lint, `-race` tests (562) and build.

A note on the review itself, since it is a document that will be read again: finding 1 is
excellent and would have been worth the whole exercise on its own. But several entries
below it are self-admitted non-issues (#6, #15), one contradicts reasoning the code
already spells out (#3), and one needs an architecture the project does not build for
(#5). The severity language runs ahead of what the code does in #4, #8, #11 and #13.
Padding a findings list is not free — it is what teaches the next reader to skim.

---

## Correctness

### 1. `checkSchema` validates column names but not physical type, so a mistyped column can panic instead of erroring

**`parquet.go:621`**

```go
func checkSchema(f *parquet.File, want parquetStamp) error {
	columns := f.Schema().Columns()
	if len(columns) != len(klineColumns) {
		return fmt.Errorf(...)
	}

	for i, path := range columns {
		if len(path) != 1 || path[0] != klineColumns[i].name {
			return fmt.Errorf(...)
		}
	}

	return nil
}
```

The check compares `path[0]` — the column's *name* — against `klineColumns[i].name`. It
never looks at the column's physical type. `readPage` (`parquet.go:704`) relies on a type
assertion to pick the fast path and falls back to `readGenericPage` (`parquet.go:778`)
whenever the assertion fails.

**Failure scenario.** A tier-2 file has a column named `"high"` that is physically stored
as `INT64` instead of `FIXED_LEN_BYTE_ARRAY(16)` — plausible from a corrupted file, a
foreign writer, or a future bug where a decimal field in `parquetRow` was declared with
the wrong Parquet type. `checkSchema` passes because the name matches. `readPage`'s
assertion to `parquet.BE128Reader` fails, so it falls to `readGenericPage`, which
unconditionally does `raw := v.ByteArray()` when `col.setDec != nil`
(`parquet.go:795`). parquet-go's `Value.ByteArray()` is `unsafe.Slice(v.ptr, v.u64)`; for
an `Int64`-kind `Value`, `ptr` is nil and `u64` holds the actual integer payload.
`unsafe.Slice` panics whenever `ptr` is nil and the length is non-zero — so any nonzero
int64 value (a real timestamp, say) crashes the calling goroutine instead of returning
`ErrCorruptArchive` or `errCacheStale` the way every other corruption path in this file
is designed to.

Confirmed by reading parquet-go v0.32.0's `value.go` — `makeValueInt64` leaves `ptr` nil
and stores the integer in the `u64` field that `byteArray()` later slices from.

`checkSchema`'s own doc comment says the point of the check is that "a file with the same
columns in a different order... is rejected rather than read into the wrong fields" — the
same reasoning applies to a column with the right name and the wrong physical type, and
nothing here catches it.

---

### 2. `ensureArchive` treats any `os.Stat` error as "archive absent," not just "does not exist"

**`cache.go:410`**

```go
func (c *cache) ensureArchive(ctx context.Context, ref archiveRef, p cachePaths) (string, error) {
	if sum, err := readSidecar(p.sidecar, p.name); err == nil {
		if _, err := os.Stat(p.archive); err == nil {
			return sum, nil
		}
	}

	// ... falls through to download and overwrite
}
```

The inner `if _, err := os.Stat(p.archive); err == nil` treats *every* non-nil `Stat`
error identically — there is no `errors.Is(err, fs.ErrNotExist)` distinction.

**Failure scenario.** The cache directory's containing folder loses search/execute
permission, or a network-mounted cache hits a transient `EIO`/stale-handle, while a valid
sidecar and archive are both on disk. `readSidecar` succeeds. `os.Stat(p.archive)` returns
a non-`ErrNotExist` error (`EACCES`/`EIO`). Either way the function falls straight through
to a full re-download and overwrite — contradicting `ensureArchive`'s own doc comment,
which says tier 1 "is immutable once written, and it is never re-downloaded." A real
filesystem fault is silently treated as ordinary cache-fill traffic instead of being
surfaced as an error.

---

### 3. The foreign-cancellation retry cap can still leak a stranger's cancellation to a live caller

**`cache.go:300`**

```go
case res := <-ch:
	if res.Err != nil {
		if attempt < cacheRetriesOnForeignCancel && isForeignCancellation(ctx, res.Err) {
			continue
		}

		return nil, res.Err
	}
```

`cacheRetriesOnForeignCancel` bounds the retry at a fixed count.

**Failure scenario.** Under Stage 7's bounded worker pool, three or more short-timeout
callers in sequence each become the singleflight leader for one hot archive key and are
each cancelled before finishing. A fourth caller whose own `ctx` is still alive retries
twice (attempts 0 and 1), exhausts `cacheRetriesOnForeignCancel = 2` on the third failure,
and returns `res.Err` — a `context.Canceled`/`context.DeadlineExceeded` that has nothing
to do with its own context — to its caller, who may reasonably do
`errors.Is(err, context.Canceled)` and wrongly conclude it was the one that gave up.

---

### 4. `writeAtomic` never fsyncs the containing directory after the rename

**`cache.go:626`**

```go
if err := os.Rename(tmp, path); err != nil {
	return fmt.Errorf("cache: renaming %s: %w", tmp, err)
}

renamed = true

return nil
```

The function syncs the file's own contents (`f.Sync()`, line 612) before renaming, but
nothing afterwards syncs `dir` — the directory the rename's entry lands in.

**Failure scenario.** A power loss or kernel panic occurs immediately after
`os.Rename(tmp, path)` returns success but before the filesystem journal commits the
directory-entry change. On some filesystems and mount options this can revert the rename
on next boot even though the calling code already believed the write succeeded — and, for
tier 1, went on to write the sidecar next. That undermines the "archive written first,
sidecar second" crash invariant the design's own comment (lines 405-409) relies on, since
the archive's rename is not itself durably guaranteed at the point the sidecar write
begins.

---

### 5. `rowGroup.NumRows()` is truncated to `int` before the bounds check meant to catch a corrupt count

**`parquet.go:399`**

```go
rows := int(rowGroup.NumRows())

if rows < 0 || rows > parquetRowGroupRows {
	return nil, fmt.Errorf(...)
}
```

`NumRows()` returns `int64`; the conversion to `int` happens *before* either guard runs.

**Failure scenario.** On a 32-bit Go build (`GOARCH=386`/`arm`), a row group whose real
row count is `2^32 + N` for `N` in `[0, 65536]` truncates via `int(...)` to a small
positive `N` that then sails past both `rows < 0` and `rows > parquetRowGroupRows`, since
the truncation happens before either guard runs. The reader then proceeds to allocate and
read `N` rows from a page that does not actually hold that many values — most likely
surfacing as a confusing downstream error rather than the clear "row group claims N rows"
message the guard exists to produce.

---

### 6. `stamped`, the footer's untrusted row count, has no negative-value guard unlike its neighbors

**`parquet.go:385`, used at `parquet.go:411`**

```go
stamped, err := lookupInt(f, metaRows)
...
if len(out)+rows > stamped {
	return nil, fmt.Errorf(...)
}
```

`rows` and `f.NumRows()` are both explicitly checked/clamped against negative values
before use (lines 380, 406), but `stamped` — read from `lookupInt`, itself parsed from an
attacker- or corruption-controlled footer string — is not.

**Failure scenario.** Currently benign: a negative `stamped` just makes
`len(out)+rows > stamped` fire immediately and return an error. But it is the one
untrusted arithmetic input in this function without a defensive lower-bound guard, unlike
every other count this function reads from the file. Worth a guard on the same principle
as its neighbors, so the next change to this function doesn't build on the assumption that
all footer-derived counts are already checked.

---

## Quality / convention

### 7. `readSidecar` duplicates the bounded-read logic `internal/vision`'s `Checksum` already has

**`cache.go:531-541`**, compare **`internal/vision/download.go:235-241`**

```go
// cache.go
b, err := io.ReadAll(io.LimitReader(f, maxSidecarBytes+1))
...
if len(b) > maxSidecarBytes {
	return "", fmt.Errorf("cache: %s is larger than %d bytes", filepath.Base(path), maxSidecarBytes)
}
```

`internal/vision/download.go`'s `Checksum` has the same shape — `io.LimitReader(src,
max+1)`, the `len(b) > max` check, an equivalent "larger than %d bytes" error — with two
separately-maintained size constants (`maxSidecarBytes` vs `maxChecksumSize`).

This is the same two-parsers-for-one-format drift that `ParseChecksum`/`FormatChecksum`
were exported in this diff to prevent, but the surrounding bounded-read wrapper itself was
left unshared. The next tuning of the size limit or the error wording fixes only one of
the two copies.

---

### 8. The sidecar is read and parsed twice per request on the cold path

**`cache.go:357` and `cache.go:411`**

`load` reads the sidecar to get `sum` (line 357). If that read fails with anything other
than a stale/corrupt-but-present file, control falls through to `ensureArchive`, whose
first line (411) opens and parses the same sidecar file again, discarding the first
result.

**Failure scenario (not a bug, a cost).** On every first-ever cache miss, and on every
mass rebuild (e.g. a `CodecVersion` bump making every entry stale), this doubles the
file-open/read/parse cost of the cold path for no benefit — worth folding into a single
read passed through, since both call sites want the same value.

---

### 9. `readSidecar` and `writeAtomic` don't take `ctx context.Context` first, unlike their sibling in the same file

**`cache.go:523`, `cache.go:574`**

CLAUDE.md states: *"Every function that does I/O takes `ctx context.Context` first."*
`readSidecar` does `os.Open`/`io.ReadAll` with no `ctx` parameter; `writeAtomic` does
`CreateTemp`/`Sync`/`Close`/`Rename` with no `ctx` parameter — while `readParquetFile`, a
plain local-file read in the same file, does take `ctx` first. A large `writeAtomic` write
(the parquet-build path) currently has no way to be cancelled or context-annotated at this
layer, which is inconsistent with the stated convention.

---

### 10. The benchmark numbers in `parquet.go`'s comment disagree with both docs files

**`parquet.go:335`**, compare **`docs/caching.md:103,122`** and **`docs/architecture.md:362`**

```go
// Reading each column as one contiguous run of values instead takes 6.4 ms and
// allocates 461 times: ...
```

`docs/caching.md` and `docs/architecture.md` both cite "6.1 ms, 498 allocations" for what
is described as the identical measurement. `docs/caching.md`'s Stage-5 status note
explicitly claims "the measurements are from the code rather than from estimates that
preceded it" — one of the two was not updated after the other, which undermines the
credibility of the exact numbers either document cites.

---

### 11. `readInt64Page`, `readDecimalPage` and `readGenericPage` each allocate a fresh batch buffer per page instead of per column

**`parquet.go:723, 749, 779`**

```go
func readInt64Page(dst []Kline, col klineColumn, reader parquet.Int64Reader) (int, error) {
	buf := make([]int64, parquetBatchRows)
	...
}
```

`readColumn` calls `readPage` once per page (`parquet.go:663`), and each call re-allocates
`buf` from scratch, rather than a buffer hoisted to `readColumn` and reused across pages.

**Cost.** For any column chunk spanning more than one page — which grows more likely as
row-group size approaches `parquetRowGroupRows` — this adds allocations proportional to
page count, on the hot read path the design's whole performance case rests on (see
finding 10's numbers).

---

### 12. The three page readers duplicate the same batch-loop/bounds-check shape

**`parquet.go:722, 748, 778`**

```go
if n > len(dst)-filled {
	return 0, errTooManyValues(len(dst))
}
```

`readInt64Page` and `readDecimalPage` differ only in element type (`int64` vs
`[16]byte`) and reader method name; the identical guard above is typed out twice, and
adapted a third time in `readGenericPage`. A future change to that guard — a different
error, an added check — is one easy-to-miss edit away from drifting between the copies. A
generic helper parameterized over the batch type would collapse at least the `int64` and
decimal cases.

---

### 13. `klineColumns` and `toParquetRow`'s `fields` table are two independently hand-maintained lists, with no test tying them together

**`parquet.go:507-520`** (write side) vs **`parquet.go:561-611`** (read side)

Adding, renaming, or reordering a decimal field on `Kline` requires editing three places
in sync: `parquetRow`'s struct tag, `toParquetRow`'s `fields` array, and `klineColumns`.
`TestKlineColumnsMatchTheSchema` (per `parquet_test.go`) only checks `klineColumns`
against `parquetRow`'s own schema — nothing checks `fields` against `klineColumns`. A
mismatch between the write-side and read-side name tables (e.g. a typo'd field name in one
but not the other) would not be caught by that test.

---

### 14. `readInt64Page` and `readDecimalPage` carry no doc comments, unlike every neighboring function in the file

**`parquet.go:722, 748`**

CLAUDE.md states heavy in-code comments explaining *why* are "a primary deliverable, not
polish." The file otherwise comments every function this densely — `readPage`
immediately above and `readGenericPage` immediately below both carry substantial
explanations of the two-path design and its trade-offs. `readInt64Page` and
`readDecimalPage` have zero doc comments, standing out as thinner than the file's own bar
in a file where that bar is unusually high.

---

### 15. The discarded-read error is flattened to a bare string, a one-off special case for a pattern that could recur

**`cache.go:384-391`**

```go
if stale != nil {
	// The discarded read's message is flattened to a string on
	// purpose. Wrapping it with a second %w would put it in the error
	// chain, and errors.Is would then answer questions about a file
	// that is already gone...
	return nil, fmt.Errorf("%w (rebuilt after: %s)", err, stale.Error())
}
```

The comment explains *why* this is correct for today's one call site: `stale.Error()`
rather than a second `%w` deliberately keeps the discarded cause out of the error chain.
That reasoning is sound as written, but if Stage 7 adds its own rebuild-on-failure layer
on top, this ad hoc string-flattening is the kind of thing likely to be copy-pasted rather
than factored into a general "annotate with a superseded cause" helper. Not a defect —
worth watching if the pattern shows up a second time.

---

## Process notes

All ten finder angles (five correctness, three cleanup, one altitude/design, one
conventions) completed via forked sub-agents reading `cache.go`/`parquet.go` directly.
Two angles — removed-behavior and cross-file tracing — returned no findings after genuine
investigation. Finding 1 (the `checkSchema`/`readGenericPage` panic) was not raised by any
single finder angle on its own; it surfaced while chasing a more general observation that
`checkSchema` only checks names, and was confirmed by reading parquet-go v0.32.0's actual
source (`value.go`) to trace `Value.ByteArray()`'s `unsafe.Slice` behavior for a mistyped
column.
