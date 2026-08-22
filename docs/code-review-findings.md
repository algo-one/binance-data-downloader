# Code Review Findings

**Date:** 2026-08-22  
**Branch:** cache-management (4 commits: Stage 8/9 review fixes, cache accounting + pruning, multi-symbol download)  
**Status:** `go build`, `go vet`, and `go test ./...` all green. No compile or test failures.

## Critical: Silent Wrong-Answer Bugs

### 1. walkCacheFiles swallows mid-walk ErrNotExist — cache.go:1368

**Summary:** `walkCacheFiles` maps *any* error wrapping `fs.ErrNotExist` to nil—including one raised by the callback mid-walk—so `cache.usage` returns a partially-counted `CacheUsage` with a nil error, and `verify`/`prune` silently stop early.

**Failure scenario:** `bmd cache` runs while a `bmd download` is in flight. `WalkDir`'s `os.ReadDir` lists `.BTCUSDT-1h-2024-01.zip.tmp-9134`; `writeAtomic` renames it away before the callback reaches `d.Info()` (cache.go:1162), which returns ENOENT. The callback returns that error, `WalkDir` aborts the whole tree, and line 1368 maps it to nil. `usage()` returns whatever it had counted so far with `err == nil`, printing `archives 12  8.4 MB / total 36  24 MB` for a 400-entry, 2 GB cache as if that were complete. The same swallow turns a vanished subdirectory into `bmd verify` reporting `checked 40 archives, 0 failed` and exiting 0 on a cache it only partly looked at—exactly the outcome the comment above `WalkDir` says it refuses to produce.

**Fix approach:** Establish root-does-not-exist by `stat`'ing root once before the walk, not by pattern-matching the final error. Have `usage`'s callback record the `Info` failure (as `verify` and `prune` do) instead of aborting.

---

### 2. Zero-byte-only cache reports as empty — cmd/bmd/cache.go:88

**Summary:** `bmd cache` decides the cache is empty from a byte total, not a file count, so a cache holding only zero-byte files prints "empty" and hides the `other` row that exists to surface exactly those files.

**Failure scenario:** `writeAtomic` calls `os.CreateTemp`, which creates a 0-byte file before anything is written to it. Kill the process there (Ctrl-C during a download, OOM) and the cache holds `.BTCUSDT-1h-2024-01.zip.tmp-77` at 0 bytes. `u.Total()` is 0 while `u.OtherCount` is 1, so `writeCacheUsage` takes the `empty` branch at line 88 and prints the cache dir followed by `empty`. The stray file—the one thing the `Other` category was added to make visible—is reported as an empty cache. `TestCacheUsageCountsAStrayFile` writes `"interrupted write"` into its stray, so it never reaches this branch.

**Fix approach:** Test should be `u.ArchiveCount+u.SidecarCount+u.ParquetCount+u.OtherCount == 0`. Condition change from `u.Total() == 0` to checking file counts ensures empty output only when no files exist.

---

### 3. All-failed prune prints "no cached archives" — cmd/bmd/prune.go:163

**Summary:** `writePruneSummary` is never told about failures, so a prune in which every delete failed reports "no cached archives".

**Failure scenario:** A cache on a read-only mount holding three prunable archives. Every `os.Remove` returns EACCES, so each `PruneResult` has `Err` set: `failed` reaches 3 while `pruned` and `kept` both stay 0. `runPrune` then calls `writePruneSummary(stderr, dryRun, 0, 0, 0)`, which takes the `pruned == 0 && kept == 0` branch at line 163 and prints `no cached archives` to stderr—flatly contradicting the three `permission denied` lines just written to stdout and the `3 archives could not be removed` error that follows.

**Fix approach:** Pass `failed` count to `writePruneSummary` and check it before the early return at line 163. `TestPruneExitsNonZeroWhenADeleteFails` always mixes in one success, so this case is uncovered.

---

## Important: Contract & Coordination Issues

### 4. cache.prune races the cache build path — cache.go:1240

**Summary:** `cache.prune` deletes archives with no coordination with the cache's own read/build path (no singleflight key, no lock, no cross-process guard), so a prune landing between `ensureArchive`'s `Stat` and `build`'s `Open` turns a recoverable rebuild into a hard error.

**Failure scenario:** A parquet whose footer is intact but whose data pages have rotted. `cache.load` step 1 fails on the page CRC inside `readKlines`, so it falls through; `ensureArchive` (cache.go:481) stats the archive, finds it, and returns. Concurrently—another goroutine on the same `Loader` calling `PruneArchives`, or a second process running `bmd prune`—`checkParquet` passes (footer-only, see finding 5), the archive is judged prunable, and `os.Remove` runs. `cache.build` then does `os.Open(p.archive)` and gets ENOENT, returning `cache: opening BTCUSDT-1h-2024-01.zip: no such file or directory` to the caller instead of re-downloading, which is what a missing tier 1 is supposed to trigger.

**Risk:** Nothing in `Loader.PruneArchives`' doc warns that it must not overlap fetches, and `Loader` exposes both on the same object.

---

### 5. checkParquet omits the metaRows gate readKlines needs — parquet.go:438

**Summary:** `checkParquet` is documented and used as "the same gates `readKlines` opens with", but `readKlines` additionally requires the `metaRows` footer key to be present and parseable; a file failing only that is judged prunable yet cannot be read.

**Failure scenario:** A parquet whose footer lost or corrupted its `rows` key (truncated metadata, a file written by a different tool that copied the stamp keys, a partially-overwritten footer). `checkStamp` and `checkSchema` both pass, so `archivePrunable` returns nil, `bmd cache` counts the archive as reclaimable and `bmd prune` deletes it. The next read reaches `readKlines` line 518, `lookupInt(f, metaRows)` returns `errCacheStale`, `cache.load` falls through to `ensureArchive`, finds no archive, and downloads it over the network. The stated invariant "every read that succeeds before a prune succeeds after it" survives only because the download papers over it—the "no network on a hit" property that `TestPrunedCacheStillServesReadsWithoutRequests` asserts does not.

**Fix approach:** Move the `metaRows` lookup into `checkParquet` or stop claiming the two rules are identical.

---

### 6. verify checks nothing after a prune, still exits 0 — cmd/bmd/verify.go:75

**Summary:** After `bmd prune`, `bmd verify` has nothing left to walk and reports a clean cache with exit 0, because it only inspects `.zip` files and nothing verifies tier 2 or the orphaned sidecars.

**Failure scenario:** User follows docs/cli.md: runs `bmd prune`, which deletes every archive and leaves 412 sidecars and 412 parquet files. The next `bmd verify` walks the tree, matches no `.zip`, and prints `checked 0 archives (0 B), 0 failed` with exit 0—indistinguishable from a healthy verified cache, and read as such by the cron job the verify help text advertises. The 1.2 GB that reads are actually served from is never checked by anything except a per-page CRC at read time.

**Note:** Neither docs/cli.md's prune section nor its verify section notes that pruning retires the integrity command.

---

## Convention & Code Quality Issues

### 7. archivePrunable does I/O without a context — cache.go:1294

**Summary:** `archivePrunable` and `checkParquetFile` perform I/O without taking a context, violating CLAUDE.md's rule, and unlike this file's two existing exceptions they carry no justification for it.

**Details:** CLAUDE.md states: "Every function that does I/O takes `ctx context.Context` first." `archivePrunable` (cache.go:1294) opens and reads the sidecar and then calls `checkParquetFile` (cache.go:1315), which does `os.Open`, `Stat` and `parquet.OpenFile`—a seek and a footer read whose size is set by the file, not by a 91-byte bound. The file's two sanctioned exceptions, `readSidecar` and `writeAtomic`, each carry an explicit "Why there is no context here" section arguing the work cannot block long enough to matter; these two carry none.

**Risk:** On a slow network mount, `bmd cache` calls `archivePrunable` once per archive (100k opens on a large cache) with cancellation only observable between files, in `walkCacheFiles`.

---

### 8. Dead sidecar guard; comment states a false mechanism — cache.go:1243

**Summary:** The `strings.HasSuffix(path, vision.ChecksumSuffix)` guard in `prune` is dead, and the parallel comment in `usage` asserts the opposite of what its own next sentence establishes.

**Failure scenario:** A sidecar is named `BTCUSDT-1h-2024-01.zip.CHECKSUM`, so `filepath.Ext` is `.CHECKSUM`; the first half of the condition at line 1243 already excludes it and the `HasSuffix` half can never fire. In `usage`, the comment at line 1169 says the sidecar case "is tested first because it is the one that would be caught by the wrong arm otherwise" and is then contradicted two sentences later by "its filepath.Ext is `.CHECKSUM` and not `.zip`".

**Note:** Since CLAUDE.md makes the comments a primary deliverable, a comment that states a false mechanism as its justification is the defect here; the code is merely redundant.

---

### 9. Per-symbol wrapped errors discarded; comment now stale — cmd/bmd/download.go:217

**Summary:** `runDownload` wraps every per-symbol failure into a `failed []error` slice whose elements are discarded for multi-symbol runs, and the comment claiming the single-symbol error is "unchanged" is now false—it gained a symbol prefix.

**Details:** With `len(reqs) > 1` only `len(failed)` is ever read (line 249), so each `fmt.Errorf("%s: %w", req.Symbol, err)` allocates a wrapped error that is thrown away—a counter and the errors already printed to stderr would do the same job. With `len(reqs) == 1` the wrapped error IS returned (line 246), so `bmd download -symbol BTCUSDT ...` on a stream failure now prints `bmd: BTCUSDT: data not available` where Stage 8 printed `bmd: data not available`, while the comment at line 247 says "Unchanged from when this command took one symbol".

---

### 10. done() leaves width set, padding the next symbol's line — cmd/bmd/progress.go:121

**Summary:** `done()` clears `active` but leaves `width` set, so on a terminal the first progress line of each subsequent symbol is padded with trailing spaces to the previous symbol's line width.

**Failure scenario:** `bmd download -symbol BTC/USDT,ETH/USDT -interval 1h ... -out ./data` on a tty. BTCUSDT's last redraw sets `p.width` to 62. `downloadOne` calls `done()`, which prints `\n` and sets `active=false` but not `width=0`. ETHUSDT's first report builds a 41-character line, computes `pad = 62-41 = 21`, and emits `\r` + line + 21 trailing spaces on a fresh, empty line. Cosmetic, but the padding logic exists specifically to overwrite a line that is no longer there.

**Fix:** `done()` should reset `width` to 0.

---

### 11. parseSymbols empty-slice check is unreachable — cmd/bmd/flags.go:386

**Summary:** `parseSymbols` re-raises "-symbol is required" for an empty slice, a branch `checkSymbolFlag` has already made unreachable on the only path that calls it.

**Details:** `download()` calls `checkSymbolFlag(fs, symbols)` before `buildRequests`, and `checkSymbolFlag` returns an error for every empty case—distinguishing "never given" from "given but names no symbol", which is the whole reason it exists. By the time `parseSymbols` runs, `len(symbols) > 0` is guaranteed, so the check at line 386 is dead code whose message is strictly worse than the one that already fired (it cannot tell the two cases apart).

**Note:** It is the kind of duplicate guard that later invites someone to drop `checkSymbolFlag` on the grounds that `parseSymbols` "already handles it".

---

## Documentation Issues

### 12. Sidecar size comment says 91; measured value is 88 — cache.go:1064

**Summary:** The `CacheUsage.Sidecars` comment states sidecars are "Ninety-one bytes each", which disagrees with the 88 used everywhere else in this change and with what `FormatChecksum` actually produces.

**Details:** `FormatChecksum` is `sum + "  " + name` (internal/vision/download.go:442), so a sidecar is `64 + 2 + len(name)` bytes: 88 for `BTCUSDT-1m-2024-01.zip`, and 92 for `1000SATSUSDT-1mo-2024-01.zip`—i.e. neither fixed nor capped at 91.

**Discrepancy:**
- docs/caching.md says "an 88-byte sidecar"
- cmd/bmd/cache_test.go's `fullCache` fixture uses 88
- Working-tree docs/architecture.md now says "88–91 bytes"
- cache.go:1064 says "Ninety-one bytes each"

In a codebase whose comments are the deliverable and whose numbers are all claimed as measured, three different figures for the same file is exactly the drift the rest of this change avoids.

---

### 13. Multi-symbol example's candle counts contradict its range — docs/cli.md:91

**Summary:** The multi-symbol worked example prints candle counts that do not match the range it shows.

**Details:** The example run is `-interval 1h -start 2024-01-01 -end 2024-03-31`, an inclusive 91-day range, which is 2,184 hourly candles per symbol. The sample output claims `wrote 1440 candles` per symbol and `2 of 3 symbols, 2880 candles in total`. 1440 is a day of minutes, not three months of hours.

**Impact:** Anyone using the block to sanity-check their own run compares against a number that cannot occur.

---

## Priority for Fixing

1. **Critical (silent wrong-answer):** Findings 1–3 (walkCacheFiles, zero-byte cache, all-failed prune)
2. **High (contract/coordination):** Findings 4–6 (prune race, checkParquet gate, verify after prune)
3. **Medium (convention, cleanup):** Findings 7–11
4. **Low (docs):** Findings 12–13

---

## Resolution (2026-08-22)

All thirteen findings were checked against the source. Eleven were fixed, two were
answered without a code change. `mise run ci` is green: build, `golangci-lint` (0
issues), fmt, and 947 tests under `-race`.

Every fix below has a test that fails without it, except where noted.

| # | Verdict | What was done |
| --- | --- | --- |
| 1 | **Fixed.** Confirmed exactly as described. | The "root does not exist" question is now settled by one `os.Stat` before the walk, so no `fs.ErrNotExist` from *inside* the walk is swallowed. `usage`'s callback additionally tolerates `ErrNotExist` from `d.Info()` — a file that is gone occupies nothing, so counting nothing for it is accurate, and aborting would make `bmd cache` fail whenever it ran during a download. Every other `Info` error still stops the walk. |
| 2 | **Fixed.** | `writeCacheUsage` decides emptiness from the file counts, which are now computed once and shared with the total row. |
| 3 | **Fixed.** | `writePruneSummary` takes `failed` and the "no cached archives" branch requires all three counts to be zero. |
| 4 | **Documented, not fixed in code.** The race is real; the suggested remedies are not available. | Notably *not* a singleflight key: that group deduplicates identical work and would hand a prune somebody else's candles rather than serialise against it. A mutex would close only the in-process half — a second `bmd prune` process is outside any lock this one can take. `Loader.PruneArchives` now states the coordination rule plainly, which is the honest fix for a constraint nothing enforces. **A one-shot retry in `cache.load`** — re-download when `build` finds tier 1 gone — would close it for every cause including the cross-process one, and is the right follow-up if this is worth more than a doc line. |
| 5 | **Fixed, and one gap wider than reported.** | `checkParquet` was missing *two* footer-decidable gates `readKlines` applies, not one: the `bmd.rows` lookup, and the check that the stamped count equals the row groups' total. Both are now there. The test is written as the invariant rather than as a case list — every footer `readKlines` refuses for a footer reason must be refused here too — and four of its cases fail against the old code. |
| 6 | **Fixed.** | `bmd verify` on a cache with no archives now says `no cached archives to verify (a pruned cache keeps only sidecars and parquet)` instead of borrowing the healthy-cache sentence with a zero in it. It still exits 0, because nothing failed — what changed is that a 0 no longer reads as "every archive hashes to what Binance published". docs/cli.md gains a *Pruning retires `bmd verify`* section saying so and naming what does still cover tier 2. |
| 7 | **Answered with a justification, not a context.** | Threading `ctx` here would buy nothing real: every call underneath is an `os.File` operation and the `os` package consults no context, so the parameter would advertise a cancellation that cannot be honoured mid-read. The cancellation a caller can actually observe already exists at the right granularity — `walkCacheFiles` checks `ctx.Err` before every file. `archivePrunable` now carries the "Why there is no context here" section the finding correctly noted was missing, in the form `readSidecar` and `writeAtomic` use, and names what would change the answer. |
| 8 | **Fixed, both halves.** | The `HasSuffix` guard in `prune` is provably dead — `HasSuffix(path, ".CHECKSUM")` implies `filepath.Ext(path) == ".CHECKSUM"`, so the extension test has already excluded it — and is gone. `usage`'s comment no longer claims the sidecar arm is ordered first out of necessity; it says the arms are disjoint and that the ordering is what keeps this switch from depending on a naming rule that belongs to Binance. |
| 9 | **Fixed by dropping the wrap, not by amending the comment.** | The prefix was redundant in both directions: the multi-symbol branch prints its own `SYMBOL: ...` line and reads only `len(failed)`, and the single-symbol branch returns `failed[0]` to `main`, where naming the only symbol the user typed is noise. Storing the error unwrapped makes the comment true again and honours what docs/cli.md promises about the one-symbol case. |
| 10 | **Fixed.** | `done()` resets `width`. |
| 11 | **Kept, with the reason written down. Disagreed with removing it.** | The guard is unreachable today, but the finding argues from what it invites rather than from what it costs, and what the alternative fails as is worse than a duplicate message: an empty slice produces zero requests, `runDownload` loops zero times, and the command prints `0 of 0 symbols` and exits 0 — a silent success for a run that downloaded nothing. It stays as a labelled backstop that names `checkSymbolFlag` as the check with the better message. |
| 12 | **Fixed, and the finding's own arithmetic corrected.** | A sidecar is `64 + 2 + len(name)`, so the size moves with the name: 88 for `BTCUSDT-1m-2024-01.zip`, 89 for `BTCUSDT-1mo-2024-01.zip`, 91 for a daily one, 95 for the longest symbol at a daily period. The finding gives 92 for `1000SATSUSDT-1mo-2024-01.zip`; it is 94. `CacheUsage.Sidecars` and docs/architecture.md now say "about ninety bytes" and explain what moves it, rather than any one figure. The remaining `91 bytes` mentions describe specific daily fixtures, which are genuinely 91. |
| 13 | **Fixed.** | 2,184 candles per symbol and 4,368 in total, with the arithmetic — 91 inclusive days × 24 — written under the block so the next person can check it. |

### What this says about the tests

Every one of these passed the suite before the fix. Findings 2, 3 and 10 were each
one assertion away from being caught by a test that already existed, and finding 5
was uncovered because nothing tested `checkParquet` directly at all — it was only
ever exercised through prune, which cannot tell a gate that is missing from a gate
that passed. The new tests are: two on `walkCacheFiles`, two on the
`checkParquet`/`readKlines` invariant (eight sub-cases), one zero-byte cache, two
prune summary, one progress width, one single-symbol error, and two on verify's
empty-cache line.
