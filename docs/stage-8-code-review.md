# Stage 8 code review

**Date:** 2026-08-21
**Branch:** `stage-8-cli`, reviewed as `git diff stage-7-loader...stage-8-cli`
**Commits:** `8ee79d6 Request.End is inclusive`, `67a5e23 Stage 8: the public API the CLI needs`,
`f425548 Stage 8: the bmd CLI`
**Scope:** every non-test hunk in `request.go`, `loader.go`, `options.go`, `kline.go`,
`availability.go`, `cache.go`, `parquet.go`, `doc.go`, all of `cmd/bmd/*.go`, plus
`docs/cli.md` and the stage table.

6 findings. `go vet ./...` and `go test ./...` are green, so each one below is a logic or
design defect rather than something the toolchain would have caught.

Fix order recommendation: **#1** and **#2** first — #2 can destroy a good archive on a
transient I/O error, and #1 is a visible defect on every real terminal that the tests
structurally cannot see. The rest are documentation-versus-behaviour drift and can ride
along.

## What was checked and holds up

The half-open → closed range conversion is the risky part of this branch, and it is
correct. `endExclusive` is the single conversion point; `resolve` feeds `plan.Expand` the
exclusive end; `checkGap` makes both `min` operands exclusive before comparing; the consume
filter uses the caller's own convention; `sizeHint` resolves before converting; and
`vision`'s `endTime = End-1ns` composes correctly with the new `+1ns` applied to both
bare-date and RFC 3339 `-end` values. No path loses or duplicates a candle.

---

## Correctness

### 1. The download summary is printed on top of the progress line

**`cmd/bmd/download.go:96`**

`defer progress.done()` runs *after* the summary at line 118 — the opposite of what its own
comment claims ("Finishing it before anything else is printed is what stops a summary
landing halfway through a redraw"). On a real terminal the last `report` call leaves the
cursor mid-line, having written `\r<line>` with no trailing newline, so the summary starts
at that column.

**Failure scenario.** `bmd download -symbol BTCUSDT -interval 1h ...` in an interactive
terminal. The final progress line is longer than the summary, so
`BTCUSDT 1h: wrote 720 candles to /path/x.csv` overwrites its prefix and the tail of the
progress line stays visible after it.

The tests never catch this: they pass a `bytes.Buffer`, so `tty` is false and `done()` is a
no-op — the ordering bug is invisible to every assertion in the suite.

**Fix.** Call `progress.done()` explicitly just before the summary. It is idempotent (it
clears `active`), so the `defer` stays for the error paths.

### 2. `bmd verify -rm` deletes intact archives on transient read errors

**`cmd/bmd/verify.go:101`**

The removal fires on *any* non-nil `entry.Err`. But `CacheEntry.Err` (`cache.go:855–865`)
documents three distinct outcomes, and only the `ErrChecksum` case means "delete the file";
the other two are described there as "not corruption" and "a fact about the disk rather
than about the data".

**Failure scenario.** The cache lives on a flaky external or NFS volume and one `io.Copy`
inside `hashFile` returns `EIO` — or a file is mode 0600 from another user and `os.Open`
returns `EACCES`. `bmd verify -rm` deletes an intact 93 MB archive and its sidecar because
of a transient read failure, and the next run re-downloads it.

**Fix.** Gate the removal on `errors.Is(entry.Err, binancedata.ErrChecksum)` — optionally
also the missing-sidecar case, which is genuinely unusable — and print the others as
"not removed".

---

## Contract and documentation drift

### 3. A negative `-concurrency` is accepted and ignored

**`cmd/bmd/flags.go:155`**

`if c.concurrency > 0` silently discards a negative value. That contradicts the rule stated
a few lines above in the same file — "an accepted-and-ignored setting is a defect, not a
stub" — and echoed in `docs/architecture.md`.

`bmd download -concurrency -4` runs at the default 8 and says nothing; the user has to read
the source to find out. Reject `< 0` as a usage error; 0 legitimately means "not given".
Same shape, lower stakes, at line 151 for `-cache-dir ""`.

### 4. The output file mode is not what its comment says

**`cmd/bmd/output.go:360`**

`os.Chmod(name, outputFilePerm)` is not subject to the umask, so the comment at lines 34–39
("0644 is what an ordinary shell redirect would produce", "before the umask") is wrong on
both counts.

**Failure scenario.** A user with `umask 077`, whose every other file lands 0600, gets a
group- and world-readable download.

`os.CreateTemp` already produces 0600. Either drop the `Chmod` and let the temp file's own
mode stand, or apply the umask explicitly. At minimum the comment must not claim umask
semantics it does not have.

### 5. The checksum suffix is hardcoded instead of shared

**`cmd/bmd/verify.go:133`**

The sidecar suffix is written as the literal `".CHECKSUM"` rather than
`vision.ChecksumSuffix` (`internal/vision/download.go:29`), which `cmd/bmd` *can* import —
it is inside the same module.

The two are equal today, so this is not a live bug. The failure mode if the constant ever
moves is silent: the archive is removed, the orphaned sidecar is left behind, and
`errors.Is(err, os.ErrNotExist)` swallows the mismatch without a word.

### 6. `CacheEntry.Path` is not always absolute

**`cache.go:851`**

The field is documented as "the archive's absolute path on disk", but it comes from
`filepath.WalkDir(c.root, …)` and is therefore relative whenever `WithCacheDir` was given a
relative path — `bmd verify -cache-dir ./cache` prints `cache/spot/…zip: …`.

Everything still works, because `removeEntry` resolves against the same working directory.
But a caller who stores or forwards the path, or a `bmd verify` run whose output is read
after a `cd`, gets a path that no longer resolves. Either `filepath.Abs` the root in
`verify`, or soften the doc comment.

---

## Resolution

Verified 2026-08-21 against the code, not taken on trust. **Five of the six hold
and were applied. One does not**, and it is recorded below rather than quietly
skipped.

Three were reproduced before anything was changed — two as failing tests, one by
running the built binary — and each was then watched failing again against the
unfixed code.

| # | Verdict | What changed |
| --- | --- | --- |
| 1 | **Confirmed**, mechanism corrected | `download` calls `progress.done()` explicitly before the summary; the `defer` stays for the error paths. The review describes the summary "overwriting the prefix" of the progress line, which would need it to start with `\r` — it does not, so what actually happened was concatenation: `[60/60] monthly archive 2024-03-31  720 candlesBTCUSDT 1h: wrote 720 ...`. `newProgress` is now a var so a test can reach the terminal branch at all; `TestDownloadSummaryStartsOnItsOwnLine` fails against the deferred call with `the summary is "\n"` |
| 2 | **Confirmed**, reproduced | Reproduced against the built binary: an intact archive at mode 000 was deleted, sidecar and all, on `permission denied`. Removal is now gated on the new `removable`, which passes `ErrChecksum` and `os.ErrNotExist` — a bad hash, or half an entry that can never be verified — and keeps everything else with a `not removed` line beside the failure. `TestVerifyKeepsAnArchiveItCouldNotRead` and a table over the five error shapes |
| 3 | **Confirmed** | Reproduced: `-concurrency -4` got past parsing, `Validate` and `newLoader`, which proves `WithConcurrency` was never called with it. `commonFlags.options` now takes the `FlagSet` and returns an error, so the check cannot be skipped. It uses `fs.Visit`, which separates "not given" from "given as empty" — the difference the string alone cannot carry, and the one that decides whether `-cache-dir "$CACHE_DIR"` with the variable unset points a `-rm` at the user's real cache. `-concurrency 0` is rejected too, for the same reason |
| 4 | **Confirmed**, and the same bug found in the library | Measured under `umask 077`: `os.Chmod(0644)` gives `-rw-r--r--` where a shell redirect gives `-rw-------`. The behaviour is kept and the comment now says what it does — `os.Chmod` is not umask-filtered, and Go has no portable way to read the umask back, since `syscall.Umask` is absent on Windows and is process-global. `cache.go` made the identical false claim ("the process umask applies on top, as always") over a const block covering both a `MkdirAll` mode, where it is true, and a `Chmod` mode, where it is not |
| 5 | **Confirmed**, different fix | The defect is real and the failure mode is silent, as described. The suggested fix is not taken: `cmd/bmd` imports only the root package, and `docs/cli.md` opens by claiming anything the CLI does can be done from Go code — which importing `internal/vision` would make false, since a consumer cannot. `CacheEntry` carries `Sidecar` instead, filled in before anything can fail, so the CLI needs no copy of Binance's naming rule and neither does any other caller |
| 6 | **Does not hold** — see below | Nothing changed |

### Why #6 does not hold

The finding claims `CacheEntry.Path` is relative whenever `WithCacheDir` was
given a relative path, and predicts that `bmd verify -cache-dir ./cache` prints
`cache/spot/…zip`.

`newCache` resolves the root before storing it — `cache.go:147`, "Resolved once,
here, rather than on every path built from it" — so `WalkDir` is handed an
absolute root and every path it yields is absolute. Run against the built
binary, `bmd verify -cache-dir ./relcache` prints the absolute path. The only
other construction of a `cache` is a test using `t.TempDir()`, which is absolute
as well. The doc comment was accurate and is unchanged.

### Also fixed, not in the review

`cache.go`'s `cacheFilePerm` comment carried the same false umask claim as #4.
It predates Stage 8 and was fairly out of the review's scope, but it is the
sentence `cmd/bmd/output.go` cross-references as "the same reason", so leaving
one right and the other wrong would have been worse than either.

`mise run ci` is green: 0 lint issues, 875 tests under `-race`, up from 859.
