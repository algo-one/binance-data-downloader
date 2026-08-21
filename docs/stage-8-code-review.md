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
