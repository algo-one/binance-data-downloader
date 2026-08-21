# Caching

> **Status:** live as of Stage 5. `cache.go` holds the tiers and the atomic
> writes, `parquet.go` holds the schema and the footer stamp, and `CodecVersion`
> lives in `codec.go`. Everything below is implemented; the measurements are
> from the code rather than from estimates that preceded it.

## The problem

This library is embedded in a backtesting framework, so the same historical
range is read on every run. Measured on a real `BTCUSDT-1m-2024-01.zip`
(44,640 rows, 2.2 MB compressed):

| Step | Cost |
| --- | --- |
| deflate decompression alone | ~40 ms |
| decompress + split all 12 CSV fields | ~150 ms |
| udecimal parsing (8 fields × 44,640 rows @ ~38 ns) | ~14 ms |

A realistic Go implementation lands around **60–70 ms per symbol-month**. That
is ~4 s per symbol for five years of one-minute data, or **~40 s of startup for
ten symbols — on every single backtest run.**

**Measured, now that the decoder exists (Stage 3):** 1.39 µs per row on the same
machine, so a 44,640-row month decodes in **~62 ms**. The estimate above was made
before the code was written and the code landed inside it, so the rest of this
document rests on a measurement rather than a projection.

Downloading is not the problem; re-parsing is.

## Two tiers

**Tier 1 — the raw ZIP plus its `.CHECKSUM`.** Byte-for-byte what Binance
served. Never re-downloaded once stored, re-verifiable forever, no schema of our
own, no dependency. This is the source of truth.

**Tier 2 — a Parquet file derived from tier 1.** This is what reads actually
hit. Columnar, so a backtest that only needs OHLCV skips the taker-volume
columns entirely; snappy decompresses far faster than deflate; and there is no
text parsing at all.

Disk cost is roughly 2×, about 5 MB per symbol-month. Parquet being readable by
other tools is a free bonus, not a design driver.

## Layout

```
cache/
└── spot/klines/BTCUSDT/1h/
    ├── monthly/
    │   ├── BTCUSDT-1h-2024-01.zip
    │   ├── BTCUSDT-1h-2024-01.zip.CHECKSUM
    │   └── BTCUSDT-1h-2024-01.parquet
    └── daily/
        ├── BTCUSDT-1h-2026-08-16.zip
        ├── BTCUSDT-1h-2026-08-16.zip.CHECKSUM
        └── BTCUSDT-1h-2026-08-16.parquet
```

## Schema

Every decimal column is written as a **fixed `decimal128(38, 8)`** — one schema
for every market and every symbol.

The measured worst case across BTC, PEPE, SHIB, BONK, FLOKI, DOGE and both
futures markets is 20 significant digits and 8 decimal places, so 38 digits of
precision leaves large headroom. Fixing the schema matters: an inferred
per-file precision lets two cached files disagree about the same column, and
every reader then has to reconcile them.

## Binding tier 2 to tier 1

The interesting part. A cached Parquet is only usable if it was built from the
archive still sitting next to it, *and* by the same conversion code. Both facts
are recorded in the Parquet footer at build time
(`parquet.KeyValueMetadata` on write, `(*parquet.File).Lookup` on read):

```
bmd.source.sha256  = <SHA-256 of the source ZIP, i.e. Binance's own checksum>
bmd.source.file    = BTCUSDT-1h-2024-01.zip
bmd.codec.version  = 1
bmd.rows           = 44640
```

The read path's validity rule:

```
parquet is usable  ⟺  footer["bmd.source.sha256"] == sha from the .CHECKSUM sidecar
                   ∧  footer["bmd.codec.version"] == current CodecVersion
```

**Cost of a cache hit:** read the ~90-byte `.CHECKSUM` file and the Parquet
footer (one seek to end of file), then the rows. On a hit **the ZIP is never
opened and never re-hashed** — re-hashing it on every read would defeat the
entire point. `TestCacheHitDoesNotReadTheArchive` proves it by replacing the
archive with garbage and requiring the read to succeed anyway.

Measured on an Apple M1 Pro, for one symbol-month of `1m` candles (44,640 rows):

| | |
| --- | --- |
| Decode the ZIP (Stage 3, the baseline this replaces) | ~62 ms |
| Read the Parquet | **6.1 ms**, 479 allocations |
| Build the Parquet, once per archive | 15 ms on top of the decode |

So a hit is about **ten times cheaper** than re-parsing, which is the margin
that makes a second copy on disk worth having.

### The rows are read a column at a time

That margin was not there in the first implementation, and the difference is
worth recording because it very nearly sank the design.

parquet-go's `GenericReader` reconstructs one Go struct per row through
reflection. Reading the same month that way took **26.5 ms and 99,573
allocations** — against 62 ms to parse the CSV. A cache twice as fast as the
work it replaces does not earn a second copy of the data, a second format, and
a dependency.

Reading each column as one contiguous run instead — `parquet.Int64Reader` for
the two timestamps and the trade count, `parquet.BE128Reader` for the eight
decimals, straight into the `Kline` fields — takes 6.1 ms and 479 allocations.
Same file, same library, 4.3× apart. The column layout is what makes it
possible: 44,640 open times sit together on disk, so a run of one type is
copied in bulk rather than assembled a field at a time.

Both typed readers are an optimisation parquet-go offers rather than a
guarantee it makes, so `readPage` falls back to the generic `ValueReader` for a
page encoded some other way. The fallback is slower and correct, which is the
right trade for a path whose purpose is that a library upgrade cannot turn into
a cache that refuses to read itself.

Reading positionally is only safe because the file's schema is checked against
the expected column names *and* their physical types before any value is read.
A column order that shifted by one would put the high price in the low price's
field, and every value would still parse — the quietest possible bug, and the
same class the twelve named CSV column constants in `codec.go` exist to prevent.

The type half of that check is not symmetry for its own sake. `readPage` picks
its fast path by asserting on the decoded page's type and falls back to a
generic reader when the assertion fails, so a column with the right name and the
wrong storage — an `INT64` where a `DECIMAL(38,8)` belongs — reaches the
fallback's decimal branch and asks an integer for its bytes. parquet-go answers
that with an `unsafe.Slice` over a nil pointer, which panics the goroutine
rather than returning an error the cache could rebuild from. Checking the type
keeps the whole class inside the error type.

## When a rebuild happens

**In the steady state, never.** Same ZIP, same library version, the Parquet is
reused forever. There are exactly three triggers:

| Trigger | How often |
| --- | --- |
| The Parquet does not exist yet | Once, the first time that month is touched |
| ZIP sha ≠ stamped sha | Effectively never — only if Binance republishes a corrected archive |
| `CodecVersion` ≠ stamped version | Once per library upgrade that changed the parser |

Only the third can fire while the cache is byte-identical, and `CodecVersion` is
a **compile-time constant** — it cannot differ between two runs of the same
binary.

### Why `CodecVersion` has to exist

"Same ZIP ⟹ same Parquet" only holds if the *conversion* is also unchanged.

Concretely: fix the millisecond/microsecond detection bug and every 2025-or-later
Parquet already in the cache is wrong, while its source ZIP is untouched. No
checksum comparison can detect that, because nothing about the source changed.
Without a codec version the corrected parser would simply never reach data that
was already cached.

The granularity is deliberately coarse — one global `CodecVersion`, not
per-concern tracking. A fix that only affects microsecond files still rebuilds
millisecond ones. That over-rebuilds on upgrade, roughly 40 s for 600 cached
files, offline. It is worth paying to fail safe.

## Three properties that follow

**Reproducible output.** With row-group size, compression and column order
pinned, and no wall-clock timestamp anywhere in the footer, the same
`(ZIP, CodecVersion)` pair yields a byte-identical Parquet. Golden-file tests
become trivial and caches can be diffed across machines. This is worth
deliberately *omitting* a `created_at` stamp to keep.

One knob had to be pinned that the design did not anticipate. parquet-go builds
its `created by` footer string from the module's own build information, so it
changes whenever the library is rebuilt from a different commit — two identical
caches would differ in their footers for a reason having nothing to do with
their contents. It is pinned to `CodecVersion` instead, so the string changes
exactly when the meaning of the rows does. `TestCacheRebuildIsReproducible`
builds the same archive through two independent caches and compares the bytes.

**Free corruption detection.** Parquet writes a CRC32 per data page, so a
damaged tier-2 file is caught on read and rebuilt from tier 1.

**Optional archive pruning.** Tier 1 is only needed to build or rebuild, so it
can be deleted to reclaim disk — at the cost of re-downloading if `CodecVersion`
ever bumps. A command, never the default. The read path has always supported it:
a hit needs the sidecar and the Parquet, never the archive, so a pruned cache
serves every read that does not need a rebuild.

This is built, as `Loader.PruneArchives` and `bmd prune`. Two things about it
were settled by measurement rather than by the obvious guess.

**It reclaims about 40%, not half and not most.** Measured on BTCUSDT `1m` for
2024-01: 2,169,570 bytes of archive against 3,226,820 of Parquet and an 88-byte
sidecar. Tier 2 is the *larger* tier — snappy against the zip's deflate, and
fixed-width `DECIMAL(38,8)` against text — which is the 2× disk cost this
document already quotes at the top, seen from the other side. An earlier draft of
this line said the archive was the bulk of an entry; it is not.

**What may be deleted is decided by the reader's own rule.** An archive goes only
when the Parquet beside it would be accepted by a read: same source hash, same
`CodecVersion`, same schema. That is not a similar rule, it is `checkParquet` —
the same two footer gates `cache.load` opens with — so the pruner and the reader
cannot drift into disagreeing about which files are usable. The invariant that
buys is worth stating plainly: **every read that succeeds before a prune succeeds
after it.**

One residual is accepted rather than closed. The gates read the footer, not the
pages, so bit rot inside a data page is caught by Parquet's per-page CRC on a
later read and not by the prune. That entry then needs a download instead of a
decode — the same cost class as a `CodecVersion` bump, and the alternative was
decoding every cached file to answer a question about disk space.

`bmd cache` reports what `bmd prune` would free, from the same predicate, so the
number in the report is the number the command delivers.

## Integrity

Tier 1 is verified against its `.CHECKSUM` **at download time**, and on demand
via `bmd verify`. It is not re-hashed on every read.

The download half is live as of Stage 4: `fetchArchive` fetches the sidecar
first, streams the archive through a `sha256.Hash` in the same pass that writes
it, and returns `ErrChecksum` on a mismatch — so unverified bytes never reach
the cache, and the hash the cache stamps into the Parquet footer is one that was
computed and checked rather than copied from the sidecar on faith.

A **rebuild** does not re-hash either. It reads the stored archive and stamps
the Parquet with the hash from the sidecar beside it, so bit rot in a stored
archive would be inherited by the file built from it rather than caught. That
is the same trade as the read path — verification happens where it is
affordable — and `bmd verify` in Stage 8 is what closes it.

The cache writes the sidecar itself, in the format Binance publishes: 64 hex
digits, two spaces, the archive's name, no trailing newline. Two things follow.
`sha256sum -c` verifies a cache directory with no tooling from this project, and
`TestCacheWritesThePublishedSidecar` can compare what the cache wrote against
the genuine published file byte for byte — which it does. The hash in it is one
this process computed over the bytes as they streamed past, not one copied from
a server on faith.

All cache writes go to a temporary file in the destination directory and are
then renamed into place. `rename(2)` within a filesystem is atomic, so a crash
mid-write leaves either the old file or the new one, never a truncated one.
Writing straight to the final path is the trap here: an interrupted run leaves
a torn file that looks valid to the next process to open it. The temporary file
has to be in the *destination directory* rather than the system temp directory,
because a rename across filesystems is a copy followed by a delete — which is
the non-atomic write this is avoiding.

Tier 1 is written archive first, sidecar second. That order gives the stronger
invariant: a sidecar implies the archive beside it, so a crash between the two
leaves an archive with no sidecar, which the next run treats as absent and
downloads again. The reverse would leave a sidecar vouching for a file that is
not there.

## Concurrency

Two overlapping requests — January–March and February–April for one symbol —
both want February. `singleflight` collapses them: the first caller to ask for
an archive does the work and the others wait on its result, keyed on the Parquet
path. Nothing is remembered once the call returns, which makes it a
deduplicator rather than a third tier.

The key is registered on the way in, before any I/O. That is correctness
requirement 5, and the bug it exists to avoid: the ported implementation
registered its deduplication entry *after* taking a concurrency permit, so a
saturated pool let several tasks past the check before any of them registered
and the same month downloaded several times over. Stage 7's bounded pool sits
outside this, never between the check and the work.

Waiters receive a copy of the candles rather than the slice itself. Stage 7
merges and trims ranges in place, and one caller's trim writing through into
another's range would be a silent, data-dependent bug.

`singleflight` has one sharp edge worth knowing about: the shared call runs
under the context of whichever caller arrived first, so if *that* caller cancels,
everyone waiting is handed a cancellation that has nothing to do with them. The
cache detects exactly that case — a context error on a call whose own context is
still alive — and asks again, at most twice.

## What was left out

**A third, in-memory tier.** The stage plan listed one; it was dropped
deliberately. A `Kline` is 312 bytes, so a month of `1m` candles is ~14 MB and a
month of `1s` candles is ~810 MB — a map of decoded ranges needs an eviction
policy that nothing here specifies, and the gain is small for the case that
matters: a backtest reads each candle once, in order, so the second read a
memory tier would accelerate often never happens. The Parquet read is 6 ms and
the operating system's page cache keeps the file warm regardless. Stage 7 can
revisit it with `Fetch` in hand and a benchmark to argue from.
