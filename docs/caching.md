# Caching

> **Status:** design document. The cache lands in Stage 5. `CodecVersion`, the
> constant this design leans on, exists as of Stage 3 and lives in `codec.go`.

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
footer (one seek to end of file). Sub-millisecond, against ~60–70 ms to
re-parse. On a hit **the ZIP is never opened and never re-hashed** — re-hashing
it on every read would defeat the entire point.

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

**Free corruption detection.** Parquet writes a CRC32 per data page, so a
damaged tier-2 file is caught on read and rebuilt from tier 1.

**Optional archive pruning.** Tier 1 is only needed to build or rebuild, so a
`--prune-archives` flag could reclaim half the disk — at the cost of
re-downloading if `CodecVersion` ever bumps. A flag, never the default.

## Integrity

Tier 1 is verified against its `.CHECKSUM` **at download time**, and on demand
via `bmd verify`. It is not re-hashed on every read.

The download half is live as of Stage 4: `fetchArchive` fetches the sidecar
first, streams the archive through a `sha256.Hash` in the same pass that writes
it, and returns `ErrChecksum` on a mismatch — so unverified bytes never reach
the cache, and the hash the cache stamps into the Parquet footer is one that was
computed and checked rather than copied from the sidecar on faith.

All cache writes go to a temporary file in the destination directory and are
then renamed into place. `rename(2)` within a filesystem is atomic, so a crash
mid-write leaves either the old file or the new one, never a truncated one.
Writing straight to the final path is the trap here: an interrupted run leaves
a torn file that looks valid to the next process to open it.
