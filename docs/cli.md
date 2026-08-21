# The `bmd` command-line tool

`bmd` is a thin shell over the library. Every flag maps onto a field of
`binancedata.Request` or an option passed to `binancedata.NewLoader`, and the
CLI holds no logic of its own beyond turning text into those values and candles
into a file. Anything you can do here you can do from Go code, and vice versa —
`bmd verify` is `Loader.VerifyCache`, `bmd list` is `Loader.Available`,
`bmd cache` is `Loader.CacheUsage`, `bmd prune` is `Loader.PruneArchives`, and
`--format parquet` is `binancedata.WriteParquet`.

## Build

```bash
mise run build      # writes ./bin/bmd
```

mise puts `./bin` on your PATH inside this project, so `bmd` works directly
afterwards.

Or install the released binary:

```bash
go install github.com/algo-one/binance-data-downloader/cmd/bmd@latest
```

## `bmd download`

```bash
bmd download \
  -symbol BTC/USDT -interval 1h \
  -start 2024-01-01 -end 2024-03-31 \
  -market spot \
  -out ./data -format csv \
  -cache-dir ~/.cache/bmd \
  -concurrency 8
```

| Flag | Meaning |
| --- | --- |
| `-symbol` | `BTC/USDT`, `BTC-USDT` or `BTCUSDT` — all normalised |
| `-interval` | `1s` … `1mo` (`1M` is accepted too); validated against what Binance publishes |
| `-start`, `-end` | `YYYY-MM-DD` or RFC 3339, UTC. **Both ends are included** |
| `-market` | `spot` (the only implemented value today) |
| `-out` | A file, a directory, or `-` for stdout. Default: a generated name here |
| `-format` | `csv`, `json` or `parquet` |
| `-cache-dir` | Where the two-tier cache lives. Rejected if given as an empty string |
| `-concurrency` | Parallel chunk fetches (default 8). Must be at least 1 |
| `-quiet` | Print nothing to stderr but errors — no progress, no summary |
| `-verbose` | Log what the pipeline is doing to stderr |

### Both ends are included

`-end 2024-03-31` covers **all** of the 31st. A bare date is expanded to that
day's last instant, so it means the same thing at every interval — 24 candles of
the 31st at `1h`, one at `1d`.

This matters more than it looks, and it is why `Request.End` in the library is
inclusive too. `End` is inclusive of an *instant*: a candle is returned when its
open time is at or before it. Passing a bare `2024-03-31` straight through would
return the single candle that opened at midnight, because that is the last open
time at or before midnight — twenty-three candles short, with nothing to show
for it.

Writing `-end` as a full timestamp opts out of the expansion, since somebody who
wrote the time has said which instant they mean:

```bash
bmd download -symbol BTC/USDT -interval 1h \
  -start 2024-01-15T06:00:00Z -end 2024-01-15T18:00:00Z
```

### Where the file goes

Four spellings, in the order they are checked:

| `-out` | Result |
| --- | --- |
| `-` | Standard output |
| *(not given)* | `BTCUSDT-1h-2024-01-01_2024-03-31.csv` in the current directory |
| an existing directory | that generated name, inside it |
| anything else | exactly that file |

Files are written through a temporary file in the same directory and renamed
into place once the encoder finishes. An interrupted download therefore leaves
nothing behind rather than a CSV that is silently short — or a parquet file with
no footer, which is not a parquet file at all. Standard output gets no such
protection, because there is nothing to rename.

A second run with the same flags replaces the file. That is deliberate: running
a download twice is the documented way to check the cache, and it must produce
byte-identical output rather than an error.

### Output formats

All three carry the same eleven columns, in the same order, under the same
names — `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`,
`quote_volume`, `taker_buy_base_volume`, `taker_buy_quote_volume`, `trades`.

**csv** has a header row. Times are RFC 3339 in UTC and keep their fractional
seconds; prices and volumes are written exactly, never through a `float64`.

**json** is one array of objects, streamed, so `bmd download … -out - | jq` works
on a range too large to hold in memory. Every price and volume is a JSON
*string*: a quote volume can reach twenty significant digits, and a bare JSON
number that wide loses its tail in any consumer that parses into a `float64`,
JavaScript included.

**parquet** is the same schema the cache's second tier uses — `DECIMAL(38,8)`
values and `TIMESTAMP(MICROS)` times — so DuckDB, Polars and pandas read it
directly. It carries no source-archive stamp, because an export can come from
many archives and the REST API at once and there is no single provenance to
claim. Writing it to a terminal is refused; redirect it or use `-out`.

Note that the values are numerically exact but not textually identical to
Binance's own CSV: `42380.00000000` is written as `42380`. The number is the
same one, with the trailing zeros the decimal type does not carry.

## `bmd list`

```bash
bmd list -symbol BTC/USDT -interval 1mo
```

```
BTCUSDT 1mo spot

monthly archives  105  2017-08-01 .. 2026-06-01  2 missing
archives through       2026-07-01
```

Reports what Binance actually publishes, by asking the S3 bucket. Nothing is
downloaded.

The `missing` count is the point. Binance's archives have holes — as of
2026-08-21, BTCUSDT's monthly `1mo` archives are missing **2024-03 and
2026-03** while their neighbours exist — and no date arithmetic predicts that. A
summary reporting only the first and last date would describe that range as
complete. `-archives` prints every period with the holes marked in place:

```bash
bmd list -symbol BTC/USDT -interval 1mo -archives
```

`archives through` is the frontier: everything at or after it has to come from
the REST API, because no archive covers it yet.

`bmd list` takes no `-cache-dir` or `-concurrency`, because it opens no cache
and makes no parallel fetches. A flag a command advertises and then ignores
costs a debugging session to discover, so each command registers only what it
honours.

The same rule applies to a flag's *value*. `-concurrency -4` and `-concurrency 0`
are usage errors rather than silent falls back to the default, and so is
`-cache-dir ""` — which is what `-cache-dir "$CACHE_DIR"` becomes when the
variable is unset. Defaulting quietly there is the dangerous reading: the caller
believes they named a directory, and on `bmd verify -rm` the default is the
user's real cache.

`-since 2024-01-01` bounds the answer *and* its cost. The bucket listing is
seeked to it, so asking about one year is a single round trip where asking about
a pair's whole history can be seven.

## `bmd cache`

```bash
bmd cache
bmd cache -cache-dir ~/.cache/bmd
```

```
/Users/ivan/Library/Caches/bmd

  archives   412  852.5 MB
  sidecars   412   35.4 KB
   parquet   412    1.2 GB
     total  1236    2.1 GB

846.2 MB in 409 archives can be freed with 'bmd prune'
```

Reports what the cache holds, by tier. Nothing is downloaded, written or
deleted.

The last line is why the command exists, and why it costs slightly more than a
size walk: deciding what is reclaimable means opening the Parquet beside each
archive and reading its footer. That is one open and one seek per archive —
against the full re-hash `bmd verify` pays — and it is what makes the report
answer "is pruning worth it?" rather than merely reciting sizes.

**Tier 2 is the bigger tier.** That surprises most people, including an earlier
draft of this documentation. A Parquet file is larger than the archive it was
built from — snappy where the zip uses deflate, fixed-width `DECIMAL(38,8)`
where the CSV uses text — because it is the tier that gets read. Measured on
BTCUSDT `1m` for 2024-01: 2.2 MB of archive, 3.2 MB of Parquet. So pruning
reclaims about 40% of a cache, not most of it.

An `other` row appears only when there is something in it. It counts files that
are none of the three, which in a healthy cache is nothing at all — the one
ordinary cause is a process killed mid-write, since every cache write goes to a
temporary file in its destination directory and nothing ever collects the
leftovers.

## `bmd prune`

```bash
bmd prune                         # delete archives the cache no longer reads
bmd prune -n                      # say what would go, delete nothing
bmd prune -cache-dir ~/.cache/bmd
bmd prune -quiet                  # exceptions only, no summary
```

Deletes cached archives that the Parquet tier no longer needs, and **only the
archive** — the `.CHECKSUM` sidecar and the Parquet both stay. That is not
tidiness: the Parquet is what reads are served from, and the hash in the sidecar
is what the Parquet is validated against, so deleting either would strand the
entry as surely as deleting both.

The guarantee is worth stating plainly: **every read that succeeds before a
prune succeeds after it.** A cache hit reads the sidecar and the Parquet footer
and opens the archive neither to decode nor to re-hash it, and an archive is
deleted only when that read would succeed — checked with the same code the read
path uses, so the two cannot drift apart.

`-n` reaches every verdict and deletes nothing. Run it first, or run `bmd cache`,
which reports the same number from the same rule.

### What it costs

A download later, in the one case tier 1 is still needed: rebuilding. That
happens when `CodecVersion` changes — the parser moved, so every cached Parquet
has to be built again — or when a Parquet file fails one of its per-page
checksums. Either would have been a local decode with the archive on disk and
becomes a fetch without it.

This is why pruning is a command and never something the cache does on its own.
Spending somebody's bandwidth is not a decision a cache should take by itself.

### What it prints

The removals are the expected outcome and there can be thousands of them, so
they are counted in the summary rather than listed. stdout carries the
exceptions, one per line, the same split `bmd verify` uses:

| Line on stdout | Meaning |
| --- | --- |
| `kept NAME: reason` | The Parquet cannot serve this range on its own, so the archive is the only copy there is. Usually a Parquet that was never built, or one built by an older `CodecVersion` |
| `PATH: error` | The archive was prunable and deleting it failed |

A kept archive is a verdict and exits 0. A failed delete is a failure and exits
1, because a script that prunes to make room needs to know the room is not
there.

## `bmd verify`

```bash
bmd verify                        # the default cache directory
bmd verify -cache-dir ~/.cache/bmd
bmd verify -rm                    # delete what fails
bmd verify -quiet                 # failures only, no summary
```

Re-hashes every cached archive against the `.CHECKSUM` sidecar Binance published
with it. The normal read path deliberately does not do this — see
[caching.md](caching.md) — because re-hashing a 93 MB archive on every read
would cost more than the parse the second tier exists to avoid. That leaves one
gap, a file that was correct when written and rotted afterwards, and this is how
it is closed.

Failures are printed to stdout, one per line, and the exit status is 1 if there
were any — so it works in a cron job. A clean cache prints nothing but the
one-line summary on stderr.

`-rm` deletes each failed archive **and its sidecar**, so the next download
replaces them. The derived parquet is left alone: it carries the archive's
published hash in its footer, so it is still valid data, and the cache is
documented to keep serving from tier 2 when tier 1 has been pruned.

It does not delete everything it reports, and the distinction matters more than
it looks. A failure is only a reason to throw away up to 93 MB when it is a fact
about the *data*:

| What went wrong | `-rm` |
| --- | --- |
| The hash does not match the sidecar | **removes** — the bytes are not what Binance published |
| The sidecar is missing | **removes** — the cache writes the archive first, so this is a crash between the two writes; what is left can never be verified and the read path already ignores it |
| The file could not be read — `EIO`, `EACCES` | **keeps**, and says `not removed` |
| The sidecar will not parse | **keeps**, and says `not removed` |

The last two are facts about the disk, not about the archive, and the archive is
very probably intact. Deleting on a transient read error turns one bad moment on
a flaky volume into a re-download — and if the volume is what is having the bad
moment, into a re-download of the whole cache.

Tier 2 is not walked, because it does not need to be. Every read checks the
parquet footer against the archive's hash and the codec version and rebuilds
when either fails, so tier 2 is verified continuously by the code that uses it.
Tier 1 is the only tier nothing re-reads.

## Conventions

**Ctrl-C stops promptly.** `SIGINT` and `SIGTERM` cancel the context that every
download and every worker goroutine runs under, so an interrupted run stops
immediately rather than finishing the current file — and, because output is
written through a temporary file, leaves no partial file behind.

**stdout carries data, stderr carries everything else.** Usage text, progress,
the summary line and errors all go to stderr, so `bmd download … -out - -format
csv > out.csv` produces a clean file. There is a test asserting it.

The one exception is `bmd verify`, whose failures *are* its output and go to
stdout, so piping them into a file works.

**Progress** is one redrawn line on a terminal and one line per chunk anywhere
else, so a redirected stderr stays readable. It counts chunks rather than bytes,
because that is what the library reports — see `Progress` in
[architecture.md](architecture.md) for why there is no byte counter.

**Exit status**

| Code | Meaning |
| --- | --- |
| 0 | Success, including `-h` |
| 1 | The work was attempted and failed |
| 2 | The command line could not be acted on |
| 130 | Interrupted (128 + `SIGINT`) |
