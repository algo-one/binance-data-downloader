# The `bmd` command-line tool

`bmd` is a thin shell over the library. Every flag maps onto a field of
`binancedata.Request` or an option passed to `binancedata.NewLoader`, and the
CLI holds no logic of its own beyond turning text into those values and candles
into a file. Anything you can do here you can do from Go code, and vice versa —
`bmd verify` is `Loader.VerifyCache`, `bmd list` is `Loader.Available`,
`bmd cache` is `Loader.CacheUsage`, `bmd prune` is `Loader.PruneArchives`,
`bmd evict` is `Loader.EvictCache`, and `--format parquet` is
`binancedata.WriteParquet`.

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
| `-symbol` | `BTC/USDT`, `BTC-USDT` or `BTCUSDT` — all normalised. Repeat it or comma-separate for several |
| `-interval` | `1s` … `1mo` (`1M` is accepted too); validated against what Binance publishes. Repeat it or comma-separate for several |
| `-start`, `-end` | `YYYY-MM-DD` or RFC 3339, UTC. **Both ends are included** |
| `-market` | `spot` (the only implemented value today) |
| `-out` | A file, a directory, or `-` for stdout. Default: a generated name here |
| `-format` | `csv`, `json` or `parquet` |
| `-cache-dir` | Where the two-tier cache lives. Rejected if given as an empty string |
| `-concurrency` | Parallel chunk fetches (default 8). Must be at least 1 |
| `-quiet` | Print nothing to stderr but errors — no progress, no summary |
| `-verbose` | Log what the pipeline is doing to stderr |

### Several symbols and intervals

```bash
bmd download -symbol BTC/USDT,ETH/USDT,SOL/USDT \
  -interval 1h -start 2024-01-01 -end 2024-03-31 -out ./data

bmd download -symbol BTC/USDT -symbol ETH/USDT -interval 1h -start 2024-01-01 -out ./data

bmd download -symbol BTC/USDT -interval 1m,1h,1d \
  -start 2024-01-01 -end 2024-03-31 -out ./data
```

Both flags take lists, both spellings work, and they mix freely. A list is what
you type by hand; repetition is what a shell loop building an argument slice
produces.

**Every pair is downloaded.** Two symbols at three intervals is six downloads,
in symbol order — a symbol's intervals stay together in the output directory.
There is no positional pairing of the two lists: that would require them to be
the same length, and would make `-symbol BTC/USDT,ETH/USDT -interval 1h` mean one
download rather than two.

**Do this rather than running one `bmd` per symbol or per interval, and the
reason is the rate limit.** Binance enforces `REQUEST_WEIGHT` per IP address —
6000 per minute, which is 100 per second — and the limiter that respects it is
built once per process and shared by everything in it. That is not an
optimisation; it is the requirement, because two limiters each honouring the
documented 40 weight per second permit 80 between them. Three `bmd download`
processes started together are at 120 against a ceiling of 100, and Binance
answers a client that keeps exceeding it with an HTTP 418: an IP ban of two
minutes to three days, which locks out everything else on the machine and no
retry undoes.

A shell loop over intervals breaks this in exactly the way a shell loop over
symbols does. The quota is counted per address, not per symbol, so nothing about
varying the interval instead makes it cheaper.

One process, one limiter, however many symbols and intervals.

The pairs are fetched one after another. Each one's chunks still go out in
parallel up to `-concurrency`, and the fetch pool is shared across the whole run,
so a range worth downloading keeps it busy on its own.

**Each download gets its own file**, so `-out` must name a directory or be left
off. `-out -` and `-out somefile.csv` are refused for more than one download —
neither has a reading, and both failure modes are silent: one file would
interleave headers into nonsense, and honouring one download would drop the rest
without a word. A directory that does not exist is refused rather than created.
This counts downloads and not symbols: one symbol at two intervals is two files
and needs a directory exactly as two symbols at one interval do.

Duplicates are dropped from both lists, after parsing rather than before, since
that is the only point at which they are visible. `-symbol BTC/USDT,BTCUSDT` is
one symbol, and `-interval 1mo,1M` is one interval — Binance spells the monthly
archive `1mo` and the monthly REST parameter `1M`, and both are accepted on
purpose. Without this, two downloads would write the same generated file name and
the second would silently replace the first.

**One failure does not abandon the rest.** A delisted pair among four good ones
is reported, counted, and the other three are still written:

```
BTCUSDT 1h: wrote 2184 candles to ./data/BTCUSDT-1h-2024-01-01_2024-03-31.csv
NOSUCHPAIR 1h: klines NOSUCHPAIR 1h: 400 Bad Request: Invalid symbol. (code -1121)
SOLUSDT 1h: wrote 2184 candles to ./data/SOLUSDT-1h-2024-01-01_2024-03-31.csv
2 of 3 symbols, 4368 candles in total
bmd: 1 of 3 symbols failed
```

2184 is what the range above actually holds: 2024-01-01 to 2024-03-31 inclusive
is 91 days, and 91 × 24 hourly candles is 2184 per symbol.

The summary counts in whatever varied — symbols, intervals, or "downloads" when
both lists are plural and neither noun fits — and the progress display labels its
lines the same way. Two symbols at one interval and one symbol at two intervals
are both two downloads, so a count alone could not tell you which run you were
looking at.

The exit status is 1. A failed download leaves no file behind, because output is
written through a temporary file and renamed only once the encoder finishes.
Ctrl-C is different from a failure: it stops the run rather than moving on to the
next download.

With one symbol and one interval nothing above changes. There is no run summary,
no label on the progress lines, the error is reported as it always was, and
`-out -` works.

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

### Pruning retires `bmd verify`

Worth knowing before you put both in a cron job. `bmd verify` walks `.zip` files
and nothing else, so on a fully pruned cache it finds nothing to hash. It says
so rather than printing a clean-looking summary —

```
no cached archives to verify (a pruned cache keeps only sidecars and parquet)
```

— and it still exits 0, because nothing failed. What changes is what a 0 means:
before a prune it says every archive still hashes to what Binance published;
after one it says only that there were no archives to ask about.

Tier 2 is not left unchecked, but it is checked by a different mechanism and on
a different schedule. Every read compares the Parquet footer against the
sidecar's hash and the codec version, and Parquet checksums each data page as it
is decoded — so damage surfaces as a read error on the range that touches it,
continuously, rather than on demand across the whole cache. There is no command
that sweeps tier 2 the way `bmd verify` sweeps tier 1.

## `bmd evict`

```bash
bmd evict -before 2023-01-01                    # drop everything ending before 2023
bmd evict -symbol BTC/USDT,ETH/USDT             # drop two pairs entirely
bmd evict -symbol BTC/USDT -interval 1s         # one pair at one interval
bmd evict -all -n                               # what removing everything would free
```

| Flag | Meaning |
| --- | --- |
| `-symbol` | Limit to these pairs. Repeat it or comma-separate for several |
| `-interval` | Limit to these intervals. Repeat it or comma-separate for several |
| `-before` | Limit to entries ending at or before this instant. `YYYY-MM-DD` or RFC 3339, UTC |
| `-all` | Evict the whole cache. Cannot be combined with the three above |
| `-n` | Say what would be evicted and delete nothing |
| `-cache-dir` | Where the cache lives |
| `-quiet` | Suppress the summary on stderr; the receipt on stdout is unaffected |

**This deletes data, and `bmd prune` does not.** That is the whole reason they
are two commands. A prune removes archives whose parquet already answers reads,
so everything that worked before it works after it. An eviction removes the
entry — archive, sidecar and parquet together — and every read of it goes back
to Binance.

**At least one selector is required.** A bare `bmd evict` is a usage error
rather than "evict everything", because the cost of reading it the other way is
a cache somebody did not mean to delete. `-all` is the spelling for everything,
and it cannot be combined with a filter: a command that says both "everything"
and "only these" has two readings and neither is safe to guess.

**`-before` bounds the data, not the files.** An entry goes only if it *ends* at
or before the instant given:

```
-before 2024-01-15   keeps BTCUSDT-1h-2024-01.zip   (January is not over yet)
                     removes BTCUSDT-1h-2023-12.zip
                     removes BTCUSDT-1h-2024-01-14.zip

-before 2024-02-01   removes all of January, monthly and daily
```

A bare date means midnight UTC, which is the opposite treatment from
`bmd download -end`, and deliberately. `-end` names the last instant you want,
so a bare date there covers that whole day. `-before` names the first instant
you do *not* want, so `-before 2024-01-01` keeps every candle of 2024.

The output is a receipt: one line per entry on stdout, and the summary on
stderr.

```
BTCUSDT 1h BTCUSDT-1h-2023-11
BTCUSDT 1h BTCUSDT-1h-2023-12
ETHUSDT 1d ETHUSDT-1d-2023-06
evicted 3 entries, freed 48.0 MB
```

The receipt is on stdout rather than stderr because it is the part worth
keeping, and `-quiet` leaves it there — it takes away the summary, not the
record of what was deleted. This is where `bmd evict` differs from `bmd prune`,
which prints only its exceptions: a prune's removals cost nothing, and each line
here is data that is gone.

**Nothing is evicted automatically.** There is no size cap and no expiry, and
that is a conclusion rather than a missing feature. Expiring by file age would
key on when an entry was downloaded rather than when it was used, so the
symbol-month your backtest reads every day would expire on schedule. A
least-recently-used cap needs a recency signal the filesystem does not reliably
give — access times are off or coarse on most Linux setups — so `bmd` would have
to write to disk on every cache hit to record one. What you have that the tool
does not is knowledge of your own window, which is what the flags above take.

Entries this library did not write are left alone, at every level: a stray file
in a data directory, a directory that is not part of the cache layout. A
directory emptied by an eviction is removed, and so are the parents that leaves
empty, up to but not including the cache root.

Run `bmd cache` first to see what is there, or `-n` to see what would go.

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
one-line summary on stderr. A cache with no archives at all — a fresh one, or a
pruned one — says so in its own words instead; see
[Pruning retires `bmd verify`](#pruning-retires-bmd-verify) for why that is not
the same sentence.

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

**Progress** for `bmd download` is a bar on a terminal and one line per chunk
anywhere else, so a redirected stderr stays readable. It fills by chunks of work,
not bytes, because that is what the library reports — see `Progress` in
[architecture.md](architecture.md) for why there is no byte counter.

**A spinner** covers the slow commands that produce nothing until they finish:
`bmd list` (up to seven bucket listings), `bmd verify` (re-hashes every archive),
`bmd cache`, `bmd prune` and `bmd evict` (each walks the whole cache), and the
planning phase of a download. It is a spinner rather than a bar because none of
those reports a total in advance; `verify` and `prune` show a running count
beside it. It draws only on a terminal and erases itself, so a piped or
redirected run is exactly what it was without it. `-quiet` removes it along with
the summary on the commands that have that flag, and `-verbose` removes it on
every command, because that flag routes the pipeline's log to stderr and the
spinner must not draw over it. See "Terminal feedback" in
[architecture.md](architecture.md).

**Exit status**

| Code | Meaning |
| --- | --- |
| 0 | Success, including `-h` |
| 1 | The work was attempted and failed |
| 2 | The command line could not be acted on |
| 130 | Interrupted (128 + `SIGINT`) |
