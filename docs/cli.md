# The `bmd` command-line tool

> **Status:** the scaffold exists and the commands are stubs. They are
> implemented in Stage 8. Run `bmd -h` to see the current surface.

`bmd` is a thin shell over the library. Every flag maps onto a field of
`binancedata.Request` or an option passed to `binancedata.NewLoader`; the CLI
holds no logic of its own. Anything you can do here you can do from Go code, and
vice versa.

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

## Available now

```bash
bmd -version    # print the module version
bmd -h          # usage
bmd help        # same, on stdout so it can be piped
```

## Planned commands (Stage 8)

### `bmd download`

```bash
bmd download \
  --symbol BTC/USDT --interval 1h \
  --start 2024-01-01 --end 2024-03-31 \
  --market spot \
  --out ./data --format csv \
  --cache-dir ~/.cache/bmd \
  --concurrency 8
```

| Flag | Meaning |
| --- | --- |
| `--symbol` | `BTC/USDT`, `BTC-USDT` or `BTCUSDT` — all normalised |
| `--interval` | `1s` … `1mo`; validated against what Binance publishes |
| `--start`, `--end` | `YYYY-MM-DD` or RFC 3339; interpreted as UTC |
| `--market` | `spot` (the only implemented value today) |
| `--out` | Output directory |
| `--format` | `csv`, `json` or `parquet` |
| `--cache-dir` | Where the two-tier cache lives |
| `--concurrency` | Parallel downloads |

### `bmd verify`

```bash
bmd verify --cache-dir ~/.cache/bmd
```

Re-hashes every cached tier-1 archive against its `.CHECKSUM` sidecar. The
normal read path deliberately does not do this — see
[caching.md](caching.md) — so this is how you check the cache on demand.

### `bmd list`

```bash
bmd list --symbol BTC/USDT --interval 1h
```

Reports what Binance actually publishes for a symbol and interval, by querying
the public S3 bucket listing. Useful for answering "how far back does this
symbol go?" without downloading anything.

## Conventions

**Ctrl-C stops promptly.** `SIGINT` and `SIGTERM` cancel the context that every
download and every worker goroutine is running under, so an interrupted run
stops immediately rather than finishing the current file.

**stdout carries data, stderr carries everything else.** Usage text, progress
and errors all go to stderr, so `bmd download ... --format csv > out.csv`
produces a clean file. There is a test asserting this.

**Exit status** is 0 on success (including `-h`), 1 on any failure.
