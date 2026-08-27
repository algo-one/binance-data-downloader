# binance-data-downloader

[![Go Reference](https://pkg.go.dev/badge/github.com/algo-one/binance-data-downloader.svg)](https://pkg.go.dev/github.com/algo-one/binance-data-downloader)
[![CI](https://github.com/algo-one/binance-data-downloader/actions/workflows/ci.yml/badge.svg)](https://github.com/algo-one/binance-data-downloader/actions/workflows/ci.yml)
[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

**Historical Binance candles, downloaded once and cached properly.** A Go
library, plus a `bmd` command-line tool built on it.

Ask for a date range. You get back every candle in it, verified against
Binance's published checksums. Ask again tomorrow and it comes from disk — no
network, no re-parsing. A backtest can re-read five years of one-minute data on
every run without paying for it twice.

Binance splits its history across two sources: bulk ZIP archives that lag about
a day behind, and a REST endpoint for everything newer. This library stitches
them together so you never have to think about the seam.

**Scope:** spot klines only. Not futures, not order books, not trades.

## Install

Requires Go 1.25 or newer.

```bash
# As a library
go get github.com/algo-one/binance-data-downloader

# As a CLI
go install github.com/algo-one/binance-data-downloader/cmd/bmd@latest
```

## Try it

```bash
bmd download -symbol BTC/USDT -interval 1h -start 2024-01-01 -end 2024-03-31
```

That writes a CSV to the current directory. Run it a second time and it
finishes almost instantly — everything is already cached.

The other commands:

```bash
bmd list   -symbol BTC/USDT -interval 1mo      # what Binance actually publishes
bmd cache                                      # what your cache holds
bmd prune                                      # reclaim disk; -n to preview
bmd evict  -symbol BTC/USDT -before 2023-01-01 # delete data you are done with
bmd verify                                     # re-hash the cache
```

Output is `csv`, `json` or `parquet`. Full flag reference in
[docs/cli.md](docs/cli.md), or run `bmd help`.

## Use it from Go

```go
// No options needed — the cache lands in your OS cache directory.
loader, err := binancedata.NewLoader()
if err != nil {
    return err
}

klines, err := loader.Fetch(ctx, binancedata.Request{
    Symbol:   "BTC/USDT",
    Interval: binancedata.Interval1h,
    Market:   binancedata.MarketSpot,
    Start:    start,
    End:      end, // leave zero for "now, at call time"
})
```

Each `Kline` carries open/high/low/close, both volumes, the taker-buy split and
the trade count.

Two variants for bigger jobs:

- `loader.Stream(ctx, req)` yields candles one at a time, for ranges too large
  to hold in memory.
- `loader.FetchAll(ctx, reqs)` runs several requests under one concurrency
  budget and downloads shared archives only once.

## Things worth knowing

**Ranges are inclusive at both ends.** A candle comes back when
`Start <= OpenTime <= End`. So a full year of 2024 is `Start` 2024-01-01 and
`End` 2024-12-31T23:59:59.999999999Z. Read `End` as *the open time of the last
candle you want* — writing `End` 2025-01-01 is legal, but it asks for the candle
opening on New Year's Day and costs you January's archive to fetch.

**Batch your symbols, don't batch your processes.** Pass lists —
`-symbol BTC/USDT,ETH/USDT -interval 1m,1h` — rather than running one `bmd` per
pair. Binance rate-limits per IP address, and the limiter enforcing it lives in
one process, so parallel processes will blow through the limit between them.
Every pair gets its own output file.

**Moving the cache.** `export BMD_CACHE_DIR=/mnt/big-disk/bmd` redirects every
command that takes `-cache-dir`; the flag wins if both are set. The library
reads no environment variables at all — Go callers pass
`binancedata.WithCacheDir` instead, so an unrelated env var can never redirect
where your program writes.

## How it works

**Numbers are exact.** Prices and volumes are
[`udecimal.Decimal`](https://github.com/quagmt/udecimal), never `float64`.
Binance quote volumes reach 20 significant digits; `float64` holds 15.95. No
`int64` fixed-point scale saves you either — real PEPE daily volume overflows
`int64` at 1e8 scaling, silently and negatively. The archives are text, the
numbers in them are exact, and they stay that way. Details in
[docs/numbers.md](docs/numbers.md).

**The cache has two tiers.** Tier 1 is the raw ZIP exactly as Binance served it,
next to its `.CHECKSUM`. Tier 2 is a Parquet file derived from it — that's what
reads actually hit. Each Parquet stores the SHA-256 of the archive it came from
in its own footer, so a cached file is trusted without re-hashing anything. In
the steady state nothing is ever rebuilt. Details in
[docs/caching.md](docs/caching.md).

**Nothing is evicted automatically.** File timestamps record when data was
*downloaded*, not when it was *used*, so there's no honest expiry rule to apply.
You decide: `bmd prune` drops archives that reads no longer need (~40% of the
cache), `bmd evict` removes entries you name.

**Library first.** This is built to sit inside a backtesting framework. The CLI
is a thin shell over the library, not the other way round — anything `bmd` can
do, your Go code can do.

## Documentation

Start with the [API reference on
pkg.go.dev](https://pkg.go.dev/github.com/algo-one/binance-data-downloader), or
read the same thing offline with `go doc github.com/algo-one/binance-data-downloader`.

[`example_test.go`](example_test.go) holds sixteen worked examples and is
probably the fastest way in. Seven of them run on every test run with their
output checked, so they can't quietly go stale.

Longer form:

| Document | Contents |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | How the pieces fit together, and the staged build plan |
| [docs/caching.md](docs/caching.md) | The two-tier cache, its invariants, and why it exists |
| [docs/cli.md](docs/cli.md) | The `bmd` command-line tool |
| [docs/numbers.md](docs/numbers.md) | Why prices are decimals, and what the alternatives measured |
| [docs/go-notes.md](docs/go-notes.md) | The Go idioms this codebase leans on, in one place |

The code is commented far more heavily than typical Go. That's deliberate — this
repository doubles as a way to learn the language, so comments explain *why* a
construct is used, not just what it does.

## Development

Tooling is managed with [mise](https://mise.jdx.dev/), which pins the Go
version, the linter and the test runner so every machine and CI runner use
identical bits.

```bash
mise trust      # once, to allow this repo's mise.toml
mise install    # fetch the pinned toolchain
mise tasks      # list everything below
```

| Task | What it does |
| --- | --- |
| `mise run build` | Compile the `bmd` CLI into `./bin` |
| `mise run test` | Run all tests with the race detector |
| `mise run lint` | Run golangci-lint |
| `mise run fmt` | Format all Go code in place |
| `mise run fmt:check` | Fail if anything is unformatted (CI uses this) |
| `mise run cover` | Test with coverage and open the HTML report |
| `mise run tidy` | Sync `go.mod` with the imports in the code |
| `mise run audit` | Check dependencies against the Go vulnerability database |
| `mise run release:snapshot` | Build the release artefacts locally, publishing nothing |
| `mise run ci` | Everything CI runs, in order |

No test in this repository touches Binance. Network paths run against
`httptest` servers with committed fixtures.

## Versioning

`v0.x`. Everything documented here works and is covered by tests, but the API
can still change between minor versions — `v0.2.0` may rename something
`v0.1.0` used. Pin an exact version if that matters to you.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
